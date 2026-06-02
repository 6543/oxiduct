//! TCP proxy: accept loop + per-connection relay with layered liveness.
//!
//! Liveness layers per connection (both inbound and outbound sockets):
//!   L1 – SO_KEEPALIVE + TCP_KEEPIDLE / TCP_KEEPINTVL / TCP_KEEPCNT   (socket_opts)
//!   L2 – TCP_USER_TIMEOUT                                              (socket_opts, Linux)
//!   L3 – Application idle timeout  (no bytes either direction → kill)
//!   L4 – Half-close grace          (one side EOF → deadline on other)

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::clock::now_ms;
use crate::config::ProxyConfig;
use crate::limits::{ConnLimits, Reject};
use crate::metrics::Metrics;
use crate::socket_opts;

/// Relay buffer size, per direction.
const BUF_SIZE: usize = 16 * 1024;
/// How often the watchdog re-checks the idle / half-close deadlines.
const WATCHDOG_TICK: Duration = Duration::from_secs(5);

/// Monotonically increasing connection ID for log correlation.
static CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Identifies a relay direction (for completion signalling + logs).
#[derive(Clone, Copy)]
enum Dir {
    /// client → upstream
    Up,
    /// upstream → client
    Down,
}

impl Dir {
    fn label(self) -> &'static str {
        match self {
            Dir::Up => "up",
            Dir::Down => "down",
        }
    }
}

/// How a direction's copy loop ended.
#[derive(Clone, Copy, PartialEq, Eq)]
enum End {
    /// Clean EOF (read returned 0).
    Eof,
    /// Read or write error — typically a reset connection.
    Err,
}

// ── Accept loop ──────────────────────────────────────────────────────────────

pub async fn run(
    cfg: Arc<ProxyConfig>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<()> {
    let listener = TcpListener::bind(&cfg.listen)
        .await
        .with_context(|| format!("bind {}", cfg.listen))?;

    info!(proxy = %cfg.name, "TCP listening");
    serve(listener, cfg, metrics, shutdown).await
}

/// Run the accept loop on a pre-bound listener.
///
/// Public so integration tests can bind to port 0, observe the assigned port,
/// then start the proxy on that listener.
pub async fn serve(
    listener: TcpListener,
    cfg: Arc<ProxyConfig>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<()> {
    let limits = ConnLimits::new(cfg.max_connections, cfg.max_per_ip);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                info!(proxy = %cfg.name, "TCP listener shutting down");
                break;
            }
            result = listener.accept() => match result {
                Ok((stream, peer)) => {
                    let guard = match limits.try_acquire(peer.ip()) {
                        Ok(g) => g,
                        Err(Reject::Total) => {
                            error!(
                                proxy = %cfg.name,
                                src_ip = %peer.ip(),
                                limit = limits.max_total,
                                "TCP connection rejected: total limit reached"
                            );
                            metrics.connections_rejected
                                .with_label_values(&[&cfg.name, "total"]).inc();
                            drop(stream);
                            continue;
                        }
                        Err(Reject::PerIp) => {
                            error!(
                                proxy = %cfg.name,
                                src_ip = %peer.ip(),
                                limit = limits.max_per_ip,
                                "TCP connection rejected: per-IP limit reached"
                            );
                            metrics.connections_rejected
                                .with_label_values(&[&cfg.name, "per_ip"]).inc();
                            drop(stream);
                            continue;
                        }
                    };

                    metrics.connections_total
                        .with_label_values(&[&cfg.name, "tcp"]).inc();

                    let id = CONN_ID.fetch_add(1, Ordering::Relaxed);
                    let cfg = cfg.clone();
                    let metrics = metrics.clone();
                    let token = shutdown.child_token();
                    tokio::spawn(async move {
                        let _guard = guard; // released on task end
                        handle(id, stream, peer, cfg, metrics, token).await
                    });
                }
                Err(e) => warn!(proxy = %cfg.name, "accept error: {e}"), // non-fatal
            }
        }
    }
    Ok(())
}

// ── Per-connection handler ───────────────────────────────────────────────────

async fn handle(
    id: u64,
    inbound: TcpStream,
    peer: SocketAddr,
    cfg: Arc<ProxyConfig>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) {
    socket_opts::apply_tcp(&inbound, &cfg);

    let outbound = match tokio::time::timeout(
        Duration::from_secs(cfg.connect_timeout_secs),
        TcpStream::connect(&cfg.target),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            warn!(proxy = %cfg.name, id, %peer, target = %cfg.target, "connect failed: {e}");
            metrics
                .connect_failures
                .with_label_values(&[&cfg.name])
                .inc();
            return;
        }
        Err(_) => {
            warn!(proxy = %cfg.name, id, %peer, target = %cfg.target, timeout = cfg.connect_timeout_secs, "connect timed out");
            metrics
                .connect_timeouts
                .with_label_values(&[&cfg.name])
                .inc();
            return;
        }
    };

    socket_opts::apply_tcp(&outbound, &cfg);

    info!(proxy = %cfg.name, id, %peer, target = %cfg.target, "connected");

    // Active gauge: up for the lifetime of the relay, down on any exit path.
    let active = metrics.active.with_label_values(&[&cfg.name]);
    active.inc();
    relay(id, peer, inbound, outbound, &cfg, &metrics, shutdown).await;
    active.dec();
}

// ── Bidirectional relay ──────────────────────────────────────────────────────

async fn relay(
    id: u64,
    peer: SocketAddr,
    inbound: TcpStream,
    outbound: TcpStream,
    cfg: &ProxyConfig,
    metrics: &Metrics,
    shutdown: CancellationToken,
) {
    let (ir, iw) = inbound.into_split();
    let (or_, ow) = outbound.into_split();

    // Shared idle clock (bumped on every byte) + a token that stops both
    // directions. Each direction reports completion once over `done_tx`.
    let last_activity = Arc::new(AtomicU64::new(now_ms()));
    let cancel = CancellationToken::new();
    let (done_tx, done_rx) = mpsc::channel::<(Dir, End)>(2);

    let bytes_up_ctr = metrics.bytes_total.with_label_values(&[&cfg.name, "up"]);
    let bytes_down_ctr = metrics.bytes_total.with_label_values(&[&cfg.name, "down"]);

    let h_up = tokio::spawn(copy_dir(
        id,
        Dir::Up,
        ir,
        ow,
        last_activity.clone(),
        cancel.clone(),
        done_tx.clone(),
        bytes_up_ctr,
    ));
    let h_down = tokio::spawn(copy_dir(
        id,
        Dir::Down,
        or_,
        iw,
        last_activity.clone(),
        cancel.clone(),
        done_tx.clone(),
        bytes_down_ctr,
    ));
    // Drop our extra sender so the channel closes once both directions end.
    drop(done_tx);

    let reason = watchdog(
        done_rx,
        last_activity,
        cancel,
        shutdown,
        cfg.idle_timeout_secs,
        cfg.half_close_timeout_secs,
    )
    .await;

    let bytes_up = h_up.await.unwrap_or(0);
    let bytes_down = h_down.await.unwrap_or(0);

    metrics
        .connections_closed
        .with_label_values(&[&cfg.name, reason])
        .inc();

    info!(proxy = %cfg.name, id, %peer, target = %cfg.target, bytes_up, bytes_down, reason, "connection closed");
}

/// Copy one direction until EOF, error, or cancellation. Returns bytes copied.
#[allow(clippy::too_many_arguments)]
async fn copy_dir<R, W>(
    id: u64,
    dir: Dir,
    mut reader: R,
    mut writer: W,
    last_activity: Arc<AtomicU64>,
    cancel: CancellationToken,
    done: mpsc::Sender<(Dir, End)>,
    bytes: prometheus::IntCounter,
) -> u64
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; BUF_SIZE];
    let mut total: u64 = 0;
    let mut end = End::Eof;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            res = reader.read(&mut buf) => match res {
                Ok(0) => {
                    // Clean EOF: half-close the peer's write side.
                    let _ = writer.shutdown().await;
                    break;
                }
                Ok(n) => {
                    last_activity.store(now_ms(), Ordering::Relaxed);
                    total += n as u64;
                    if let Err(e) = writer.write_all(&buf[..n]).await {
                        debug!(id, dir = dir.label(), "write: {e}");
                        end = End::Err;
                        break;
                    }
                    bytes.inc_by(n as u64);
                }
                Err(e) => {
                    debug!(id, dir = dir.label(), "read: {e}");
                    let _ = writer.shutdown().await;
                    end = End::Err;
                    break;
                }
            }
        }
    }

    // Always report completion (ignored if the watchdog already returned).
    let _ = done.send((dir, end)).await;
    total
}

/// Enforce L3 (idle) and L4 (half-close) and forward global shutdown.
/// Returns the reason the connection ended, for logging.
async fn watchdog(
    mut done_rx: mpsc::Receiver<(Dir, End)>,
    last_activity: Arc<AtomicU64>,
    cancel: CancellationToken,
    shutdown: CancellationToken,
    idle_secs: u64,
    half_close_secs: u64,
) -> &'static str {
    let mut up_done = false;
    let mut down_done = false;
    let mut saw_error = false;
    let mut channel_open = true;
    let mut half_close_since: Option<u64> = None;

    loop {
        // Sleep only as long as the nearest deadline needs, capped at
        // WATCHDOG_TICK. This keeps small idle/half-close timeouts accurate
        // instead of rounding up to a fixed tick.
        let now = now_ms();
        let mut wake_ms = WATCHDOG_TICK.as_millis() as u64;
        if idle_secs > 0 {
            let deadline = last_activity.load(Ordering::Relaxed) + idle_secs * 1000;
            wake_ms = wake_ms.min(deadline.saturating_sub(now));
        }
        if half_close_secs > 0 {
            if let Some(since) = half_close_since {
                let deadline = since + half_close_secs * 1000;
                wake_ms = wake_ms.min(deadline.saturating_sub(now));
            }
        }
        // Floor to avoid a busy spin when a deadline is essentially now.
        let wake = Duration::from_millis(wake_ms.max(20));

        tokio::select! {
            _ = shutdown.cancelled() => {
                cancel.cancel();
                return "shutdown";
            }
            // Disabled once the channel closes, to avoid a busy loop.
            maybe = done_rx.recv(), if channel_open => match maybe {
                Some((Dir::Up, end)) => {
                    up_done = true;
                    saw_error |= end == End::Err;
                }
                Some((Dir::Down, end)) => {
                    down_done = true;
                    saw_error |= end == End::Err;
                }
                None => channel_open = false,
            },
            _ = sleep(wake) => {}
        }

        // Both directions finished → clean EOF, or reset if either errored.
        if up_done && down_done {
            return if saw_error { "reset" } else { "eof" };
        }

        let now = now_ms();

        // L3: application-level idle timeout.
        if idle_secs > 0 {
            let idle_ms = now.saturating_sub(last_activity.load(Ordering::Relaxed));
            if idle_ms >= idle_secs * 1000 {
                cancel.cancel();
                return "idle_timeout";
            }
        }

        // L4: half-close grace period.
        if half_close_secs > 0 && (up_done || down_done) {
            let since = *half_close_since.get_or_insert(now);
            if now.saturating_sub(since) >= half_close_secs * 1000 {
                cancel.cancel();
                return "half_close_timeout";
            }
        } else if !up_done && !down_done {
            half_close_since = None;
        }
    }
}
