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
use tracing::{debug, info, warn};

use crate::clock::now_ms;
use crate::config::ProxyConfig;
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

// ── Accept loop ──────────────────────────────────────────────────────────────

pub async fn run(cfg: Arc<ProxyConfig>, shutdown: CancellationToken) -> Result<()> {
    let listener = TcpListener::bind(&cfg.listen)
        .await
        .with_context(|| format!("bind {}", cfg.listen))?;

    info!(proxy = %cfg.name, "TCP listening");
    serve(listener, cfg, shutdown).await
}

/// Run the accept loop on a pre-bound listener.
///
/// Public so integration tests can bind to port 0, observe the assigned port,
/// then start the proxy on that listener.
pub async fn serve(
    listener: TcpListener,
    cfg: Arc<ProxyConfig>,
    shutdown: CancellationToken,
) -> Result<()> {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                info!(proxy = %cfg.name, "TCP listener shutting down");
                break;
            }
            result = listener.accept() => match result {
                Ok((stream, peer)) => {
                    let id = CONN_ID.fetch_add(1, Ordering::Relaxed);
                    let cfg = cfg.clone();
                    let token = shutdown.child_token();
                    tokio::spawn(async move { handle(id, stream, peer, cfg, token).await });
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
            warn!(id, %peer, target = %cfg.target, "connect failed: {e}");
            return;
        }
        Err(_) => {
            warn!(id, %peer, target = %cfg.target, timeout = cfg.connect_timeout_secs, "connect timed out");
            return;
        }
    };

    socket_opts::apply_tcp(&outbound, &cfg);

    info!(id, %peer, target = %cfg.target, "connected");
    relay(
        id,
        peer,
        inbound,
        outbound,
        cfg.idle_timeout_secs,
        cfg.half_close_timeout_secs,
        &cfg.target,
        shutdown,
    )
    .await;
}

// ── Bidirectional relay ──────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn relay(
    id: u64,
    peer: SocketAddr,
    inbound: TcpStream,
    outbound: TcpStream,
    idle_secs: u64,
    half_close_secs: u64,
    target: &str,
    shutdown: CancellationToken,
) {
    let (ir, iw) = inbound.into_split();
    let (or_, ow) = outbound.into_split();

    // Shared idle clock (bumped on every byte) + a token that stops both
    // directions. Each direction reports completion once over `done_tx`.
    let last_activity = Arc::new(AtomicU64::new(now_ms()));
    let cancel = CancellationToken::new();
    let (done_tx, done_rx) = mpsc::channel::<Dir>(2);

    let h_up = tokio::spawn(copy_dir(
        id,
        Dir::Up,
        ir,
        ow,
        last_activity.clone(),
        cancel.clone(),
        done_tx.clone(),
    ));
    let h_down = tokio::spawn(copy_dir(
        id,
        Dir::Down,
        or_,
        iw,
        last_activity.clone(),
        cancel.clone(),
        done_tx.clone(),
    ));
    // Drop our extra sender so the channel closes once both directions end.
    drop(done_tx);

    let reason = watchdog(
        done_rx,
        last_activity,
        cancel,
        shutdown,
        idle_secs,
        half_close_secs,
    )
    .await;

    let bytes_up = h_up.await.unwrap_or(0);
    let bytes_down = h_down.await.unwrap_or(0);

    info!(id, %peer, %target, bytes_up, bytes_down, reason, "connection closed");
}

/// Copy one direction until EOF, error, or cancellation. Returns bytes copied.
async fn copy_dir<R, W>(
    id: u64,
    dir: Dir,
    mut reader: R,
    mut writer: W,
    last_activity: Arc<AtomicU64>,
    cancel: CancellationToken,
    done: mpsc::Sender<Dir>,
) -> u64
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; BUF_SIZE];
    let mut total: u64 = 0;

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
                        break;
                    }
                }
                Err(e) => {
                    debug!(id, dir = dir.label(), "read: {e}");
                    let _ = writer.shutdown().await;
                    break;
                }
            }
        }
    }

    // Always report completion (ignored if the watchdog already returned).
    let _ = done.send(dir).await;
    total
}

/// Enforce L3 (idle) and L4 (half-close) and forward global shutdown.
/// Returns the reason the connection ended, for logging.
async fn watchdog(
    mut done_rx: mpsc::Receiver<Dir>,
    last_activity: Arc<AtomicU64>,
    cancel: CancellationToken,
    shutdown: CancellationToken,
    idle_secs: u64,
    half_close_secs: u64,
) -> &'static str {
    let mut up_done = false;
    let mut down_done = false;
    let mut channel_open = true;
    let mut half_close_since: Option<u64> = None;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                cancel.cancel();
                return "shutdown";
            }
            // Disabled once the channel closes, to avoid a busy loop.
            maybe = done_rx.recv(), if channel_open => match maybe {
                Some(Dir::Up) => up_done = true,
                Some(Dir::Down) => down_done = true,
                None => channel_open = false,
            },
            _ = sleep(WATCHDOG_TICK) => {}
        }

        // Both directions finished naturally → clean close.
        if up_done && down_done {
            return "eof";
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
