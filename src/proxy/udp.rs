//! UDP proxy: per-source-address session map with idle timeout.
//!
//! UDP is connectionless so we track sessions by (listen_addr, src_addr).
//! Each session gets an ephemeral upstream socket. Liveness is L3 only —
//! keepalive and TCP_USER_TIMEOUT don't apply to UDP.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::clock::now_ms;
use crate::config::ProxyConfig;
use crate::limits::{ConnLimits, Guard, Reject};
use crate::metrics::Metrics;

// ── Session ────────────────────────────────────────────────────────────────

struct Session {
    /// Channel to forward client packets to the upstream relay task.
    tx: mpsc::Sender<Vec<u8>>,
    last_activity: Arc<AtomicU64>,
    /// Cancels both relay tasks for this session.
    cancel: CancellationToken,
    /// Holds the limits slot for the duration of this session.
    _slot: Guard,
}

// ── Main loop ──────────────────────────────────────────────────────────────

pub async fn run(
    cfg: Arc<ProxyConfig>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<()> {
    let listen_sock = Arc::new(
        UdpSocket::bind(&cfg.listen)
            .await
            .with_context(|| format!("UDP bind {}", cfg.listen))?,
    );

    info!(proxy = %cfg.name, "UDP listening");
    serve(listen_sock, cfg, metrics, shutdown).await
}

/// Run the UDP relay on a pre-bound socket.
///
/// Public so integration tests can bind to port 0, observe the assigned port,
/// then start the proxy on that socket.
pub async fn serve(
    listen_sock: Arc<UdpSocket>,
    cfg: Arc<ProxyConfig>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<()> {
    let sessions: Arc<Mutex<HashMap<SocketAddr, Session>>> = Arc::new(Mutex::new(HashMap::new()));
    let limits = ConnLimits::new(cfg.max_connections, cfg.max_per_ip);
    let active = metrics.active.with_label_values(&[&cfg.name]);

    // Cleanup task: evict idle sessions on a 5-second tick
    if cfg.idle_timeout_secs > 0 {
        let sessions2 = sessions.clone();
        let idle_secs = cfg.idle_timeout_secs;
        let shut = shutdown.clone();
        let closed = metrics
            .connections_closed
            .with_label_values(&[&cfg.name, "idle_timeout"]);
        let active2 = active.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shut.cancelled() => break,
                    _ = sleep(Duration::from_secs(5)) => {}
                }
                let now = now_ms();
                let mut map = sessions2.lock().await;
                map.retain(|src, s| {
                    let last = s.last_activity.load(Ordering::Relaxed);
                    let stale = now.saturating_sub(last) >= idle_secs.saturating_mul(1000);
                    if stale {
                        debug!(%src, "UDP session idle timeout");
                        s.cancel.cancel();
                        closed.inc();
                    }
                    !stale
                });
                active2.set(map.len() as i64);
            }
        });
    }

    let mut recv_buf = vec![0u8; 65535];

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                info!(proxy = %cfg.name, "UDP listener shutting down");
                // Cancel all live sessions
                let map = sessions.lock().await;
                for s in map.values() {
                    s.cancel.cancel();
                }
                let closed = metrics
                    .connections_closed
                    .with_label_values(&[&cfg.name, "shutdown"]);
                closed.inc_by(map.len() as u64);
                active.set(0);
                break;
            }
            result = listen_sock.recv_from(&mut recv_buf) => {
                let (n, src) = match result {
                    Ok(v) => v,
                    // A recv error here is non-fatal: on Linux a prior send to a
                    // closed target can surface as ECONNREFUSED (ICMP port
                    // unreachable) on a later recv_from. Logging and continuing
                    // prevents a remote peer from killing the whole listener.
                    Err(e) => {
                        warn!(proxy = %cfg.name, "UDP recv error: {e}");
                        continue;
                    }
                };
                let data = recv_buf[..n].to_vec();

                // Fast path: existing session. Short critical section only.
                {
                    let map = sessions.lock().await;
                    if let Some(session) = map.get(&src) {
                        session.last_activity.store(now_ms(), Ordering::Relaxed);
                        // Non-blocking send: drop packet if relay task is behind
                        if session.tx.try_send(data).is_err() {
                            debug!(%src, "UDP relay channel full, packet dropped");
                        }
                        continue;
                    }
                }

                // Slow path: first packet from this source. Admit, then build
                // the session WITHOUT holding the sessions lock — open_session
                // does DNS + bind + connect and could otherwise stall the whole
                // listener (and every other session) for a hostile/slow target.
                let slot = match limits.try_acquire(src.ip()) {
                    Ok(g) => g,
                    Err(Reject::Total) => {
                        error!(
                            proxy = %cfg.name,
                            src_ip = %src.ip(),
                            limit = limits.max_total,
                            "UDP session rejected: total limit reached"
                        );
                        metrics.connections_rejected
                            .with_label_values(&[&cfg.name, "total"]).inc();
                        continue;
                    }
                    Err(Reject::PerIp) => {
                        error!(
                            proxy = %cfg.name,
                            src_ip = %src.ip(),
                            limit = limits.max_per_ip,
                            "UDP session rejected: per-IP limit reached"
                        );
                        metrics.connections_rejected
                            .with_label_values(&[&cfg.name, "per_ip"]).inc();
                        continue;
                    }
                };

                let session = match open_session(src, &cfg, &metrics, listen_sock.clone(), shutdown.clone(), slot).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(%src, "UDP session open failed: {e:#}");
                        continue;
                    }
                };

                // Re-acquire to insert. Another packet from the same source may
                // have raced us to create a session while we were resolving; if
                // so, keep the existing one and drop ours (its Guard releases).
                let mut map = sessions.lock().await;
                if let Some(existing) = map.get(&src) {
                    existing.last_activity.store(now_ms(), Ordering::Relaxed);
                    let _ = existing.tx.try_send(data);
                    session.cancel.cancel(); // tear down the loser's relay tasks
                } else {
                    let _ = session.tx.try_send(data);
                    map.insert(src, session);
                    metrics.connections_total
                        .with_label_values(&[&cfg.name, "udp"]).inc();
                    active.set(map.len() as i64);
                }
            }
        }
    }
    Ok(())
}

// ── Session lifecycle ──────────────────────────────────────────────────────

async fn open_session(
    src: SocketAddr,
    cfg: &ProxyConfig,
    metrics: &Metrics,
    listen: Arc<UdpSocket>,
    shutdown: CancellationToken,
    slot: Guard,
) -> Result<Session> {
    // Resolve target to know which IP family to bind the upstream socket to
    let target_addr = tokio::net::lookup_host(&cfg.target)
        .await
        .with_context(|| format!("DNS lookup {}", cfg.target))?
        .next()
        .ok_or_else(|| anyhow::anyhow!("no address resolved for {}", cfg.target))?;

    let bind_addr: SocketAddr = if target_addr.is_ipv6() {
        "[::]:0".parse().unwrap()
    } else {
        "0.0.0.0:0".parse().unwrap()
    };

    let upstream = Arc::new(
        UdpSocket::bind(bind_addr)
            .await
            .context("UDP upstream bind")?,
    );

    tokio::time::timeout(
        Duration::from_secs(cfg.connect_timeout_secs),
        upstream.connect(target_addr),
    )
    .await
    .context("UDP connect timeout")?
    .context("UDP connect")?;

    info!(%src, target = %cfg.target, "UDP session opened");

    let last_activity = Arc::new(AtomicU64::new(now_ms()));
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(256);
    let cancel = CancellationToken::new();

    let bytes_up = metrics.bytes_total.with_label_values(&[&cfg.name, "up"]);
    let bytes_down = metrics.bytes_total.with_label_values(&[&cfg.name, "down"]);

    // ── client → upstream ─────────────────────────────────────────────────
    {
        let up = upstream.clone();
        let la = last_activity.clone();
        let c = cancel.clone();
        let s = shutdown.clone();
        let bytes_up = bytes_up.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = s.cancelled() => break,
                    _ = c.cancelled() => break,
                    data = rx.recv() => match data {
                        None => break,
                        Some(d) => {
                            la.store(now_ms(), Ordering::Relaxed);
                            match up.send(&d).await {
                                Ok(n) => bytes_up.inc_by(n as u64),
                                Err(e) => {
                                    debug!(%src, "UDP send upstream: {e}");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            debug!(%src, "UDP client→upstream task ended");
        });
    }

    // ── upstream → client ─────────────────────────────────────────────────
    {
        let la = last_activity.clone();
        let c = cancel.clone();
        let s = shutdown.clone();
        let bytes_down = bytes_down.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            loop {
                tokio::select! {
                    biased;
                    _ = s.cancelled() => break,
                    _ = c.cancelled() => break,
                    res = upstream.recv(&mut buf) => match res {
                        Err(e) => {
                            debug!(%src, "UDP recv upstream: {e}");
                            break;
                        }
                        Ok(n) => {
                            la.store(now_ms(), Ordering::Relaxed);
                            match listen.send_to(&buf[..n], src).await {
                                Ok(sent) => bytes_down.inc_by(sent as u64),
                                Err(e) => {
                                    debug!(%src, "UDP send client: {e}");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            debug!(%src, "UDP upstream→client task ended");
        });
    }

    Ok(Session {
        tx,
        last_activity,
        cancel,
        _slot: slot,
    })
}
