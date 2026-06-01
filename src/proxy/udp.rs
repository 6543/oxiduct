//! UDP proxy: per-source-address session map with idle timeout.
//!
//! UDP is connectionless so we track sessions by (listen_addr, src_addr).
//! Each session gets an ephemeral upstream socket. Liveness is L3 only —
//! keepalive and TCP_USER_TIMEOUT don't apply to UDP.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::ProxyConfig;

// ── Session ────────────────────────────────────────────────────────────────

struct Session {
    /// Channel to forward client packets to the upstream relay task.
    tx: mpsc::Sender<Vec<u8>>,
    last_activity: Arc<AtomicU64>,
    /// Cancels both relay tasks for this session.
    cancel: CancellationToken,
}

// ── Main loop ──────────────────────────────────────────────────────────────

pub async fn run(cfg: Arc<ProxyConfig>, shutdown: CancellationToken) -> Result<()> {
    let listen_sock = Arc::new(
        UdpSocket::bind(&cfg.listen)
            .await
            .with_context(|| format!("UDP bind {}", cfg.listen))?,
    );

    info!(proxy = %cfg.name, "UDP listening");

    let sessions: Arc<Mutex<HashMap<SocketAddr, Session>>> = Arc::new(Mutex::new(HashMap::new()));

    // Cleanup task: evict idle sessions on a 5-second tick
    if cfg.idle_timeout_secs > 0 {
        let sessions2 = sessions.clone();
        let idle_secs = cfg.idle_timeout_secs;
        let shut = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shut.cancelled() => break,
                    _ = sleep(Duration::from_secs(5)) => {}
                }
                let now = now_ms();
                sessions2.lock().await.retain(|src, s| {
                    let last = s.last_activity.load(Ordering::Relaxed);
                    let stale = now.saturating_sub(last) >= idle_secs * 1000;
                    if stale {
                        debug!(%src, "UDP session idle timeout");
                        s.cancel.cancel();
                    }
                    !stale
                });
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
                for s in sessions.lock().await.values() {
                    s.cancel.cancel();
                }
                break;
            }
            result = listen_sock.recv_from(&mut recv_buf) => {
                let (n, src) = result.context("UDP recv_from")?;
                let data = recv_buf[..n].to_vec();

                let mut map = sessions.lock().await;

                if let Some(session) = map.get(&src) {
                    session.last_activity.store(now_ms(), Ordering::Relaxed);
                    // Non-blocking send: drop packet if relay task is behind
                    if session.tx.try_send(data).is_err() {
                        debug!(%src, "UDP relay channel full, packet dropped");
                    }
                } else {
                    // First packet from this source: open a new session
                    match open_session(src, &cfg, listen_sock.clone(), shutdown.clone()).await {
                        Err(e) => warn!(%src, "UDP session open failed: {e:#}"),
                        Ok(session) => {
                            let _ = session.tx.try_send(data);
                            map.insert(src, session);
                        }
                    }
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
    listen: Arc<UdpSocket>,
    shutdown: CancellationToken,
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

    // ── client → upstream ─────────────────────────────────────────────────
    {
        let up = upstream.clone();
        let la = last_activity.clone();
        let c = cancel.clone();
        let s = shutdown.clone();
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
                            if let Err(e) = up.send(&d).await {
                                debug!(%src, "UDP send upstream: {e}");
                                break;
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
                            if let Err(e) = listen.send_to(&buf[..n], src).await {
                                debug!(%src, "UDP send client: {e}");
                                break;
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
    })
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
