//! TCP proxy: accept loop + per-connection relay with layered liveness.
//!
//! Liveness layers per connection (both inbound and outbound sockets):
//!   L1 – SO_KEEPALIVE + TCP_KEEPIDLE / TCP_KEEPINTVL / TCP_KEEPCNT   (socket_opts)
//!   L2 – TCP_USER_TIMEOUT                                              (socket_opts, Linux)
//!   L3 – Application idle timeout  (no bytes either direction → kill)
//!   L4 – Half-close grace          (one side EOF → deadline on other)

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::ProxyConfig;
use crate::socket_opts;

/// Monotonically increasing connection ID for log correlation.
static CONN_ID: AtomicU64 = AtomicU64::new(1);

// ── Accept loop ────────────────────────────────────────────────────────────

pub async fn run(cfg: Arc<ProxyConfig>, shutdown: CancellationToken) -> Result<()> {
    let listener = TcpListener::bind(&cfg.listen)
        .await
        .with_context(|| format!("bind {}", cfg.listen))?;

    info!(proxy = %cfg.name, "TCP listening");

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
                    // Child token: cancelled on both global shutdown and locally
                    let token = shutdown.child_token();
                    tokio::spawn(async move {
                        handle(id, stream, peer, cfg, token).await;
                    });
                }
                Err(e) => {
                    // Non-fatal: log and keep accepting
                    warn!(proxy = %cfg.name, "accept error: {e}");
                }
            }
        }
    }
    Ok(())
}

// ── Per-connection handler ─────────────────────────────────────────────────

async fn handle(
    id: u64,
    inbound: TcpStream,
    peer: SocketAddr,
    cfg: Arc<ProxyConfig>,
    shutdown: CancellationToken,
) {
    // L1 + L2 on inbound socket (errors are logged inside, non-fatal)
    let _ = socket_opts::apply_tcp(&inbound, &cfg);

    // Connect to target with configurable timeout
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
            warn!(
                id, %peer, target = %cfg.target,
                timeout = cfg.connect_timeout_secs,
                "connect timed out"
            );
            return;
        }
    };

    // L1 + L2 on outbound socket (errors are logged inside, non-fatal)
    let _ = socket_opts::apply_tcp(&outbound, &cfg);

    info!(id, %peer, target = %cfg.target, "connected");
    relay(id, peer, inbound, outbound, &cfg, shutdown).await;
}

// ── Bidirectional relay ────────────────────────────────────────────────────

async fn relay(
    id: u64,
    peer: SocketAddr,
    inbound: TcpStream,
    outbound: TcpStream,
    cfg: &ProxyConfig,
    shutdown: CancellationToken,
) {
    let (mut ir, mut iw) = inbound.into_split();
    let (mut or_, mut ow) = outbound.into_split();

    // Shared state between relay tasks and watchdog
    let last_activity = Arc::new(AtomicU64::new(now_ms()));
    let a_done = Arc::new(AtomicBool::new(false)); // inbound → outbound finished
    let b_done = Arc::new(AtomicBool::new(false)); // outbound → inbound finished
    let relay_cancel = CancellationToken::new();

    // Notify wakes the watchdog early when a relay task finishes,
    // avoiding a full 5-second sleep on clean closes.
    let done_notify = Arc::new(Notify::new());

    // ── Direction A: inbound → outbound ───────────────────────────────────
    let la_a = last_activity.clone();
    let ad = a_done.clone();
    let rc_a = relay_cancel.clone();
    let dn_a = done_notify.clone();

    let h_a = tokio::spawn(async move {
        let mut buf = vec![0u8; 16 * 1024];
        let mut total: u64 = 0;

        loop {
            tokio::select! {
                biased;
                _ = rc_a.cancelled() => break,
                res = ir.read(&mut buf) => match res {
                    Ok(0) => {
                        // Clean EOF from client: half-close the upstream write side
                        let _ = ow.shutdown().await;
                        break;
                    }
                    Ok(n) => {
                        la_a.store(now_ms(), Ordering::Relaxed);
                        total += n as u64;
                        if let Err(e) = ow.write_all(&buf[..n]).await {
                            debug!(id, "write→upstream: {e}");
                            break;
                        }
                    }
                    Err(e) => {
                        debug!(id, "read←client: {e}");
                        let _ = ow.shutdown().await;
                        break;
                    }
                }
            }
        }

        ad.store(true, Ordering::Release);
        dn_a.notify_one();
        total
    });

    // ── Direction B: outbound → inbound ───────────────────────────────────
    let la_b = last_activity.clone();
    let bd = b_done.clone();
    let rc_b = relay_cancel.clone();
    let dn_b = done_notify.clone();

    let h_b = tokio::spawn(async move {
        let mut buf = vec![0u8; 16 * 1024];
        let mut total: u64 = 0;

        loop {
            tokio::select! {
                biased;
                _ = rc_b.cancelled() => break,
                res = or_.read(&mut buf) => match res {
                    Ok(0) => {
                        let _ = iw.shutdown().await;
                        break;
                    }
                    Ok(n) => {
                        la_b.store(now_ms(), Ordering::Relaxed);
                        total += n as u64;
                        if let Err(e) = iw.write_all(&buf[..n]).await {
                            debug!(id, "write→client: {e}");
                            break;
                        }
                    }
                    Err(e) => {
                        debug!(id, "read←upstream: {e}");
                        let _ = iw.shutdown().await;
                        break;
                    }
                }
            }
        }

        bd.store(true, Ordering::Release);
        dn_b.notify_one();
        total
    });

    // ── Watchdog: L3 idle + L4 half-close + shutdown forwarding ──────────
    let idle_secs = cfg.idle_timeout_secs;
    let half_close_secs = cfg.half_close_timeout_secs;

    let h_w = tokio::spawn(async move {
        let mut half_close_since: Option<u64> = None;

        loop {
            // Wake on: shutdown signal, a relay task finished, or periodic tick
            tokio::select! {
                _ = shutdown.cancelled() => {
                    relay_cancel.cancel();
                    return "shutdown";
                }
                _ = done_notify.notified() => {}   // quick re-check after task exit
                _ = sleep(Duration::from_secs(5)) => {}
            }

            let now = now_ms();
            let a_fin = a_done.load(Ordering::Acquire);
            let b_fin = b_done.load(Ordering::Acquire);

            // Both done naturally — clean close
            if a_fin && b_fin {
                return "eof";
            }

            // L3: application-level idle timeout
            if idle_secs > 0 {
                let idle_ms = now.saturating_sub(last_activity.load(Ordering::Relaxed));
                if idle_ms >= idle_secs * 1000 {
                    relay_cancel.cancel();
                    return "idle_timeout";
                }
            }

            // L4: half-close grace period
            if half_close_secs > 0 && (a_fin || b_fin) {
                let since = *half_close_since.get_or_insert(now);
                if now.saturating_sub(since) >= half_close_secs * 1000 {
                    relay_cancel.cancel();
                    return "half_close_timeout";
                }
            } else if !a_fin && !b_fin {
                // Both still running — reset the half-close clock
                half_close_since = None;
            }
        }
    });

    let (bytes_up, bytes_down, reason) = tokio::join!(h_a, h_b, h_w);
    let bytes_up = bytes_up.unwrap_or(0);
    let bytes_down = bytes_down.unwrap_or(0);
    let reason = reason.as_deref().unwrap_or("task_panic");

    info!(
        id,
        %peer,
        target = %cfg.target,
        bytes_up,
        bytes_down,
        reason,
        "connection closed"
    );
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
