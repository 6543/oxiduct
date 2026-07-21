//! UDP proxy: per-source-address session map with idle timeout.
//!
//! UDP is connectionless so we track sessions by (listen_addr, src_addr).
//! Each session gets an ephemeral upstream socket. Liveness is L3 only —
//! keepalive and TCP_USER_TIMEOUT don't apply to UDP.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use anyhow::{Context, Result};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::clock::now_ms;
use crate::config::ProxyConfig;
use crate::limits::{ConnLimits, Guard};
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

/// Marks a source as "session setup in flight" so the listener doesn't spawn
/// duplicate setup tasks for a burst of first packets. Drop-based so the mark
/// is cleared on every exit path, including panics.
struct PendingGuard {
    set: Arc<StdMutex<HashSet<SocketAddr>>>,
    src: SocketAddr,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.set
            .lock()
            .expect("pending mutex poisoned")
            .remove(&self.src);
    }
}

// ── Main loop ──────────────────────────────────────────────────────────────

pub async fn run(
    cfg: Arc<ProxyConfig>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<()> {
    run_graceful(cfg, metrics, shutdown.clone(), shutdown).await
}

pub async fn run_graceful(
    cfg: Arc<ProxyConfig>,
    metrics: Arc<Metrics>,
    stop_accepting: CancellationToken,
    force_shutdown: CancellationToken,
) -> Result<()> {
    let listen_sock = Arc::new(
        UdpSocket::bind(&cfg.listen)
            .await
            .with_context(|| format!("UDP bind {}", cfg.listen))?,
    );

    info!(proxy = %cfg.name, "UDP listening");
    serve_graceful(listen_sock, cfg, metrics, stop_accepting, force_shutdown).await
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
    serve_graceful(listen_sock, cfg, metrics, shutdown.clone(), shutdown).await
}

pub async fn serve_graceful(
    listen_sock: Arc<UdpSocket>,
    cfg: Arc<ProxyConfig>,
    metrics: Arc<Metrics>,
    stop_accepting: CancellationToken,
    force_shutdown: CancellationToken,
) -> Result<()> {
    let sessions: Arc<Mutex<HashMap<SocketAddr, Session>>> = Arc::new(Mutex::new(HashMap::new()));
    // Sources with a session-setup task in flight (see PendingGuard).
    let pending: Arc<StdMutex<HashSet<SocketAddr>>> = Arc::new(StdMutex::new(HashSet::new()));
    let cleanup_wakeup = Arc::new(Notify::new());
    let cleanup_shutdown = force_shutdown.child_token();
    let limits = ConnLimits::new(cfg.max_connections, cfg.max_per_ip);
    let active = metrics.active.with_label_values(&[cfg.name.as_str()]);

    // Cleanup task: sleep until the nearest idle deadline, capped at five
    // seconds so dead relay tasks are also evicted promptly. New sessions wake
    // it to recalculate. Without this, a dead session would pin its limits slot
    // forever.
    {
        let sessions2 = sessions.clone();
        let idle_secs = cfg.idle_timeout_secs;
        let shut = cleanup_shutdown.clone();
        let closed_idle = metrics
            .connections_closed
            .with_label_values(&[cfg.name.as_str(), "idle_timeout"]);
        let closed_error = metrics
            .connections_closed
            .with_label_values(&[cfg.name.as_str(), "error"]);
        let active2 = active.clone();
        let wakeup = cleanup_wakeup.clone();
        tokio::spawn(async move {
            loop {
                let wait = {
                    let map = sessions2.lock().await;
                    if map.values().any(|s| s.cancel.is_cancelled()) {
                        Duration::ZERO
                    } else if idle_secs > 0 {
                        let now = now_ms();
                        let idle_ms = idle_secs.saturating_mul(1000);
                        let until_idle = map
                            .values()
                            .map(|s| {
                                s.last_activity
                                    .load(Ordering::Relaxed)
                                    .saturating_add(idle_ms)
                                    .saturating_sub(now)
                            })
                            .min()
                            .unwrap_or(5000);
                        Duration::from_millis(until_idle.min(5000))
                    } else {
                        Duration::from_secs(5)
                    }
                };

                tokio::select! {
                    _ = shut.cancelled() => break,
                    _ = wakeup.notified() => continue,
                    _ = sleep(wait) => {}
                }
                let now = now_ms();
                let mut map = sessions2.lock().await;
                map.retain(|src, s| {
                    if s.cancel.is_cancelled() {
                        debug!(%src, "UDP session dead, evicting");
                        closed_error.inc();
                        return false;
                    }
                    let last = s.last_activity.load(Ordering::Relaxed);
                    let stale =
                        idle_secs > 0 && now.saturating_sub(last) >= idle_secs.saturating_mul(1000);
                    if stale {
                        debug!(%src, "UDP session idle timeout");
                        s.cancel.cancel();
                        closed_idle.inc();
                    }
                    !stale
                });
                active2.set(map.len() as i64);
            }
        });
    }

    let mut recv_buf = vec![0u8; 65535];
    let mut draining = false;

    loop {
        tokio::select! {
            biased;
            _ = force_shutdown.cancelled() => {
                info!(proxy = %cfg.name, "UDP sessions shutting down");
                // Cancel all live sessions
                let map = sessions.lock().await;
                for s in map.values() {
                    s.cancel.cancel();
                }
                let closed = metrics
                    .connections_closed
                    .with_label_values(&[cfg.name.as_str(), "shutdown"]);
                closed.inc_by(map.len() as u64);
                active.set(0);
                break;
            }
            _ = stop_accepting.cancelled(), if !draining => {
                draining = true;
                info!(proxy = %cfg.name, "UDP listener draining existing sessions");
            }
            _ = sleep(Duration::from_millis(100)), if draining => {
                let no_sessions = sessions.lock().await.is_empty();
                let no_pending = pending.lock().expect("pending mutex poisoned").is_empty();
                if no_sessions && no_pending {
                    info!(proxy = %cfg.name, "UDP listener drained");
                    break;
                }
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
                // A dead session (relay task errored and cancelled itself) is
                // evicted right here so this packet re-creates it below,
                // instead of being dropped until the cleanup tick fires.
                {
                    let mut map = sessions.lock().await;
                    match map.get(&src) {
                        Some(session) if session.cancel.is_cancelled() => {
                            debug!(%src, "UDP session dead, re-creating");
                            map.remove(&src);
                            metrics.connections_closed
                                .with_label_values(&[cfg.name.as_str(), "error"]).inc();
                            active.set(map.len() as i64);
                            // fall through to the slow path
                        }
                        Some(session) => {
                            session.last_activity.store(now_ms(), Ordering::Relaxed);
                            // Non-blocking send: drop packet if relay task is behind
                            if session.tx.try_send(data).is_err() {
                                debug!(%src, "UDP relay channel full, packet dropped");
                            }
                            continue;
                        }
                        None => {}
                    }
                }

                if draining {
                    debug!(%src, "UDP listener draining, new session packet dropped");
                    continue;
                }

                // Slow path: first packet from this source. Admit, then build
                // the session in its OWN task — open_session does DNS + bind +
                // connect and would otherwise stall the whole listener (and
                // every other session) on a slow resolver or hostile target.
                // Concurrency is naturally bounded by the limits Guard.
                //
                // A burst of first packets from the same source must not spawn
                // one setup task each: mark the source pending and drop
                // follow-up packets until setup resolves (drops are fine, UDP
                // clients retransmit).
                let pending_guard = {
                    let mut p = pending.lock().expect("pending mutex poisoned");
                    if !p.insert(src) {
                        debug!(%src, "UDP session setup already in flight, packet dropped");
                        continue;
                    }
                    PendingGuard {
                        set: pending.clone(),
                        src,
                    }
                };

                let slot = match limits.try_acquire(src.ip()) {
                    Ok(g) => g,
                    Err(rej) => {
                        // error! level + src_ip field so log scrapers
                        // (fail2ban etc.) can match rejected sources.
                        error!(
                            proxy = %cfg.name,
                            src_ip = %src.ip(),
                            limit = rej.limit(&limits),
                            reason = rej.label(),
                            "UDP session rejected: limit reached"
                        );
                        metrics.connections_rejected
                            .with_label_values(&[cfg.name.as_str(), rej.label()]).inc();
                        continue;
                    }
                };

                let cfg = cfg.clone();
                let metrics = metrics.clone();
                let sessions = sessions.clone();
                let listen_sock = listen_sock.clone();
                let force_shutdown = force_shutdown.clone();
                let active = active.clone();
                let cleanup_wakeup = cleanup_wakeup.clone();
                tokio::spawn(async move {
                    // Cleared when this task ends, whatever the outcome.
                    let _pending = pending_guard;
                    let session = match open_session(
                        src,
                        &cfg,
                        &metrics,
                        listen_sock,
                        force_shutdown.clone(),
                        slot,
                    ).await {
                        Ok(s) => s,
                        Err(e) => {
                            if !force_shutdown.is_cancelled() {
                                warn!(%src, "UDP session open failed: {e:#}");
                            }
                            return;
                        }
                    };
                    if force_shutdown.is_cancelled() {
                        session.cancel.cancel();
                        return;
                    }

                    // Insert. Another packet from the same source may have
                    // raced us to create a session while we were resolving; if
                    // so, keep the existing one (unless it already died) and
                    // drop ours (its Guard releases).
                    let mut map = sessions.lock().await;
                    if let Some(existing) = map.get(&src).filter(|s| !s.cancel.is_cancelled()) {
                        existing.last_activity.store(now_ms(), Ordering::Relaxed);
                        let _ = existing.tx.try_send(data);
                        session.cancel.cancel(); // tear down the loser's relay tasks
                    } else {
                        let _ = session.tx.try_send(data);
                        if map.insert(src, session).is_some() {
                            // Replaced a session that died while we were resolving.
                            metrics.connections_closed
                                .with_label_values(&[cfg.name.as_str(), "error"]).inc();
                        }
                        metrics.connections_total
                            .with_label_values(&[cfg.name.as_str(), "udp"]).inc();
                        active.set(map.len() as i64);
                        cleanup_wakeup.notify_one();
                    }
                });
            }
        }
    }
    cleanup_shutdown.cancel();
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
    // Resolve + bind + connect, all under one connect-timeout budget. DNS in
    // particular has no inherent bound and must not be allowed to hang a
    // session-setup task forever.
    let upstream = tokio::select! {
        biased;
        _ = shutdown.cancelled() => anyhow::bail!("UDP session setup cancelled"),
        result = tokio::time::timeout(Duration::from_secs(cfg.connect_timeout_secs), async {
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

            let upstream = UdpSocket::bind(bind_addr)
                .await
                .context("UDP upstream bind")?;
            upstream.connect(target_addr).await.context("UDP connect")?;
            Ok::<_, anyhow::Error>(Arc::new(upstream))
        }) => result.context("UDP session setup timed out")??,
    };

    info!(%src, target = %cfg.target, "UDP session opened");

    let last_activity = Arc::new(AtomicU64::new(now_ms()));
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(256);
    let cancel = CancellationToken::new();

    let bytes_up = metrics
        .bytes_total
        .with_label_values(&[cfg.name.as_str(), "up"]);
    let bytes_down = metrics
        .bytes_total
        .with_label_values(&[cfg.name.as_str(), "down"]);

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
            // Mark the whole session dead so the partner task stops and the
            // listener evicts (and can transparently re-create) the session.
            c.cancel();
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
            c.cancel();
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
