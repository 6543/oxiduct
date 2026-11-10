//! UDP proxy: per-source-address session map with idle timeout.
//!
//! UDP is connectionless so we track sessions by (listen_addr, src_addr).
//! Each session gets an ephemeral upstream socket. Liveness is L3 only —
//! keepalive and TCP_USER_TIMEOUT don't apply to UDP.

use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use anyhow::{Context, Result};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::{sleep, Duration, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::clock::now_ms;
use crate::config::ProxyConfig;
use crate::limits::{ConnLimits, Guard};
use crate::metrics::Metrics;

/// How often a draining listener checks whether its sessions are gone.
const DRAIN_POLL: Duration = Duration::from_millis(100);

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
    // Drain poll. Built once, outside the loop: a `sleep()` inside `select!`
    // is recreated on every pass, so a steady stream of packets would keep
    // resetting it and the drained check would never run.
    let mut drain_tick = tokio::time::interval(DRAIN_POLL);
    drain_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

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
            _ = drain_tick.tick(), if draining => {
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

                    // Insert. No session for this source can be in the map:
                    // the listener only gets here when it found none (or just
                    // evicted a dead one), and the pending mark keeps a second
                    // setup task for the same source from running until this
                    // one is done.
                    let _ = session.tx.try_send(data);
                    let mut map = sessions.lock().await;
                    map.insert(src, session);
                    metrics.connections_total
                        .with_label_values(&[cfg.name.as_str(), "udp"]).inc();
                    active.set(map.len() as i64);
                    cleanup_wakeup.notify_one();
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
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host(&cfg.target)
                .await
                .with_context(|| format!("DNS lookup {}", cfg.target))?
                .collect();
            Upstream::connect(addrs)
                .await
                .with_context(|| format!("UDP connect {}", cfg.target))
        }) => result.context("UDP session setup timed out")??,
    };

    info!(%src, target = %cfg.target, "UDP session opened");

    let last_activity = Arc::new(AtomicU64::new(now_ms()));
    let (tx, rx) = mpsc::channel::<Vec<u8>>(256);
    let cancel = CancellationToken::new();

    let relay = Relay {
        src,
        upstream,
        rx,
        listen,
        last_activity: last_activity.clone(),
        cancel: cancel.clone(),
        shutdown,
        bytes_up: metrics
            .bytes_total
            .with_label_values(&[cfg.name.as_str(), "up"]),
        bytes_down: metrics
            .bytes_total
            .with_label_values(&[cfg.name.as_str(), "down"]),
    };
    tokio::spawn(relay.run());

    Ok(Session {
        tx,
        last_activity,
        cancel,
        _slot: slot,
    })
}

/// The upstream side of a session.
///
/// A target name can resolve to several addresses (typically IPv6 and IPv4).
/// Like a TCP connect, try them in order: when the current address refuses
/// (ICMP port unreachable shows up as ECONNREFUSED on a connected UDP
/// socket), move on to the next one.
struct Upstream {
    addrs: Vec<SocketAddr>,
    /// Index into `addrs` of the address `sock` is connected to.
    current: usize,
    sock: UdpSocket,
}

impl Upstream {
    async fn connect(addrs: Vec<SocketAddr>) -> Result<Self> {
        let mut last_err = anyhow::anyhow!("no address resolved");
        for (current, addr) in addrs.iter().enumerate() {
            match socket_to(*addr).await {
                Ok(sock) => {
                    return Ok(Self {
                        addrs,
                        current,
                        sock,
                    })
                }
                Err(e) => last_err = e,
            }
        }
        Err(last_err)
    }

    fn addr(&self) -> SocketAddr {
        self.addrs[self.current]
    }

    /// Switch to the next address that takes a socket. `false` once all
    /// addresses are used up.
    async fn try_next(&mut self) -> bool {
        while self.current + 1 < self.addrs.len() {
            self.current += 1;
            match socket_to(self.addr()).await {
                Ok(sock) => {
                    self.sock = sock;
                    return true;
                }
                Err(e) => debug!(addr = %self.addr(), "UDP upstream unusable: {e:#}"),
            }
        }
        false
    }
}

/// An ephemeral UDP socket of the target's address family, connected to it.
async fn socket_to(addr: SocketAddr) -> Result<UdpSocket> {
    let bind_addr: SocketAddr = if addr.is_ipv6() {
        (Ipv6Addr::UNSPECIFIED, 0).into()
    } else {
        (Ipv4Addr::UNSPECIFIED, 0).into()
    };
    let sock = UdpSocket::bind(bind_addr)
        .await
        .context("UDP upstream bind")?;
    sock.connect(addr).await.context("UDP connect")?;
    Ok(sock)
}

fn is_refused(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::ConnectionRefused
}

/// Moves datagrams between one client and its upstream until the session
/// ends. One task for both directions, so a switch to the next upstream
/// address cannot race with a send or receive on the old socket.
struct Relay {
    src: SocketAddr,
    upstream: Upstream,
    rx: mpsc::Receiver<Vec<u8>>,
    listen: Arc<UdpSocket>,
    last_activity: Arc<AtomicU64>,
    cancel: CancellationToken,
    shutdown: CancellationToken,
    bytes_up: prometheus::IntCounter,
    bytes_down: prometheus::IntCounter,
}

impl Relay {
    async fn run(mut self) {
        let src = self.src;
        let mut buf = vec![0u8; 65535];
        // The last datagram sent upstream, repeated after a switch to the
        // next address: its refusal is what triggered the switch.
        let mut last_sent: Option<Vec<u8>> = None;
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => break,
                _ = self.cancel.cancelled() => break,
                data = self.rx.recv() => match data {
                    None => break,
                    Some(d) => {
                        self.last_activity.store(now_ms(), Ordering::Relaxed);
                        if !self.send_upstream(&d).await {
                            break;
                        }
                        last_sent = Some(d);
                    }
                },
                res = self.upstream.sock.recv(&mut buf) => match res {
                    Err(e) if is_refused(&e) && self.next_upstream().await => {
                        if let Some(d) = &last_sent {
                            if !self.send_upstream(d).await {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        debug!(%src, "UDP recv upstream: {e}");
                        break;
                    }
                    Ok(n) => {
                        self.last_activity.store(now_ms(), Ordering::Relaxed);
                        match self.listen.send_to(&buf[..n], src).await {
                            Ok(sent) => self.bytes_down.inc_by(sent as u64),
                            Err(e) => {
                                debug!(%src, "UDP send client: {e}");
                                break;
                            }
                        }
                    }
                }
            }
        }
        // Mark the whole session dead so the listener evicts (and can
        // transparently re-create) it.
        self.cancel.cancel();
        debug!(%src, "UDP relay task ended");
    }

    /// Send one datagram upstream; `false` ends the session.
    async fn send_upstream(&mut self, d: &[u8]) -> bool {
        loop {
            match self.upstream.sock.send(d).await {
                Ok(n) => {
                    self.bytes_up.inc_by(n as u64);
                    return true;
                }
                // A refusal of an earlier datagram can surface on this send.
                Err(e) if is_refused(&e) && self.next_upstream().await => continue,
                Err(e) => {
                    debug!(src = %self.src, "UDP send upstream: {e}");
                    return false;
                }
            }
        }
    }

    async fn next_upstream(&mut self) -> bool {
        let refused = self.upstream.addr();
        if !self.upstream.try_next().await {
            return false;
        }
        info!(
            src = %self.src,
            %refused,
            next = %self.upstream.addr(),
            "UDP target refused, trying the next address"
        );
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// UDP echo on 127.0.0.1, echoes every datagram back to its sender.
    async fn echo() -> SocketAddr {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sock.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            while let Ok((n, peer)) = sock.recv_from(&mut buf).await {
                let _ = sock.send_to(&buf[..n], peer).await;
            }
        });
        addr
    }

    /// A local UDP port nobody listens on, so datagrams to it get refused.
    fn closed_port() -> SocketAddr {
        let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        sock.local_addr().unwrap()
    }

    /// Relay one datagram from a client through a session whose target
    /// resolved to `addrs`, and return the reply the client got.
    async fn round_trip(addrs: Vec<SocketAddr>) -> Option<Vec<u8>> {
        let listen = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.connect(listen.local_addr().unwrap()).await.unwrap();

        let metrics = Metrics::new();
        let (tx, rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let relay = Relay {
            src: client.local_addr().unwrap(),
            upstream: Upstream::connect(addrs).await.unwrap(),
            rx,
            listen,
            last_activity: Arc::new(AtomicU64::new(now_ms())),
            cancel: cancel.clone(),
            shutdown: CancellationToken::new(),
            bytes_up: metrics.bytes_total.with_label_values(&["test", "up"]),
            bytes_down: metrics.bytes_total.with_label_values(&["test", "down"]),
        };
        tokio::spawn(relay.run());

        tx.send(b"ping".to_vec()).await.unwrap();
        let mut buf = [0u8; 64];
        let reply = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf)).await;
        cancel.cancel();
        match reply {
            Ok(Ok(n)) => Some(buf[..n].to_vec()),
            _ => None,
        }
    }

    #[tokio::test]
    async fn relays_to_the_first_address() {
        let reply = round_trip(vec![echo().await]).await;
        assert_eq!(reply.as_deref(), Some(&b"ping"[..]));
    }

    #[tokio::test]
    async fn moves_on_when_an_address_refuses() {
        let reply = round_trip(vec![closed_port(), echo().await]).await;
        assert_eq!(reply.as_deref(), Some(&b"ping"[..]));
    }

    #[tokio::test]
    async fn ends_the_session_when_every_address_refuses() {
        let reply = round_trip(vec![closed_port(), closed_port()]).await;
        assert_eq!(reply, None);
    }
}
