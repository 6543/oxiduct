//! Shared helpers for integration tests.
//!
//! `pub use` re-exports keep call sites short. The `#[allow(dead_code)]` is
//! needed because each test binary uses only a subset of these helpers.

#![allow(dead_code, unused_imports)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio_util::sync::CancellationToken;

use oxiduct::config::{Protocol, ProxyConfig};

// ── Test configs ────────────────────────────────────────────────────────────

pub fn cfg_tcp(target: SocketAddr) -> ProxyConfig {
    ProxyConfig {
        name: format!("test → {target}"),
        listen: "127.0.0.1:0".into(),
        target: target.to_string(),
        protocol: Protocol::Tcp,
        connect_timeout_secs: 3,
        keepalive_idle_secs: 0,
        keepalive_interval_secs: 0,
        keepalive_retries: 0,
        user_timeout_ms: 0,
        idle_timeout_secs: 0,
        half_close_timeout_secs: 0,
        max_connections: 0,
        max_per_ip: 0,
    }
}

pub fn cfg_udp(target: SocketAddr) -> ProxyConfig {
    let mut c = cfg_tcp(target);
    c.protocol = Protocol::Udp;
    c
}

// ── Servers ─────────────────────────────────────────────────────────────────

/// Spawn a TCP echo server. Each connection echoes bytes back until EOF.
pub async fn spawn_tcp_echo() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => return,
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            if stream.write_all(&buf[..n]).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });
    addr
}

/// TCP server that accepts connections and holds them open indefinitely
/// without reading or writing. Simulates a stuck or slow upstream.
pub async fn spawn_tcp_blackhole() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => return,
            };
            tokio::spawn(async move {
                // Hold ownership so the FD stays open
                let _s = stream;
                std::future::pending::<()>().await
            });
        }
    });
    addr
}

/// TCP server that sends one fixed message then keeps the connection open
/// indefinitely (doesn't read, doesn't close). Useful for testing
/// half-close: the proxy gets the message back to the client, then the
/// client EOFs, and we want to verify the proxy eventually drops the
/// stuck server side via L4 half_close_timeout.
pub async fn spawn_tcp_send_then_hold(msg: &'static [u8]) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => return,
            };
            tokio::spawn(async move {
                let _ = stream.write_all(msg).await;
                std::future::pending::<()>().await
            });
        }
    });
    addr
}

/// UDP echo server. Echoes any received datagram back to its sender.
pub async fn spawn_udp_echo() -> SocketAddr {
    let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let addr = sock.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 65535];
        loop {
            let (n, src) = match sock.recv_from(&mut buf).await {
                Ok(x) => x,
                Err(_) => return,
            };
            let _ = sock.send_to(&buf[..n], src).await;
        }
    });
    addr
}

/// UDP server that swallows datagrams without replying.
pub async fn spawn_udp_blackhole() -> SocketAddr {
    let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let addr = sock.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 65535];
        loop {
            if sock.recv_from(&mut buf).await.is_err() {
                return;
            }
        }
    });
    addr
}

// ── Proxy spawn helpers ─────────────────────────────────────────────────────

pub struct ProxyHandle {
    pub addr: SocketAddr,
    pub shutdown: CancellationToken,
    pub task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl ProxyHandle {
    /// Cancel and wait for the proxy task to finish.
    pub async fn stop(self) {
        self.shutdown.cancel();
        let _ = self.task.await;
    }
}

pub async fn spawn_tcp_proxy(cfg: ProxyConfig) -> ProxyHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(oxiduct::proxy::tcp::serve(
        listener,
        Arc::new(cfg),
        shutdown.clone(),
    ));
    // Yield so the accept loop can start polling
    tokio::task::yield_now().await;
    ProxyHandle {
        addr,
        shutdown,
        task,
    }
}

pub async fn spawn_udp_proxy(cfg: ProxyConfig) -> ProxyHandle {
    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let addr = socket.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(oxiduct::proxy::udp::serve(
        socket,
        Arc::new(cfg),
        shutdown.clone(),
    ));
    tokio::task::yield_now().await;
    ProxyHandle {
        addr,
        shutdown,
        task,
    }
}

// ── Misc ────────────────────────────────────────────────────────────────────

/// Sleep that's slightly shorter than the named duration. Used to "wait
/// past" a timeout without being right on the boundary.
pub async fn wait_past(secs: u64) {
    tokio::time::sleep(Duration::from_millis(secs * 1000 + 500)).await;
}

/// Find a free TCP port by binding to 0, getting the addr, and dropping.
/// Racy but fine for tests that need an address that will *fail* to connect.
pub async fn unused_tcp_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    addr
}
