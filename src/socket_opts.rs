//! Apply socket-level liveness options to TCP streams.
//!
//! L1 – SO_KEEPALIVE + TCP_KEEPIDLE / TCP_KEEPINTVL / TCP_KEEPCNT
//! L2 – TCP_USER_TIMEOUT  (Linux/Android only)
//!
//! Failures are logged and swallowed: a socket that won't take an option is
//! still usable, just less protected, and that shouldn't drop the connection.

use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;
use tracing::warn;

use crate::config::ProxyConfig;

pub fn apply_tcp(stream: &TcpStream, cfg: &ProxyConfig) {
    let sr = SockRef::from(stream);

    // L1: keepalive
    if cfg.keepalive_idle_secs > 0 {
        let mut ka = TcpKeepalive::new().with_time(Duration::from_secs(cfg.keepalive_idle_secs));
        if cfg.keepalive_interval_secs > 0 {
            ka = ka.with_interval(Duration::from_secs(cfg.keepalive_interval_secs));
        }
        if cfg.keepalive_retries > 0 {
            ka = ka.with_retries(cfg.keepalive_retries);
        }
        if let Err(e) = sr.set_tcp_keepalive(&ka) {
            warn!("set_tcp_keepalive: {e}");
        }
    }

    // L2: TCP_USER_TIMEOUT (Linux / Android only)
    #[cfg(any(target_os = "linux", target_os = "android"))]
    if cfg.user_timeout_ms > 0 {
        if let Err(e) =
            sr.set_tcp_user_timeout(Some(Duration::from_millis(cfg.user_timeout_ms as u64)))
        {
            warn!("set_tcp_user_timeout: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::apply_tcp;
    use crate::config::{Protocol, ProxyConfig};
    use tokio::net::{TcpListener, TcpStream};

    fn base_cfg() -> ProxyConfig {
        ProxyConfig {
            name: "test".into(),
            listen: "127.0.0.1:0".into(),
            target: "127.0.0.1:0".into(),
            protocol: Protocol::Tcp,
            connect_timeout_secs: 3,
            keepalive_idle_secs: 0,
            keepalive_interval_secs: 0,
            keepalive_retries: 0,
            user_timeout_ms: 0,
            idle_timeout_secs: 0,
            half_close_timeout_secs: 0,
        }
    }

    async fn client_stream() -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, _server) = tokio::join!(TcpStream::connect(addr), listener.accept());
        client.unwrap()
    }

    #[tokio::test]
    async fn apply_all_disabled() {
        let stream = client_stream().await;
        apply_tcp(&stream, &base_cfg());
    }

    #[tokio::test]
    async fn apply_with_keepalive() {
        let stream = client_stream().await;
        let cfg = ProxyConfig {
            keepalive_idle_secs: 60,
            keepalive_interval_secs: 10,
            keepalive_retries: 6,
            ..base_cfg()
        };
        apply_tcp(&stream, &cfg);
    }

    #[tokio::test]
    async fn apply_with_user_timeout() {
        let stream = client_stream().await;
        let cfg = ProxyConfig {
            user_timeout_ms: 30_000,
            ..base_cfg()
        };
        apply_tcp(&stream, &cfg);
    }

    #[tokio::test]
    async fn apply_all_options() {
        let stream = client_stream().await;
        let cfg = ProxyConfig {
            keepalive_idle_secs: 60,
            keepalive_interval_secs: 10,
            keepalive_retries: 6,
            user_timeout_ms: 90_000,
            ..base_cfg()
        };
        apply_tcp(&stream, &cfg);
    }
}
