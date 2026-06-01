#![allow(dead_code)]
//! Apply socket-level liveness options to TCP streams.
//!
//! Layers applied:
//!  L1 – SO_KEEPALIVE + TCP_KEEPIDLE / TCP_KEEPINTVL / TCP_KEEPCNT
//!  L2 – TCP_USER_TIMEOUT  (Linux/Android only)

use anyhow::Result;
use socket2::{SockRef, TcpKeepalive};
use std::time::Duration;
use tokio::net::TcpStream;
use tracing::warn;

use crate::config::ProxyConfig;

pub fn apply_tcp(stream: &TcpStream, cfg: &ProxyConfig) -> Result<()> {
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

    Ok(())
}
