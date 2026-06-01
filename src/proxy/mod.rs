pub mod tcp;
pub mod udp;

use crate::config::{Protocol, ProxyConfig};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::error;

pub async fn run(cfg: Arc<ProxyConfig>, shutdown: CancellationToken) {
    let result = match cfg.protocol {
        Protocol::Tcp => tcp::run(cfg.clone(), shutdown).await,
        Protocol::Udp => udp::run(cfg.clone(), shutdown).await,
    };
    if let Err(e) = result {
        error!(proxy = %cfg.name, "fatal: {e:#}");
    }
}
