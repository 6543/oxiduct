pub mod tcp;
pub mod udp;

use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::config::{Protocol, ProxyConfig};

pub async fn run(cfg: Arc<ProxyConfig>, shutdown: CancellationToken) -> Result<()> {
    match cfg.protocol {
        Protocol::Tcp => tcp::run(cfg, shutdown).await,
        Protocol::Udp => udp::run(cfg, shutdown).await,
    }
}
