pub mod proxy_protocol;
pub mod tcp;
pub mod udp;

use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::config::{Protocol, ProxyConfig};
use crate::metrics::Metrics;

pub async fn run(
    cfg: Arc<ProxyConfig>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<()> {
    // Publish the configured limits as gauges up front.
    metrics
        .limit_max_connections
        .with_label_values(&[cfg.name.as_str()])
        .set(cfg.max_connections as i64);
    metrics
        .limit_max_per_ip
        .with_label_values(&[cfg.name.as_str()])
        .set(cfg.max_per_ip as i64);

    match cfg.protocol {
        Protocol::Tcp => tcp::run(cfg, metrics, shutdown).await,
        Protocol::Udp => udp::run(cfg, metrics, shutdown).await,
    }
}
