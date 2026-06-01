//! TCP proxy: accept loop + per-connection relay with layered liveness.

use crate::config::ProxyConfig;
use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub async fn run(_cfg: Arc<ProxyConfig>, _shutdown: CancellationToken) -> Result<()> {
    todo!("TCP proxy not yet implemented")
}
