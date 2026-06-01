mod cli;
mod config;
mod proxy;
mod socket_opts;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tokio::signal;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let args = cli::Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&args.log_level)),
        )
        .init();

    let proxies: Vec<config::ProxyConfig> = if let Some(ref path) = args.config {
        config::load(path)?
    } else {
        vec![config::ProxyConfig::from_cli(&args)?]
    };

    let shutdown = CancellationToken::new();
    let mut handles = Vec::with_capacity(proxies.len());

    for cfg in proxies {
        let cfg = Arc::new(cfg);
        info!(proxy = %cfg.name, "starting");
        let token = shutdown.clone();
        handles.push(tokio::spawn(proxy::run(cfg, token)));
    }

    // Wait for SIGINT or SIGTERM
    tokio::select! {
        _ = signal::ctrl_c()       => info!("received SIGINT"),
        _ = sigterm()              => info!("received SIGTERM"),
    }

    let grace = Duration::from_secs(args.shutdown_grace);
    info!(?grace, "shutting down");
    shutdown.cancel();

    let _ = tokio::time::timeout(grace, async {
        for h in handles {
            let _ = h.await;
        }
    })
    .await;

    info!("bye");
    Ok(())
}

#[cfg(unix)]
async fn sigterm() {
    use tokio::signal::unix::{signal, SignalKind};
    signal(SignalKind::terminate())
        .expect("SIGTERM handler")
        .recv()
        .await;
}

// Windows: SIGTERM doesn't exist; ctrl_c covers it
#[cfg(not(unix))]
async fn sigterm() {
    std::future::pending::<()>().await
}
