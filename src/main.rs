use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio::signal;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

use oxiduct::{cli, config, proxy};

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

    tokio::select! {
        // A proxy task that exits early means bind failed — abort immediately.
        result = wait_any(&mut handles) => {
            if let Err(e) = result {
                tracing::error!("proxy failed: {e:#}");
                std::process::exit(1);
            }
            // All proxies exited cleanly without a signal (shouldn't happen normally).
            return Ok(());
        }
        _ = signal::ctrl_c() => info!("received SIGINT"),
        _ = sigterm()        => info!("received SIGTERM"),
    }

    let grace = Duration::from_secs(args.shutdown_grace);
    info!(?grace, "shutting down");
    shutdown.cancel();

    let mut any_error = false;
    let _ = tokio::time::timeout(grace, async {
        for h in handles {
            match h.await {
                Ok(Err(e)) => {
                    tracing::error!("proxy error: {e:#}");
                    any_error = true;
                }
                Err(e) => {
                    tracing::error!("proxy task panicked: {e}");
                    any_error = true;
                }
                Ok(Ok(())) => {}
            }
        }
    })
    .await;

    info!("bye");
    if any_error {
        std::process::exit(1);
    }
    Ok(())
}

/// Wait for the first handle to finish. Returns its result (or the join error).
async fn wait_any(handles: &mut [tokio::task::JoinHandle<Result<()>>]) -> Result<()> {
    loop {
        let mut all_done = true;
        for h in handles.iter_mut() {
            if h.is_finished() {
                return h
                    .await
                    .unwrap_or_else(|e| Err(anyhow::anyhow!("task panicked: {e}")));
            }
            all_done = false;
        }
        if all_done {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(unix)]
async fn sigterm() {
    use tokio::signal::unix::{signal, SignalKind};
    signal(SignalKind::terminate())
        .expect("SIGTERM handler")
        .recv()
        .await;
}

#[cfg(not(unix))]
async fn sigterm() {
    std::future::pending::<()>().await
}
