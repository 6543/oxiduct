use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio::signal;
use tokio::task::JoinSet;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

use oxiduct::{cli, config, metrics, proxy};

#[tokio::main]
async fn main() -> Result<()> {
    let args = cli::Args::parse();

    // clap already resolves RUST_LOG vs --log-level (env attr on the flag),
    // so one EnvFilter built from the resolved value is the whole story.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(&args.log_level))
        .init();

    // Resolve proxies + global settings. CLI flags win over the TOML keys
    // (metrics_listen, shutdown_grace); built-in defaults fill the rest.
    let (proxies, metrics_listen, shutdown_grace) = if let Some(ref path) = args.config {
        let loaded = config::load(path)?;
        let addr = args.metrics_listen.clone().or(loaded.metrics_listen);
        let grace = args.shutdown_grace.or(loaded.shutdown_grace);
        (loaded.proxies, addr, grace)
    } else {
        (
            vec![config::ProxyConfig::from_cli(&args)?],
            args.metrics_listen.clone(),
            args.shutdown_grace,
        )
    };
    let shutdown_grace = shutdown_grace.unwrap_or(config::defaults::SHUTDOWN_GRACE_SECS);

    let shutdown = CancellationToken::new();
    let stats = metrics::Metrics::new();
    let mut tasks: JoinSet<Result<()>> = JoinSet::new();

    // Optional Prometheus exporter. Treated like a proxy task: if it fails to
    // bind, startup aborts with a non-zero exit.
    if let Some(addr) = metrics_listen {
        tasks.spawn(metrics::serve(addr, stats.clone(), shutdown.clone()));
    }

    for cfg in proxies {
        let cfg = Arc::new(cfg);
        info!(proxy = %cfg.name, "starting");
        tracing::debug!(
            proxy = %cfg.name,
            protocol = ?cfg.protocol,
            idle_timeout_secs = cfg.idle_timeout_secs,
            half_close_timeout_secs = cfg.half_close_timeout_secs,
            max_connections = cfg.max_connections,
            max_per_ip = cfg.max_per_ip,
            "resolved config"
        );
        tasks.spawn(proxy::run(cfg, stats.clone(), shutdown.clone()));
    }

    tokio::select! {
        // A task finishing before any signal means a bind failure or an
        // unexpected exit — abort immediately.
        finished = tasks.join_next() => {
            match finished {
                Some(Ok(Err(e))) => {
                    tracing::error!("proxy failed: {e:#}");
                    std::process::exit(1);
                }
                Some(Err(e)) => {
                    tracing::error!("proxy task panicked: {e}");
                    std::process::exit(1);
                }
                // Clean early exit (shouldn't happen normally) or empty set.
                Some(Ok(Ok(()))) | None => return Ok(()),
            }
        }
        _ = signal::ctrl_c() => info!("received SIGINT"),
        _ = sigterm()        => info!("received SIGTERM"),
    }

    let grace = Duration::from_secs(shutdown_grace);
    info!(?grace, "shutting down");
    shutdown.cancel();

    let mut any_error = false;
    let _ = tokio::time::timeout(grace, async {
        while let Some(finished) = tasks.join_next().await {
            match finished {
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
