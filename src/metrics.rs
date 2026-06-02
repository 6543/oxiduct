//! Prometheus metrics: an explicit registry + a tiny HTTP exporter.
//!
//! No global recorder — a single `Arc<Metrics>` is threaded through the
//! proxies, matching the rest of the codebase. The exporter is a minimal
//! HTTP/1.1 responder (GET /metrics) so we don't pull in a web framework.
//!
//! Cardinality note: labels are limited to `proxy`, `protocol`, `direction`
//! and a small fixed set of `reason` values. Source IPs are deliberately NOT
//! used as labels — that would let any client blow up series cardinality.

use std::sync::Arc;

use anyhow::{Context, Result};
use prometheus::{Encoder, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// All metric families, registered against one private registry.
pub struct Metrics {
    registry: Registry,

    /// Connections accepted / UDP sessions opened. Labels: proxy, protocol.
    pub connections_total: IntCounterVec,
    /// Connections refused by the limiter. Labels: proxy, reason (total|per_ip).
    pub connections_rejected: IntCounterVec,
    /// Upstream connect failures (TCP). Labels: proxy.
    pub connect_failures: IntCounterVec,
    /// Upstream connect timeouts (TCP). Labels: proxy.
    pub connect_timeouts: IntCounterVec,
    /// Bytes relayed. Labels: proxy, direction (up|down).
    pub bytes_total: IntCounterVec,
    /// Connections/sessions closed. Labels: proxy, reason.
    pub connections_closed: IntCounterVec,

    /// Currently active connections / sessions. Labels: proxy.
    pub active: IntGaugeVec,
    /// Configured total cap (0 = unlimited). Labels: proxy.
    pub limit_max_connections: IntGaugeVec,
    /// Configured per-IP cap (0 = unlimited). Labels: proxy.
    pub limit_max_per_ip: IntGaugeVec,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();

        let counter = |name: &str, help: &str, labels: &[&str]| {
            let c = IntCounterVec::new(Opts::new(name, help), labels).expect("valid metric");
            registry.register(Box::new(c.clone())).expect("register");
            c
        };
        let gauge = |name: &str, help: &str, labels: &[&str]| {
            let g = IntGaugeVec::new(Opts::new(name, help), labels).expect("valid metric");
            registry.register(Box::new(g.clone())).expect("register");
            g
        };

        Arc::new(Self {
            connections_total: counter(
                "oxiduct_connections_total",
                "Connections accepted / UDP sessions opened",
                &["proxy", "protocol"],
            ),
            connections_rejected: counter(
                "oxiduct_connections_rejected_total",
                "Connections refused by the connection limiter",
                &["proxy", "reason"],
            ),
            connect_failures: counter(
                "oxiduct_connect_failures_total",
                "Upstream TCP connect failures",
                &["proxy"],
            ),
            connect_timeouts: counter(
                "oxiduct_connect_timeouts_total",
                "Upstream TCP connect timeouts",
                &["proxy"],
            ),
            bytes_total: counter(
                "oxiduct_bytes_total",
                "Bytes relayed",
                &["proxy", "direction"],
            ),
            connections_closed: counter(
                "oxiduct_connections_closed_total",
                "Connections / sessions closed, by reason",
                &["proxy", "reason"],
            ),
            active: gauge(
                "oxiduct_active_connections",
                "Currently active connections / sessions",
                &["proxy"],
            ),
            limit_max_connections: gauge(
                "oxiduct_max_connections",
                "Configured total connection cap (0 = unlimited)",
                &["proxy"],
            ),
            limit_max_per_ip: gauge(
                "oxiduct_max_per_ip",
                "Configured per-source-IP connection cap (0 = unlimited)",
                &["proxy"],
            ),
            registry,
        })
    }

    /// Render the registry in Prometheus text exposition format.
    fn render(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4096);
        let encoder = TextEncoder::new();
        if let Err(e) = encoder.encode(&self.registry.gather(), &mut buf) {
            warn!("metrics encode error: {e}");
        }
        buf
    }
}

/// Serve `GET /metrics` on `addr` until `shutdown` fires.
///
/// Minimal HTTP/1.1: reads the request line, replies, closes the connection.
/// Anything other than `GET /metrics` gets a 404.
pub async fn serve(addr: String, metrics: Arc<Metrics>, shutdown: CancellationToken) -> Result<()> {
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("metrics bind {addr}"))?;
    info!(%addr, "metrics exporter listening on /metrics");

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                info!("metrics exporter shutting down");
                return Ok(());
            }
            accepted = listener.accept() => {
                let (stream, _peer) = match accepted {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("metrics accept error: {e}");
                        continue;
                    }
                };
                let metrics = metrics.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_request(stream, &metrics).await {
                        warn!("metrics request error: {e}");
                    }
                });
            }
        }
    }
}

async fn handle_request(mut stream: tokio::net::TcpStream, metrics: &Metrics) -> Result<()> {
    // Read just enough to see the request line. Scrapers send a tiny GET.
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let head = String::from_utf8_lossy(&buf[..n]);
    let first_line = head.lines().next().unwrap_or("");

    let is_metrics = {
        let mut parts = first_line.split_whitespace();
        let method = parts.next().unwrap_or("");
        let path = parts.next().unwrap_or("");
        method == "GET" && (path == "/metrics" || path.starts_with("/metrics?"))
    };

    if is_metrics {
        let body = metrics.render();
        let header = format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: text/plain; version=0.0.4\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(&body).await?;
    } else {
        let body = b"404 not found\n";
        let header = format!(
            "HTTP/1.1 404 Not Found\r\n\
             Content-Type: text/plain\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(body).await?;
    }
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_contains_registered_metrics() {
        let m = Metrics::new();
        m.connections_total.with_label_values(&["p1", "tcp"]).inc();
        m.bytes_total.with_label_values(&["p1", "up"]).inc_by(123);
        m.active.with_label_values(&["p1"]).set(2);

        let out = String::from_utf8(m.render()).unwrap();
        assert!(out.contains("oxiduct_connections_total"));
        assert!(out.contains("oxiduct_bytes_total"));
        assert!(out.contains("oxiduct_active_connections"));
        assert!(out.contains("123"));
        // HELP/TYPE lines present (valid exposition format).
        assert!(out.contains("# HELP oxiduct_bytes_total"));
        assert!(out.contains("# TYPE oxiduct_bytes_total counter"));
    }

    #[test]
    fn labels_isolated() {
        let m = Metrics::new();
        m.connections_closed.with_label_values(&["p1", "eof"]).inc();
        m.connections_closed
            .with_label_values(&["p1", "reset"])
            .inc_by(3);
        let out = String::from_utf8(m.render()).unwrap();
        assert!(out.contains("reason=\"eof\""));
        assert!(out.contains("reason=\"reset\""));
    }
}
