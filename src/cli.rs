use std::path::PathBuf;

use clap::Parser;

use crate::config::defaults;

/// Pipe traffic through oxidized steel — robust TCP/UDP proxy.
///
/// Single-proxy mode:  supply --listen and --target flags.
/// Multi-proxy mode:   supply --config path/to/config.toml
#[derive(Parser, Debug)]
#[command(name = "oxiduct", version, about)]
#[command(group(
    clap::ArgGroup::new("mode")
        .required(true)
        .args(["config", "listen"]),
))]
pub struct Args {
    /// TOML config file (multi-proxy mode; conflicts with --listen / --target)
    #[arg(short, long, conflicts_with_all = ["listen", "target", "protocol"])]
    pub config: Option<PathBuf>,

    /// Listen address: "host:port" or bare "port" (expands to 0.0.0.0:PORT)
    #[arg(long, requires = "target")]
    pub listen: Option<String>,

    /// Target "host:port" to forward traffic to
    #[arg(long, requires = "listen")]
    pub target: Option<String>,

    /// Protocol to proxy
    #[arg(long, default_value = "tcp", value_parser = ["tcp", "udp"])]
    pub protocol: String,

    /// Connect timeout (seconds)
    #[arg(long, default_value_t = defaults::CONNECT_TIMEOUT_SECS)]
    pub connect_timeout: u64,

    /// TCP keepalive: idle time before first probe (seconds, 0 = disable)
    #[arg(long, default_value_t = defaults::KEEPALIVE_IDLE_SECS)]
    pub keepalive_idle: u64,

    /// TCP keepalive: interval between probes (seconds)
    #[arg(long, default_value_t = defaults::KEEPALIVE_INTERVAL_SECS)]
    pub keepalive_interval: u64,

    /// TCP keepalive: max probes before dropping connection
    #[arg(long, default_value_t = defaults::KEEPALIVE_RETRIES)]
    pub keepalive_retries: u32,

    /// TCP_USER_TIMEOUT in milliseconds (0 = OS default, Linux/Android only)
    #[arg(long, default_value_t = defaults::USER_TIMEOUT_MS)]
    pub user_timeout_ms: u32,

    /// Application idle timeout (seconds, 0 = disable)
    #[arg(long, default_value_t = defaults::IDLE_TIMEOUT_SECS)]
    pub idle_timeout: u64,

    /// Half-close grace period (seconds, 0 = disable)
    #[arg(long, default_value_t = defaults::HALF_CLOSE_TIMEOUT_SECS)]
    pub half_close_timeout: u64,

    /// Hard cap on simultaneous connections / UDP sessions per proxy (0 = unlimited)
    #[arg(long, default_value_t = defaults::MAX_CONNECTIONS)]
    pub max_connections: u32,

    /// Hard cap on simultaneous connections / UDP sessions per source IP (0 = unlimited)
    #[arg(long, default_value_t = defaults::MAX_PER_IP)]
    pub max_per_ip: u32,

    /// Grace period on SIGTERM/SIGINT before force-closing connections (seconds)
    #[arg(long, default_value_t = defaults::SHUTDOWN_GRACE_SECS)]
    pub shutdown_grace: u64,

    /// Serve Prometheus metrics at GET /metrics on this address (e.g. 127.0.0.1:9090).
    /// Unset = exporter disabled.
    #[arg(long)]
    pub metrics_listen: Option<String>,

    /// Log level — also read from RUST_LOG env var
    #[arg(long, default_value = "info", env = "RUST_LOG")]
    pub log_level: String,
}

/// Expand a bare port number "587" to "0.0.0.0:587". Anything that doesn't
/// parse as a `u16` is returned unchanged.
pub fn expand_listen(s: &str) -> String {
    if s.parse::<u16>().is_ok() {
        format!("0.0.0.0:{s}")
    } else {
        s.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::expand_listen;

    #[test]
    fn expand_bare_port() {
        assert_eq!(expand_listen("587"), "0.0.0.0:587");
        assert_eq!(expand_listen("1"), "0.0.0.0:1");
        assert_eq!(expand_listen("65535"), "0.0.0.0:65535");
    }

    #[test]
    fn expand_passes_through_full_addr() {
        assert_eq!(expand_listen("127.0.0.1:80"), "127.0.0.1:80");
        assert_eq!(expand_listen("0.0.0.0:443"), "0.0.0.0:443");
        assert_eq!(expand_listen("[::1]:80"), "[::1]:80");
    }

    #[test]
    fn expand_passes_through_non_port_input() {
        assert_eq!(expand_listen("65536"), "65536"); // out of u16 range
        assert_eq!(expand_listen("-1"), "-1");
        assert_eq!(expand_listen(""), "");
        assert_eq!(expand_listen("foobar"), "foobar");
    }

    #[test]
    fn expand_zero_port() {
        assert_eq!(expand_listen("0"), "0.0.0.0:0");
    }
}
