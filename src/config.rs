//! Proxy configuration: resolved `ProxyConfig`, plus CLI and TOML loaders.
//!
//! Default tuning values live in [`defaults`] and nowhere else; both the CLI
//! (`clap default_value_t`) and the TOML loader reference those constants.

use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::cli::{self, Args};

/// Built-in default tuning values — the single source of truth.
pub mod defaults {
    pub const CONNECT_TIMEOUT_SECS: u64 = 3;
    pub const KEEPALIVE_IDLE_SECS: u64 = 60;
    pub const KEEPALIVE_INTERVAL_SECS: u64 = 10;
    pub const KEEPALIVE_RETRIES: u32 = 6;
    pub const USER_TIMEOUT_MS: u32 = 90_000;
    pub const IDLE_TIMEOUT_SECS: u64 = 300;
    pub const HALF_CLOSE_TIMEOUT_SECS: u64 = 30;
    pub const SHUTDOWN_GRACE_SECS: u64 = 10;
    /// Hard cap on simultaneous connections / UDP sessions per proxy. 0 = unlimited.
    pub const MAX_CONNECTIONS: u32 = 32_000;
    /// Hard cap on simultaneous connections / UDP sessions per source IP. 0 = unlimited.
    pub const MAX_PER_IP: u32 = 320;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    #[default]
    Tcp,
    Udp,
}

/// Fully resolved configuration for one proxy (no optional fields).
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub name: String,
    pub listen: String,
    pub target: String,
    pub protocol: Protocol,
    pub connect_timeout_secs: u64,
    pub keepalive_idle_secs: u64,
    pub keepalive_interval_secs: u64,
    pub keepalive_retries: u32,
    pub user_timeout_ms: u32,
    pub idle_timeout_secs: u64,
    pub half_close_timeout_secs: u64,
    pub max_connections: u32,
    pub max_per_ip: u32,
    /// Emit a PROXY protocol v2 header to the target on connect (TCP only).
    pub proxy_protocol: bool,
}

// ── TOML shapes ──────────────────────────────────────────────────────────────

/// Optional tuning knobs. Used for the `[defaults]` table and, field-for-field,
/// inside each `[[proxy]]`. `None` means "inherit from defaults, then const".
///
/// `#[serde(flatten)]` is intentionally NOT used here: it is unreliable for
/// typed/integer fields with the `toml` crate. Each proxy lists the knobs
/// explicitly and converts into this struct via [`TomlProxy::tuning`].
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlTuning {
    connect_timeout: Option<u64>,
    keepalive_idle: Option<u64>,
    keepalive_interval: Option<u64>,
    keepalive_retries: Option<u32>,
    user_timeout_ms: Option<u32>,
    idle_timeout: Option<u64>,
    half_close_timeout: Option<u64>,
    max_connections: Option<u32>,
    max_per_ip: Option<u32>,
    proxy_protocol: Option<bool>,
}

impl TomlTuning {
    /// Resolve into concrete values: per-proxy override → `[defaults]` → const.
    fn resolve(self, base: TomlTuning) -> ResolvedTuning {
        ResolvedTuning {
            connect_timeout_secs: self
                .connect_timeout
                .or(base.connect_timeout)
                .unwrap_or(defaults::CONNECT_TIMEOUT_SECS),
            keepalive_idle_secs: self
                .keepalive_idle
                .or(base.keepalive_idle)
                .unwrap_or(defaults::KEEPALIVE_IDLE_SECS),
            keepalive_interval_secs: self
                .keepalive_interval
                .or(base.keepalive_interval)
                .unwrap_or(defaults::KEEPALIVE_INTERVAL_SECS),
            keepalive_retries: self
                .keepalive_retries
                .or(base.keepalive_retries)
                .unwrap_or(defaults::KEEPALIVE_RETRIES),
            user_timeout_ms: self
                .user_timeout_ms
                .or(base.user_timeout_ms)
                .unwrap_or(defaults::USER_TIMEOUT_MS),
            idle_timeout_secs: self
                .idle_timeout
                .or(base.idle_timeout)
                .unwrap_or(defaults::IDLE_TIMEOUT_SECS),
            half_close_timeout_secs: self
                .half_close_timeout
                .or(base.half_close_timeout)
                .unwrap_or(defaults::HALF_CLOSE_TIMEOUT_SECS),
            max_connections: self
                .max_connections
                .or(base.max_connections)
                .unwrap_or(defaults::MAX_CONNECTIONS),
            max_per_ip: self
                .max_per_ip
                .or(base.max_per_ip)
                .unwrap_or(defaults::MAX_PER_IP),
            proxy_protocol: self.proxy_protocol.or(base.proxy_protocol).unwrap_or(false),
        }
    }
}

struct ResolvedTuning {
    connect_timeout_secs: u64,
    keepalive_idle_secs: u64,
    keepalive_interval_secs: u64,
    keepalive_retries: u32,
    user_timeout_ms: u32,
    idle_timeout_secs: u64,
    half_close_timeout_secs: u64,
    max_connections: u32,
    max_per_ip: u32,
    proxy_protocol: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlProxy {
    name: Option<String>,
    listen: String,
    target: String,
    #[serde(default)]
    protocol: Protocol,
    connect_timeout: Option<u64>,
    keepalive_idle: Option<u64>,
    keepalive_interval: Option<u64>,
    keepalive_retries: Option<u32>,
    user_timeout_ms: Option<u32>,
    idle_timeout: Option<u64>,
    half_close_timeout: Option<u64>,
    max_connections: Option<u32>,
    max_per_ip: Option<u32>,
    proxy_protocol: Option<bool>,
}

impl TomlProxy {
    fn tuning(&self) -> TomlTuning {
        TomlTuning {
            connect_timeout: self.connect_timeout,
            keepalive_idle: self.keepalive_idle,
            keepalive_interval: self.keepalive_interval,
            keepalive_retries: self.keepalive_retries,
            user_timeout_ms: self.user_timeout_ms,
            idle_timeout: self.idle_timeout,
            half_close_timeout: self.half_close_timeout,
            max_connections: self.max_connections,
            max_per_ip: self.max_per_ip,
            proxy_protocol: self.proxy_protocol,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlFile {
    #[serde(default)]
    defaults: TomlTuning,
    /// Optional global Prometheus exporter address.
    metrics_listen: Option<String>,
    #[serde(rename = "proxy")]
    proxies: Vec<TomlProxy>,
}

/// Result of loading a config file: the proxies plus global settings.
#[derive(Debug)]
pub struct LoadedConfig {
    pub proxies: Vec<ProxyConfig>,
    pub metrics_listen: Option<String>,
}

// ── Constructors ─────────────────────────────────────────────────────────────

impl ProxyConfig {
    /// Build a single-proxy config from CLI args.
    pub fn from_cli(args: &Args) -> Result<Self> {
        let listen = args
            .listen
            .clone()
            .map(|s| cli::expand_listen(&s))
            .ok_or_else(|| anyhow::anyhow!("--listen required in single-proxy mode"))?;
        let target = args
            .target
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--target required in single-proxy mode"))?;

        Ok(Self {
            name: format!("{listen} -> {target}"),
            listen,
            target,
            protocol: parse_protocol(&args.protocol)?,
            connect_timeout_secs: args.connect_timeout,
            keepalive_idle_secs: args.keepalive_idle,
            keepalive_interval_secs: args.keepalive_interval,
            keepalive_retries: args.keepalive_retries,
            user_timeout_ms: args.user_timeout_ms,
            idle_timeout_secs: args.idle_timeout,
            half_close_timeout_secs: args.half_close_timeout,
            max_connections: args.max_connections,
            max_per_ip: args.max_per_ip,
            proxy_protocol: args.proxy_protocol,
        })
    }

    fn from_toml(index: usize, p: TomlProxy, base: TomlTuning) -> Self {
        let listen = cli::expand_listen(&p.listen);
        let name = p
            .name
            .clone()
            .unwrap_or_else(|| format!("proxy-{index}: {listen} -> {}", p.target));
        let t = p.tuning().resolve(base);
        Self {
            name,
            listen,
            target: p.target,
            protocol: p.protocol,
            connect_timeout_secs: t.connect_timeout_secs,
            keepalive_idle_secs: t.keepalive_idle_secs,
            keepalive_interval_secs: t.keepalive_interval_secs,
            keepalive_retries: t.keepalive_retries,
            user_timeout_ms: t.user_timeout_ms,
            idle_timeout_secs: t.idle_timeout_secs,
            half_close_timeout_secs: t.half_close_timeout_secs,
            max_connections: t.max_connections,
            max_per_ip: t.max_per_ip,
            proxy_protocol: t.proxy_protocol,
        }
    }
}

/// Load and resolve every `[[proxy]]` entry from a TOML config file.
pub fn load(path: &Path) -> Result<LoadedConfig> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let file: TomlFile =
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;

    if file.proxies.is_empty() {
        anyhow::bail!("config file defines no [[proxy]] entries");
    }

    let metrics_listen = file.metrics_listen.clone();
    let proxies = file
        .proxies
        .into_iter()
        .enumerate()
        .map(|(i, p)| ProxyConfig::from_toml(i, p, file.defaults))
        .collect();

    Ok(LoadedConfig {
        proxies,
        metrics_listen,
    })
}

fn parse_protocol(s: &str) -> Result<Protocol> {
    match s {
        "tcp" => Ok(Protocol::Tcp),
        "udp" => Ok(Protocol::Udp),
        other => anyhow::bail!("unknown protocol '{other}'; expected tcp or udp"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args_with(listen: Option<&str>, target: Option<&str>, protocol: &str) -> Args {
        Args {
            config: None,
            listen: listen.map(String::from),
            target: target.map(String::from),
            protocol: protocol.into(),
            connect_timeout: defaults::CONNECT_TIMEOUT_SECS,
            keepalive_idle: defaults::KEEPALIVE_IDLE_SECS,
            keepalive_interval: defaults::KEEPALIVE_INTERVAL_SECS,
            keepalive_retries: defaults::KEEPALIVE_RETRIES,
            user_timeout_ms: defaults::USER_TIMEOUT_MS,
            idle_timeout: defaults::IDLE_TIMEOUT_SECS,
            half_close_timeout: defaults::HALF_CLOSE_TIMEOUT_SECS,
            max_connections: defaults::MAX_CONNECTIONS,
            max_per_ip: defaults::MAX_PER_IP,
            shutdown_grace: defaults::SHUTDOWN_GRACE_SECS,
            metrics_listen: None,
            log_level: "info".into(),
            proxy_protocol: false,
        }
    }

    // ── parse_protocol ─────────────────────────────────────────────────────

    #[test]
    fn parse_protocol_tcp() {
        assert_eq!(parse_protocol("tcp").unwrap(), Protocol::Tcp);
    }

    #[test]
    fn parse_protocol_udp() {
        assert_eq!(parse_protocol("udp").unwrap(), Protocol::Udp);
    }

    #[test]
    fn parse_protocol_unknown_errors() {
        assert!(parse_protocol("quic").is_err());
        assert!(parse_protocol("").is_err());
        assert!(parse_protocol("TCP").is_err()); // case-sensitive on purpose
    }

    // ── ProxyConfig::from_cli ──────────────────────────────────────────────

    #[test]
    fn from_cli_full() {
        let args = args_with(Some("587"), Some("mail.example.com:587"), "tcp");
        let cfg = ProxyConfig::from_cli(&args).unwrap();
        assert_eq!(cfg.listen, "0.0.0.0:587");
        assert_eq!(cfg.target, "mail.example.com:587");
        assert_eq!(cfg.protocol, Protocol::Tcp);
        assert_eq!(cfg.connect_timeout_secs, defaults::CONNECT_TIMEOUT_SECS);
        assert_eq!(cfg.keepalive_idle_secs, defaults::KEEPALIVE_IDLE_SECS);
        assert_eq!(cfg.user_timeout_ms, defaults::USER_TIMEOUT_MS);
        assert!(cfg.name.contains("0.0.0.0:587"));
        assert!(cfg.name.contains("mail.example.com:587"));
    }

    #[test]
    fn from_cli_udp() {
        let args = args_with(Some("5353"), Some("1.1.1.1:53"), "udp");
        let cfg = ProxyConfig::from_cli(&args).unwrap();
        assert_eq!(cfg.protocol, Protocol::Udp);
    }

    #[test]
    fn from_cli_missing_listen_errors() {
        let args = args_with(None, Some("a:1"), "tcp");
        assert!(ProxyConfig::from_cli(&args).is_err());
    }

    #[test]
    fn from_cli_missing_target_errors() {
        let args = args_with(Some("1"), None, "tcp");
        assert!(ProxyConfig::from_cli(&args).is_err());
    }

    #[test]
    fn from_cli_bad_protocol_errors() {
        let args = args_with(Some("1"), Some("a:1"), "icmp");
        assert!(ProxyConfig::from_cli(&args).is_err());
    }

    // ── TOML loading ───────────────────────────────────────────────────────

    fn load_str(s: &str) -> Result<Vec<ProxyConfig>> {
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(f.path(), s).unwrap();
        load(f.path()).map(|c| c.proxies)
    }

    #[test]
    fn load_metrics_listen_present() {
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            f.path(),
            r#"
            metrics_listen = "127.0.0.1:9090"
            [[proxy]]
            listen = "127.0.0.1:1"
            target = "a:1"
            "#,
        )
        .unwrap();
        let loaded = load(f.path()).unwrap();
        assert_eq!(loaded.metrics_listen.as_deref(), Some("127.0.0.1:9090"));
        assert_eq!(loaded.proxies.len(), 1);
    }

    #[test]
    fn load_metrics_listen_absent() {
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            f.path(),
            r#"
            [[proxy]]
            listen = "127.0.0.1:1"
            target = "a:1"
            "#,
        )
        .unwrap();
        let loaded = load(f.path()).unwrap();
        assert_eq!(loaded.metrics_listen, None);
    }

    #[test]
    fn unknown_top_level_field_rejected() {
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            f.path(),
            r#"
            bogus_field = "oops"
            [[proxy]]
            listen = "127.0.0.1:1"
            target = "a:1"
            "#,
        )
        .unwrap();
        let err = format!("{:#}", load(f.path()).unwrap_err());
        assert!(err.contains("bogus_field"), "got: {err}");
    }

    #[test]
    fn metrics_listen_inside_defaults_rejected() {
        // Regression: metrics_listen accidentally placed inside [defaults]
        // used to be silently ignored. Must now error.
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            f.path(),
            r#"
            [defaults]
            keepalive_idle = 60
            metrics_listen = "127.0.0.1:9090"

            [[proxy]]
            listen = "127.0.0.1:1"
            target = "a:1"
            "#,
        )
        .unwrap();
        let err = format!("{:#}", load(f.path()).unwrap_err());
        assert!(err.contains("metrics_listen"), "got: {err}");
    }

    #[test]
    fn unknown_proxy_field_rejected() {
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            f.path(),
            r#"
            [[proxy]]
            listen = "127.0.0.1:1"
            target = "a:1"
            typo_here = 42
            "#,
        )
        .unwrap();
        let err = format!("{:#}", load(f.path()).unwrap_err());
        assert!(err.contains("typo_here"), "got: {err}");
    }

    #[test]
    fn load_minimal_tcp() {
        let cfgs = load_str(
            r#"
            [[proxy]]
            listen = "127.0.0.1:8080"
            target = "127.0.0.1:80"
            "#,
        )
        .unwrap();
        assert_eq!(cfgs.len(), 1);
        assert_eq!(cfgs[0].listen, "127.0.0.1:8080");
        assert_eq!(cfgs[0].target, "127.0.0.1:80");
        assert_eq!(cfgs[0].protocol, Protocol::Tcp);
        assert_eq!(cfgs[0].connect_timeout_secs, defaults::CONNECT_TIMEOUT_SECS);
        assert_eq!(cfgs[0].keepalive_idle_secs, defaults::KEEPALIVE_IDLE_SECS);
        assert_eq!(cfgs[0].user_timeout_ms, defaults::USER_TIMEOUT_MS);
        assert_eq!(cfgs[0].idle_timeout_secs, defaults::IDLE_TIMEOUT_SECS);
        assert_eq!(
            cfgs[0].half_close_timeout_secs,
            defaults::HALF_CLOSE_TIMEOUT_SECS
        );
    }

    #[test]
    fn load_multiple_proxies() {
        let cfgs = load_str(
            r#"
            [[proxy]]
            listen = "127.0.0.1:1"
            target = "a:1"

            [[proxy]]
            listen = "127.0.0.1:2"
            target = "b:2"
            protocol = "udp"
            "#,
        )
        .unwrap();
        assert_eq!(cfgs.len(), 2);
        assert_eq!(cfgs[0].protocol, Protocol::Tcp);
        assert_eq!(cfgs[1].protocol, Protocol::Udp);
    }

    #[test]
    fn load_with_defaults_section() {
        let cfgs = load_str(
            r#"
            [defaults]
            connect_timeout    = 7
            keepalive_idle     = 120
            idle_timeout       = 0
            half_close_timeout = 0

            [[proxy]]
            listen = "127.0.0.1:1"
            target = "a:1"
            "#,
        )
        .unwrap();
        assert_eq!(cfgs[0].connect_timeout_secs, 7);
        assert_eq!(cfgs[0].keepalive_idle_secs, 120);
        assert_eq!(cfgs[0].idle_timeout_secs, 0);
        assert_eq!(cfgs[0].half_close_timeout_secs, 0);
        // Untouched knobs fall back to consts
        assert_eq!(
            cfgs[0].keepalive_interval_secs,
            defaults::KEEPALIVE_INTERVAL_SECS
        );
    }

    #[test]
    fn load_per_proxy_override_beats_defaults() {
        let cfgs = load_str(
            r#"
            [defaults]
            connect_timeout = 5

            [[proxy]]
            listen          = "127.0.0.1:1"
            target          = "a:1"
            connect_timeout = 99
            "#,
        )
        .unwrap();
        assert_eq!(cfgs[0].connect_timeout_secs, 99);
    }

    #[test]
    fn proxy_protocol_defaults_false() {
        let cfgs = load_str(
            r#"
            [[proxy]]
            listen = "127.0.0.1:1"
            target = "a:1"
            "#,
        )
        .unwrap();
        assert!(!cfgs[0].proxy_protocol);
    }

    #[test]
    fn proxy_protocol_per_proxy_true() {
        let cfgs = load_str(
            r#"
            [[proxy]]
            listen         = "127.0.0.1:1"
            target         = "a:1"
            proxy_protocol = true
            "#,
        )
        .unwrap();
        assert!(cfgs[0].proxy_protocol);
    }

    #[test]
    fn proxy_protocol_from_defaults_section() {
        let cfgs = load_str(
            r#"
            [defaults]
            proxy_protocol = true

            [[proxy]]
            listen = "127.0.0.1:1"
            target = "a:1"
            "#,
        )
        .unwrap();
        assert!(cfgs[0].proxy_protocol);
    }

    #[test]
    fn proxy_protocol_per_proxy_false_beats_defaults_true() {
        let cfgs = load_str(
            r#"
            [defaults]
            proxy_protocol = true

            [[proxy]]
            listen         = "127.0.0.1:1"
            target         = "a:1"
            proxy_protocol = false
            "#,
        )
        .unwrap();
        assert!(!cfgs[0].proxy_protocol);
    }

    #[test]
    fn load_empty_proxies_errors() {
        assert!(load_str(r#"[defaults]"#).is_err());
        assert!(load_str("").is_err());
    }

    #[test]
    fn load_missing_required_field_errors() {
        assert!(load_str(
            r#"
            [[proxy]]
            listen = "127.0.0.1:1"
            "#
        )
        .is_err());
        assert!(load_str(
            r#"
            [[proxy]]
            target = "a:1"
            "#
        )
        .is_err());
    }

    #[test]
    fn load_unknown_protocol_errors() {
        assert!(load_str(
            r#"
            [[proxy]]
            listen   = "127.0.0.1:1"
            target   = "a:1"
            protocol = "icmp"
            "#
        )
        .is_err());
    }

    #[test]
    fn load_bare_port_expansion() {
        let cfgs = load_str(
            r#"
            [[proxy]]
            listen = "8080"
            target = "a:1"
            "#,
        )
        .unwrap();
        assert_eq!(cfgs[0].listen, "0.0.0.0:8080");
    }

    #[test]
    fn load_explicit_name_preserved() {
        let cfgs = load_str(
            r#"
            [[proxy]]
            name   = "my-proxy"
            listen = "127.0.0.1:1"
            target = "a:1"
            "#,
        )
        .unwrap();
        assert_eq!(cfgs[0].name, "my-proxy");
    }

    #[test]
    fn load_auto_name_when_unset() {
        let cfgs = load_str(
            r#"
            [[proxy]]
            listen = "127.0.0.1:1"
            target = "a:1"
            "#,
        )
        .unwrap();
        assert!(cfgs[0].name.contains("127.0.0.1:1"));
        assert!(cfgs[0].name.contains("a:1"));
    }

    #[test]
    fn load_garbage_toml_errors() {
        assert!(load_str("this is not toml @#$%").is_err());
    }

    #[test]
    fn load_nonexistent_file_errors() {
        let path = std::path::Path::new("/nonexistent/oxiduct/test/file.toml");
        assert!(load(path).is_err());
    }
}
