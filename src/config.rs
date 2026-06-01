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
}

// ── TOML shapes ──────────────────────────────────────────────────────────────

/// Optional tuning knobs. Used for the `[defaults]` table and, field-for-field,
/// inside each `[[proxy]]`. `None` means "inherit from defaults, then const".
///
/// `#[serde(flatten)]` is intentionally NOT used here: it is unreliable for
/// typed/integer fields with the `toml` crate. Each proxy lists the knobs
/// explicitly and converts into this struct via [`TomlProxy::tuning`].
#[derive(Debug, Clone, Copy, Default, Deserialize)]
struct TomlTuning {
    connect_timeout: Option<u64>,
    keepalive_idle: Option<u64>,
    keepalive_interval: Option<u64>,
    keepalive_retries: Option<u32>,
    user_timeout_ms: Option<u32>,
    idle_timeout: Option<u64>,
    half_close_timeout: Option<u64>,
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
}

#[derive(Debug, Deserialize)]
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
        }
    }
}

#[derive(Debug, Deserialize)]
struct TomlFile {
    #[serde(default)]
    defaults: TomlTuning,
    #[serde(rename = "proxy")]
    proxies: Vec<TomlProxy>,
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
        }
    }
}

/// Load and resolve every `[[proxy]]` entry from a TOML config file.
pub fn load(path: &Path) -> Result<Vec<ProxyConfig>> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let file: TomlFile =
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;

    if file.proxies.is_empty() {
        anyhow::bail!("config file defines no [[proxy]] entries");
    }

    Ok(file
        .proxies
        .into_iter()
        .enumerate()
        .map(|(i, p)| ProxyConfig::from_toml(i, p, file.defaults))
        .collect())
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
            shutdown_grace: defaults::SHUTDOWN_GRACE_SECS,
            log_level: "info".into(),
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
        load(f.path())
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
