use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

use crate::cli::{self, Args};

/// Resolved, ready-to-use proxy configuration.
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

#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    #[default]
    Tcp,
    Udp,
}

// ── TOML file shape ────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
struct TomlDefaults {
    connect_timeout: Option<u64>,
    keepalive_idle: Option<u64>,
    keepalive_interval: Option<u64>,
    keepalive_retries: Option<u32>,
    user_timeout_ms: Option<u32>,
    idle_timeout: Option<u64>,
    half_close_timeout: Option<u64>,
}

#[derive(Deserialize)]
struct TomlProxy {
    /// Human-readable label shown in log output
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

#[derive(Deserialize)]
struct TomlFile {
    #[serde(default)]
    defaults: TomlDefaults,
    #[serde(rename = "proxy")]
    proxies: Vec<TomlProxy>,
}

// ── Constructors ───────────────────────────────────────────────────────────

impl ProxyConfig {
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
        let protocol = parse_protocol(&args.protocol)?;
        Ok(Self {
            name: format!("{listen} -> {target}"),
            listen,
            target,
            protocol,
            connect_timeout_secs: args.connect_timeout,
            keepalive_idle_secs: args.keepalive_idle,
            keepalive_interval_secs: args.keepalive_interval,
            keepalive_retries: args.keepalive_retries,
            user_timeout_ms: args.user_timeout_ms,
            idle_timeout_secs: args.idle_timeout,
            half_close_timeout_secs: args.half_close_timeout,
        })
    }
}

pub fn load(path: &Path) -> Result<Vec<ProxyConfig>> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let file: TomlFile =
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;

    let d = &file.defaults;
    let proxies = file
        .proxies
        .into_iter()
        .enumerate()
        .map(|(i, p)| {
            let listen = cli::expand_listen(&p.listen);
            let name = p
                .name
                .unwrap_or_else(|| format!("proxy-{i}: {listen} -> {}", p.target));
            Ok(ProxyConfig {
                name,
                listen,
                target: p.target,
                protocol: p.protocol,
                connect_timeout_secs: p.connect_timeout.or(d.connect_timeout).unwrap_or(3),
                keepalive_idle_secs: p.keepalive_idle.or(d.keepalive_idle).unwrap_or(60),
                keepalive_interval_secs: p
                    .keepalive_interval
                    .or(d.keepalive_interval)
                    .unwrap_or(10),
                keepalive_retries: p.keepalive_retries.or(d.keepalive_retries).unwrap_or(6),
                user_timeout_ms: p.user_timeout_ms.or(d.user_timeout_ms).unwrap_or(90_000),
                idle_timeout_secs: p.idle_timeout.or(d.idle_timeout).unwrap_or(300),
                half_close_timeout_secs: p
                    .half_close_timeout
                    .or(d.half_close_timeout)
                    .unwrap_or(30),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if proxies.is_empty() {
        anyhow::bail!("config file defines no [[proxy]] entries");
    }
    Ok(proxies)
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
            connect_timeout: 3,
            keepalive_idle: 60,
            keepalive_interval: 10,
            keepalive_retries: 6,
            user_timeout_ms: 90_000,
            idle_timeout: 300,
            half_close_timeout: 30,
            shutdown_grace: 10,
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
        assert_eq!(cfg.connect_timeout_secs, 3);
        assert_eq!(cfg.keepalive_idle_secs, 60);
        assert_eq!(cfg.user_timeout_ms, 90_000);
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
        // Defaults applied
        assert_eq!(cfgs[0].connect_timeout_secs, 3);
        assert_eq!(cfgs[0].keepalive_idle_secs, 60);
        assert_eq!(cfgs[0].user_timeout_ms, 90_000);
        assert_eq!(cfgs[0].idle_timeout_secs, 300);
        assert_eq!(cfgs[0].half_close_timeout_secs, 30);
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
        // Untouched defaults stay at built-in
        assert_eq!(cfgs[0].keepalive_interval_secs, 10);
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
