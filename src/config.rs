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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Default, clap::ValueEnum)]
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

// ── Tuning knobs (single source of truth) ────────────────────────────────────

/// Declares every tuning knob exactly once:
///
/// ```text
/// TOML/CLI key => ProxyConfig field : type = built-in default
/// ```
///
/// and generates all the plumbing around it: the `[defaults]` table shape
/// (`TomlTuning`), the `[[proxy]]` entry shape (`TomlProxy`), and the
/// CLI/TOML → [`ProxyConfig`] constructors. Adding a knob means adding one
/// line here, the concrete field on [`ProxyConfig`], and the clap arg on
/// [`Args`] — nothing else.
macro_rules! tuning_knobs {
    ($( $toml:ident => $cfg:ident : $ty:ty = $default:expr ),+ $(,)?) => {
        /// Optional tuning knobs for the `[defaults]` table.
        /// `None` means "inherit from built-in defaults".
        #[derive(Debug, Clone, Copy, Default, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TomlTuning {
            $( $toml: Option<$ty>, )+
        }

        /// One `[[proxy]]` entry. The knobs are listed explicitly rather than
        /// `#[serde(flatten)]`-ing `TomlTuning`: flatten silently disables
        /// `deny_unknown_fields` and is unreliable for typed/integer fields
        /// with the `toml` crate.
        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TomlProxy {
            name: Option<String>,
            listen: String,
            target: String,
            #[serde(default)]
            protocol: Protocol,
            $( $toml: Option<$ty>, )+
        }

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

                let cfg = Self {
                    name: format!("{listen} -> {target}"),
                    listen,
                    target,
                    protocol: args.protocol,
                    $( $cfg: args.$toml, )+
                };
                cfg.validate()?;
                Ok(cfg)
            }

            /// Resolve one TOML entry: per-proxy value → `[defaults]` → const.
            fn from_toml(index: usize, p: TomlProxy, base: TomlTuning) -> Self {
                let listen = cli::expand_listen(&p.listen);
                let name = p
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("proxy-{index}: {listen} -> {}", p.target));
                Self {
                    name,
                    listen,
                    protocol: p.protocol,
                    $( $cfg: p.$toml.or(base.$toml).unwrap_or($default), )+
                    target: p.target,
                }
            }
        }
    };
}

tuning_knobs! {
    connect_timeout    => connect_timeout_secs:    u64  = defaults::CONNECT_TIMEOUT_SECS,
    keepalive_idle     => keepalive_idle_secs:     u64  = defaults::KEEPALIVE_IDLE_SECS,
    keepalive_interval => keepalive_interval_secs: u64  = defaults::KEEPALIVE_INTERVAL_SECS,
    keepalive_retries  => keepalive_retries:       u32  = defaults::KEEPALIVE_RETRIES,
    user_timeout_ms    => user_timeout_ms:         u32  = defaults::USER_TIMEOUT_MS,
    idle_timeout       => idle_timeout_secs:       u64  = defaults::IDLE_TIMEOUT_SECS,
    half_close_timeout => half_close_timeout_secs: u64  = defaults::HALF_CLOSE_TIMEOUT_SECS,
    max_connections    => max_connections:         u32  = defaults::MAX_CONNECTIONS,
    max_per_ip         => max_per_ip:              u32  = defaults::MAX_PER_IP,
    proxy_protocol     => proxy_protocol:          bool = false,
}

impl ProxyConfig {
    /// Reject configurations that cannot work, with an error naming the
    /// proxy, instead of failing later at bind time or per-connection.
    pub fn validate(&self) -> Result<()> {
        check_host_port(&self.listen)
            .with_context(|| format!("proxy \"{}\": invalid listen address", self.name))?;
        let target_port = check_host_port(&self.target)
            .with_context(|| format!("proxy \"{}\": invalid target address", self.name))?;
        if target_port == 0 {
            anyhow::bail!("proxy \"{}\": target port must not be 0", self.name);
        }
        // 0 would mean "time out instantly", never "disabled" — every
        // connection would fail. Catch the footgun at startup.
        if self.connect_timeout_secs == 0 {
            anyhow::bail!(
                "proxy \"{}\": connect_timeout must be at least 1 second",
                self.name
            );
        }
        Ok(())
    }
}

/// Require `host:port` shape with a non-empty host and a valid port.
/// Returns the port so callers can add their own constraints.
fn check_host_port(s: &str) -> Result<u16> {
    let (host, port) = s
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("\"{s}\" is missing a :port"))?;
    if host.is_empty() {
        anyhow::bail!("\"{s}\" is missing a host");
    }
    port.parse::<u16>()
        .map_err(|_| anyhow::anyhow!("\"{s}\" has an invalid port \"{port}\""))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlFile {
    #[serde(default)]
    defaults: TomlTuning,
    /// Optional global Prometheus exporter address.
    metrics_listen: Option<String>,
    /// Optional grace period on SIGTERM/SIGINT (seconds).
    shutdown_grace: Option<u64>,
    #[serde(rename = "proxy")]
    proxies: Vec<TomlProxy>,
}

/// Result of loading a config file: the proxies plus global settings.
#[derive(Debug)]
pub struct LoadedConfig {
    pub proxies: Vec<ProxyConfig>,
    pub metrics_listen: Option<String>,
    pub shutdown_grace: Option<u64>,
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
    let shutdown_grace = file.shutdown_grace;
    let proxies: Vec<ProxyConfig> = file
        .proxies
        .into_iter()
        .enumerate()
        .map(|(i, p)| ProxyConfig::from_toml(i, p, file.defaults))
        .collect();

    // Per-proxy sanity, then cross-proxy uniqueness: duplicate names would
    // silently merge metrics series and make logs ambiguous; duplicate listen
    // addresses would only fail later at bind time with a worse message.
    let mut names = std::collections::HashSet::new();
    let mut listens = std::collections::HashSet::new();
    for p in &proxies {
        p.validate()?;
        if !names.insert(p.name.as_str()) {
            anyhow::bail!("duplicate proxy name \"{}\"", p.name);
        }
        if !listens.insert((p.protocol, p.listen.as_str())) {
            anyhow::bail!("duplicate listen address \"{}\"", p.listen);
        }
    }

    Ok(LoadedConfig {
        proxies,
        metrics_listen,
        shutdown_grace,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args_with(listen: Option<&str>, target: Option<&str>, protocol: Protocol) -> Args {
        Args {
            config: None,
            listen: listen.map(String::from),
            target: target.map(String::from),
            protocol,
            connect_timeout: defaults::CONNECT_TIMEOUT_SECS,
            keepalive_idle: defaults::KEEPALIVE_IDLE_SECS,
            keepalive_interval: defaults::KEEPALIVE_INTERVAL_SECS,
            keepalive_retries: defaults::KEEPALIVE_RETRIES,
            user_timeout_ms: defaults::USER_TIMEOUT_MS,
            idle_timeout: defaults::IDLE_TIMEOUT_SECS,
            half_close_timeout: defaults::HALF_CLOSE_TIMEOUT_SECS,
            max_connections: defaults::MAX_CONNECTIONS,
            max_per_ip: defaults::MAX_PER_IP,
            shutdown_grace: None,
            metrics_listen: None,
            log_level: "info".into(),
            proxy_protocol: false,
        }
    }

    // ── ProxyConfig::from_cli ──────────────────────────────────────────────

    #[test]
    fn from_cli_full() {
        let args = args_with(Some("587"), Some("mail.example.com:587"), Protocol::Tcp);
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
        let args = args_with(Some("5353"), Some("1.1.1.1:53"), Protocol::Udp);
        let cfg = ProxyConfig::from_cli(&args).unwrap();
        assert_eq!(cfg.protocol, Protocol::Udp);
    }

    #[test]
    fn from_cli_missing_listen_errors() {
        let args = args_with(None, Some("a:1"), Protocol::Tcp);
        assert!(ProxyConfig::from_cli(&args).is_err());
    }

    #[test]
    fn from_cli_missing_target_errors() {
        let args = args_with(Some("1"), None, Protocol::Tcp);
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

    // ── Validation ─────────────────────────────────────────────────────────

    #[test]
    fn load_shutdown_grace_top_level() {
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            f.path(),
            r#"
            shutdown_grace = 42

            [[proxy]]
            listen = "127.0.0.1:1"
            target = "a:1"
            "#,
        )
        .unwrap();
        let loaded = load(f.path()).unwrap();
        assert_eq!(loaded.shutdown_grace, Some(42));
    }

    #[test]
    fn load_shutdown_grace_absent_is_none() {
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
        assert_eq!(load(f.path()).unwrap().shutdown_grace, None);
    }

    #[test]
    fn validate_rejects_zero_connect_timeout() {
        let err = load_str(
            r#"
            [[proxy]]
            listen          = "127.0.0.1:1"
            target          = "a:1"
            connect_timeout = 0
            "#,
        )
        .unwrap_err();
        assert!(
            format!("{err:#}").contains("connect_timeout"),
            "got: {err:#}"
        );
    }

    #[test]
    fn validate_rejects_target_without_port() {
        assert!(load_str(
            r#"
            [[proxy]]
            listen = "127.0.0.1:1"
            target = "example.com"
            "#
        )
        .is_err());
    }

    #[test]
    fn validate_rejects_target_port_zero() {
        assert!(load_str(
            r#"
            [[proxy]]
            listen = "127.0.0.1:1"
            target = "example.com:0"
            "#
        )
        .is_err());
    }

    #[test]
    fn validate_rejects_bad_listen() {
        // Neither a bare port nor host:port.
        assert!(load_str(
            r#"
            [[proxy]]
            listen = "foobar"
            target = "a:1"
            "#
        )
        .is_err());
    }

    #[test]
    fn validate_allows_listen_port_zero() {
        // Port 0 on listen = "pick a free port"; useful and legal.
        assert!(load_str(
            r#"
            [[proxy]]
            listen = "127.0.0.1:0"
            target = "a:1"
            "#
        )
        .is_ok());
    }

    #[test]
    fn validate_rejects_duplicate_names() {
        let err = load_str(
            r#"
            [[proxy]]
            name   = "same"
            listen = "127.0.0.1:1"
            target = "a:1"

            [[proxy]]
            name   = "same"
            listen = "127.0.0.1:2"
            target = "b:2"
            "#,
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("duplicate proxy name"));
    }

    #[test]
    fn validate_rejects_duplicate_listen() {
        let err = load_str(
            r#"
            [[proxy]]
            listen = "127.0.0.1:9"
            target = "a:1"

            [[proxy]]
            listen = "127.0.0.1:9"
            target = "b:2"
            "#,
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("duplicate listen address"));
    }

    #[test]
    fn validate_allows_tcp_and_udp_on_same_listen_address() {
        let cfgs = load_str(
            r#"
            [[proxy]]
            listen   = "127.0.0.1:53"
            target   = "a:53"
            protocol = "tcp"

            [[proxy]]
            listen   = "127.0.0.1:53"
            target   = "b:53"
            protocol = "udp"
            "#,
        )
        .expect("TCP and UDP may bind the same address");

        assert_eq!(cfgs.len(), 2);
    }

    #[test]
    fn from_cli_zero_connect_timeout_errors() {
        let mut args = args_with(Some("1"), Some("a:1"), Protocol::Tcp);
        args.connect_timeout = 0;
        assert!(ProxyConfig::from_cli(&args).is_err());
    }

    #[test]
    fn check_host_port_shapes() {
        assert!(check_host_port("example.com:25").is_ok());
        assert!(check_host_port("127.0.0.1:8080").is_ok());
        assert!(check_host_port("[::1]:80").is_ok());
        assert!(check_host_port("no-port").is_err());
        assert!(check_host_port(":80").is_err());
        assert!(check_host_port("host:").is_err());
        assert!(check_host_port("host:99999").is_err());
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

    /// Guard against contrib/example.toml drifting from the real schema.
    #[test]
    fn contrib_example_parses() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("contrib/example.toml");
        let loaded = load(&path).unwrap();
        assert_eq!(loaded.proxies.len(), 4);
        assert_eq!(loaded.metrics_listen.as_deref(), Some("127.0.0.1:9090"));
        assert!(loaded.proxies.iter().any(|p| p.proxy_protocol));
    }
}
