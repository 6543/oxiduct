use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

use crate::cli::{self, Args};

#[allow(dead_code)]
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
