# oxiduct

[![CI](https://ci.woodpecker-ci.org/api/badges/8998/status.svg)](https://ci.woodpecker-ci.org/repos/8998)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/oxiduct.svg)](https://crates.io/crates/oxiduct)

> A TCP/UDP proxy that actually cleans up after itself.

`socat` is great — until a client crashes hard and the connection hangs open forever. `oxiduct` fixes that. It proxies TCP and UDP traffic while automatically detecting and closing dead connections, so you never have to restart your proxy to clean up ghost sessions.

---

## Install

```sh
cargo install oxiduct
```

Or grab a binary from the [releases page](https://github.com/6543/oxiduct/releases).

---

## Quick start

**Forward a port (TCP):**
```sh
oxiduct --listen 587 --target mail.example.com:587
```

**Forward UDP:**
```sh
oxiduct --listen 5353 --target 1.1.1.1:53 --protocol udp
```

**Multiple proxies via config file:**
```sh
oxiduct --config /etc/oxiduct/config.toml
```

That's it. Logs go to stdout. Press Ctrl-C or send `SIGTERM` to shut down gracefully.

---

## Why not just use socat?

`socat` works well for quick jobs, but has one painful failure mode: if a client dies hard (power loss, `kill -9`, network drop) the proxy connection stays open indefinitely — no error, no cleanup, just a ghost session eating a file descriptor.

`oxiduct` layers four liveness checks to catch these situations:

| Layer | What it does | Default |
|-------|-------------|---------|
| L1 | TCP keepalive probes at the kernel level | Every 10s, 6 retries |
| L2 | Force-close unacknowledged connections (`TCP_USER_TIMEOUT`, Linux) | After 90s |
| L3 | Close if no data flows in either direction | After 5 min |
| L4 | If one side closes, give the other side a deadline | 30s grace |

Any one of these is usually enough. Together they cover virtually every dead-connection scenario.

---

## Config file

For multiple proxies or persistent settings, use a TOML config:

```toml
# Defaults apply to all proxies (you can override per-proxy)
[defaults]
idle_timeout       = 300   # close if silent for 5 min (seconds, 0 = off)
half_close_timeout = 30    # grace period when one side closes (seconds)
max_connections    = 32000 # total connection cap (0 = unlimited)
max_per_ip         = 320   # per-source-IP cap (0 = unlimited)

[[proxy]]
name     = "smtp"
listen   = "0.0.0.0:587"
target   = "mail.example.com:587"
protocol = "tcp"

[[proxy]]
name     = "dns"
listen   = "5353"          # bare port expands to 0.0.0.0:5353
target   = "1.1.1.1:53"
protocol = "udp"

# Per-proxy override: SSH sessions can be idle for longer
[[proxy]]
name         = "git-ssh"
listen       = "0.0.0.0:2222"
target       = "github.com:22"
idle_timeout = 3600
```

See [`contrib/example.toml`](contrib/example.toml) for all available options with comments.

---

## All options

| Flag | Default | Description |
|------|---------|-------------|
| `--listen` | (required) | Address or port to listen on |
| `--target` | (required) | Upstream host:port to forward to |
| `--protocol` | `tcp` | `tcp` or `udp` |
| `--config` | | Load from TOML config instead of flags |
| `--connect-timeout` | 3s | How long to wait for upstream to connect |
| `--idle-timeout` | 300s | Close connection if silent this long (0 = off) |
| `--half-close-timeout` | 30s | Grace period after one side closes (0 = off) |
| `--max-connections` | 32000 | Max simultaneous connections (0 = unlimited) |
| `--max-per-ip` | 320 | Max connections per source IP (0 = unlimited) |
| `--shutdown-grace` | 10s | Time to finish active connections on SIGTERM |
| `--metrics-listen` | (off) | Expose Prometheus metrics at this address |
| `--log-level` | `info` | Log verbosity (`trace`/`debug`/`info`/`warn`/`error`) |
| `--keepalive-idle` | 60s | TCP keepalive idle time |
| `--keepalive-interval` | 10s | TCP keepalive probe interval |
| `--keepalive-retries` | 6 | TCP keepalive max probes |
| `--user-timeout-ms` | 90000 | `TCP_USER_TIMEOUT` in ms (Linux only, 0 = OS default) |

---

## Metrics (Prometheus)

Enable the metrics endpoint:

```sh
oxiduct --listen 587 --target mail.example.com:587 --metrics-listen 127.0.0.1:9090
curl http://127.0.0.1:9090/metrics
```

Or in the config file (top level):
```toml
metrics_listen = "127.0.0.1:9090"
```

Available metrics (all labelled by `proxy`):

| Metric | What it counts |
|--------|---------------|
| `oxiduct_connections_total` | Connections accepted (or UDP sessions opened) |
| `oxiduct_connections_rejected_total` | Connections refused by rate limits |
| `oxiduct_connect_failures_total` | Failed upstream connect attempts |
| `oxiduct_connect_timeouts_total` | Timed-out upstream connect attempts |
| `oxiduct_bytes_total` | Bytes relayed, split by `up`/`down` direction |
| `oxiduct_connections_closed_total` | Closed connections, split by reason |
| `oxiduct_active_connections` | Currently open connections |
| `oxiduct_max_connections` | Configured connection cap |
| `oxiduct_max_per_ip` | Configured per-IP cap |

Throughput in Prometheus: `rate(oxiduct_bytes_total[1m])`

Close reasons: `eof` / `reset` / `idle_timeout` / `half_close_timeout` / `shutdown`

---

## Testing locally

Verify the proxy works with just `nc` and `socat`:

```sh
# Terminal 1: start an echo server
socat TCP-LISTEN:19001,fork,reuseaddr EXEC:cat

# Terminal 2: start oxiduct in front of it
oxiduct --listen 127.0.0.1:19002 --target 127.0.0.1:19001 --log-level debug

# Terminal 3: send something through
echo "hello via oxiduct" | nc -q1 127.0.0.1 19002
```

The proxy logs the connection, byte counts, and why it closed.

---

## Why does this exist?

No existing tool did exactly this one job well.

HAProxy is great — but it's a load balancer, not a simple port forwarder. `socat` works as a quick hack but the dead-connection problem makes it unsuitable for long-running services. Everything else either does too much or doesn't handle connection cleanup at all.

`oxiduct` is not a passion project. It's a tool that was needed, so it was built. The focus is deliberately narrow:

- just a proxy — small, does one thing well
- metrics and rate limiting included (you're on the frontline)
- secure by default, no surprises

If you need a simple, reliable TCP/UDP forwarder you can run as a one-shot command or a system service and then forget about, this is it.

---

## License

MIT — see [LICENSE](LICENSE)
