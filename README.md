# oxiduct

Pipe traffic through oxidized steel — a robust TCP/UDP proxy with dead-connection detection.

## Why not socat?

`socat` is great for quick jobs but has a known failure mode: when a client is killed
ungracefully (`kill -9`, suspend, network drop) the proxy connection hangs open indefinitely.
`oxiduct` layers multiple liveness mechanisms to prevent this:

| Layer | Mechanism | Default |
|-------|-----------|---------|
| L1 | `SO_KEEPALIVE` + `TCP_KEEPIDLE` / `TCP_KEEPINTVL` / `TCP_KEEPCNT` | 60s / 10s / 6 |
| L2 | `TCP_USER_TIMEOUT` — kernel force-closes unacked connections (Linux) | 90 000 ms |
| L3 | Application idle timeout — no bytes either direction → close | 300 s |
| L4 | Half-close grace — one side EOFs → deadline on the other | 30 s |

## Usage

### Single-proxy (replaces one socat invocation)

```sh
oxiduct --listen 0.0.0.0:587 --target mail.example.com:587
# or just the port number:
oxiduct --listen 587 --target mail.example.com:587
```

### Multi-proxy (config file)

```sh
oxiduct --config /etc/oxiduct/config.toml
```

See [`contrib/example.toml`](contrib/example.toml) for all options.

### UDP

```sh
oxiduct --listen 5353 --target 1.1.1.1:53 --protocol udp
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--connect-timeout` | 3 | Connect timeout (s) |
| `--keepalive-idle` | 60 | TCP keepalive idle time (s) |
| `--keepalive-interval` | 10 | TCP keepalive probe interval (s) |
| `--keepalive-retries` | 6 | TCP keepalive max probes |
| `--user-timeout-ms` | 90000 | `TCP_USER_TIMEOUT` (ms, Linux only) |
| `--idle-timeout` | 300 | App idle timeout (s, 0=off) |
| `--half-close-timeout` | 30 | Half-close grace (s, 0=off) |
| `--shutdown-grace` | 10 | SIGTERM grace period (s) |
| `--log-level` | info | Tracing level / `RUST_LOG` |

## NixOS

NixOS module is planned (`contrib/nixos/`).

## License

MIT — see [LICENSE](LICENSE)
