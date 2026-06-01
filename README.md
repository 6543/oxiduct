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

## Demo / local testing

You can verify oxiduct against a local echo server with just `socat` and `nc`.

### TCP — echo through proxy

```sh
# Terminal 1 — start an echo server on port 19001
socat TCP-LISTEN:19001,fork,reuseaddr EXEC:cat

# Terminal 2 — run oxiduct in front of it on port 19002
oxiduct --listen 127.0.0.1:19002 --target 127.0.0.1:19001 --log-level debug

# Terminal 3 — talk to the proxy
echo "hello via oxiduct" | nc -q1 127.0.0.1 19002
```

The proxy will log the connection, the byte counts, and the close reason.

### Verify the idle timeout fires

```sh
# Start a proxy with a 4-second idle timeout
oxiduct --listen 127.0.0.1:19002 --target 127.0.0.1:19001 --idle-timeout 4

# Open a connection that sends nothing (sleep keeps stdin open)
sleep 60 | nc 127.0.0.1 19002 &

# Within ~10s (4s idle + 5s watchdog tick) the proxy will log:
#   reason="idle_timeout"
```

### Verify the half-close timeout fires

Use a server that sends data and then stops responding (without closing):

```sh
# Server: send a greeting, then hold forever
socat TCP-LISTEN:19001,fork,reuseaddr EXEC:'sh -c "echo hello; sleep 3600"'

# Proxy with a 3s half-close grace
oxiduct --listen 127.0.0.1:19002 --target 127.0.0.1:19001 \
        --idle-timeout 0 --half-close-timeout 3

# Client: read the greeting, then close write side and watch the proxy
# close the connection after ~3s (instead of hanging forever).
exec 3<>/dev/tcp/127.0.0.1/19002
read line <&3 ; echo "got: $line"
exec 3>&-
read line <&3   # blocks briefly, then EOF
```

### UDP echo

```sh
# Terminal 1 — UDP echo with Python (one-shot)
python3 -c '
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("127.0.0.1", 19031))
while True:
    data, addr = s.recvfrom(65535)
    s.sendto(data, addr)
'

# Terminal 2
oxiduct --listen 127.0.0.1:19032 --target 127.0.0.1:19031 --protocol udp

# Terminal 3
echo "hello udp" | socat -T2 - UDP:127.0.0.1:19032
```

## Config-file example

`config.toml`:

```toml
# Defaults apply to every [[proxy]] unless overridden per-entry.
[defaults]
connect_timeout    = 3       # seconds
keepalive_idle     = 60      # seconds
keepalive_interval = 10      # seconds
keepalive_retries  = 6
user_timeout_ms    = 90000   # Linux/Android; 0 = OS default
idle_timeout       = 300     # seconds; 0 = disable
half_close_timeout = 30      # seconds; 0 = disable

[[proxy]]
name     = "smtp-submission"
listen   = "0.0.0.0:587"
target   = "mail.example.com:587"
protocol = "tcp"

[[proxy]]
name     = "dns-relay"
listen   = "5353"            # bare port → 0.0.0.0:5353
target   = "1.1.1.1:53"
protocol = "udp"

# Per-proxy override: long-lived git over SSH needs a bigger idle window
[[proxy]]
name               = "git-ssh"
listen             = "0.0.0.0:2222"
target             = "github.com:22"
protocol           = "tcp"
idle_timeout       = 3600
half_close_timeout = 60
```

Run it:

```sh
oxiduct --config config.toml
```

See [`contrib/example.toml`](contrib/example.toml) for the version that ships with the repo.

## NixOS

NixOS module is planned (`contrib/nixos/`).

## License

MIT — see [LICENSE](LICENSE)
