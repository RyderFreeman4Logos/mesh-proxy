# mesh-proxy

Decentralized P2P port forwarding and service discovery. Share local services across machines with zero-config NAT traversal.

## Why mesh-proxy?

- **NAT traversal just works** — Built on [iroh](https://iroh.computer/) (QUIC), automatically punches through NATs and firewalls. No port forwarding, no dynamic DNS, no VPN setup.
- **True P2P data plane** — Traffic flows directly between nodes, not through a central server. The control node only coordinates; it never sees your data.
- **Survives control node outage** — Edge nodes cache route tables locally. Existing connections and routes keep working even if the control node goes offline.
- **Runtime service management** — Add or remove services with `mesh-proxy expose` / `unexpose` without restarting any daemon. Changes propagate to all nodes within seconds.
- **Two-step onboarding** — Admin generates an invite token on the control node; the edge node joins with a single `mesh-proxy join` command. Auto-accepted, zero manual steps. A manual `accept` flow is also available for finer control.
- **Single binary, zero dependencies** — One static binary per platform. TOML config. JSON state files. No database, no container runtime.

## Install

### Via mise (auto-updates on `mise upgrade`)

```bash
mise use -g cargo:https://github.com/RyderFreeman4Logos/mesh-proxy
```

### Via cargo-binstall (recommended — downloads prebuilt binary)

```bash
# Install cargo-binstall if you don't have it
mise use -g cargo-binstall

# Install mesh-proxy (fetches prebuilt from GitHub Releases)
cargo binstall mesh-proxy
```

### From source

```bash
cargo install --git https://github.com/RyderFreeman4Logos/mesh-proxy mesh-proxy
```

### Prebuilt binaries

Download from [GitHub Releases](https://github.com/RyderFreeman4Logos/mesh-proxy/releases). Available for:

| Platform | Architecture |
|----------|-------------|
| Linux | x86_64, aarch64 |
| macOS | x86_64 (Intel), aarch64 (Apple Silicon) |

## Quick Start

### 1. Start the control node

Pick one machine to be the control node. This machine coordinates service registration and route distribution.

```bash
mesh-proxy start --role control
```

### 2. Generate an invite token

On the control node, generate an invite for each edge node:

```bash
mesh-proxy invite --name my-laptop
# Invite token: mesh0-1a2b3c4d...
```

The token encodes the control node's address and a one-time secret. Send it to the edge node operator via any channel.

### 3. Join from the edge node

On the edge machine, one command does everything — decodes the invite, writes config, starts the daemon, and auto-registers with the control node:

```bash
mesh-proxy join mesh0-1a2b3c4d...
# ✔ Config written to ~/.config/mesh-proxy/config.toml
# ✔ Daemon started (PID 12345)
# ✔ Registered as 'my-laptop' on control node
```

### 4. Expose a service

On any edge node, expose a local service to the mesh:

```bash
mesh-proxy expose --name my-api --addr 127.0.0.1:8080
# Service 'my-api' exposed on port 40000
```

That's it. Every other node in the mesh can now reach `my-api` at `127.0.0.1:40000`.

### 5. Verify

```bash
# On any node
mesh-proxy status
```

```
[ONLINE] my-laptop (edge)
  Peers: 1  Services: 2  Routes: 1
  [OK]     my-api (my-rpi) -> :40000
  [ROUTED] db-proxy (my-server) -> :40001
```

```bash
# From another machine — reaches my-rpi's port 8080 through the mesh
curl http://127.0.0.1:40000/health
```

### Manual Setup (advanced)

If you prefer explicit control over each step, you can skip `invite`/`join` and use the manual flow:

```bash
# On the edge machine — start daemon with explicit control address
mesh-proxy start --role edge --control-addr <CONTROL_CONNECT_ADDR>

# The daemon prints a join ticket:
#   Join ticket saved. Run on control node: mesh-proxy accept 3sRKfx9v...

# On the control node — accept the edge node
mesh-proxy accept 3sRKfx9v... --name my-laptop
```

## Commands

| Command | Description |
|---------|-------------|
| `mesh-proxy start --role {control\|edge}` | Start daemon |
| `mesh-proxy stop` | Stop daemon |
| `mesh-proxy restart` | Restart daemon |
| `mesh-proxy status [--output json]` | Show status, peers, services |
| `mesh-proxy expose --name NAME --addr ADDR` | Expose local service to mesh |
| `mesh-proxy unexpose --name NAME` | Remove exposed service |
| `mesh-proxy invite --name NAME [--ttl SECS]` | Generate invite token (control only) |
| `mesh-proxy join TOKEN` | Join mesh with invite token (edge) |
| `mesh-proxy accept TICKET [--name NAME]` | Accept edge node manually (control only) |
| `mesh-proxy install-service [--user]` | Install systemd/launchd service |
| `mesh-proxy quota show` | Show service quotas (control only) |
| `mesh-proxy quota set ENDPOINT_ID LIMIT` | Set node quota (control only) |

## Configuration

Config file: `~/.config/mesh-proxy/config.toml` (auto-created on first run)

```toml
node_name = "my-server"
role = "edge"                          # "control" or "edge"
control_addr = "ba446bc1343b..."       # control node address (edge only)
enable_local_proxy = false             # expose routes on localhost (control only)
health_bind = "127.0.0.1:49000"       # optional health HTTP endpoint

[[services]]
name = "my-api"
local_addr = "127.0.0.1:8080"
protocol = "tcp"                       # "tcp" or "unix"

[services.health_check]
mode = "http_get"                      # "tcp_connect", "unix_connect", or "http_get"
target = "http://127.0.0.1:8080/health"
interval_seconds = 10
```

You rarely need to edit this file by hand — the CLI manages it.

## Running as a System Service

Generate and install a service unit so mesh-proxy starts automatically on boot:

```bash
mesh-proxy install-service         # system-wide (requires root)
mesh-proxy install-service --user  # user-level (no root needed)
```

- **Linux**: generates a systemd unit file and prints the `systemctl enable` command to activate it.
- **macOS**: generates a launchd plist and prints the `launchctl load` command to activate it.

## Architecture

```
                    ┌─────────────────┐
                    │  Control Node   │
                    │  (coordinator)  │
                    └──┬──────────┬───┘
           control     │          │     control
           plane       │          │     plane
                ┌──────┘          └──────┐
                │                        │
          ┌─────┴─────┐           ┌──────┴────┐
          │ Edge Node │◄─────────►│ Edge Node  │
          │ (my-api)  │ data plane│ (db-proxy) │
          └───────────┘   (P2P)   └────────────┘
```

- **Control plane** (`/mesh/ctrl/1` ALPN): Service registration, port assignment, heartbeats. Star topology — all edges connect to control.
- **Data plane** (`/mesh/proxy/1` ALPN): Actual proxy traffic. Mesh topology — edges connect directly to each other.
- **Port pool**: Services are assigned ports from 40000–48999. Each edge node can expose up to 5 services by default (configurable via `quota set`).

## License

[AGPL-3.0-or-later](LICENSE)
