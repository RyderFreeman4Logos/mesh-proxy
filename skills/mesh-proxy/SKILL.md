---
name: mesh-proxy
description: Deploy and operate mesh-proxy — a decentralized P2P port forwarding tool. Use when the user needs to set up a mesh network, onboard nodes, expose services, or troubleshoot connectivity.
---

# mesh-proxy Operations

Decentralized P2P port forwarding and service discovery built on iroh (QUIC). Control plane (star) coordinates; data plane (mesh) carries traffic directly between nodes.

## Install

```bash
# Preferred (builds from source) — auto-updates on `mise upgrade`
mise use -g cargo:https://github.com/RyderFreeman4Logos/mesh-proxy

# Alternative — prebuilt binary
cargo binstall mesh-proxy

# From source
cargo install --git https://github.com/RyderFreeman4Logos/mesh-proxy mesh-proxy
```

## Deploy a mesh (invite flow)

### 1. Control node

```bash
mesh-proxy start --role control
```

### 2. Generate invite for each edge node

```bash
mesh-proxy invite --name <node-name>          # default TTL: 5 min
mesh-proxy invite --name <node-name> --ttl 3600  # max: 1 hour
```

Prints a one-time token (bs58-encoded, ~100 chars). Send it to the edge operator.

### 3. Edge node joins

```bash
mesh-proxy join <TOKEN>
# Writes config, starts daemon, auto-registers — one command
```

### 4. Expose a service

```bash
mesh-proxy expose --name my-api --addr 127.0.0.1:8080
# Assigns a port from 40000-48999, propagates to all nodes
```

Every other node can now reach `my-api` at `127.0.0.1:<assigned-port>`.

## Manual onboarding (no invite)

```bash
# Edge — start daemon, prints join ticket
mesh-proxy start --role edge --control-addr <CONTROL_ADDR>

# Control — accept the edge node
mesh-proxy accept <TICKET> --name <node-name>
```

## Day-to-day commands

| Command | Description |
|---------|-------------|
| `mesh-proxy status` | Show node state, peers, services, routes |
| `mesh-proxy status --output json` | Machine-readable status |
| `mesh-proxy expose --name N --addr A` | Expose local service |
| `mesh-proxy unexpose --name N` | Remove exposed service |
| `mesh-proxy stop` | Stop daemon |
| `mesh-proxy restart` | Restart daemon |
| `mesh-proxy quota show` | Show service quotas (control) |
| `mesh-proxy quota set <ID> <LIMIT>` | Set per-node quota (control) |

## System service

```bash
mesh-proxy install-service         # system-wide (root)
mesh-proxy install-service --user  # user-level (no root)
```

- Linux: generates systemd unit, prints `systemctl enable` command
- macOS: generates launchd plist, prints `launchctl load` command

## Configuration

Config: `~/.config/mesh-proxy/config.toml` (auto-created, rarely needs manual editing)

```toml
node_name = "my-server"
role = "edge"                          # "control" or "edge"
control_addr = "ba446bc1..."           # control node address (edge only)

[[services]]
name = "my-api"
local_addr = "127.0.0.1:8080"
protocol = "tcp"

[services.health_check]
mode = "http_get"
target = "http://127.0.0.1:8080/health"
interval_seconds = 10
```

State files: `~/.local/share/mesh-proxy/` (route cache, PID, daemon socket, invite nonces)

## Architecture notes

- **NAT traversal**: iroh/QUIC handles hole-punching automatically
- **Resilience**: edge nodes cache routes locally; data plane survives control outage
- **Invite tokens**: encode control address + 128-bit nonce + TTL. Single-use, bs58-encoded
- **Port pool**: 40000-48999, default quota 5 services per edge node
- **ALPNs**: `/mesh/ctrl/1` (control plane), `/mesh/proxy/1` (data plane)

## Troubleshooting

- **Edge can't reach control**: check firewall, verify `control_addr` in config matches `mesh-proxy status` on the control node
- **Invite expired**: tokens default to 5 min TTL; generate a new one with `mesh-proxy invite`
- **Port conflict**: the pool uses 40000-48999 which overlaps the Linux ephemeral range; if conflicts occur, adjust in config or narrow your system's ephemeral range
- **Service shows UNKNOWN**: route propagation takes a few seconds; run `mesh-proxy status` again after a moment
