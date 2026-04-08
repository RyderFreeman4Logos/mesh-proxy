# mesh-proxy

去中心化 P2P 端口转发与服务发现工具。跨机器共享本地服务，自动穿透 NAT。

## 为什么选择 mesh-proxy？

- **NAT 穿透开箱即用** — 基于 [iroh](https://iroh.computer/)（QUIC 协议），自动打洞穿透 NAT 和防火墙。无需端口映射、动态 DNS 或 VPN。
- **真正的 P2P 数据面** — 流量在节点之间直连传输，不经过中心服务器。控制节点只做协调，看不到你的数据。
- **控制节点宕机不影响数据面** — 边缘节点本地缓存路由表。控制节点离线后，已有连接和路由继续工作。
- **运行时服务管理** — 通过 `mesh-proxy expose` / `unexpose` 动态添加或移除服务，不需要重启守护进程。变更在几秒内传播到所有节点。
- **两步加入** — 管理员在控制节点生成邀请令牌，边缘节点执行一条 `mesh-proxy join` 命令即可加入，自动注册无需手动操作。也支持手动 `accept` 流程以获得更细粒度的控制。
- **单一二进制，零依赖** — 每个平台一个静态二进制文件。TOML 配置。JSON 状态文件。不需要数据库或容器运行时。

## 安装

### 通过 mise（从源码编译，`mise upgrade` 自动更新）

```bash
mise use -g cargo:https://github.com/RyderFreeman4Logos/mesh-proxy
```

### 通过 cargo-binstall（推荐 — 直接下载预编译二进制）

```bash
# 如果没有 cargo-binstall，先安装
mise use -g cargo-binstall

# 安装 mesh-proxy（从 GitHub Releases 下载预编译包）
cargo binstall mesh-proxy
```

### 从源码构建

```bash
cargo install --git https://github.com/RyderFreeman4Logos/mesh-proxy mesh-proxy
```

### 预编译二进制

从 [GitHub Releases](https://github.com/RyderFreeman4Logos/mesh-proxy/releases) 下载。支持平台：

| 平台 | 架构 |
|------|------|
| Linux | x86_64, aarch64 |
| macOS | x86_64 (Intel), aarch64 (Apple Silicon) |

## 快速上手

### 1. 启动控制节点

选择一台机器作为控制节点，负责服务注册和路由分发。

```bash
mesh-proxy start --role control
```

### 2. 生成邀请令牌

在控制节点为每个边缘节点生成邀请：

```bash
mesh-proxy invite --name my-laptop
# Invite token: mesh0-1a2b3c4d...
```

令牌中编码了控制节点地址和一次性密钥。通过任意渠道发送给边缘节点操作者。

### 3. 边缘节点加入

在边缘机器上，一条命令完成全部操作 — 解码邀请、写入配置、启动守护进程、自动注册到控制节点：

```bash
mesh-proxy join mesh0-1a2b3c4d...
# ✔ Config written to ~/.config/mesh-proxy/config.toml
# ✔ Daemon started (PID 12345)
# ✔ Registered as 'my-laptop' on control node
```

### 4. 暴露服务

在任意边缘节点上，将本地服务暴露到 mesh 网络：

```bash
mesh-proxy expose --name my-api --addr 127.0.0.1:8080
# Service 'my-api' exposed on port 40000
```

完成。mesh 网络中的所有节点现在都可以通过 `127.0.0.1:40000` 访问 `my-api`。

### 5. 验证

```bash
# 在任意节点上
mesh-proxy status
```

```
[ONLINE] my-laptop (edge)
  Peers: 1  Services: 2  Routes: 1
  [OK]     my-api (my-rpi) -> :40000
  [ROUTED] db-proxy (my-server) -> :40001
```

```bash
# 从另一台机器 — 通过 mesh 网络访问 my-rpi 的 8080 端口
curl http://127.0.0.1:40000/health
```

### 手动配置（高级）

如果需要更细粒度的控制，可以跳过 `invite`/`join`，使用手动流程：

```bash
# 在边缘机器上 — 指定控制节点地址启动守护进程
mesh-proxy start --role edge --control-addr <控制节点连接地址>

# 守护进程会输出 join ticket：
#   Join ticket saved. Run on control node: mesh-proxy accept 3sRKfx9v...

# 在控制节点上 — 接受边缘节点
mesh-proxy accept 3sRKfx9v... --name my-laptop
```

## 典型场景

### 在家远程访问开发机服务

你的开发机在公司内网，家里的笔记本在另一个网络。传统方案需要 VPN 或公网中转服务器。mesh-proxy 通过 iroh 的 relay 基础设施自动穿透双层 NAT：

```bash
# 任选一台机器做控制节点（可以是云服务器，也可以是你的某台设备）
# 开发机和笔记本都作为边缘节点加入
# 在开发机上 expose 你的服务，笔记本直接 curl 127.0.0.1:<port>
```

### 树莓派 / ARM 设备接入

mesh-proxy 原生支持 aarch64。在树莓派上运行边缘节点，将传感器 API、Home Assistant 等服务暴露给其他设备。

### 临时分享本地服务给同事

启动 mesh 网络 → 同事加入 → expose 服务 → 用完 unexpose。不需要修改任何网络配置。

## 命令参考

| 命令 | 说明 |
|------|------|
| `mesh-proxy start --role {control\|edge}` | 启动守护进程 |
| `mesh-proxy stop` | 停止守护进程 |
| `mesh-proxy restart` | 重启守护进程 |
| `mesh-proxy status [--output json]` | 查看状态、节点、服务 |
| `mesh-proxy expose --name 名称 --addr 地址` | 暴露本地服务到 mesh |
| `mesh-proxy unexpose --name 名称` | 移除已暴露的服务 |
| `mesh-proxy invite --name 名称 [--ttl 秒数]` | 生成邀请令牌（仅控制节点） |
| `mesh-proxy join TOKEN` | 使用邀请令牌加入 mesh（边缘节点） |
| `mesh-proxy accept TICKET [--name 名称]` | 手动接受边缘节点（仅控制节点） |
| `mesh-proxy install-service [--user]` | 安装 systemd/launchd 系统服务 |
| `mesh-proxy quota show` | 查看服务配额（仅控制节点） |
| `mesh-proxy quota set 节点ID 数量` | 设置节点配额（仅控制节点） |

## 配置

配置文件：`~/.config/mesh-proxy/config.toml`（首次运行自动创建）

```toml
node_name = "my-server"
role = "edge"                          # "control" 或 "edge"
control_addr = "ba446bc1343b..."       # 控制节点地址（仅边缘节点）
enable_local_proxy = false             # 在 localhost 暴露路由（仅控制节点）
health_bind = "127.0.0.1:49000"       # 可选的健康检查 HTTP 端点

[[services]]
name = "my-api"
local_addr = "127.0.0.1:8080"
protocol = "tcp"                       # "tcp" 或 "unix"

[services.health_check]
mode = "http_get"                      # "tcp_connect"、"unix_connect" 或 "http_get"
target = "http://127.0.0.1:8080/health"
interval_seconds = 10
```

通常不需要手动编辑配置文件 — CLI 会管理它。

## 注册为系统服务

生成并安装服务单元，使 mesh-proxy 在系统启动时自动运行：

```bash
mesh-proxy install-service         # 系统级（需要 root 权限）
mesh-proxy install-service --user  # 用户级（无需 root）
```

- **Linux**：生成 systemd unit 文件，输出 `systemctl enable` 命令。
- **macOS**：生成 launchd plist 文件，输出 `launchctl load` 命令。

## 架构

```
                    ┌─────────────────┐
                    │    控制节点      │
                    │   (协调者)       │
                    └──┬──────────┬───┘
            控制面     │          │     控制面
                ┌──────┘          └──────┐
                │                        │
          ┌─────┴─────┐           ┌──────┴────┐
          │ 边缘节点   │◄─────────►│ 边缘节点  │
          │ (my-api)  │   数据面   │ (db-proxy)│
          └───────────┘   (P2P)   └────────────┘
```

- **控制面**（`/mesh/ctrl/1` ALPN）：服务注册、端口分配、心跳。星型拓扑 — 所有边缘节点连接到控制节点。
- **数据面**（`/mesh/proxy/1` ALPN）：实际代理流量。网状拓扑 — 边缘节点之间直连。
- **端口池**：服务从 40000–48999 分配端口。每个边缘节点默认最多暴露 5 个服务（可通过 `quota set` 调整）。

## 许可证

[AGPL-3.0-or-later](LICENSE)
