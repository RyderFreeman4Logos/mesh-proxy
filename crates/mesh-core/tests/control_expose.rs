//! Integration tests for control-node service exposure.
//!
//! Control nodes own the route table directly, so `mesh-proxy expose` on a
//! control daemon writes the service into `config.toml` and the daemon
//! registers it against the local route table (no control-plane RPC).

use std::time::Duration;

use anyhow::{Context, Result};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr};
use mesh_core::Daemon;
use mesh_proto::{
    ALPN_PROXY, IpcRequest, IpcResponse, MeshConfig, NodeRole, Protocol, ProxyHandshake,
    SERVICE_PORT_END, SERVICE_PORT_START, StatusInfo, frame,
};
use tempfile::Builder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixStream};
use tokio::time::{sleep, timeout};

const TEST_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

fn writable_tempdir(prefix: &str) -> tempfile::TempDir {
    Builder::new()
        .prefix(prefix)
        .tempdir_in("/tmp")
        .expect("tempdir should be created in /tmp")
}

fn make_control_config(data_dir: std::path::PathBuf) -> MeshConfig {
    MeshConfig {
        node_name: "control-expose-test".to_string(),
        role: NodeRole::Control,
        data_dir,
        // enable_local_proxy left at default (None) → enabled on control.
        ..MeshConfig::default()
    }
}

async fn send_ipc_request(
    socket_path: &std::path::Path,
    request: &IpcRequest,
) -> Result<IpcResponse> {
    let mut stream = UnixStream::connect(socket_path).await.with_context(|| {
        format!(
            "failed to connect to IPC server at {}",
            socket_path.display()
        )
    })?;
    frame::write_json(&mut stream, request).await?;
    let response = frame::read_json(&mut stream).await?;
    Ok(response)
}

async fn wait_for_ipc_ready(socket_path: &std::path::Path) -> Result<()> {
    timeout(Duration::from_secs(8), async {
        loop {
            if socket_path.exists()
                && let Ok(IpcResponse::Status(status)) =
                    send_ipc_request(socket_path, &IpcRequest::Status).await
                && status.endpoint_addr.is_some()
            {
                return Ok::<(), anyhow::Error>(());
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .context("timed out waiting for IPC server to become ready")?
}

async fn assert_local_proxy_listener_opens(port: u16) -> Result<()> {
    timeout(Duration::from_secs(2), async {
        loop {
            match TcpStream::connect(("127.0.0.1", port)).await {
                Ok(mut stream) => {
                    // Local proxy listener returns 0 bytes when no peer is
                    // routed; the test target service was never started, so
                    // the proxy closes the connection promptly.
                    let mut buf = [0_u8; 1];
                    let _ = timeout(Duration::from_millis(500), stream.read(&mut buf)).await;
                    return Ok::<(), anyhow::Error>(());
                }
                Err(_) => sleep(Duration::from_millis(25)).await,
            }
        }
    })
    .await
    .context("local proxy listener did not open on assigned port")?
}

async fn assert_local_proxy_listener_stops(port: u16) -> Result<()> {
    timeout(Duration::from_secs(2), async {
        loop {
            match std::net::TcpListener::bind(("127.0.0.1", port)) {
                Ok(listener) => {
                    drop(listener);
                    return Ok::<(), anyhow::Error>(());
                }
                Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                    sleep(Duration::from_millis(25)).await;
                }
                Err(other) => {
                    anyhow::bail!("failed to probe listener shutdown on {port}: {other}");
                }
            }
        }
    })
    .await
    .context("local proxy listener did not stop after unexpose")?
}

struct ControlDaemonHarness {
    _tempdir: tempfile::TempDir,
    socket_path: std::path::PathBuf,
    shutdown_tx: mesh_core::ShutdownTx,
    handle: tokio::task::JoinHandle<Result<()>>,
}

impl ControlDaemonHarness {
    async fn start() -> Result<Self> {
        Self::start_with_config(make_control_config).await
    }

    async fn start_with_config(
        configure: impl FnOnce(std::path::PathBuf) -> MeshConfig,
    ) -> Result<Self> {
        let tempdir = writable_tempdir("mesh-control-expose-");
        let config_path = tempdir.path().join("config.toml");
        let config = configure(tempdir.path().join("data"));
        config.save(&config_path)?;

        let socket_path = config.data_dir.join("daemon.sock");
        let mut daemon = Daemon::new_without_startup_signal(config, config_path);
        let shutdown_tx = daemon.shutdown_tx().clone();
        let handle = tokio::spawn(async move { daemon.run().await });

        wait_for_ipc_ready(&socket_path).await?;

        Ok(Self {
            _tempdir: tempdir,
            socket_path,
            shutdown_tx,
            handle,
        })
    }

    async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(());
        match timeout(Duration::from_secs(5), self.handle).await {
            Ok(join_result) => join_result
                .context("control daemon panicked")?
                .context("control daemon returned error"),
            Err(_) => Ok(()),
        }
    }
}

async fn read_status(socket_path: &std::path::Path) -> Result<StatusInfo> {
    match send_ipc_request(socket_path, &IpcRequest::Status).await? {
        IpcResponse::Status(status) => Ok(status),
        other => anyhow::bail!("expected Status response, got {other:?}"),
    }
}

async fn read_control_endpoint_addr(socket_path: &std::path::Path) -> Result<EndpointAddr> {
    let status = read_status(socket_path).await?;
    let raw = status
        .endpoint_addr
        .context("daemon status has no endpoint address yet")?;
    serde_json::from_str(&raw).context("failed to deserialize control endpoint address")
}

/// Spawn a stub TCP echo server on an ephemeral port.
async fn spawn_echo_server() -> Result<(std::net::SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind echo server")?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            if stream.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }
    });
    Ok((addr, handle))
}

#[tokio::test]
async fn test_control_node_expose_registers_route() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let control = ControlDaemonHarness::start().await?;

        let test_result: Result<()> = async {
            // Bind a stub TCP service on an ephemeral port so the local
            // address is reachable; the route table only checks the
            // address string, but a real port avoids surprising the local
            // proxy when it dials the target.
            let stub_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            let stub_addr = stub_listener.local_addr()?;

            let expose_request = IpcRequest::ExposeService {
                name: "admin-api".to_string(),
                local_addr: stub_addr.to_string(),
                protocol: Protocol::Tcp,
                health_check: None,
            };
            let response = send_ipc_request(&control.socket_path, &expose_request).await?;

            let IpcResponse::ServiceExposed {
                name,
                assigned_port,
            } = response
            else {
                anyhow::bail!("expected ServiceExposed, got {response:?}");
            };
            assert_eq!(name, "admin-api");
            assert!(
                (SERVICE_PORT_START..=SERVICE_PORT_END).contains(&assigned_port),
                "assigned port {assigned_port} should fall inside the mesh service range"
            );

            // Status should reflect the new service in the control's route
            // table.
            let status = match send_ipc_request(&control.socket_path, &IpcRequest::Status).await? {
                IpcResponse::Status(status) => status,
                other => anyhow::bail!("expected Status, got {other:?}"),
            };
            assert!(
                status
                    .services
                    .iter()
                    .any(|service| service.name == "admin-api"
                        && service.assigned_port == Some(assigned_port)),
                "control status should advertise the new admin-api route"
            );

            // The control local proxy should open a listener on the
            // assigned port so the service is reachable from localhost.
            assert_local_proxy_listener_opens(assigned_port).await?;

            // Unexpose should remove the route and tear down the listener.
            let unexpose_request = IpcRequest::UnexposeService {
                name: "admin-api".to_string(),
            };
            let response = send_ipc_request(&control.socket_path, &unexpose_request).await?;
            assert!(matches!(response, IpcResponse::ServiceUnexposed { .. }));

            assert_local_proxy_listener_stops(assigned_port).await?;

            let post_status =
                match send_ipc_request(&control.socket_path, &IpcRequest::Status).await? {
                    IpcResponse::Status(status) => status,
                    other => anyhow::bail!("expected Status after unexpose, got {other:?}"),
                };
            assert!(
                !post_status
                    .services
                    .iter()
                    .any(|service| service.name == "admin-api"),
                "control status should not list admin-api after unexpose"
            );

            drop(stub_listener);
            Ok(())
        }
        .await;

        let shutdown_result = control.shutdown().await;
        shutdown_result?;
        test_result
    })
    .await
    .context("test_control_node_expose_registers_route timed out")?
}

/// End-to-end proxy reachability: an edge-style peer forwards traffic
/// through the mesh to a service hosted on the control node. Regression
/// coverage for the HIGH review finding where the control accept loop
/// rejected ALPN_PROXY, leaving control self-services unreachable from
/// peers.
///
/// The "edge" is modeled as a standalone iroh endpoint dialed with
/// ALPN_PROXY rather than a full edge daemon. That keeps the test focused
/// on the bug under repair (control accepting proxy ALPN for its own
/// services) and avoids cross-test port-allocation races on the
/// dynamically assigned listener port.
#[tokio::test]
async fn test_edge_reaches_control_self_service_via_mesh() -> Result<()> {
    timeout(Duration::from_secs(30), async {
        let (echo_addr, echo_handle) = spawn_echo_server().await?;

        // Disable the control's local proxy so the only path to the echo
        // service runs through the proxy ALPN we are exercising.
        let control = ControlDaemonHarness::start_with_config(|data_dir| MeshConfig {
            node_name: "control-expose-test".to_string(),
            role: NodeRole::Control,
            enable_local_proxy: Some(false),
            data_dir,
            ..MeshConfig::default()
        })
        .await?;

        let test_result: Result<()> = async {
            let expose = IpcRequest::ExposeService {
                name: "ctrl-echo".to_string(),
                local_addr: echo_addr.to_string(),
                protocol: Protocol::Tcp,
                health_check: None,
            };
            let IpcResponse::ServiceExposed {
                name,
                assigned_port,
            } = send_ipc_request(&control.socket_path, &expose).await?
            else {
                anyhow::bail!("expected ServiceExposed response");
            };
            assert_eq!(name, "ctrl-echo");
            assert!(
                (SERVICE_PORT_START..=SERVICE_PORT_END).contains(&assigned_port),
                "assigned port {assigned_port} should fall inside the mesh service range"
            );

            let control_endpoint_addr = read_control_endpoint_addr(&control.socket_path).await?;

            // Stand up a peer iroh endpoint and dial the control via the
            // proxy ALPN exactly as a real edge would.
            let peer = Endpoint::builder(presets::N0)
                .alpns(vec![ALPN_PROXY.to_vec()])
                .bind()
                .await
                .context("failed to bind test peer endpoint")?;

            let connection = peer
                .connect(control_endpoint_addr, ALPN_PROXY)
                .await
                .context("peer failed to dial control via ALPN_PROXY")?;
            let (mut send, mut recv) = connection
                .open_bi()
                .await
                .context("failed to open bi-stream on proxy connection")?;

            let handshake = ProxyHandshake {
                service_name: "ctrl-echo".to_string(),
                port: assigned_port,
                protocol: Protocol::Tcp,
            };
            frame::write_json(&mut send, &handshake)
                .await
                .context("failed to write proxy handshake")?;

            let payload = b"mesh-proxy-control-roundtrip";
            send.write_all(payload)
                .await
                .context("failed to write payload to proxy stream")?;
            send.flush()
                .await
                .context("failed to flush payload to proxy stream")?;

            let mut buf = vec![0_u8; payload.len()];
            timeout(Duration::from_secs(5), recv.read_exact(&mut buf))
                .await
                .context("timed out waiting for echo response")?
                .context("failed to read echo response from proxy stream")?;
            assert_eq!(
                buf, payload,
                "control's proxy ALPN should round-trip the payload"
            );

            // Cleanly tear down the proxy stream before closing the peer.
            send.finish().ok();
            drop(connection);
            peer.close().await;
            Ok(())
        }
        .await;

        let control_shutdown = control.shutdown().await;
        echo_handle.abort();
        let _ = echo_handle.await;

        control_shutdown?;
        test_result
    })
    .await
    .context("test_edge_reaches_control_self_service_via_mesh timed out")?
}
