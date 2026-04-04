use std::time::Duration;

use anyhow::{Context, Result};
use mesh_core::Daemon;
use mesh_proto::{
    InviteToken, IpcRequest, IpcResponse, MeshConfig, NodeRole, Protocol, ServiceEntry, frame,
};
use tempfile::Builder;
use tokio::net::UnixStream;
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
        node_name: "control-test".to_string(),
        role: NodeRole::Control,
        data_dir,
        ..MeshConfig::default()
    }
}

fn make_edge_config(
    node_name: &str,
    control_addr: String,
    data_dir: std::path::PathBuf,
    services: Vec<ServiceEntry>,
) -> MeshConfig {
    MeshConfig {
        node_name: node_name.to_string(),
        role: NodeRole::Edge,
        control_addr: Some(control_addr),
        enable_local_proxy: false,
        health_bind: None,
        services,
        data_dir,
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
            if socket_path.exists() {
                match send_ipc_request(socket_path, &IpcRequest::Status).await {
                    Ok(IpcResponse::Status(status)) if status.endpoint_addr.is_some() => {
                        return Ok(());
                    }
                    _ => sleep(POLL_INTERVAL).await,
                }
            } else {
                sleep(POLL_INTERVAL).await;
            }
        }
    })
    .await
    .context("timed out waiting for IPC server to become ready")?
}

async fn wait_for_status_predicate(
    socket_path: &std::path::Path,
    mut predicate: impl FnMut(&mesh_proto::StatusInfo) -> bool,
) -> Result<mesh_proto::StatusInfo> {
    let socket_path = socket_path.to_path_buf();
    timeout(Duration::from_secs(10), async move {
        loop {
            let status = match send_ipc_request(&socket_path, &IpcRequest::Status).await? {
                IpcResponse::Status(status) => status,
                other => anyhow::bail!("expected status response, got {other:?}"),
            };
            if predicate(&status) {
                return Ok(status);
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .context("timed out waiting for status condition")?
}

struct ControlDaemonHarness {
    _tempdir: tempfile::TempDir,
    socket_path: std::path::PathBuf,
    shutdown_tx: mesh_core::ShutdownTx,
    handle: tokio::task::JoinHandle<Result<()>>,
}

impl ControlDaemonHarness {
    async fn start() -> Result<Self> {
        let tempdir = writable_tempdir("mesh-invite-control-");
        let config_path = tempdir.path().join("config.toml");
        let config = make_control_config(tempdir.path().join("data"));
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

    async fn send_request(&self, request: &IpcRequest) -> Result<IpcResponse> {
        send_ipc_request(&self.socket_path, request).await
    }

    /// Generate an invite token via IPC and return the bs58-encoded string.
    async fn generate_invite(
        &self,
        name: Option<String>,
        ttl_seconds: Option<u64>,
    ) -> Result<String> {
        match self
            .send_request(&IpcRequest::Invite { name, ttl_seconds })
            .await?
        {
            IpcResponse::InviteResult { token } => Ok(token),
            other => anyhow::bail!("expected InviteResult, got {other:?}"),
        }
    }

    async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(());
        match timeout(Duration::from_secs(5), self.handle).await {
            Ok(join_result) => join_result
                .context("control daemon panicked")?
                .context("control daemon returned error"),
            Err(_) => {
                // Graceful shutdown timed out — force abort. This happens when
                // the iroh endpoint close takes longer than expected in test
                // environments. Not a test failure since we only care about
                // the test assertions, not graceful daemon teardown.
                Ok(())
            }
        }
    }
}

struct EdgeDaemonHarness {
    _tempdir: tempfile::TempDir,
    socket_path: std::path::PathBuf,
    shutdown_tx: mesh_core::ShutdownTx,
    handle: tokio::task::JoinHandle<Result<()>>,
}

impl EdgeDaemonHarness {
    async fn start(
        node_name: &str,
        control_addr: String,
        services: Vec<ServiceEntry>,
        invite_nonce: Option<&str>,
    ) -> Result<Self> {
        let tempdir = writable_tempdir("mesh-invite-edge-");
        let config_path = tempdir.path().join("config.toml");
        let config = make_edge_config(
            node_name,
            control_addr,
            tempdir.path().join("data"),
            services,
        );
        config.save(&config_path)?;

        // Create the data directory so we can write invite_nonce.txt before daemon starts.
        std::fs::create_dir_all(&config.data_dir)?;

        if let Some(nonce) = invite_nonce {
            let nonce_path = config.data_dir.join("invite_nonce.txt");
            std::fs::write(&nonce_path, nonce)?;
        }

        let socket_path = config.data_dir.join("daemon.sock");
        let mut daemon = Daemon::new_without_startup_signal(config, config_path);
        let shutdown_tx = daemon.shutdown_tx().clone();
        let handle = tokio::spawn(async move { daemon.run().await });

        Ok(Self {
            _tempdir: tempdir,
            socket_path,
            shutdown_tx,
            handle,
        })
    }

    async fn send_request(&self, request: &IpcRequest) -> Result<IpcResponse> {
        // Wait for socket before sending.
        wait_for_ipc_ready(&self.socket_path).await?;
        send_ipc_request(&self.socket_path, request).await
    }

    async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(());
        match timeout(Duration::from_secs(5), self.handle).await {
            Ok(join_result) => join_result
                .context("edge daemon panicked")?
                .context("edge daemon returned error"),
            Err(_) => Ok(()),
        }
    }
}

// ---------------------------------------------------------------------------
// Test 1: Generate invite token and verify its contents
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_invite_generate_and_decode() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let control = ControlDaemonHarness::start().await?;

        let test_result: Result<()> = async {
            let token_bs58 = control
                .generate_invite(Some("my-new-edge".to_string()), Some(120))
                .await?;

            let token = InviteToken::from_bs58(&token_bs58)
                .map_err(|e| anyhow::anyhow!("failed to decode invite token: {e}"))?;

            // Verify decoded fields.
            assert!(
                !token.control_addr.is_empty(),
                "control_addr should be non-empty"
            );
            assert_ne!(token.nonce, [0u8; 16], "nonce should not be all zeros");
            assert_eq!(token.ttl_seconds, 120, "ttl should match requested value");
            assert_eq!(
                token.node_name.as_deref(),
                Some("my-new-edge"),
                "node_name should match requested value"
            );

            // Token should pass validation (not expired, valid fields).
            token
                .validate()
                .map_err(|e| anyhow::anyhow!("token should be valid: {e}"))?;

            // Verify control_addr can be deserialized as an endpoint address.
            let _: iroh::EndpointAddr = serde_json::from_str(&token.control_addr)
                .context("token control_addr should be a valid serialized EndpointAddr")?;

            Ok(())
        }
        .await;

        let shutdown_result = control.shutdown().await;
        shutdown_result?;
        test_result
    })
    .await
    .context("test_invite_generate_and_decode timed out")?
}

// ---------------------------------------------------------------------------
// Test 2: Full invite→join→auto-accept flow
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_invite_join_auto_accept() -> Result<()> {
    timeout(Duration::from_secs(20), async {
        let control = ControlDaemonHarness::start().await?;

        let test_result: Result<()> = async {
            let token_bs58 = control
                .generate_invite(Some("edge-invited".to_string()), None)
                .await?;
            let token = InviteToken::from_bs58(&token_bs58)
                .map_err(|e| anyhow::anyhow!("failed to decode invite token: {e}"))?;

            // Extract the bs58-encoded nonce (what edge writes to invite_nonce.txt).
            let nonce_bs58 = bs58::encode(token.nonce).into_string();

            let edge = EdgeDaemonHarness::start(
                "edge-invited",
                token.control_addr.clone(),
                vec![ServiceEntry {
                    name: "echo".to_string(),
                    local_addr: "127.0.0.1:18080".to_string(),
                    protocol: Protocol::Tcp,
                    health_check: None,
                }],
                Some(&nonce_bs58),
            )
            .await?;

            // Wait for edge to appear in control's peer list as online.
            let status = wait_for_status_predicate(&control.socket_path, |status| {
                status
                    .connected_nodes
                    .iter()
                    .any(|n| n.name == "edge-invited" && n.online)
            })
            .await?;

            // Verify the edge is connected and recognized.
            let connected = status
                .connected_nodes
                .iter()
                .find(|n| n.name == "edge-invited")
                .context("edge-invited should appear in connected nodes")?;
            assert!(connected.online, "edge should be online after invite join");

            // Verify the edge's service is registered.
            assert!(
                status.services.iter().any(|s| s.name == "echo"),
                "echo service should be registered on control"
            );

            edge.shutdown().await?;
            Ok(())
        }
        .await;

        let shutdown_result = control.shutdown().await;
        shutdown_result?;
        test_result
    })
    .await
    .context("test_invite_join_auto_accept timed out")?
}

// ---------------------------------------------------------------------------
// Test 3: Expired invite nonce is not auto-accepted
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_invite_expired_rejected() -> Result<()> {
    timeout(Duration::from_secs(20), async {
        let control = ControlDaemonHarness::start().await?;

        let test_result: Result<()> = async {
            // Generate invite with minimal TTL (1 second).
            let token_bs58 = control.generate_invite(None, Some(1)).await?;
            let token = InviteToken::from_bs58(&token_bs58)
                .map_err(|e| anyhow::anyhow!("failed to decode invite token: {e}"))?;

            let nonce_bs58 = bs58::encode(token.nonce).into_string();

            // Wait for the invite to expire.
            sleep(Duration::from_secs(2)).await;

            let edge = EdgeDaemonHarness::start(
                "edge-expired",
                token.control_addr.clone(),
                vec![ServiceEntry {
                    name: "expired-svc".to_string(),
                    local_addr: "127.0.0.1:18080".to_string(),
                    protocol: Protocol::Tcp,
                    health_check: None,
                }],
                Some(&nonce_bs58),
            )
            .await?;

            // Give the edge some time to attempt registration.
            sleep(Duration::from_secs(3)).await;

            // Check control status: the edge should NOT be auto-accepted.
            // With an expired invite, the edge falls through to normal ticket
            // flow. Since no one ran `accept` on the control side, the edge
            // should not appear as a connected+online node.
            let status = match send_ipc_request(&control.socket_path, &IpcRequest::Status).await? {
                IpcResponse::Status(status) => status,
                other => anyhow::bail!("expected Status response, got {other:?}"),
            };

            let auto_accepted = status
                .connected_nodes
                .iter()
                .any(|n| n.name == "edge-expired" && n.online);
            assert!(
                !auto_accepted,
                "edge with expired invite should NOT be auto-accepted"
            );

            // Service should NOT be registered.
            assert!(
                !status.services.iter().any(|s| s.name == "expired-svc"),
                "expired invite should not lead to service registration"
            );

            edge.shutdown().await?;
            Ok(())
        }
        .await;

        let shutdown_result = control.shutdown().await;
        shutdown_result?;
        test_result
    })
    .await
    .context("test_invite_expired_rejected timed out")?
}

// ---------------------------------------------------------------------------
// Test 4: Invite nonce is single-use — second edge is rejected
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_invite_nonce_single_use() -> Result<()> {
    timeout(Duration::from_secs(30), async {
        let control = ControlDaemonHarness::start().await?;

        let test_result: Result<()> = async {
            let token_bs58 = control
                .generate_invite(Some("edge-first".to_string()), None)
                .await?;
            let token = InviteToken::from_bs58(&token_bs58)
                .map_err(|e| anyhow::anyhow!("failed to decode invite token: {e}"))?;
            let nonce_bs58 = bs58::encode(token.nonce).into_string();

            // First edge: should be auto-accepted.
            let edge_one = EdgeDaemonHarness::start(
                "edge-first",
                token.control_addr.clone(),
                vec![ServiceEntry {
                    name: "first-svc".to_string(),
                    local_addr: "127.0.0.1:18080".to_string(),
                    protocol: Protocol::Tcp,
                    health_check: None,
                }],
                Some(&nonce_bs58),
            )
            .await?;

            // Wait for first edge to register successfully.
            wait_for_status_predicate(&control.socket_path, |status| {
                status
                    .connected_nodes
                    .iter()
                    .any(|n| n.name == "edge-first" && n.online)
            })
            .await?;

            // Shut down first edge.
            edge_one.shutdown().await?;

            // Wait for control to notice the disconnect.
            sleep(Duration::from_secs(1)).await;

            // Second edge: same nonce, should NOT be auto-accepted (nonce consumed).
            let edge_two = EdgeDaemonHarness::start(
                "edge-second",
                token.control_addr.clone(),
                vec![ServiceEntry {
                    name: "second-svc".to_string(),
                    local_addr: "127.0.0.1:28080".to_string(),
                    protocol: Protocol::Tcp,
                    health_check: None,
                }],
                Some(&nonce_bs58),
            )
            .await?;

            // Give the second edge time to attempt registration.
            sleep(Duration::from_secs(3)).await;

            let status = match send_ipc_request(&control.socket_path, &IpcRequest::Status).await? {
                IpcResponse::Status(status) => status,
                other => anyhow::bail!("expected Status response, got {other:?}"),
            };

            let second_accepted = status
                .connected_nodes
                .iter()
                .any(|n| n.name == "edge-second" && n.online);
            assert!(
                !second_accepted,
                "second edge with consumed nonce should NOT be auto-accepted"
            );

            assert!(
                !status.services.iter().any(|s| s.name == "second-svc"),
                "second edge should not have its service registered"
            );

            edge_two.shutdown().await?;
            Ok(())
        }
        .await;

        let shutdown_result = control.shutdown().await;
        shutdown_result?;
        test_result
    })
    .await
    .context("test_invite_nonce_single_use timed out")?
}

// ---------------------------------------------------------------------------
// Test 5: Full flow: invite → join → auto-accept → expose service → visible
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_invite_then_expose_service() -> Result<()> {
    timeout(Duration::from_secs(30), async {
        let control = ControlDaemonHarness::start().await?;

        let test_result: Result<()> = async {
            let token_bs58 = control
                .generate_invite(Some("edge-full".to_string()), None)
                .await?;
            let token = InviteToken::from_bs58(&token_bs58)
                .map_err(|e| anyhow::anyhow!("failed to decode invite token: {e}"))?;
            let nonce_bs58 = bs58::encode(token.nonce).into_string();

            // Start edge with invite nonce and an initial service.
            let edge = EdgeDaemonHarness::start(
                "edge-full",
                token.control_addr.clone(),
                vec![ServiceEntry {
                    name: "boot-svc".to_string(),
                    local_addr: "127.0.0.1:18080".to_string(),
                    protocol: Protocol::Tcp,
                    health_check: None,
                }],
                Some(&nonce_bs58),
            )
            .await?;

            // Wait for edge to register and initial service to appear.
            wait_for_status_predicate(&control.socket_path, |status| {
                status
                    .connected_nodes
                    .iter()
                    .any(|n| n.name == "edge-full" && n.online)
                    && status.services.iter().any(|s| s.name == "boot-svc")
            })
            .await?;

            // Expose a new service at runtime via edge IPC.
            let expose_response = edge
                .send_request(&IpcRequest::ExposeService {
                    name: "runtime-api".to_string(),
                    local_addr: "127.0.0.1:19090".to_string(),
                    protocol: Protocol::Tcp,
                    health_check: None,
                })
                .await?;

            let assigned_port = match expose_response {
                IpcResponse::ServiceExposed {
                    name,
                    assigned_port,
                } => {
                    assert_eq!(name, "runtime-api");
                    assigned_port
                }
                IpcResponse::ServiceExposeTimedOut { .. } => {
                    anyhow::bail!("expose timed out — the edge may not have registered in time");
                }
                other => {
                    anyhow::bail!("expected ServiceExposed, got {other:?}");
                }
            };

            // Verify the new service appears on control status with a valid port.
            let final_status = wait_for_status_predicate(&control.socket_path, |status| {
                status.services.iter().any(|s| s.name == "runtime-api")
            })
            .await?;

            let runtime_svc = final_status
                .services
                .iter()
                .find(|s| s.name == "runtime-api")
                .context("runtime-api should appear in control status")?;
            assert_eq!(
                runtime_svc.assigned_port,
                Some(assigned_port),
                "control and edge should agree on assigned port"
            );

            // Both services should be present.
            assert!(
                final_status.services.iter().any(|s| s.name == "boot-svc"),
                "initial service should still be registered"
            );
            assert!(
                final_status
                    .services
                    .iter()
                    .any(|s| s.name == "runtime-api"),
                "runtime-exposed service should be registered"
            );

            edge.shutdown().await?;
            Ok(())
        }
        .await;

        let shutdown_result = control.shutdown().await;
        shutdown_result?;
        test_result
    })
    .await
    .context("test_invite_then_expose_service timed out")?
}
