use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use mesh_core::health_server::{run_control_health_server, run_health_server};
use mesh_core::{ControlNode, EdgeNode, IpcServer};
use mesh_proto::{
    HealthState, IpcRequest, IpcResponse, MeshConfig, NodeInfo, NodeRole, Protocol, RouteEntry,
    ServiceHealthEntry, ServiceRegistration, frame,
};
use serde_json::{Value, json};
use tempfile::Builder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

fn writable_tempdir(prefix: &str) -> tempfile::TempDir {
    Builder::new()
        .prefix(prefix)
        .tempdir_in("/tmp")
        .expect("tempdir should be created in /tmp")
}

fn reserve_unused_tcp_port() -> u16 {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind");
    let port = listener
        .local_addr()
        .expect("listener should expose local addr")
        .port();
    drop(listener);
    port
}

fn test_route(
    service_name: &str,
    node_name: &str,
    endpoint_id: &str,
    target_local_addr: &str,
    protocol: Protocol,
) -> RouteEntry {
    RouteEntry {
        service_name: service_name.to_string(),
        node_name: node_name.to_string(),
        endpoint_id: endpoint_id.to_string(),
        target_local_addr: target_local_addr.to_string(),
        protocol,
    }
}

struct TestHttpResponse {
    status_code: u16,
    json_body: Option<Value>,
}

struct TestHealthServer {
    addr: SocketAddr,
    shutdown_tx: broadcast::Sender<()>,
    handle: JoinHandle<Result<()>>,
}

impl TestHealthServer {
    async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(());
        self.handle.await.context("health server task panicked")?
    }
}

struct TestIpcServer {
    _dir: tempfile::TempDir,
    socket_path: PathBuf,
    shutdown_tx: broadcast::Sender<()>,
    handle: JoinHandle<Result<()>>,
}

impl TestIpcServer {
    async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(());
        self.handle.await.context("IPC server task panicked")?
    }
}

async fn spawn_edge_health_server(edge: EdgeNode) -> Result<TestHealthServer> {
    let port = reserve_unused_tcp_port();
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let state = Arc::new(RwLock::new(edge));
    let (shutdown_tx, shutdown_rx) = broadcast::channel(4);
    let handle = tokio::spawn(async move { run_health_server(addr, state, shutdown_rx).await });
    wait_for_health_server(addr).await?;
    Ok(TestHealthServer {
        addr,
        shutdown_tx,
        handle,
    })
}

async fn spawn_control_health_server(control: ControlNode) -> Result<TestHealthServer> {
    let port = reserve_unused_tcp_port();
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let state = Arc::new(RwLock::new(control));
    let (shutdown_tx, shutdown_rx) = broadcast::channel(4);
    let handle =
        tokio::spawn(async move { run_control_health_server(addr, state, shutdown_rx).await });
    wait_for_health_server(addr).await?;
    Ok(TestHealthServer {
        addr,
        shutdown_tx,
        handle,
    })
}

async fn spawn_control_ipc_server(control: ControlNode) -> Result<TestIpcServer> {
    let dir = writable_tempdir("mesh-core-phase5-ipc-");
    let config_path = dir.path().join("config.toml");
    let config = MeshConfig {
        role: NodeRole::Control,
        data_dir: dir.path().join("data"),
        ..MeshConfig::default()
    };
    config.save(&config_path)?;

    let socket_path = config.data_dir.join("daemon.sock");
    let (shutdown_tx, _) = broadcast::channel(4);
    let (reload_tx, _reload_rx) = mpsc::channel(4);
    let server = IpcServer::bind(
        &socket_path,
        shutdown_tx.clone(),
        config,
        config_path,
        reload_tx,
    )
    .await?;
    server
        .attach_control_node(Arc::new(RwLock::new(control)))
        .await;

    let shutdown_rx = shutdown_tx.subscribe();
    let handle = tokio::spawn(async move { server.run(shutdown_rx).await });

    Ok(TestIpcServer {
        _dir: dir,
        socket_path,
        shutdown_tx,
        handle,
    })
}

async fn send_ipc_request(socket_path: &Path, request: &IpcRequest) -> Result<IpcResponse> {
    let mut stream = UnixStream::connect(socket_path).await.with_context(|| {
        format!(
            "failed to connect to IPC server at {}",
            socket_path.display()
        )
    })?;
    frame::write_json(&mut stream, request).await?;
    let response: IpcResponse = frame::read_json(&mut stream).await?;
    Ok(response)
}

async fn send_get_request(addr: SocketAddr, path: &str) -> Result<TestHttpResponse> {
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("failed to connect to health server at {addr}"))?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .context("failed to write HTTP request")?;

    let mut response_bytes = Vec::new();
    stream
        .read_to_end(&mut response_bytes)
        .await
        .context("failed to read HTTP response")?;
    parse_response(&response_bytes)
}

fn parse_response(response_bytes: &[u8]) -> Result<TestHttpResponse> {
    let response = std::str::from_utf8(response_bytes)
        .context("health server response was not valid UTF-8")?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .with_context(|| format!("response did not contain header/body separator: {response:?}"))?;
    let status_code = head
        .lines()
        .next()
        .and_then(|status_line| status_line.split_whitespace().nth(1))
        .context("response did not contain a status code")?
        .parse::<u16>()
        .context("status code was not numeric")?;

    Ok(TestHttpResponse {
        status_code,
        json_body: serde_json::from_str(body).ok(),
    })
}

async fn wait_for_health_server(addr: SocketAddr) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            if send_get_request(addr, "/healthz").await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .with_context(|| format!("health server at {addr} did not become ready"))?;

    Ok(())
}

#[tokio::test]
async fn test_quota_ipc_roundtrip() -> Result<()> {
    let mut control = ControlNode::new();
    control.add_node(NodeInfo {
        endpoint_id: "edge-1".to_string(),
        node_name: "edge-alpha".to_string(),
        quota_limit: 5,
        quota_used: 0,
        is_online: true,
        last_heartbeat: None,
        addr: None,
    });
    control
        .register_services(
            "edge-1",
            "edge-alpha",
            &[ServiceRegistration {
                name: "svc-a".to_string(),
                local_addr: "127.0.0.1:8080".to_string(),
                protocol: Protocol::Tcp,
                health_check: None,
            }],
            1,
        )
        .map_err(anyhow::Error::msg)
        .context("control service registration should succeed")?;

    let server = spawn_control_ipc_server(control).await?;

    let show_response = send_ipc_request(&server.socket_path, &IpcRequest::QuotaShow).await?;
    let IpcResponse::QuotaInfo { quotas } = show_response else {
        panic!("expected quota info response");
    };
    assert_eq!(quotas, vec![("edge-1".to_string(), 1, 5)]);

    let update_response = send_ipc_request(
        &server.socket_path,
        &IpcRequest::QuotaSet {
            endpoint_id: "edge-1".to_string(),
            limit: 9,
        },
    )
    .await?;
    assert!(matches!(update_response, IpcResponse::QuotaUpdated));

    let updated_show = send_ipc_request(&server.socket_path, &IpcRequest::QuotaShow).await?;
    let IpcResponse::QuotaInfo { quotas } = updated_show else {
        panic!("expected quota info response after update");
    };
    assert_eq!(quotas, vec![("edge-1".to_string(), 1, 9)]);

    server.shutdown().await
}

#[tokio::test]
async fn test_health_server_service_by_name() -> Result<()> {
    let mut edge = EdgeNode::new();
    edge.update_routes(
        HashMap::from([(
            41000,
            test_route(
                "test_svc",
                "edge-a",
                "endpoint-a",
                "127.0.0.1:3000",
                Protocol::Tcp,
            ),
        )]),
        1,
    );

    let server = spawn_edge_health_server(edge).await?;
    let response = send_get_request(server.addr, "/v1/services/test_svc").await?;

    assert_eq!(response.status_code, 200);
    assert_eq!(
        response.json_body,
        Some(json!({
            "published_port": 41000,
            "service_name": "test_svc",
            "node_name": "edge-a",
            "endpoint_id": "endpoint-a",
            "target_local_addr": "127.0.0.1:3000",
            "protocol": "tcp",
            "health_state": "unknown",
            "last_seen": null
        }))
    );

    server.shutdown().await
}

#[tokio::test]
async fn test_health_server_nodes_endpoint() -> Result<()> {
    let mut edge = EdgeNode::new();
    edge.update_routes(
        HashMap::from([
            (
                41000,
                test_route(
                    "svc-a",
                    "edge-a",
                    "endpoint-a",
                    "127.0.0.1:3000",
                    Protocol::Tcp,
                ),
            ),
            (
                41001,
                test_route(
                    "svc-b",
                    "edge-b",
                    "endpoint-b",
                    "/tmp/service.sock",
                    Protocol::Unix,
                ),
            ),
        ]),
        1,
    );

    let server = spawn_edge_health_server(edge).await?;
    let response = send_get_request(server.addr, "/v1/nodes").await?;

    assert_eq!(response.status_code, 200);
    assert_eq!(
        response.json_body,
        Some(json!([
            {
                "endpoint_id": "endpoint-a",
                "node_name": "edge-a",
                "online": null,
                "last_seen": null,
                "quota_limit": null,
                "quota_used": null
            },
            {
                "endpoint_id": "endpoint-b",
                "node_name": "edge-b",
                "online": null,
                "last_seen": null,
                "quota_limit": null,
                "quota_used": null
            }
        ]))
    );

    server.shutdown().await
}

#[tokio::test]
async fn test_stale_health_marking() -> Result<()> {
    let mut control = ControlNode::new();
    control.add_node(NodeInfo {
        endpoint_id: "endpoint-a".to_string(),
        node_name: "edge-a".to_string(),
        quota_limit: 5,
        quota_used: 0,
        is_online: true,
        last_heartbeat: Some(1),
        addr: None,
    });
    control
        .register_services(
            "endpoint-a",
            "edge-a",
            &[ServiceRegistration {
                name: "test_svc".to_string(),
                local_addr: "127.0.0.1:3000".to_string(),
                protocol: Protocol::Tcp,
                health_check: None,
            }],
            1,
        )
        .map_err(anyhow::Error::msg)
        .context("control service registration should succeed")?;
    control.aggregate_health(
        "endpoint-a",
        vec![ServiceHealthEntry {
            service_name: "test_svc".to_string(),
            health_state: HealthState::Healthy,
            last_error: None,
        }],
        1,
    );
    control.record_pong("endpoint-a", 1);

    let server = spawn_control_health_server(control).await?;

    let service_response = send_get_request(server.addr, "/v1/services/test_svc").await?;
    assert_eq!(service_response.status_code, 200);
    assert_eq!(
        service_response.json_body,
        Some(json!({
            "published_port": 40000,
            "service_name": "test_svc",
            "node_name": "edge-a",
            "endpoint_id": "endpoint-a",
            "target_local_addr": "127.0.0.1:3000",
            "protocol": "tcp",
            "health_state": "unknown",
            "last_seen": 1
        }))
    );

    let node_response = send_get_request(server.addr, "/v1/nodes").await?;
    assert_eq!(node_response.status_code, 200);
    assert_eq!(
        node_response.json_body,
        Some(json!([
            {
                "endpoint_id": "endpoint-a",
                "node_name": "edge-a",
                "online": false,
                "last_seen": 1,
                "quota_limit": 5,
                "quota_used": 1
            }
        ]))
    );

    server.shutdown().await
}
