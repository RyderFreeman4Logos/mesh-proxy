use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use mesh_core::health_prober::HealthProber;
use mesh_core::health_server::run_health_server;
use mesh_core::{ConfigWatcher, EdgeNode};
use mesh_proto::{
    HealthCheckConfig, HealthCheckMode, HealthState, MeshConfig, Protocol, RouteEntry,
};
use serde_json::{Value, json};
use tempfile::Builder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::time::{Instant, sleep, timeout};

fn writable_tempdir() -> tempfile::TempDir {
    Builder::new()
        .prefix("mesh-core-phase3-")
        .tempdir_in("/tmp")
        .expect("tempdir should be created in /tmp")
}

fn reserve_unused_tcp_ports(count: usize) -> Vec<u16> {
    let listeners = (0..count)
        .map(|_| {
            std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind")
        })
        .collect::<Vec<_>>();
    let ports = listeners
        .iter()
        .map(|listener| {
            listener
                .local_addr()
                .expect("listener should have a local addr")
                .port()
        })
        .collect::<Vec<_>>();
    drop(listeners);
    ports
}

fn test_route(service_name: &str, endpoint_id: &str, target_local_addr: String) -> RouteEntry {
    RouteEntry {
        service_name: service_name.to_string(),
        node_name: "edge-phase3".to_string(),
        endpoint_id: endpoint_id.to_string(),
        target_local_addr,
        protocol: Protocol::Tcp,
    }
}

async fn assert_listener_accepts(port: u16) -> Result<()> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .with_context(|| format!("listener on port {port} should accept connections"))?;
    let mut buf = [0_u8; 1];
    let bytes_read = timeout(Duration::from_secs(1), stream.read(&mut buf))
        .await
        .context("listener did not close accepted connection in time")??;
    assert_eq!(
        bytes_read, 0,
        "listener should close connections immediately"
    );
    Ok(())
}

async fn wait_for_port_to_close(port: u16) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            match TcpStream::connect(("127.0.0.1", port)).await {
                Ok(stream) => {
                    drop(stream);
                    sleep(Duration::from_millis(50)).await;
                }
                Err(_) => break,
            }
        }
    })
    .await
    .with_context(|| format!("port {port} did not stop accepting connections"))?;

    Ok(())
}

struct TestHttpResponse {
    status_code: u16,
    body: String,
    json_body: Option<Value>,
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
        body: body.to_string(),
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
async fn test_config_watcher_triggers_reload() -> Result<()> {
    let dir = writable_tempdir();
    let config_path = dir.path().join("config.toml");
    MeshConfig::default().save(&config_path)?;

    let (tx, mut rx) = mpsc::channel(4);
    let _watcher = ConfigWatcher::new(config_path.clone(), tx)?;

    sleep(Duration::from_millis(200)).await;

    let mut updated = MeshConfig::default();
    updated.node_name = "phase3-updated-node".to_string();
    let started_at = Instant::now();
    updated.save(&config_path)?;

    let received = timeout(Duration::from_secs(5), rx.recv())
        .await
        .context("timed out waiting for config watcher notification")?;
    assert_eq!(received, Some(()));
    assert!(
        started_at.elapsed() >= Duration::from_millis(500),
        "config watcher notification should be debounced"
    );

    Ok(())
}

#[tokio::test]
async fn test_route_diff_drives_listeners() -> Result<()> {
    let ports = reserve_unused_tcp_ports(2);
    let removed_port = ports[0];
    let kept_port = ports[1];
    let mut edge = EdgeNode::new();

    let initial_routes = HashMap::from([
        (
            removed_port,
            test_route("alpha", "endpoint-alpha", "127.0.0.1:3000".to_string()),
        ),
        (
            kept_port,
            test_route("beta", "endpoint-beta", "127.0.0.1:3001".to_string()),
        ),
    ]);

    assert!(edge.apply_route_update(initial_routes, 1).await);
    assert_eq!(edge.listener_count(), 2);
    assert_listener_accepts(removed_port).await?;
    assert_listener_accepts(kept_port).await?;

    let updated_routes = HashMap::from([(
        kept_port,
        test_route("beta", "endpoint-beta", "127.0.0.1:3001".to_string()),
    )]);

    assert!(edge.apply_route_update(updated_routes, 2).await);
    assert_eq!(edge.listener_count(), 1);
    assert_listener_accepts(kept_port).await?;
    wait_for_port_to_close(removed_port).await?;

    assert!(edge.apply_route_update(HashMap::new(), 3).await);
    assert_eq!(edge.listener_count(), 0);
    wait_for_port_to_close(kept_port).await?;

    Ok(())
}

#[tokio::test]
async fn test_health_prober_tcp() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind test listener")?;
    let addr = listener
        .local_addr()
        .context("listener should expose local addr")?;
    let config = HealthCheckConfig {
        mode: HealthCheckMode::TcpConnect,
        target: Some(addr.to_string()),
        interval_seconds: 10,
    };

    let healthy = HealthProber::probe(&config).await;
    assert_eq!(healthy, (HealthState::Healthy, None));

    drop(listener);
    sleep(Duration::from_millis(50)).await;

    let unhealthy = HealthProber::probe(&config).await;
    assert_eq!(unhealthy.0, HealthState::Unhealthy);
    assert!(
        unhealthy
            .1
            .as_deref()
            .is_some_and(|message| message.contains("tcp connect failed")),
        "expected tcp probe failure after listener drop, got {unhealthy:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_health_server_responds() -> Result<()> {
    let ports = reserve_unused_tcp_ports(1);
    let health_port = ports[0];
    let bind_addr = SocketAddr::from(([127, 0, 0, 1], health_port));

    let mut routes = HashMap::new();
    routes.insert(
        41_000,
        RouteEntry {
            service_name: "alpha".to_string(),
            node_name: "edge-a".to_string(),
            endpoint_id: "endpoint-a".to_string(),
            target_local_addr: "127.0.0.1:3000".to_string(),
            protocol: Protocol::Tcp,
        },
    );
    routes.insert(
        41_001,
        RouteEntry {
            service_name: "beta".to_string(),
            node_name: "edge-b".to_string(),
            endpoint_id: "endpoint-b".to_string(),
            target_local_addr: "/tmp/beta.sock".to_string(),
            protocol: Protocol::Unix,
        },
    );

    let mut edge = EdgeNode::new();
    edge.update_routes(routes, 7);
    let state = Arc::new(RwLock::new(edge));
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let server_state = Arc::clone(&state);
    let handle =
        tokio::spawn(async move { run_health_server(bind_addr, server_state, shutdown_rx).await });

    wait_for_health_server(bind_addr).await?;

    let healthz = send_get_request(bind_addr, "/healthz").await?;
    assert_eq!(healthz.status_code, 200);
    assert_eq!(healthz.json_body, Some(json!({ "status": "ok" })));

    let services = send_get_request(bind_addr, "/v1/services").await?;
    assert_eq!(services.status_code, 200);
    assert_eq!(
        services.json_body,
        Some(json!([
            {
                "published_port": 41000,
                "service_name": "alpha",
                "node_name": "edge-a",
                "endpoint_id": "endpoint-a",
                "target_local_addr": "127.0.0.1:3000",
                "protocol": "tcp"
            },
            {
                "published_port": 41001,
                "service_name": "beta",
                "node_name": "edge-b",
                "endpoint_id": "endpoint-b",
                "target_local_addr": "/tmp/beta.sock",
                "protocol": "unix"
            }
        ]))
    );
    assert!(
        services.body.contains("\"published_port\":41000"),
        "services response should contain serialized routes"
    );

    let _ = shutdown_tx.send(());
    handle.await.context("health server task panicked")??;

    Ok(())
}
