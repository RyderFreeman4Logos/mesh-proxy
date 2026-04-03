use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::http::StatusCode;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use mesh_proto::{HEARTBEAT_TIMEOUT_SECS, HealthState, Protocol};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinSet;

use crate::{ControlNode, EdgeNode};

type ResponseBody = Full<Bytes>;

#[derive(Debug, Serialize)]
struct HealthStatusResponse<'a> {
    status: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ServiceRouteResponse {
    published_port: u16,
    service_name: String,
    node_name: String,
    endpoint_id: String,
    target_local_addr: String,
    protocol: Protocol,
    health_state: HealthState,
    last_seen: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct NodeRouteResponse {
    endpoint_id: String,
    node_name: String,
    online: Option<bool>,
    last_seen: Option<u64>,
    quota_limit: Option<u16>,
    quota_used: Option<u16>,
}

#[derive(Clone)]
pub(crate) enum HealthServerState {
    Control(Arc<RwLock<ControlNode>>),
    Edge(Arc<RwLock<EdgeNode>>),
}

impl From<Arc<RwLock<ControlNode>>> for HealthServerState {
    fn from(state: Arc<RwLock<ControlNode>>) -> Self {
        Self::Control(state)
    }
}

impl From<Arc<RwLock<EdgeNode>>> for HealthServerState {
    fn from(state: Arc<RwLock<EdgeNode>>) -> Self {
        Self::Edge(state)
    }
}

impl HealthServerState {
    async fn services(&self, now_epoch: u64) -> Vec<ServiceRouteResponse> {
        match self {
            Self::Control(node) => {
                let node = node.read().await;
                build_control_service_routes(&node, now_epoch)
            }
            Self::Edge(node) => {
                let node = node.read().await;
                build_edge_service_routes(node.cached_routes())
            }
        }
    }

    async fn nodes(&self, now_epoch: u64) -> Vec<NodeRouteResponse> {
        match self {
            Self::Control(node) => {
                let node = node.read().await;
                build_control_nodes(&node, now_epoch)
            }
            Self::Edge(node) => {
                let node = node.read().await;
                build_edge_nodes(node.cached_routes())
            }
        }
    }
}

fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn is_stale(last_seen: Option<u64>, now_epoch: u64) -> bool {
    last_seen.is_some_and(|last_seen| now_epoch.saturating_sub(last_seen) > HEARTBEAT_TIMEOUT_SECS)
}

fn effective_health_state(
    health_state: HealthState,
    last_seen: Option<u64>,
    now_epoch: u64,
) -> HealthState {
    if is_stale(last_seen, now_epoch) {
        HealthState::Unknown
    } else {
        health_state
    }
}

/// Run the local health query HTTP server for a mesh node.
///
/// The server always binds to `127.0.0.1` for security, using the port from
/// `bind_addr`.
pub async fn run_health_server(
    bind_addr: SocketAddr,
    state: Arc<RwLock<EdgeNode>>,
    shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    run_health_server_with_state(bind_addr, HealthServerState::from(state), shutdown_rx).await
}

/// Run the local health query HTTP server for a control node.
///
/// The server always binds to `127.0.0.1` for security, using the port from
/// `bind_addr`.
pub async fn run_control_health_server(
    bind_addr: SocketAddr,
    state: Arc<RwLock<ControlNode>>,
    shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    run_health_server_with_state(bind_addr, HealthServerState::from(state), shutdown_rx).await
}

pub(crate) async fn run_health_server_with_state(
    bind_addr: SocketAddr,
    state: HealthServerState,
    shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], bind_addr.port()))).await?;
    serve_health_server(listener, Arc::new(state), shutdown_rx).await
}

async fn serve_health_server(
    listener: TcpListener,
    state: Arc<HealthServerState>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let local_addr = listener.local_addr()?;
    tracing::info!(%local_addr, "health query server listening");

    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, peer_addr) = match accept_result {
                    Ok(parts) => parts,
                    Err(error) => {
                        tracing::warn!(error = %error, "health query server accept failed");
                        continue;
                    }
                };

                let io = TokioIo::new(stream);
                let service_state = Arc::clone(&state);
                let mut connection_shutdown_rx = shutdown_rx.resubscribe();

                connections.spawn(async move {
                    let service = service_fn(move |request| {
                        let request_state = Arc::clone(&service_state);
                        async move { handle_request(request, request_state).await }
                    });

                    let connection = http1::Builder::new().serve_connection(io, service);
                    tokio::pin!(connection);

                    tokio::select! {
                        result = &mut connection => {
                            if let Err(error) = result {
                                tracing::warn!(%peer_addr, error = %error, "health query server connection failed");
                            }
                        }
                        recv_result = connection_shutdown_rx.recv() => {
                            if let Err(error) = recv_result {
                                tracing::debug!(%peer_addr, error = %error, "health query server shutdown channel closed");
                            }

                            connection.as_mut().graceful_shutdown();
                            if let Err(error) = connection.await {
                                tracing::warn!(%peer_addr, error = %error, "health query server graceful shutdown failed");
                            }
                        }
                    }
                });
            }
            recv_result = shutdown_rx.recv() => {
                if let Err(error) = recv_result {
                    tracing::debug!(error = %error, "health query server shutdown channel closed");
                }
                break;
            }
            join_result = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = join_result {
                    tracing::warn!(error = %error, "health query server task join failed");
                }
            }
        }
    }

    while let Some(join_result) = connections.join_next().await {
        if let Err(error) = join_result {
            tracing::warn!(error = %error, "health query server task join failed");
        }
    }

    Ok(())
}

async fn handle_request(
    request: Request<Incoming>,
    state: Arc<HealthServerState>,
) -> Result<Response<ResponseBody>, Infallible> {
    let path = request.uri().path();
    let response = match *request.method() {
        Method::GET if path == "/healthz" => {
            json_response(StatusCode::OK, &HealthStatusResponse { status: "ok" })
        }
        Method::GET if path == "/v1/services" => {
            let services = state.services(now_epoch()).await;
            json_response(StatusCode::OK, &services)
        }
        Method::GET if path.starts_with("/v1/services/") => {
            let Some(service_name) = path.strip_prefix("/v1/services/") else {
                return Ok(plain_response(StatusCode::NOT_FOUND, "not found"));
            };
            if service_name.is_empty() {
                plain_response(StatusCode::NOT_FOUND, "not found")
            } else {
                let service = state
                    .services(now_epoch())
                    .await
                    .into_iter()
                    .find(|service| service.service_name == service_name);
                match service {
                    Some(service) => json_response(StatusCode::OK, &service),
                    None => plain_response(StatusCode::NOT_FOUND, "not found"),
                }
            }
        }
        Method::GET if path == "/v1/nodes" => {
            let nodes = state.nodes(now_epoch()).await;
            json_response(StatusCode::OK, &nodes)
        }
        _ => plain_response(StatusCode::NOT_FOUND, "not found"),
    };

    Ok(response)
}

fn build_edge_service_routes(
    cached_routes: &std::collections::HashMap<u16, mesh_proto::RouteEntry>,
) -> Vec<ServiceRouteResponse> {
    let mut services = cached_routes
        .iter()
        .map(|(&published_port, route)| ServiceRouteResponse {
            published_port,
            service_name: route.service_name.clone(),
            node_name: route.node_name.clone(),
            endpoint_id: route.endpoint_id.clone(),
            target_local_addr: route.target_local_addr.clone(),
            protocol: route.protocol,
            health_state: HealthState::Unknown,
            last_seen: None,
        })
        .collect::<Vec<_>>();
    services.sort_by_key(|service| service.published_port);
    services
}

fn build_control_service_routes(
    control: &ControlNode,
    now_epoch: u64,
) -> Vec<ServiceRouteResponse> {
    let mut services = control
        .services()
        .values()
        .filter_map(|record| {
            let published_port = record.published_port?;
            Some(ServiceRouteResponse {
                published_port,
                service_name: record.service_id.service_name.clone(),
                node_name: record.node_name.clone(),
                endpoint_id: record.service_id.endpoint_id.clone(),
                target_local_addr: record.local_addr.clone(),
                protocol: record.protocol,
                health_state: effective_health_state(
                    record.health_state,
                    record.last_seen,
                    now_epoch,
                ),
                last_seen: record.last_seen,
            })
        })
        .collect::<Vec<_>>();
    services.sort_by(|left, right| {
        left.published_port
            .cmp(&right.published_port)
            .then_with(|| left.endpoint_id.cmp(&right.endpoint_id))
            .then_with(|| left.service_name.cmp(&right.service_name))
    });
    services
}

fn build_edge_nodes(
    cached_routes: &std::collections::HashMap<u16, mesh_proto::RouteEntry>,
) -> Vec<NodeRouteResponse> {
    let nodes = cached_routes.iter().fold(
        BTreeMap::<String, NodeRouteResponse>::new(),
        |mut nodes, (_port, route)| {
            nodes
                .entry(route.endpoint_id.clone())
                .or_insert_with(|| NodeRouteResponse {
                    endpoint_id: route.endpoint_id.clone(),
                    node_name: route.node_name.clone(),
                    online: None,
                    last_seen: None,
                    quota_limit: None,
                    quota_used: None,
                });
            nodes
        },
    );
    nodes.into_values().collect()
}

fn build_control_nodes(control: &ControlNode, now_epoch: u64) -> Vec<NodeRouteResponse> {
    let mut nodes = control
        .whitelist()
        .values()
        .map(|node| NodeRouteResponse {
            endpoint_id: node.endpoint_id.clone(),
            node_name: node.node_name.clone(),
            online: Some(node.is_online && !is_stale(node.last_heartbeat, now_epoch)),
            last_seen: node.last_heartbeat,
            quota_limit: Some(node.quota_limit),
            quota_used: Some(node.quota_used),
        })
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
    nodes
}

fn json_response<T>(status: StatusCode, value: &T) -> Response<ResponseBody>
where
    T: Serialize,
{
    match serde_json::to_vec(value) {
        Ok(body) => Response::builder()
            .status(status)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body)))
            .unwrap_or_else(|error| {
                tracing::error!(error = %error, "failed to build JSON response");
                plain_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
            }),
        Err(error) => {
            tracing::error!(error = %error, "failed to serialize JSON response");
            plain_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
        }
    }
}

fn plain_response(status: StatusCode, body: &str) -> Response<ResponseBody> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::copy_from_slice(body.as_bytes())))
        .unwrap_or_else(|error| {
            tracing::error!(error = %error, "failed to build plain-text response");
            Response::new(Full::new(Bytes::from_static(b"internal server error")))
        })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::{Context, Result};
    use mesh_proto::{HealthState, NodeInfo, Protocol, RouteEntry, ServiceHealthEntry};
    use serde_json::Value;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;
    use crate::ControlNode;

    #[tokio::test]
    async fn test_health_server_healthz() -> Result<()> {
        let state = Arc::new(RwLock::new(EdgeNode::new()));
        let (addr, shutdown_tx, handle) = spawn_test_server(Arc::clone(&state)).await?;

        let response = send_get_request(addr, "/healthz").await?;
        assert_eq!(response.status_code, 200);
        assert_eq!(
            response.json_body,
            Some(serde_json::json!({ "status": "ok" }))
        );

        shutdown_server(shutdown_tx, handle).await
    }

    #[tokio::test]
    async fn test_health_server_services() -> Result<()> {
        let mut routes = HashMap::new();
        routes.insert(
            41000,
            RouteEntry {
                service_name: "alpha".to_string(),
                node_name: "edge-a".to_string(),
                endpoint_id: "endpoint-a".to_string(),
                target_local_addr: "127.0.0.1:3000".to_string(),
                protocol: Protocol::Tcp,
            },
        );
        routes.insert(
            41001,
            RouteEntry {
                service_name: "beta".to_string(),
                node_name: "edge-b".to_string(),
                endpoint_id: "endpoint-b".to_string(),
                target_local_addr: "/tmp/beta.sock".to_string(),
                protocol: Protocol::Unix,
            },
        );

        let mut edge = EdgeNode::new();
        edge.update_routes(routes, 3);
        let state = Arc::new(RwLock::new(edge));

        let (addr, shutdown_tx, handle) = spawn_test_server(Arc::clone(&state)).await?;
        let response = send_get_request(addr, "/v1/services").await?;

        assert_eq!(response.status_code, 200);
        assert_eq!(
            response.json_body,
            Some(serde_json::json!([
                {
                    "published_port": 41000,
                    "service_name": "alpha",
                    "node_name": "edge-a",
                    "endpoint_id": "endpoint-a",
                    "target_local_addr": "127.0.0.1:3000",
                    "protocol": "tcp",
                    "health_state": "unknown",
                    "last_seen": null
                },
                {
                    "published_port": 41001,
                    "service_name": "beta",
                    "node_name": "edge-b",
                    "endpoint_id": "endpoint-b",
                    "target_local_addr": "/tmp/beta.sock",
                    "protocol": "unix",
                    "health_state": "unknown",
                    "last_seen": null
                }
            ]))
        );

        shutdown_server(shutdown_tx, handle).await
    }

    #[tokio::test]
    async fn test_health_server_service_by_name_and_nodes_for_edge() -> Result<()> {
        let mut routes = HashMap::new();
        routes.insert(
            41000,
            RouteEntry {
                service_name: "alpha".to_string(),
                node_name: "edge-a".to_string(),
                endpoint_id: "endpoint-a".to_string(),
                target_local_addr: "127.0.0.1:3000".to_string(),
                protocol: Protocol::Tcp,
            },
        );
        routes.insert(
            41001,
            RouteEntry {
                service_name: "beta".to_string(),
                node_name: "edge-a".to_string(),
                endpoint_id: "endpoint-a".to_string(),
                target_local_addr: "127.0.0.1:4000".to_string(),
                protocol: Protocol::Tcp,
            },
        );
        routes.insert(
            41002,
            RouteEntry {
                service_name: "gamma".to_string(),
                node_name: "edge-b".to_string(),
                endpoint_id: "endpoint-b".to_string(),
                target_local_addr: "/tmp/gamma.sock".to_string(),
                protocol: Protocol::Unix,
            },
        );

        let mut edge = EdgeNode::new();
        edge.update_routes(routes, 3);
        let state = Arc::new(RwLock::new(edge));

        let (addr, shutdown_tx, handle) = spawn_test_server(Arc::clone(&state)).await?;

        let service = send_get_request(addr, "/v1/services/alpha").await?;
        assert_eq!(service.status_code, 200);
        assert_eq!(
            service.json_body,
            Some(serde_json::json!({
                "published_port": 41000,
                "service_name": "alpha",
                "node_name": "edge-a",
                "endpoint_id": "endpoint-a",
                "target_local_addr": "127.0.0.1:3000",
                "protocol": "tcp",
                "health_state": "unknown",
                "last_seen": null
            }))
        );

        let nodes = send_get_request(addr, "/v1/nodes").await?;
        assert_eq!(nodes.status_code, 200);
        assert_eq!(
            nodes.json_body,
            Some(serde_json::json!([
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

        let missing_service = send_get_request(addr, "/v1/services/missing").await?;
        assert_eq!(missing_service.status_code, 404);
        assert_eq!(missing_service.body, "not found");

        shutdown_server(shutdown_tx, handle).await
    }

    #[tokio::test]
    async fn test_health_server_control_routes_mark_stale_health() -> Result<()> {
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
                &[mesh_proto::ServiceRegistration {
                    name: "alpha".to_string(),
                    local_addr: "127.0.0.1:3000".to_string(),
                    protocol: Protocol::Tcp,
                    health_check: None,
                }],
                1,
            )
            .expect("registering control service should succeed");
        control.aggregate_health(
            "endpoint-a",
            vec![ServiceHealthEntry {
                service_name: "alpha".to_string(),
                health_state: HealthState::Healthy,
                last_error: None,
            }],
            1,
        );
        control.record_pong("endpoint-a", 1);

        let state = Arc::new(RwLock::new(control));
        let (addr, shutdown_tx, handle) = spawn_test_server(Arc::clone(&state)).await?;

        let services = send_get_request(addr, "/v1/services").await?;
        assert_eq!(services.status_code, 200);
        assert_eq!(
            services.json_body,
            Some(serde_json::json!([
                {
                    "published_port": 40000,
                    "service_name": "alpha",
                    "node_name": "edge-a",
                    "endpoint_id": "endpoint-a",
                    "target_local_addr": "127.0.0.1:3000",
                    "protocol": "tcp",
                    "health_state": "unknown",
                    "last_seen": 1
                }
            ]))
        );

        let service = send_get_request(addr, "/v1/services/alpha").await?;
        assert_eq!(service.status_code, 200);
        assert_eq!(
            service.json_body,
            Some(serde_json::json!({
                "published_port": 40000,
                "service_name": "alpha",
                "node_name": "edge-a",
                "endpoint_id": "endpoint-a",
                "target_local_addr": "127.0.0.1:3000",
                "protocol": "tcp",
                "health_state": "unknown",
                "last_seen": 1
            }))
        );

        let nodes = send_get_request(addr, "/v1/nodes").await?;
        assert_eq!(nodes.status_code, 200);
        assert_eq!(
            nodes.json_body,
            Some(serde_json::json!([
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

        shutdown_server(shutdown_tx, handle).await
    }

    #[tokio::test]
    async fn test_health_server_404() -> Result<()> {
        let state = Arc::new(RwLock::new(EdgeNode::new()));
        let (addr, shutdown_tx, handle) = spawn_test_server(Arc::clone(&state)).await?;

        let response = send_get_request(addr, "/unknown").await?;
        assert_eq!(response.status_code, 404);
        assert_eq!(response.body, "not found");

        shutdown_server(shutdown_tx, handle).await
    }

    struct TestHttpResponse {
        status_code: u16,
        body: String,
        json_body: Option<Value>,
    }

    async fn spawn_test_server<S>(
        state: S,
    ) -> Result<(
        SocketAddr,
        broadcast::Sender<()>,
        tokio::task::JoinHandle<Result<()>>,
    )>
    where
        S: Into<HealthServerState>,
    {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let state = Arc::new(state.into());
        let handle =
            tokio::spawn(async move { serve_health_server(listener, state, shutdown_rx).await });
        Ok((addr, shutdown_tx, handle))
    }

    async fn send_get_request(addr: SocketAddr, path: &str) -> Result<TestHttpResponse> {
        let mut stream = TcpStream::connect(addr).await?;
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await?;

        let mut response_bytes = Vec::new();
        stream.read_to_end(&mut response_bytes).await?;
        parse_response(&response_bytes)
    }

    fn parse_response(response_bytes: &[u8]) -> Result<TestHttpResponse> {
        let response =
            std::str::from_utf8(response_bytes).context("response was not valid UTF-8")?;
        let (head, body) = response.split_once("\r\n\r\n").with_context(|| {
            format!("response did not contain header/body separator: {response:?}")
        })?;
        let status_code = head
            .lines()
            .next()
            .and_then(|status_line| status_line.split_whitespace().nth(1))
            .context("response did not contain a status code")?
            .parse::<u16>()
            .context("status code was not numeric")?;

        let json_body = serde_json::from_str(body).ok();

        Ok(TestHttpResponse {
            status_code,
            body: body.to_string(),
            json_body,
        })
    }

    async fn shutdown_server(
        shutdown_tx: broadcast::Sender<()>,
        handle: tokio::task::JoinHandle<Result<()>>,
    ) -> Result<()> {
        let _ = shutdown_tx.send(());
        handle.await.context("health server task panicked")?
    }
}
