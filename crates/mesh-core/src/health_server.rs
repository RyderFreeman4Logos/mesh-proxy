use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::http::StatusCode;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use mesh_proto::Protocol;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinSet;

use crate::EdgeNode;

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
}

/// Run the local health query HTTP server for an edge node.
///
/// The server always binds to `127.0.0.1` for security, using the port from
/// `bind_addr`.
pub async fn run_health_server(
    bind_addr: SocketAddr,
    state: Arc<RwLock<EdgeNode>>,
    shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], bind_addr.port()))).await?;
    serve_health_server(listener, state, shutdown_rx).await
}

async fn serve_health_server(
    listener: TcpListener,
    state: Arc<RwLock<EdgeNode>>,
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
    state: Arc<RwLock<EdgeNode>>,
) -> Result<Response<ResponseBody>, Infallible> {
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/healthz") => {
            json_response(StatusCode::OK, &HealthStatusResponse { status: "ok" })
        }
        (&Method::GET, "/v1/services") => {
            let services = {
                let node = state.read().await;
                build_service_routes(node.cached_routes())
            };
            json_response(StatusCode::OK, &services)
        }
        _ => plain_response(StatusCode::NOT_FOUND, "not found"),
    };

    Ok(response)
}

fn build_service_routes(
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
        })
        .collect::<Vec<_>>();
    services.sort_by_key(|service| service.published_port);
    services
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
    use mesh_proto::{Protocol, RouteEntry};
    use serde_json::Value;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

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

    async fn spawn_test_server(
        state: Arc<RwLock<EdgeNode>>,
    ) -> Result<(
        SocketAddr,
        broadcast::Sender<()>,
        tokio::task::JoinHandle<Result<()>>,
    )> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
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
