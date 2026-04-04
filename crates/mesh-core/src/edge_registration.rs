use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use iroh::EndpointAddr;
use mesh_proto::frame::{read_json, write_json};
use mesh_proto::{
    ALPN_CONTROL, ControlMessage, JoinTicket, MeshConfig, PortAssignment, RouteEntry,
    ServiceRegistration,
};
use tokio::sync::{RwLock, broadcast, watch};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::edge_node::{ConnectionState, EdgeNode};

const REGISTRATION_RETRY_DELAY: Duration = Duration::from_secs(1);
const JOIN_TICKET_TTL_SECONDS: u64 = 3600;

struct RegistrationSnapshot {
    data_dir: PathBuf,
    services: Vec<ServiceRegistration>,
}

enum RegistrationMode {
    Initial,
    Refresh,
}

enum RegistrationResponse {
    Ack { assignments: Vec<PortAssignment> },
    Nack { reason: String },
}

fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn parse_control_endpoint_addr(control_addr: &str) -> Result<EndpointAddr> {
    if let Ok(endpoint_addr) = serde_json::from_str::<EndpointAddr>(control_addr) {
        return Ok(endpoint_addr);
    }

    let endpoint_id = control_addr
        .parse::<iroh::EndpointId>()
        .with_context(|| format!("failed to parse control address: {control_addr}"))?;
    Ok(endpoint_id.into())
}

async fn snapshot_registration(config: &Arc<RwLock<MeshConfig>>) -> RegistrationSnapshot {
    let config = config.read().await;
    RegistrationSnapshot {
        data_dir: config.data_dir.clone(),
        services: config
            .services
            .iter()
            .map(|service| ServiceRegistration {
                name: service.name.clone(),
                local_addr: service.local_addr.clone(),
                protocol: service.protocol,
                health_check: service.health_check.clone(),
            })
            .collect(),
    }
}

async fn transition_edge_state(edge_node: &Arc<RwLock<EdgeNode>>, next: ConnectionState) {
    let mut edge = edge_node.write().await;
    if let Err(error) = edge.transition_to(next.clone()) {
        warn!(error = %error, ?next, "failed to transition edge registration state");
    }
}

async fn disconnect_edge(edge_node: &Arc<RwLock<EdgeNode>>) {
    let mut edge = edge_node.write().await;
    if let Err(error) = edge.transition_to(ConnectionState::Disconnected) {
        warn!(error = %error, "failed to reset edge registration state");
    }
}

async fn wait_before_retry(shutdown: &mut broadcast::Receiver<()>) -> bool {
    tokio::select! {
        _ = sleep(REGISTRATION_RETRY_DELAY) => false,
        _ = shutdown.recv() => true,
    }
}

async fn apply_register_ack(
    edge_node: &Arc<RwLock<EdgeNode>>,
    config: &Arc<RwLock<MeshConfig>>,
    control_endpoint_id: &str,
    endpoint_id: &str,
    node_name: &str,
    snapshot: &RegistrationSnapshot,
    assignments: &[PortAssignment],
) -> Result<()> {
    let mut services_by_name = HashMap::with_capacity(snapshot.services.len());
    for service in &snapshot.services {
        services_by_name.insert(service.name.as_str(), service);
    }

    let mut edge = edge_node.write().await;
    let mut merged_routes: HashMap<u16, RouteEntry> = edge
        .cached_routes()
        .iter()
        .filter(|(_, route)| route.endpoint_id != endpoint_id)
        .map(|(port, route)| (*port, route.clone()))
        .collect();

    for assignment in assignments {
        let service = services_by_name
            .get(assignment.service_name.as_str())
            .copied()
            .ok_or_else(|| {
                anyhow!(
                    "register ack contained unknown service assignment: {}",
                    assignment.service_name
                )
            })?;

        merged_routes.insert(
            assignment.assigned_port,
            RouteEntry {
                service_name: assignment.service_name.clone(),
                node_name: node_name.to_owned(),
                endpoint_id: endpoint_id.to_owned(),
                target_local_addr: service.local_addr.clone(),
                protocol: service.protocol,
            },
        );
    }

    // RegisterAck does not carry a route-table version, so advance a local
    // synthetic version to apply listener changes until a fresher snapshot arrives.
    let next_version = edge.route_version().saturating_add(1);
    edge.set_control_endpoint_id(Some(control_endpoint_id.to_owned()));
    let applied = edge
        .apply_route_update(merged_routes, next_version, Arc::clone(config))
        .await;
    if applied {
        edge.save_route_cache(&snapshot.data_dir)
            .await
            .context("failed to persist route cache after register ack")?;
    }

    Ok(())
}

fn log_register_ack_assignments(assignments: &[PortAssignment]) {
    for assignment in assignments {
        info!(
            service_name = %assignment.service_name,
            assigned_port = assignment.assigned_port,
            "registered service with control node"
        );
    }
}

fn is_graceful_stream_end(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_error| io_error.kind() == ErrorKind::UnexpectedEof)
    })
}

async fn read_registration_follow_up(
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<Option<(HashMap<u16, RouteEntry>, u64)>> {
    let mut route_update = None;

    loop {
        let message: ControlMessage = match read_json(recv).await {
            Ok(message) => message,
            Err(error) if is_graceful_stream_end(&error) => return Ok(route_update),
            Err(error) => return Err(error).context("failed to read registration follow-up"),
        };

        match message {
            ControlMessage::RouteTableUpdate { routes, version } => {
                route_update = Some((routes, version));
            }
            other => {
                warn!(
                    ?other,
                    "unexpected follow-up control message after register ack"
                );
            }
        }
    }
}

async fn request_full_route_table(
    connection: &iroh::endpoint::Connection,
) -> Result<(HashMap<u16, RouteEntry>, u64)> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .context("failed to open full route sync stream")?;
    write_json(&mut send, &ControlMessage::RouteTableRequest)
        .await
        .context("failed to request full route table")?;
    send.finish()
        .map_err(|error| anyhow!("failed to finish full route sync stream: {error}"))?;

    match read_json(&mut recv)
        .await
        .context("failed to read full route sync response")?
    {
        ControlMessage::RouteTableUpdate { routes, version } => Ok((routes, version)),
        other => Err(anyhow!(
            "unexpected response to full route sync request: {other:?}"
        )),
    }
}

async fn purge_routes_if_control_identity_changed(
    edge_node: &Arc<RwLock<EdgeNode>>,
    config: &Arc<RwLock<MeshConfig>>,
    control_endpoint_id: &str,
) -> Result<()> {
    let data_dir = {
        let config = config.read().await;
        config.data_dir.clone()
    };

    let mut edge = edge_node.write().await;
    let Some(cached_control_endpoint_id) = edge.control_endpoint_id().map(str::to_owned) else {
        return Ok(());
    };
    if cached_control_endpoint_id == control_endpoint_id {
        return Ok(());
    }

    info!(
        %cached_control_endpoint_id,
        %control_endpoint_id,
        "control identity changed, purging cached route table"
    );
    edge.purge_route_cache().await;
    edge.set_control_endpoint_id(Some(control_endpoint_id.to_owned()));
    edge.save_route_cache(&data_dir)
        .await
        .context("failed to persist purged route cache after control identity change")?;
    Ok(())
}

async fn apply_route_table_update(
    edge_node: &Arc<RwLock<EdgeNode>>,
    config: &Arc<RwLock<MeshConfig>>,
    control_endpoint_id: &str,
    routes: HashMap<u16, RouteEntry>,
    version: u64,
    force: bool,
) -> Result<()> {
    let data_dir = {
        let config = config.read().await;
        config.data_dir.clone()
    };

    let mut edge = edge_node.write().await;
    edge.set_control_endpoint_id(Some(control_endpoint_id.to_owned()));
    if force {
        // After fresh registration, always apply the route table from control
        // regardless of cached version (the control is authoritative).
        edge.force_route_update(routes, version, Arc::clone(config))
            .await;
        edge.save_route_cache(&data_dir)
            .await
            .context("failed to persist route cache after forced route update")?;
    } else {
        let applied = edge
            .apply_route_update(routes, version, Arc::clone(config))
            .await;
        if applied {
            edge.save_route_cache(&data_dir)
                .await
                .context("failed to persist route cache after route update")?;
        }
    }

    Ok(())
}

async fn send_register(
    connection: &iroh::endpoint::Connection,
    endpoint_id: &str,
    control_endpoint_id: &str,
    node_name: &str,
    config: &Arc<RwLock<MeshConfig>>,
    edge_node: &Arc<RwLock<EdgeNode>>,
    mode: RegistrationMode,
) -> Result<RegistrationResponse> {
    if matches!(mode, RegistrationMode::Initial) {
        transition_edge_state(edge_node, ConnectionState::Registering).await;
    }

    let snapshot = snapshot_registration(config).await;
    let ticket = JoinTicket {
        endpoint_id: endpoint_id.to_owned(),
        created_at: now_epoch(),
        ttl_seconds: JOIN_TICKET_TTL_SECONDS,
        nonce: rand::random(),
    };
    let auth_ticket = ticket
        .to_bs58()
        .context("failed to encode join ticket for registration")?;

    let message = ControlMessage::Register {
        node_name: node_name.to_owned(),
        auth_ticket,
        services: snapshot.services.clone(),
    };

    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .context("failed to open registration stream")?;
    write_json(&mut send, &message)
        .await
        .context("failed to write Register message")?;
    send.finish()
        .map_err(|error| anyhow!("failed to finish Register stream: {error}"))?;

    let response: ControlMessage = read_json(&mut recv)
        .await
        .context("failed to read registration response")?;

    match response {
        ControlMessage::RegisterAck { assignments } => {
            let initial_route_update = match read_registration_follow_up(&mut recv).await {
                Ok(route_update) => route_update,
                Err(error) => {
                    warn!(
                        error = %error,
                        "failed to read route table follow-up after register ack"
                    );
                    match request_full_route_table(connection).await {
                        Ok(route_update) => Some(route_update),
                        Err(request_error) => {
                            warn!(
                                error = %request_error,
                                "failed to request full route table after register follow-up error"
                            );
                            None
                        }
                    }
                }
            };

            if let Some((routes, version)) = initial_route_update {
                apply_route_table_update(
                    edge_node,
                    config,
                    control_endpoint_id,
                    routes,
                    version,
                    true,
                )
                .await
                .context("failed to apply initial route table after register ack")?;
            } else {
                // Older control nodes may only return RegisterAck, so keep the
                // synthetic merge path as a compatibility fallback.
                apply_register_ack(
                    edge_node,
                    config,
                    control_endpoint_id,
                    endpoint_id,
                    node_name,
                    &snapshot,
                    &assignments,
                )
                .await
                .context("failed to apply register ack")?;
            }

            log_register_ack_assignments(&assignments);
            if matches!(mode, RegistrationMode::Initial) {
                transition_edge_state(edge_node, ConnectionState::Authenticated).await;
            }
            Ok(RegistrationResponse::Ack { assignments })
        }
        ControlMessage::RegisterNack { reason } => Ok(RegistrationResponse::Nack { reason }),
        other => Err(anyhow!("unexpected response to Register: {other:?}")),
    }
}

async fn handle_control_message(
    edge_node: &Arc<RwLock<EdgeNode>>,
    config: &Arc<RwLock<MeshConfig>>,
    control_endpoint_id: &str,
    send: &mut iroh::endpoint::SendStream,
    message: ControlMessage,
) -> Result<()> {
    match message {
        ControlMessage::RouteTableUpdate { routes, version } => {
            apply_route_table_update(
                edge_node,
                config,
                control_endpoint_id,
                routes,
                version,
                false,
            )
            .await?;
        }
        ControlMessage::Ping => {
            write_json(send, &ControlMessage::Pong)
                .await
                .context("failed to write Pong response")?;
            let mut edge = edge_node.write().await;
            edge.record_ping(now_epoch());
        }
        ControlMessage::Pong => {
            let mut edge = edge_node.write().await;
            edge.record_ping(now_epoch());
        }
        other => {
            warn!(
                ?other,
                "unexpected control message in edge registration loop"
            );
        }
    }

    send.finish()
        .map_err(|error| anyhow!("failed to finish control response stream: {error}"))?;
    Ok(())
}

pub async fn run_registration_loop(
    endpoint: iroh::Endpoint,
    control_addr: String,
    node_name: String,
    config: Arc<RwLock<MeshConfig>>,
    edge_node: Arc<RwLock<EdgeNode>>,
    mut service_change_rx: watch::Receiver<()>,
    mut shutdown: broadcast::Receiver<()>,
) {
    let control_endpoint = match parse_control_endpoint_addr(&control_addr) {
        Ok(endpoint_addr) => endpoint_addr,
        Err(error) => {
            warn!(error = %error, "failed to parse control address for edge registration");
            return;
        }
    };
    let control_endpoint_id = control_endpoint.id.to_string();

    if let Err(error) =
        purge_routes_if_control_identity_changed(&edge_node, &config, &control_endpoint_id).await
    {
        warn!(
            error = %error,
            %control_endpoint_id,
            "failed to purge stale route cache after control identity change"
        );
    }

    let endpoint_id = endpoint.id().to_string();
    let mut watch_open = true;

    loop {
        transition_edge_state(&edge_node, ConnectionState::Connecting).await;

        let connection = tokio::select! {
            connect_result = endpoint.connect(control_endpoint.clone(), ALPN_CONTROL) => {
                match connect_result {
                    Ok(connection) => connection,
                    Err(error) => {
                        warn!(error = %error, "failed to connect to control node");
                        disconnect_edge(&edge_node).await;
                        if wait_before_retry(&mut shutdown).await {
                            return;
                        }
                        continue;
                    }
                }
            }
            _ = shutdown.recv() => return,
        };

        transition_edge_state(&edge_node, ConnectionState::Unauthenticated).await;

        match send_register(
            &connection,
            &endpoint_id,
            &control_endpoint_id,
            &node_name,
            &config,
            &edge_node,
            RegistrationMode::Initial,
        )
        .await
        {
            Ok(RegistrationResponse::Ack { assignments }) => {
                info!(
                    assignment_count = assignments.len(),
                    "edge registration acknowledged by control node"
                );
            }
            Ok(RegistrationResponse::Nack { reason }) => {
                warn!(reason = %reason, "edge registration rejected by control node");
                disconnect_edge(&edge_node).await;
                if wait_before_retry(&mut shutdown).await {
                    return;
                }
                continue;
            }
            Err(error) => {
                warn!(error = %error, "edge registration attempt failed");
                disconnect_edge(&edge_node).await;
                if wait_before_retry(&mut shutdown).await {
                    return;
                }
                continue;
            }
        }

        loop {
            tokio::select! {
                incoming = connection.accept_bi() => {
                    let (mut send, mut recv) = match incoming {
                        Ok(streams) => streams,
                        Err(error) => {
                            warn!(error = %error, "control connection closed during registration loop");
                            break;
                        }
                    };

                    let message: ControlMessage = match read_json(&mut recv).await {
                        Ok(message) => message,
                        Err(error) => {
                            warn!(error = %error, "failed to read control message");
                            break;
                        }
                    };

                    if let Err(error) = handle_control_message(
                        &edge_node,
                        &config,
                        &control_endpoint_id,
                        &mut send,
                        message,
                    ).await {
                        warn!(error = %error, "failed to handle control message");
                        break;
                    }
                }
                changed = service_change_rx.changed(), if watch_open => {
                    match changed {
                        Ok(()) => {
                            match send_register(
                                &connection,
                                &endpoint_id,
                                &control_endpoint_id,
                                &node_name,
                                &config,
                                &edge_node,
                                RegistrationMode::Refresh,
                            ).await {
                                Ok(RegistrationResponse::Ack { assignments }) => {
                                    info!(
                                        assignment_count = assignments.len(),
                                        "service change re-registration acknowledged by control node"
                                    );
                                }
                                Ok(RegistrationResponse::Nack { reason }) => {
                                    warn!(reason = %reason, "service change re-registration rejected");
                                    break;
                                }
                                Err(error) => {
                                    warn!(error = %error, "service change re-registration failed");
                                    break;
                                }
                            }
                        }
                        Err(_) => {
                            warn!("service change watcher closed; disabling re-registration triggers");
                            watch_open = false;
                        }
                    }
                }
                _ = shutdown.recv() => return,
            }
        }

        disconnect_edge(&edge_node).await;
        if wait_before_retry(&mut shutdown).await {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mesh_proto::{Protocol, ServiceEntry};

    const ENDPOINT_ID_A: &str = "ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6";
    const ENDPOINT_ID_B: &str = "5f3db5df65f8fd2815250f699f9d87513a86f969e5da9f634e9a55e8eb49f1a0";

    fn test_service(name: &str, local_addr: &str) -> ServiceRegistration {
        ServiceRegistration {
            name: name.to_owned(),
            local_addr: local_addr.to_owned(),
            protocol: Protocol::Tcp,
            health_check: None,
        }
    }

    #[test]
    fn test_parse_control_endpoint_addr_from_json() {
        let endpoint_id: iroh::EndpointId = ENDPOINT_ID_A.parse().unwrap();
        let expected = EndpointAddr::from(endpoint_id);
        let encoded = serde_json::to_string(&expected).unwrap();

        let parsed = parse_control_endpoint_addr(&encoded).unwrap();
        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_parse_control_endpoint_addr_from_endpoint_id() {
        let parsed = parse_control_endpoint_addr(ENDPOINT_ID_A).unwrap();
        let endpoint_id: iroh::EndpointId = ENDPOINT_ID_A.parse().unwrap();
        assert_eq!(parsed, EndpointAddr::from(endpoint_id));
    }

    #[tokio::test]
    async fn test_apply_register_ack_preserves_remote_routes() {
        let snapshot = RegistrationSnapshot {
            data_dir: tempfile::tempdir_in("/tmp").unwrap().keep(),
            services: vec![test_service("local-api", "127.0.0.1:8080")],
        };
        let control_endpoint_id = ENDPOINT_ID_B.to_owned();
        let endpoint_id = ENDPOINT_ID_A.to_owned();
        let remote_endpoint_id = ENDPOINT_ID_B.to_owned();

        let mut edge = EdgeNode::new();
        edge.update_routes(
            HashMap::from([
                (
                    41000,
                    RouteEntry {
                        service_name: "old-local".to_string(),
                        node_name: "edge-a".to_string(),
                        endpoint_id: endpoint_id.clone(),
                        target_local_addr: "127.0.0.1:7000".to_string(),
                        protocol: Protocol::Tcp,
                    },
                ),
                (
                    42000,
                    RouteEntry {
                        service_name: "remote-api".to_string(),
                        node_name: "edge-b".to_string(),
                        endpoint_id: remote_endpoint_id.clone(),
                        target_local_addr: "127.0.0.1:9000".to_string(),
                        protocol: Protocol::Tcp,
                    },
                ),
            ]),
            7,
        );

        let edge = Arc::new(RwLock::new(edge));
        let config = Arc::new(RwLock::new(MeshConfig {
            services: vec![ServiceEntry {
                name: "local-api".to_string(),
                local_addr: "127.0.0.1:8080".to_string(),
                protocol: Protocol::Tcp,
                health_check: None,
            }],
            ..MeshConfig::default()
        }));
        apply_register_ack(
            &edge,
            &config,
            &control_endpoint_id,
            &endpoint_id,
            "edge-a",
            &snapshot,
            &[PortAssignment {
                service_name: "local-api".to_string(),
                assigned_port: 43000,
            }],
        )
        .await
        .unwrap();

        let edge = edge.read().await;
        assert!(edge.cached_routes().contains_key(&42000));
        assert!(!edge.cached_routes().contains_key(&41000));
        assert_eq!(
            edge.control_endpoint_id(),
            Some(control_endpoint_id.as_str())
        );
        assert_eq!(
            edge.cached_routes().get(&43000).unwrap(),
            &RouteEntry {
                service_name: "local-api".to_string(),
                node_name: "edge-a".to_string(),
                endpoint_id,
                target_local_addr: "127.0.0.1:8080".to_string(),
                protocol: Protocol::Tcp,
            }
        );
    }

    #[tokio::test]
    async fn test_force_route_table_update_bypasses_stale_cached_version() {
        let tempdir = tempfile::tempdir_in("/tmp").unwrap();
        let config = Arc::new(RwLock::new(MeshConfig {
            data_dir: tempdir.path().to_path_buf(),
            ..MeshConfig::default()
        }));
        let mut edge = EdgeNode::new();
        edge.update_routes(
            HashMap::from([(
                41000,
                RouteEntry {
                    service_name: "stale".to_string(),
                    node_name: "edge-old".to_string(),
                    endpoint_id: ENDPOINT_ID_A.to_string(),
                    target_local_addr: "127.0.0.1:7000".to_string(),
                    protocol: Protocol::Tcp,
                },
            )]),
            99,
        );
        let edge = Arc::new(RwLock::new(edge));

        apply_route_table_update(
            &edge,
            &config,
            ENDPOINT_ID_B,
            HashMap::from([(
                42000,
                RouteEntry {
                    service_name: "fresh".to_string(),
                    node_name: "edge-new".to_string(),
                    endpoint_id: ENDPOINT_ID_B.to_string(),
                    target_local_addr: "127.0.0.1:8000".to_string(),
                    protocol: Protocol::Tcp,
                },
            )]),
            1,
            true,
        )
        .await
        .unwrap();

        let edge = edge.read().await;
        assert_eq!(edge.route_version(), 1);
        assert_eq!(edge.control_endpoint_id(), Some(ENDPOINT_ID_B));
        assert!(!edge.cached_routes().contains_key(&41000));
        assert!(edge.cached_routes().contains_key(&42000));
    }

    #[tokio::test]
    async fn test_purge_routes_if_control_identity_changed_clears_stale_cache() {
        let tempdir = tempfile::tempdir_in("/tmp").unwrap();
        let config = Arc::new(RwLock::new(MeshConfig {
            data_dir: tempdir.path().to_path_buf(),
            ..MeshConfig::default()
        }));
        let mut edge = EdgeNode::new();
        edge.update_routes(
            HashMap::from([(
                41000,
                RouteEntry {
                    service_name: "stale".to_string(),
                    node_name: "edge-old".to_string(),
                    endpoint_id: ENDPOINT_ID_A.to_string(),
                    target_local_addr: "127.0.0.1:7000".to_string(),
                    protocol: Protocol::Tcp,
                },
            )]),
            7,
        );
        edge.set_control_endpoint_id(Some(ENDPOINT_ID_A.to_string()));
        let edge = Arc::new(RwLock::new(edge));

        purge_routes_if_control_identity_changed(&edge, &config, ENDPOINT_ID_B)
            .await
            .unwrap();

        let edge = edge.read().await;
        assert!(edge.cached_routes().is_empty());
        assert_eq!(edge.route_version(), 0);
        assert_eq!(edge.control_endpoint_id(), Some(ENDPOINT_ID_B));

        let cache =
            EdgeNode::load_route_cache(tempdir.path()).expect("purged route cache should persist");
        assert!(cache.routes.is_empty());
        assert_eq!(cache.version, 0);
        assert_eq!(cache.control_endpoint_id.as_deref(), Some(ENDPOINT_ID_B));
    }
}
