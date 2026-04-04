use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use anyhow::{Context, Result};
use mesh_proto::{
    ConnectedNode, DEFAULT_SERVICE_QUOTA, HealthState, IpcRequest, IpcResponse, MeshConfig,
    NodeInfo, NodeRole, Protocol, RouteEntry, ServiceDisplayStatus, ServiceEntry, ServiceStatus,
    StatusInfo,
};
use tokio::net::{TcpStream, UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc};
use tokio::task;
use tokio::time::timeout;
use toml_edit::{ArrayOfTables, DocumentMut, Item, Table, value};
use tracing::{info, warn};

use crate::control_node::ControlNode;
use crate::daemon::{PortAssignmentNotifier, ShutdownRx, ShutdownTx};
use crate::edge_node::EdgeNode;
use mesh_proto::frame;

const INITIALIZING_ENDPOINT_ID: &str = "(initializing)";
const STATUS_PROBE_TIMEOUT: Duration = Duration::from_millis(500);
const EXPOSE_ASSIGNMENT_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Default)]
struct MeshRuntimeState {
    endpoint_id: Option<String>,
    endpoint_addr: Option<String>,
    online: bool,
}

impl MeshRuntimeState {
    fn status_fields(&self) -> (String, Option<String>, bool) {
        match &self.endpoint_id {
            Some(endpoint_id) => (endpoint_id.clone(), self.endpoint_addr.clone(), self.online),
            None => (INITIALIZING_ENDPOINT_ID.to_string(), None, false),
        }
    }
}

#[derive(Clone)]
pub(crate) struct IpcStatusHandle {
    runtime: Arc<RwLock<MeshRuntimeState>>,
}

impl IpcStatusHandle {
    /// Publish the daemon's current mesh endpoint state for status requests.
    pub fn set_mesh_node(&self, endpoint_id: String, endpoint_addr: Option<String>, online: bool) {
        let mut runtime = match self.runtime.write() {
            Ok(runtime) => runtime,
            Err(poisoned) => poisoned.into_inner(),
        };
        *runtime = MeshRuntimeState {
            endpoint_id: Some(endpoint_id),
            endpoint_addr,
            online,
        };
    }
}

/// Node-role specific state accessible to IPC handlers.
pub(crate) enum NodeState {
    Control(Arc<tokio::sync::RwLock<ControlNode>>),
    Edge(Arc<tokio::sync::RwLock<EdgeNode>>),
    /// Before the node is initialized.
    Uninit,
}

/// Shared state accessible to IPC connection handlers.
struct SharedState {
    shutdown_tx: ShutdownTx,
    config: MeshConfig,
    config_path: PathBuf,
    reload_tx: mpsc::Sender<()>,
    port_assignment_notifier: PortAssignmentNotifier,
    expose_assignment_timeout: Duration,
    runtime: Arc<RwLock<MeshRuntimeState>>,
    node_state: Arc<tokio::sync::RwLock<NodeState>>,
}

/// Unix domain socket IPC server for CLI-daemon communication.
pub struct IpcServer {
    listener: UnixListener,
    state: Arc<SharedState>,
}

impl IpcServer {
    /// Bind to the given socket path, removing any stale socket first.
    pub async fn bind(
        socket_path: &Path,
        shutdown_tx: ShutdownTx,
        config: MeshConfig,
        config_path: PathBuf,
        reload_tx: mpsc::Sender<()>,
    ) -> Result<Self> {
        Self::bind_with_port_assignment_notifier(
            socket_path,
            shutdown_tx,
            config,
            config_path,
            reload_tx,
            PortAssignmentNotifier::default(),
        )
        .await
    }

    pub(crate) async fn bind_with_port_assignment_notifier(
        socket_path: &Path,
        shutdown_tx: ShutdownTx,
        config: MeshConfig,
        config_path: PathBuf,
        reload_tx: mpsc::Sender<()>,
        port_assignment_notifier: PortAssignmentNotifier,
    ) -> Result<Self> {
        if socket_path.exists() {
            std::fs::remove_file(socket_path)?;
        }

        if let Some(parent) = socket_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let listener = UnixListener::bind(socket_path)?;
        info!(path = %socket_path.display(), "IPC server listening");

        let state = Arc::new(SharedState {
            shutdown_tx,
            config,
            config_path,
            reload_tx,
            port_assignment_notifier,
            expose_assignment_timeout: EXPOSE_ASSIGNMENT_TIMEOUT,
            runtime: Arc::new(RwLock::new(MeshRuntimeState::default())),
            node_state: Arc::new(tokio::sync::RwLock::new(NodeState::Uninit)),
        });

        Ok(Self { listener, state })
    }

    /// Returns a handle for updating mesh runtime status exposed via IPC.
    pub(crate) fn status_handle(&self) -> IpcStatusHandle {
        IpcStatusHandle {
            runtime: Arc::clone(&self.state.runtime),
        }
    }

    /// Attach an initialized control node so quota and status requests can
    /// observe live control-plane state.
    pub async fn attach_control_node(&self, control: Arc<tokio::sync::RwLock<ControlNode>>) {
        let mut node_state = self.state.node_state.write().await;
        *node_state = NodeState::Control(control);
    }

    /// Attach an initialized edge node so status requests can observe live
    /// edge-side route state.
    pub async fn attach_edge_node(&self, edge: Arc<tokio::sync::RwLock<EdgeNode>>) {
        let mut node_state = self.state.node_state.write().await;
        *node_state = NodeState::Edge(edge);
    }

    /// Returns a handle for setting the node-role state after initialization.
    pub(crate) fn node_state_handle(&self) -> Arc<tokio::sync::RwLock<NodeState>> {
        Arc::clone(&self.state.node_state)
    }

    /// Run the IPC accept loop until shutdown.
    pub async fn run(&self, mut shutdown_rx: ShutdownRx) -> Result<()> {
        loop {
            tokio::select! {
                result = self.listener.accept() => {
                    match result {
                        Ok((stream, _addr)) => {
                            let state = Arc::clone(&self.state);
                            tokio::spawn(handle_connection(stream, state));
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to accept IPC connection");
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("IPC server shutting down");
                    break;
                }
            }
        }
        Ok(())
    }
}

/// Verify the connecting process has the same UID as this daemon.
///
/// Returns `Ok(())` if the peer UID matches, or writes an error response
/// and returns `Err` if authentication fails.
async fn authenticate_peer(stream: &UnixStream) -> std::result::Result<(), ()> {
    let peer_cred = match stream.peer_cred() {
        Ok(cred) => cred,
        Err(e) => {
            warn!(error = %e, "failed to get peer credentials");
            return Err(());
        }
    };

    let peer_uid = peer_cred.uid();
    // SAFETY: libc::getuid() is always safe to call and has no failure mode.
    let my_uid = unsafe { libc::getuid() };

    if peer_uid != my_uid {
        warn!(
            peer_uid = peer_uid,
            daemon_uid = my_uid,
            "IPC connection rejected: UID mismatch"
        );
        return Err(());
    }

    Ok(())
}

/// Handle a single IPC connection: authenticate, read request, dispatch, write response.
async fn handle_connection(mut stream: UnixStream, state: Arc<SharedState>) {
    if authenticate_peer(&stream).await.is_err() {
        let response = IpcResponse::Error {
            message: "unauthorized: UID mismatch".to_string(),
        };
        let _ = frame::write_json(&mut stream, &response).await;
        return;
    }

    let request: IpcRequest = match frame::read_json(&mut stream).await {
        Ok(req) => req,
        Err(e) => {
            warn!(error = %e, "failed to read IPC request");
            return;
        }
    };

    let response = dispatch(&request, &state).await;

    if let Err(e) = frame::write_json(&mut stream, &response).await {
        warn!(error = %e, "failed to write IPC response");
    }
}

/// Map an IPC request to a response.
async fn dispatch(request: &IpcRequest, state: &SharedState) -> IpcResponse {
    match request {
        IpcRequest::Status => build_status(state).await,
        IpcRequest::QuotaShow => handle_quota_show(state).await,
        IpcRequest::QuotaSet { endpoint_id, limit } => {
            handle_quota_set(state, endpoint_id, *limit).await
        }
        IpcRequest::Stop => {
            let _ = state.shutdown_tx.send(());
            IpcResponse::Ok {
                message: "shutting down".to_string(),
            }
        }
        IpcRequest::Reload => handle_reload(state).await,
        IpcRequest::Restart => {
            let _ = state.shutdown_tx.send(());
            IpcResponse::Ok {
                message: "restarting (shutdown initiated)".to_string(),
            }
        }
        IpcRequest::ExposeService {
            name,
            local_addr,
            protocol,
            health_check,
        } => handle_expose_service(state, name, local_addr, *protocol, health_check.clone()).await,
        IpcRequest::UnexposeService { name } => handle_unexpose_service(state, name).await,
        IpcRequest::AcceptNode { ticket, node_name } => {
            handle_accept_node(state, ticket, node_name.as_deref()).await
        }
    }
}

/// Build a status response with real data from the node state.
async fn build_status(state: &SharedState) -> IpcResponse {
    // Extract runtime fields under std::sync::RwLock, then drop the guard
    // before any .await (RwLockReadGuard is not Send).
    let (endpoint_id, endpoint_addr, online) = {
        let runtime = match state.runtime.read() {
            Ok(runtime) => runtime,
            Err(poisoned) => poisoned.into_inner(),
        };
        runtime.status_fields()
    };

    let node_state = state.node_state.read().await;
    let (peer_count, connected_nodes, services, route_table_version) = match &*node_state {
        NodeState::Control(cn) => {
            let cn = cn.read().await;
            let nodes: Vec<ConnectedNode> = cn
                .whitelist()
                .values()
                .map(|n| ConnectedNode {
                    name: n.node_name.clone(),
                    endpoint_id: n.endpoint_id.clone(),
                    online: n.is_online,
                })
                .collect();
            let svcs: Vec<ServiceStatus> = cn
                .services()
                .values()
                .map(|r| ServiceStatus {
                    name: r.service_id.service_name.clone(),
                    node_name: r.node_name.clone(),
                    assigned_port: r.published_port,
                    status: map_health_state(r.health_state),
                })
                .collect();
            let peer_count = nodes.iter().filter(|node| node.online).count();
            (peer_count, nodes, svcs, cn.route_version())
        }
        NodeState::Edge(en) => {
            let (
                cached_routes,
                route_table_version,
                direct_peer_ids,
                connection_pool,
                local_endpoint_id,
            ) = {
                let en = en.read().await;
                (
                    en.cached_routes().clone(),
                    en.route_version(),
                    en.tracked_peer_ids(),
                    en.connection_pool(),
                    en.local_endpoint_id(),
                )
            };
            let outbound_peer_ids = connection_pool.active_endpoint_ids().await;
            let peer_count = direct_peer_ids
                .into_iter()
                .chain(outbound_peer_ids.into_iter())
                .collect::<std::collections::BTreeSet<_>>()
                .len();

            let mut services = Vec::with_capacity(cached_routes.len());
            for (port, entry) in cached_routes {
                services.push(
                    build_edge_service_status(port, &entry, local_endpoint_id.as_str()).await,
                );
            }

            (peer_count, vec![], services, route_table_version)
        }
        NodeState::Uninit => (0, vec![], vec![], 0),
    };

    IpcResponse::Status(StatusInfo {
        role: format!("{:?}", state.config.role),
        node_name: state.config.node_name.clone(),
        endpoint_id,
        endpoint_addr,
        online,
        peer_count,
        connected_nodes,
        services,
        health_bind: state.config.health_bind.clone(),
        route_table_version,
    })
}

fn map_health_state(health_state: HealthState) -> ServiceDisplayStatus {
    match health_state {
        HealthState::Healthy => ServiceDisplayStatus::Healthy,
        HealthState::Degraded => ServiceDisplayStatus::Degraded,
        HealthState::Unhealthy => ServiceDisplayStatus::Unhealthy,
        HealthState::Unknown => ServiceDisplayStatus::Unknown,
    }
}

async fn build_edge_service_status(
    port: u16,
    route: &RouteEntry,
    local_endpoint_id: &str,
) -> ServiceStatus {
    let status = if route.endpoint_id == local_endpoint_id {
        probe_local_service(route).await
    } else {
        ServiceDisplayStatus::Routed
    };

    ServiceStatus {
        name: route.service_name.clone(),
        node_name: route.node_name.clone(),
        assigned_port: Some(port),
        status,
    }
}

async fn probe_local_service(route: &RouteEntry) -> ServiceDisplayStatus {
    let reachable = match route.protocol {
        Protocol::Tcp => timeout(
            STATUS_PROBE_TIMEOUT,
            TcpStream::connect(route.target_local_addr.as_str()),
        )
        .await
        .is_ok_and(|result| result.is_ok()),
        Protocol::Unix => probe_local_unix_service(route.target_local_addr.as_str()).await,
    };

    if reachable {
        ServiceDisplayStatus::Ok
    } else {
        ServiceDisplayStatus::Unreachable
    }
}

#[cfg(unix)]
async fn probe_local_unix_service(target_local_addr: &str) -> bool {
    timeout(
        STATUS_PROBE_TIMEOUT,
        tokio::net::UnixStream::connect(target_local_addr),
    )
    .await
    .is_ok_and(|result| result.is_ok())
}

#[cfg(not(unix))]
async fn probe_local_unix_service(_target_local_addr: &str) -> bool {
    false
}

async fn handle_quota_show(state: &SharedState) -> IpcResponse {
    if state.config.role != NodeRole::Control {
        return IpcResponse::Error {
            message: "quota is only available on control nodes".to_string(),
        };
    }

    let node_state = state.node_state.read().await;
    let NodeState::Control(cn) = &*node_state else {
        return IpcResponse::Error {
            message: "control node not initialized".to_string(),
        };
    };

    let cn = cn.read().await;
    IpcResponse::QuotaInfo {
        quotas: cn.quota_snapshot(),
    }
}

async fn handle_quota_set(state: &SharedState, endpoint_id: &str, limit: usize) -> IpcResponse {
    if state.config.role != NodeRole::Control {
        return IpcResponse::Error {
            message: "quota is only available on control nodes".to_string(),
        };
    }

    let limit = match u16::try_from(limit) {
        Ok(limit) => limit,
        Err(_) => {
            return IpcResponse::Error {
                message: format!("invalid quota limit {limit}: must be <= {}", u16::MAX),
            };
        }
    };

    let node_state = state.node_state.read().await;
    let NodeState::Control(cn) = &*node_state else {
        return IpcResponse::Error {
            message: "control node not initialized".to_string(),
        };
    };

    let mut cn = cn.write().await;
    match cn.set_quota_limit(endpoint_id, limit) {
        Ok(()) => IpcResponse::QuotaUpdated,
        Err(error) => IpcResponse::Error {
            message: error.to_string(),
        },
    }
}

/// Handle AcceptNode: validate ticket and add node to whitelist (control only).
async fn handle_accept_node(
    state: &SharedState,
    ticket_str: &str,
    node_name: Option<&str>,
) -> IpcResponse {
    if state.config.role != NodeRole::Control {
        return IpcResponse::Error {
            message: "accept is only available on control nodes".to_string(),
        };
    }

    let ticket = match mesh_proto::JoinTicket::from_bs58(ticket_str) {
        Ok(t) => t,
        Err(e) => {
            return IpcResponse::Error {
                message: format!("invalid ticket: {e}"),
            };
        }
    };

    let node_state = state.node_state.read().await;
    let NodeState::Control(cn) = &*node_state else {
        return IpcResponse::Error {
            message: "control node not initialized".to_string(),
        };
    };

    let mut cn = cn.write().await;

    // Validate ticket.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if let Err(e) = cn.validate_ticket(&ticket, &ticket.endpoint_id, now) {
        return IpcResponse::Error {
            message: format!("ticket validation failed: {e}"),
        };
    }

    let name = node_name.map(|n| n.to_owned()).unwrap_or_else(|| {
        format!(
            "node-{}",
            &ticket.endpoint_id[..8.min(ticket.endpoint_id.len())]
        )
    });

    let accepted_node = NodeInfo {
        endpoint_id: ticket.endpoint_id.clone(),
        node_name: name.clone(),
        quota_limit: DEFAULT_SERVICE_QUOTA as u16,
        quota_used: 0,
        is_online: false,
        last_heartbeat: None,
        addr: None,
    };

    if let Err(error) = cn.accept_node(accepted_node, now) {
        return IpcResponse::Error {
            message: format!("failed to persist accepted node: {error}"),
        };
    }

    IpcResponse::Ok {
        message: format!("node {name} ({}) accepted", ticket.endpoint_id),
    }
}

async fn handle_reload(state: &SharedState) -> IpcResponse {
    match state.reload_tx.send(()).await {
        Ok(()) => IpcResponse::Reloaded,
        Err(error) => IpcResponse::Error {
            message: format!("failed to trigger reload: {error}"),
        },
    }
}

/// Handle ExposeService: add service entry to config (edge only).
async fn handle_expose_service(
    state: &SharedState,
    name: &str,
    local_addr: &str,
    protocol: mesh_proto::Protocol,
    health_check: Option<mesh_proto::HealthCheckConfig>,
) -> IpcResponse {
    if state.config.role != NodeRole::Edge {
        return IpcResponse::Error {
            message: "expose is only available on edge nodes".to_string(),
        };
    }

    if let Err(error) = validate_expose_service_request(name, local_addr, health_check.as_ref()) {
        return IpcResponse::Error {
            message: error.to_string(),
        };
    }

    let requested_service = ServiceEntry {
        name: name.to_owned(),
        local_addr: local_addr.to_owned(),
        protocol,
        health_check: health_check.clone(),
    };

    let current_config = match load_mesh_config(&state.config_path).await {
        Ok(config) => config,
        Err(error) => {
            return IpcResponse::Error {
                message: format!("failed to update config.toml: {error}"),
            };
        }
    };

    if current_config
        .services
        .iter()
        .any(|service| service == &requested_service)
    {
        return match current_assigned_port(state, &requested_service).await {
            Some(assigned_port) => IpcResponse::ServiceExposed {
                name: name.to_owned(),
                assigned_port,
            },
            None => trigger_reload_and_wait_for_port_assignment(state, name).await,
        };
    }

    let mut document = match load_config_document(&state.config_path).await {
        Ok(document) => document,
        Err(error) => {
            return IpcResponse::Error {
                message: format!("failed to update config.toml: {error}"),
            };
        }
    };

    if let Err(error) = upsert_service_entry(
        &mut document,
        name,
        local_addr,
        protocol,
        health_check.as_ref(),
    ) {
        return IpcResponse::Error {
            message: format!("failed to update config.toml: {error}"),
        };
    }

    if let Err(error) = write_config_document_atomic(&state.config_path, &document).await {
        return IpcResponse::Error {
            message: format!("failed to update config.toml: {error}"),
        };
    }

    // Subscribe after the config write so stale registrations for the previous
    // definition do not satisfy this request.
    trigger_reload_and_wait_for_port_assignment(state, name).await
}

async fn handle_unexpose_service(state: &SharedState, name: &str) -> IpcResponse {
    if state.config.role != NodeRole::Edge {
        return IpcResponse::Error {
            message: "unexpose is only available on edge nodes".to_string(),
        };
    }

    if let Err(error) = mesh_proto::validate_service_name(name) {
        return IpcResponse::Error {
            message: format!("invalid service name: {error}"),
        };
    }

    let mut document = match load_config_document(&state.config_path).await {
        Ok(document) => document,
        Err(error) => {
            return IpcResponse::Error {
                message: format!("failed to update config.toml: {error}"),
            };
        }
    };

    let removed = match remove_service_entry(&mut document, name) {
        Ok(removed) => removed,
        Err(error) => {
            return IpcResponse::Error {
                message: format!("failed to update config.toml: {error}"),
            };
        }
    };

    if removed {
        if let Err(error) = write_config_document_atomic(&state.config_path, &document).await {
            return IpcResponse::Error {
                message: format!("failed to update config.toml: {error}"),
            };
        }

        if let Err(error) = state.reload_tx.send(()).await {
            return IpcResponse::Error {
                message: format!(
                    "service removed from config but failed to trigger reload: {error}"
                ),
            };
        }
    }

    IpcResponse::ServiceUnexposed {
        name: name.to_owned(),
    }
}

fn validate_expose_service_request(
    name: &str,
    local_addr: &str,
    health_check: Option<&mesh_proto::HealthCheckConfig>,
) -> Result<()> {
    mesh_proto::validate_service_name(name)
        .map_err(|error| anyhow::anyhow!("invalid service name: {error}"))?;
    mesh_proto::validate_local_addr(local_addr)
        .map_err(|error| anyhow::anyhow!("invalid local address: {error}"))?;

    if let Some(health_check) = health_check {
        validate_health_check_config(health_check)?;
    }

    Ok(())
}

fn validate_health_check_config(health_check: &mesh_proto::HealthCheckConfig) -> Result<()> {
    if health_check.interval_seconds == 0 {
        anyhow::bail!("invalid health check interval: must be greater than 0");
    }

    let Some(target) = &health_check.target else {
        return Ok(());
    };

    if target.is_empty() {
        anyhow::bail!("invalid health check target: must not be empty");
    }

    match health_check.mode {
        mesh_proto::HealthCheckMode::HttpGet => validate_http_health_target(target),
        mesh_proto::HealthCheckMode::TcpConnect | mesh_proto::HealthCheckMode::UnixConnect => {
            mesh_proto::validate_local_addr(target)
                .map_err(|error| anyhow::anyhow!("invalid health check target: {error}"))
        }
    }
}

fn validate_http_health_target(target: &str) -> Result<()> {
    let uri = target
        .parse::<hyper::Uri>()
        .context("invalid health check target: expected absolute http:// or https:// URL")?;

    match uri.scheme_str() {
        Some("http" | "https") => {}
        _ => anyhow::bail!("invalid health check target: expected http:// or https:// URL"),
    }

    let host = uri
        .host()
        .context("invalid health check target: missing host")?;
    mesh_proto::validate_health_target(host)
        .map_err(|error| anyhow::anyhow!("invalid health check target: {error}"))?;

    Ok(())
}

async fn load_config_document(path: &Path) -> Result<DocumentMut> {
    let content = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read config at {}", path.display()))?;
    DocumentMut::from_str(&content)
        .with_context(|| format!("failed to parse config at {}", path.display()))
}

fn upsert_service_entry(
    document: &mut DocumentMut,
    name: &str,
    local_addr: &str,
    protocol: mesh_proto::Protocol,
    health_check: Option<&mesh_proto::HealthCheckConfig>,
) -> Result<()> {
    let services = ensure_services_array(document)?;

    if let Some(existing_service) = services
        .iter_mut()
        .find(|table| table.get("name").and_then(|item| item.as_str()) == Some(name))
    {
        populate_service_table(existing_service, name, local_addr, protocol, health_check);
        return Ok(());
    }

    let mut service_table = Table::new();
    populate_service_table(&mut service_table, name, local_addr, protocol, health_check);
    services.push(service_table);
    Ok(())
}

fn ensure_services_array(document: &mut DocumentMut) -> Result<&mut ArrayOfTables> {
    let should_initialize = match document.as_table().get("services") {
        None => true,
        Some(Item::Value(value)) => value.as_array().is_some_and(|array| array.is_empty()),
        Some(_) => false,
    };

    if should_initialize {
        document["services"] = Item::ArrayOfTables(ArrayOfTables::new());
    }

    document["services"]
        .as_array_of_tables_mut()
        .context("services must be an array of tables")
}

fn remove_service_entry(document: &mut DocumentMut, name: &str) -> Result<bool> {
    let should_remove_key = {
        let Some(services) = get_services_array_mut(document)? else {
            return Ok(false);
        };
        let Some(index) = services
            .iter()
            .position(|table| table.get("name").and_then(|item| item.as_str()) == Some(name))
        else {
            return Ok(false);
        };

        services.remove(index);
        services.is_empty()
    };

    if should_remove_key {
        document.as_table_mut().remove("services");
    }

    Ok(true)
}

fn get_services_array_mut(document: &mut DocumentMut) -> Result<Option<&mut ArrayOfTables>> {
    let Some(item) = document.as_table_mut().get_mut("services") else {
        return Ok(None);
    };

    if item.is_none() {
        return Ok(None);
    }

    item.as_array_of_tables_mut()
        .map(Some)
        .context("services must be an array of tables")
}

fn populate_service_table(
    service_table: &mut Table,
    name: &str,
    local_addr: &str,
    protocol: mesh_proto::Protocol,
    health_check: Option<&mesh_proto::HealthCheckConfig>,
) {
    service_table["name"] = value(name);
    service_table["local_addr"] = value(local_addr);
    service_table["protocol"] = value(protocol_name(protocol));

    match health_check {
        Some(health_check) => {
            let mut health_table = Table::new();
            health_table["mode"] = value(health_check_mode_name(&health_check.mode));
            health_table["interval_seconds"] =
                value(i64::try_from(health_check.interval_seconds).unwrap_or(i64::MAX));

            if let Some(target) = &health_check.target {
                health_table["target"] = value(target.as_str());
            } else {
                health_table.remove("target");
            }

            service_table["health_check"] = Item::Table(health_table);
        }
        None => {
            service_table.remove("health_check");
        }
    }
}

fn protocol_name(protocol: mesh_proto::Protocol) -> &'static str {
    match protocol {
        mesh_proto::Protocol::Tcp => "tcp",
        mesh_proto::Protocol::Unix => "unix",
    }
}

fn health_check_mode_name(mode: &mesh_proto::HealthCheckMode) -> &'static str {
    match mode {
        mesh_proto::HealthCheckMode::TcpConnect => "tcp_connect",
        mesh_proto::HealthCheckMode::UnixConnect => "unix_connect",
        mesh_proto::HealthCheckMode::HttpGet => "http_get",
    }
}

async fn write_config_document_atomic(path: &Path, document: &DocumentMut) -> Result<()> {
    let path = path.to_path_buf();
    let content = document.to_string();

    task::spawn_blocking(move || write_config_document_atomic_blocking(&path, &content))
        .await
        .context("config write task panicked")?
}

async fn load_mesh_config(path: &Path) -> Result<MeshConfig> {
    let path = path.to_path_buf();
    task::spawn_blocking(move || MeshConfig::load(&path))
        .await
        .context("mesh config load task panicked")?
}

async fn current_assigned_port(
    state: &SharedState,
    requested_service: &ServiceEntry,
) -> Option<u16> {
    let node_state = state.node_state.read().await;
    let NodeState::Edge(edge_node) = &*node_state else {
        return None;
    };
    let edge_node = Arc::clone(edge_node);
    drop(node_state);

    let edge = edge_node.read().await;
    edge.cached_routes().iter().find_map(|(port, route)| {
        (route.service_name == requested_service.name
            && route.node_name == state.config.node_name
            && route.target_local_addr == requested_service.local_addr
            && route.protocol == requested_service.protocol)
            .then_some(*port)
    })
}

async fn trigger_reload_and_wait_for_port_assignment(
    state: &SharedState,
    service_name: &str,
) -> IpcResponse {
    let mut assignment_rx = state.port_assignment_notifier.subscribe(service_name);

    if let Err(error) = state.reload_tx.send(()).await {
        return IpcResponse::Error {
            message: format!("failed to trigger reload after expose request: {error}"),
        };
    }

    match wait_for_port_assignment(
        service_name,
        &mut assignment_rx,
        state.expose_assignment_timeout,
    )
    .await
    {
        Ok(assigned_port) => IpcResponse::ServiceExposed {
            name: service_name.to_owned(),
            assigned_port,
        },
        Err(ExposeAssignmentWaitError::TimedOut { timeout_seconds }) => {
            IpcResponse::ServiceExposeTimedOut {
                name: service_name.to_owned(),
                timeout_seconds,
            }
        }
        Err(ExposeAssignmentWaitError::ChannelClosed) => IpcResponse::Error {
            message: format!("port assignment channel closed while exposing '{service_name}'"),
        },
    }
}

enum ExposeAssignmentWaitError {
    TimedOut { timeout_seconds: u64 },
    ChannelClosed,
}

async fn wait_for_port_assignment(
    service_name: &str,
    assignment_rx: &mut broadcast::Receiver<u16>,
    timeout_duration: Duration,
) -> std::result::Result<u16, ExposeAssignmentWaitError> {
    let deadline = tokio::time::Instant::now() + timeout_duration;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(ExposeAssignmentWaitError::TimedOut {
                timeout_seconds: timeout_duration.as_secs().max(1),
            });
        }

        match timeout(remaining, assignment_rx.recv()).await {
            Ok(Ok(assigned_port)) => return Ok(assigned_port),
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                return Err(ExposeAssignmentWaitError::ChannelClosed);
            }
            Ok(Err(broadcast::error::RecvError::Lagged(skipped))) => {
                warn!(
                    service_name,
                    skipped, "lagged while waiting for assigned port; retrying"
                );
            }
            Err(_) => {
                return Err(ExposeAssignmentWaitError::TimedOut {
                    timeout_seconds: timeout_duration.as_secs().max(1),
                });
            }
        }
    }
}

fn write_config_document_atomic_blocking(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create config dir {}", parent.display()))?;
    }

    let tmp_path = path.with_extension("toml.tmp");
    let mut open_options = std::fs::OpenOptions::new();
    open_options.create(true).truncate(true).write(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        open_options.mode(0o600);
    }

    let mut file = open_options
        .open(&tmp_path)
        .with_context(|| format!("failed to create temp config at {}", tmp_path.display()))?;
    file.write_all(content.as_bytes())
        .with_context(|| format!("failed to write temp config at {}", tmp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to fsync temp config at {}", tmp_path.display()))?;

    std::fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to rename {} to {}",
            tmp_path.display(),
            path.display()
        )
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mesh_proto::{
        HealthCheckConfig, HealthCheckMode, NodeInfo, NodeRole, Protocol, RouteEntry,
        ServiceDisplayStatus, ServiceEntry, ServiceRegistration,
    };
    use tempfile::Builder;
    use tokio::sync::{broadcast, mpsc};
    use tokio::time::timeout;

    fn shared_state(role: NodeRole) -> (SharedState, tempfile::TempDir, mpsc::Receiver<()>) {
        let dir = Builder::new()
            .prefix("mesh-core-ipc-")
            .tempdir_in("/tmp")
            .unwrap();
        let config_path = dir.path().join("config.toml");
        let config = MeshConfig {
            role,
            data_dir: dir.path().join("data"),
            ..MeshConfig::default()
        };
        config.save(&config_path).unwrap();

        let (shutdown_tx, _) = broadcast::channel(1);
        let (reload_tx, reload_rx) = mpsc::channel(4);
        let state = SharedState {
            shutdown_tx,
            config,
            config_path,
            reload_tx,
            port_assignment_notifier: PortAssignmentNotifier::default(),
            expose_assignment_timeout: Duration::from_millis(200),
            runtime: Arc::new(RwLock::new(MeshRuntimeState::default())),
            node_state: Arc::new(tokio::sync::RwLock::new(NodeState::Uninit)),
        };

        (state, dir, reload_rx)
    }

    #[tokio::test]
    async fn test_status_reports_initializing_before_mesh_node_is_ready() {
        let (state, _dir, _reload_rx) = shared_state(NodeRole::Control);

        let response = dispatch(&IpcRequest::Status, &state).await;

        let IpcResponse::Status(info) = response else {
            panic!("expected status response");
        };
        assert_eq!(info.endpoint_id, INITIALIZING_ENDPOINT_ID);
        assert_eq!(info.endpoint_addr, None);
        assert!(!info.online);
        assert_eq!(info.peer_count, 0);
    }

    #[tokio::test]
    async fn test_status_reports_mesh_node_state_after_update() {
        let (state, _dir, _reload_rx) = shared_state(NodeRole::Control);
        let handle = IpcStatusHandle {
            runtime: Arc::clone(&state.runtime),
        };
        handle.set_mesh_node(
            "ep-123".to_string(),
            Some(r#"{"node_id":"ep-123"}"#.to_string()),
            true,
        );

        let response = dispatch(&IpcRequest::Status, &state).await;

        let IpcResponse::Status(info) = response else {
            panic!("expected status response");
        };
        assert_eq!(info.endpoint_id, "ep-123");
        assert_eq!(
            info.endpoint_addr.as_deref(),
            Some(r#"{"node_id":"ep-123"}"#)
        );
        assert!(info.online);
        assert_eq!(info.peer_count, 0);
    }

    #[tokio::test]
    async fn test_status_includes_service_node_name_for_control_services() {
        let (state, _dir, _reload_rx) = shared_state(NodeRole::Control);
        let mut control = ControlNode::new();
        control.add_node(NodeInfo {
            endpoint_id: "edge-1".to_string(),
            node_name: "node-alpha".to_string(),
            quota_limit: 5,
            quota_used: 1,
            is_online: true,
            last_heartbeat: None,
            addr: None,
        });
        control
            .register_services(
                "edge-1",
                "node-alpha",
                &[ServiceRegistration {
                    name: "web".to_string(),
                    local_addr: "127.0.0.1:8080".to_string(),
                    protocol: Protocol::Tcp,
                    health_check: None,
                }],
                1,
            )
            .unwrap();
        *state.node_state.write().await =
            NodeState::Control(Arc::new(tokio::sync::RwLock::new(control)));

        let response = dispatch(&IpcRequest::Status, &state).await;

        let IpcResponse::Status(info) = response else {
            panic!("expected status response");
        };
        assert_eq!(info.peer_count, 1);
        assert_eq!(info.services.len(), 1);
        assert_eq!(info.services[0].node_name, "node-alpha");
        assert_eq!(info.services[0].status, ServiceDisplayStatus::Unknown);
    }

    #[tokio::test]
    async fn test_status_reports_edge_peer_count_and_local_service_health() {
        let (state, _dir, _reload_rx) = shared_state(NodeRole::Edge);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let accept_task = tokio::spawn(async move {
            let _ = listener.accept().await.unwrap();
        });

        let mut edge = EdgeNode::new();
        let local_endpoint_id = edge.local_endpoint_id();
        let handle = IpcStatusHandle {
            runtime: Arc::clone(&state.runtime),
        };
        handle.set_mesh_node(local_endpoint_id.clone(), None, true);
        edge.record_peer_connected("control-1");
        edge.update_routes(
            std::iter::once((
                40000,
                RouteEntry {
                    service_name: "web".to_string(),
                    node_name: "edge-local".to_string(),
                    endpoint_id: local_endpoint_id,
                    target_local_addr: local_addr.to_string(),
                    protocol: Protocol::Tcp,
                },
            ))
            .collect(),
            1,
        );
        *state.node_state.write().await = NodeState::Edge(Arc::new(tokio::sync::RwLock::new(edge)));

        let response = dispatch(&IpcRequest::Status, &state).await;

        let IpcResponse::Status(info) = response else {
            panic!("expected status response");
        };
        assert_eq!(info.peer_count, 1);
        assert_eq!(info.services.len(), 1);
        assert_eq!(info.services[0].status, ServiceDisplayStatus::Ok);
        timeout(Duration::from_secs(1), accept_task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_status_marks_remote_edge_service_as_routed() {
        let (state, _dir, _reload_rx) = shared_state(NodeRole::Edge);
        let edge = EdgeNode::new();
        let local_endpoint_id = edge.local_endpoint_id();
        let handle = IpcStatusHandle {
            runtime: Arc::clone(&state.runtime),
        };
        handle.set_mesh_node(local_endpoint_id, None, true);

        let mut edge = edge;
        edge.update_routes(
            std::iter::once((
                40001,
                RouteEntry {
                    service_name: "api".to_string(),
                    node_name: "edge-remote".to_string(),
                    endpoint_id: "edge-remote".to_string(),
                    target_local_addr: "127.0.0.1:9000".to_string(),
                    protocol: Protocol::Tcp,
                },
            ))
            .collect(),
            2,
        );
        *state.node_state.write().await = NodeState::Edge(Arc::new(tokio::sync::RwLock::new(edge)));

        let response = dispatch(&IpcRequest::Status, &state).await;

        let IpcResponse::Status(info) = response else {
            panic!("expected status response");
        };
        assert_eq!(info.services.len(), 1);
        assert_eq!(info.services[0].status, ServiceDisplayStatus::Routed);
    }

    #[tokio::test]
    async fn test_accept_node_requires_control_role() {
        let (state, _dir, _reload_rx) = shared_state(NodeRole::Edge);

        let response = dispatch(
            &IpcRequest::AcceptNode {
                ticket: "dummy".to_string(),
                node_name: None,
            },
            &state,
        )
        .await;

        let IpcResponse::Error { message } = response else {
            panic!("expected error response");
        };
        assert!(message.contains("control nodes"));
    }

    #[tokio::test]
    async fn test_expose_service_requires_edge_role() {
        let (state, _dir, _reload_rx) = shared_state(NodeRole::Control);

        let response = dispatch(
            &IpcRequest::ExposeService {
                name: "web".to_string(),
                local_addr: "127.0.0.1:8080".to_string(),
                protocol: mesh_proto::Protocol::Tcp,
                health_check: None,
            },
            &state,
        )
        .await;

        let IpcResponse::Error { message } = response else {
            panic!("expected error response");
        };
        assert!(message.contains("edge nodes"));
    }

    #[tokio::test]
    async fn test_reload_triggers_daemon_signal() {
        let (state, _dir, mut reload_rx) = shared_state(NodeRole::Edge);

        let response = dispatch(&IpcRequest::Reload, &state).await;

        assert!(matches!(response, IpcResponse::Reloaded));
        assert_eq!(reload_rx.recv().await, Some(()));
    }

    #[tokio::test]
    async fn test_quota_show_returns_control_snapshot() {
        let (state, _dir, _reload_rx) = shared_state(NodeRole::Control);
        let mut control = ControlNode::new();
        control.add_node(NodeInfo {
            endpoint_id: "edge-1".to_string(),
            node_name: "node-alpha".to_string(),
            quota_limit: 5,
            quota_used: 0,
            is_online: true,
            last_heartbeat: None,
            addr: None,
        });
        control
            .register_services(
                "edge-1",
                "node-alpha",
                &[ServiceRegistration {
                    name: "web".to_string(),
                    local_addr: "127.0.0.1:8080".to_string(),
                    protocol: Protocol::Tcp,
                    health_check: None,
                }],
                1,
            )
            .unwrap();
        *state.node_state.write().await =
            NodeState::Control(Arc::new(tokio::sync::RwLock::new(control)));

        let response = dispatch(&IpcRequest::QuotaShow, &state).await;

        let IpcResponse::QuotaInfo { quotas } = response else {
            panic!("expected quota info response");
        };
        assert_eq!(quotas, vec![("edge-1".to_string(), 1, 5)]);
    }

    #[tokio::test]
    async fn test_quota_set_updates_existing_limit() {
        let (state, _dir, _reload_rx) = shared_state(NodeRole::Control);
        let mut control = ControlNode::new();
        control.add_node(NodeInfo {
            endpoint_id: "edge-1".to_string(),
            node_name: "node-alpha".to_string(),
            quota_limit: 5,
            quota_used: 0,
            is_online: true,
            last_heartbeat: None,
            addr: None,
        });
        let control = Arc::new(tokio::sync::RwLock::new(control));
        *state.node_state.write().await = NodeState::Control(Arc::clone(&control));

        let response = dispatch(
            &IpcRequest::QuotaSet {
                endpoint_id: "edge-1".to_string(),
                limit: 9,
            },
            &state,
        )
        .await;

        assert!(matches!(response, IpcResponse::QuotaUpdated));
        assert_eq!(control.read().await.whitelist()["edge-1"].quota_limit, 9);
    }

    #[tokio::test]
    async fn test_expose_service_writes_service_to_config() {
        let (state, _dir, mut reload_rx) = shared_state(NodeRole::Edge);
        let health_check = Some(HealthCheckConfig {
            mode: HealthCheckMode::HttpGet,
            target: Some("http://127.0.0.1:8080/health".to_string()),
            interval_seconds: 30,
        });

        let notifier = state.port_assignment_notifier.clone();
        let response_request = IpcRequest::ExposeService {
            name: "web".to_string(),
            local_addr: "127.0.0.1:8080".to_string(),
            protocol: Protocol::Tcp,
            health_check: health_check.clone(),
        };
        let notify_assignment = async {
            assert_eq!(reload_rx.recv().await, Some(()));
            notifier.notify_assignments(&[mesh_proto::PortAssignment {
                service_name: "web".to_string(),
                assigned_port: 41000,
            }]);
        };
        let (response, ()) = tokio::join!(dispatch(&response_request, &state), notify_assignment);

        let IpcResponse::ServiceExposed {
            name,
            assigned_port,
        } = response
        else {
            panic!("expected service exposed response");
        };
        assert_eq!(name, "web");
        assert_eq!(assigned_port, 41000);

        let updated = MeshConfig::load(&state.config_path).unwrap();
        assert_eq!(
            updated.services,
            vec![ServiceEntry {
                name: "web".to_string(),
                local_addr: "127.0.0.1:8080".to_string(),
                protocol: Protocol::Tcp,
                health_check,
            }]
        );
    }

    #[tokio::test]
    async fn test_expose_service_updates_existing_service_entry() {
        let (state, _dir, mut reload_rx) = shared_state(NodeRole::Edge);
        let original = MeshConfig {
            services: vec![ServiceEntry {
                name: "web".to_string(),
                local_addr: "127.0.0.1:8080".to_string(),
                protocol: Protocol::Tcp,
                health_check: None,
            }],
            ..state.config.clone()
        };
        original.save(&state.config_path).unwrap();

        let notifier = state.port_assignment_notifier.clone();
        let response_request = IpcRequest::ExposeService {
            name: "web".to_string(),
            local_addr: "/tmp/web.sock".to_string(),
            protocol: Protocol::Unix,
            health_check: None,
        };
        let notify_assignment = async {
            assert_eq!(reload_rx.recv().await, Some(()));
            notifier.notify_assignments(&[mesh_proto::PortAssignment {
                service_name: "web".to_string(),
                assigned_port: 41001,
            }]);
        };
        let (response, ()) = tokio::join!(dispatch(&response_request, &state), notify_assignment);

        assert!(matches!(response, IpcResponse::ServiceExposed { .. }));

        let updated = MeshConfig::load(&state.config_path).unwrap();
        assert_eq!(updated.services.len(), 1);
        assert_eq!(
            updated.services[0],
            ServiceEntry {
                name: "web".to_string(),
                local_addr: "/tmp/web.sock".to_string(),
                protocol: Protocol::Unix,
                health_check: None,
            }
        );
    }

    #[tokio::test]
    async fn test_expose_service_returns_existing_port_when_service_is_unchanged() {
        let (state, _dir, mut reload_rx) = shared_state(NodeRole::Edge);
        let service = ServiceEntry {
            name: "web".to_string(),
            local_addr: "127.0.0.1:8080".to_string(),
            protocol: Protocol::Tcp,
            health_check: None,
        };
        let original = MeshConfig {
            services: vec![service.clone()],
            ..state.config.clone()
        };
        original.save(&state.config_path).unwrap();

        let edge_node = Arc::new(tokio::sync::RwLock::new(EdgeNode::new()));
        {
            let mut edge = edge_node.write().await;
            edge.update_routes(
                std::iter::once((
                    41002,
                    RouteEntry {
                        service_name: service.name.clone(),
                        node_name: state.config.node_name.clone(),
                        endpoint_id: "edge-1".to_string(),
                        target_local_addr: service.local_addr.clone(),
                        protocol: service.protocol,
                    },
                ))
                .collect(),
                1,
            );
        }
        *state.node_state.write().await = NodeState::Edge(edge_node);

        let response = dispatch(
            &IpcRequest::ExposeService {
                name: service.name.clone(),
                local_addr: service.local_addr.clone(),
                protocol: service.protocol,
                health_check: service.health_check.clone(),
            },
            &state,
        )
        .await;

        let IpcResponse::ServiceExposed {
            name,
            assigned_port,
        } = response
        else {
            panic!("expected service exposed response");
        };
        assert_eq!(name, "web");
        assert_eq!(assigned_port, 41002);
        assert!(
            timeout(Duration::from_millis(50), reload_rx.recv())
                .await
                .is_err(),
            "unchanged expose should not trigger reload"
        );
    }

    #[tokio::test]
    async fn test_expose_service_retries_unchanged_config_until_port_assignment_arrives() {
        let (state, _dir, mut reload_rx) = shared_state(NodeRole::Edge);
        let service = ServiceEntry {
            name: "web".to_string(),
            local_addr: "127.0.0.1:8080".to_string(),
            protocol: Protocol::Tcp,
            health_check: None,
        };
        MeshConfig {
            services: vec![service.clone()],
            ..state.config.clone()
        }
        .save(&state.config_path)
        .unwrap();

        let notifier = state.port_assignment_notifier.clone();
        let response_request = IpcRequest::ExposeService {
            name: service.name.clone(),
            local_addr: service.local_addr.clone(),
            protocol: service.protocol,
            health_check: service.health_check.clone(),
        };
        let notify_assignment = async {
            assert_eq!(reload_rx.recv().await, Some(()));
            notifier.notify_assignments(&[mesh_proto::PortAssignment {
                service_name: service.name.clone(),
                assigned_port: 41003,
            }]);
        };
        let (response, ()) = tokio::join!(dispatch(&response_request, &state), notify_assignment);

        let IpcResponse::ServiceExposed {
            name,
            assigned_port,
        } = response
        else {
            panic!("expected service exposed response");
        };
        assert_eq!(name, "web");
        assert_eq!(assigned_port, 41003);

        let updated = MeshConfig::load(&state.config_path).unwrap();
        assert_eq!(updated.services, vec![service]);
    }

    #[tokio::test]
    async fn test_expose_service_returns_timeout_when_assignment_is_missing() {
        let (state, _dir, mut reload_rx) = shared_state(NodeRole::Edge);

        let response_request = IpcRequest::ExposeService {
            name: "web".to_string(),
            local_addr: "127.0.0.1:8080".to_string(),
            protocol: Protocol::Tcp,
            health_check: None,
        };
        let wait_for_reload = async {
            assert_eq!(reload_rx.recv().await, Some(()));
        };
        let (response, ()) = tokio::join!(dispatch(&response_request, &state), wait_for_reload);

        let IpcResponse::ServiceExposeTimedOut {
            name,
            timeout_seconds,
        } = response
        else {
            panic!("expected service expose timed out response");
        };
        assert_eq!(name, "web");
        assert_eq!(timeout_seconds, 1);
    }

    #[tokio::test]
    async fn test_unexpose_service_removes_service_from_config_and_triggers_reload() {
        let (state, _dir, mut reload_rx) = shared_state(NodeRole::Edge);
        let original = MeshConfig {
            services: vec![
                ServiceEntry {
                    name: "web".to_string(),
                    local_addr: "127.0.0.1:8080".to_string(),
                    protocol: Protocol::Tcp,
                    health_check: None,
                },
                ServiceEntry {
                    name: "ssh".to_string(),
                    local_addr: "127.0.0.1:22".to_string(),
                    protocol: Protocol::Tcp,
                    health_check: None,
                },
            ],
            ..state.config.clone()
        };
        original.save(&state.config_path).unwrap();

        let response = dispatch(
            &IpcRequest::UnexposeService {
                name: "web".to_string(),
            },
            &state,
        )
        .await;

        let IpcResponse::ServiceUnexposed { name } = response else {
            panic!("expected service unexposed response");
        };
        assert_eq!(name, "web");
        assert_eq!(reload_rx.recv().await, Some(()));

        let updated = MeshConfig::load(&state.config_path).unwrap();
        assert_eq!(
            updated.services,
            vec![ServiceEntry {
                name: "ssh".to_string(),
                local_addr: "127.0.0.1:22".to_string(),
                protocol: Protocol::Tcp,
                health_check: None,
            }]
        );
    }

    #[tokio::test]
    async fn test_expose_service_rejects_public_http_health_target() {
        let (state, _dir, _reload_rx) = shared_state(NodeRole::Edge);

        let response = dispatch(
            &IpcRequest::ExposeService {
                name: "web".to_string(),
                local_addr: "127.0.0.1:8080".to_string(),
                protocol: Protocol::Tcp,
                health_check: Some(HealthCheckConfig {
                    mode: HealthCheckMode::HttpGet,
                    target: Some("https://example.com/health".to_string()),
                    interval_seconds: 30,
                }),
            },
            &state,
        )
        .await;

        let IpcResponse::Error { message } = response else {
            panic!("expected error response");
        };
        assert!(message.contains("health check target"));

        let updated = MeshConfig::load(&state.config_path).unwrap();
        assert!(updated.services.is_empty());
    }
}
