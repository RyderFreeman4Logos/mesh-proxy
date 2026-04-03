use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::{Context, Result};
use mesh_proto::{
    ConnectedNode, DEFAULT_SERVICE_QUOTA, IpcRequest, IpcResponse, MeshConfig, NodeInfo, NodeRole,
    ServiceStatus, StatusInfo,
};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use toml_edit::{ArrayOfTables, DocumentMut, Item, Table, value};
use tracing::{info, warn};

use crate::control_node::ControlNode;
use crate::daemon::{ShutdownRx, ShutdownTx};
use crate::edge_node::EdgeNode;
use mesh_proto::frame;

const INITIALIZING_ENDPOINT_ID: &str = "(initializing)";

#[derive(Debug, Clone, Default)]
struct MeshRuntimeState {
    endpoint_id: Option<String>,
    online: bool,
}

impl MeshRuntimeState {
    fn status_fields(&self) -> (String, bool) {
        match &self.endpoint_id {
            Some(endpoint_id) => (endpoint_id.clone(), self.online),
            None => (INITIALIZING_ENDPOINT_ID.to_string(), false),
        }
    }
}

#[derive(Clone)]
pub(crate) struct IpcStatusHandle {
    runtime: Arc<RwLock<MeshRuntimeState>>,
}

impl IpcStatusHandle {
    /// Publish the daemon's current mesh endpoint state for status requests.
    pub fn set_mesh_node(&self, endpoint_id: String, online: bool) {
        let mut runtime = match self.runtime.write() {
            Ok(runtime) => runtime,
            Err(poisoned) => poisoned.into_inner(),
        };
        *runtime = MeshRuntimeState {
            endpoint_id: Some(endpoint_id),
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
        IpcRequest::AcceptNode { ticket, node_name } => {
            handle_accept_node(state, ticket, node_name.as_deref()).await
        }
    }
}

/// Build a status response with real data from the node state.
async fn build_status(state: &SharedState) -> IpcResponse {
    // Extract runtime fields under std::sync::RwLock, then drop the guard
    // before any .await (RwLockReadGuard is not Send).
    let (endpoint_id, online) = {
        let runtime = match state.runtime.read() {
            Ok(runtime) => runtime,
            Err(poisoned) => poisoned.into_inner(),
        };
        runtime.status_fields()
    };

    let node_state = state.node_state.read().await;
    let (connected_nodes, services, route_table_version) = match &*node_state {
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
                    assigned_port: r.published_port,
                    status: format!("{:?}", r.health_state),
                })
                .collect();
            (nodes, svcs, cn.route_version())
        }
        NodeState::Edge(en) => {
            let en = en.read().await;
            let svcs: Vec<ServiceStatus> = en
                .cached_routes()
                .iter()
                .map(|(port, entry)| ServiceStatus {
                    name: entry.service_name.clone(),
                    assigned_port: Some(*port),
                    status: "cached".to_string(),
                })
                .collect();
            (vec![], svcs, en.route_version())
        }
        NodeState::Uninit => (vec![], vec![], 0),
    };

    IpcResponse::Status(StatusInfo {
        role: format!("{:?}", state.config.role),
        node_name: state.config.node_name.clone(),
        endpoint_id,
        online,
        connected_nodes,
        services,
        health_bind: state.config.health_bind.clone(),
        route_table_version,
    })
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

    cn.add_node(NodeInfo {
        endpoint_id: ticket.endpoint_id.clone(),
        node_name: name.clone(),
        quota_limit: DEFAULT_SERVICE_QUOTA as u16,
        quota_used: 0,
        is_online: false,
        last_heartbeat: None,
        addr: None,
    });

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

    if let Err(error) = load_config_document(&state.config_path)
        .and_then(|mut document| {
            upsert_service_entry(
                &mut document,
                name,
                local_addr,
                protocol,
                health_check.as_ref(),
            )?;
            Ok(document)
        })
        .and_then(|document| write_config_document_atomic(&state.config_path, &document))
    {
        return IpcResponse::Error {
            message: format!("failed to update config.toml: {error}"),
        };
    }

    IpcResponse::ServiceExposed {
        name: name.to_owned(),
        assigned_port: Some(0),
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

fn load_config_document(path: &Path) -> Result<DocumentMut> {
    let content = std::fs::read_to_string(path)
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

fn write_config_document_atomic(path: &Path, document: &DocumentMut) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create config dir {}", parent.display()))?;
    }

    let tmp_path = path.with_extension("toml.tmp");
    let mut file = std::fs::File::create(&tmp_path)
        .with_context(|| format!("failed to create temp config at {}", tmp_path.display()))?;
    file.write_all(document.to_string().as_bytes())
        .with_context(|| format!("failed to write temp config at {}", tmp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to fsync temp config at {}", tmp_path.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to set permissions on {}", tmp_path.display()))?;
    }

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
    use mesh_proto::{HealthCheckConfig, HealthCheckMode, NodeRole, Protocol, ServiceEntry};
    use tempfile::Builder;
    use tokio::sync::{broadcast, mpsc};

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
        assert!(!info.online);
    }

    #[tokio::test]
    async fn test_status_reports_mesh_node_state_after_update() {
        let (state, _dir, _reload_rx) = shared_state(NodeRole::Control);
        let handle = IpcStatusHandle {
            runtime: Arc::clone(&state.runtime),
        };
        handle.set_mesh_node("ep-123".to_string(), true);

        let response = dispatch(&IpcRequest::Status, &state).await;

        let IpcResponse::Status(info) = response else {
            panic!("expected status response");
        };
        assert_eq!(info.endpoint_id, "ep-123");
        assert!(info.online);
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
    async fn test_expose_service_writes_service_to_config() {
        let (state, _dir, _reload_rx) = shared_state(NodeRole::Edge);
        let health_check = Some(HealthCheckConfig {
            mode: HealthCheckMode::HttpGet,
            target: Some("http://127.0.0.1:8080/health".to_string()),
            interval_seconds: 30,
        });

        let response = dispatch(
            &IpcRequest::ExposeService {
                name: "web".to_string(),
                local_addr: "127.0.0.1:8080".to_string(),
                protocol: Protocol::Tcp,
                health_check: health_check.clone(),
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
        assert_eq!(assigned_port, Some(0));

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
        let (state, _dir, _reload_rx) = shared_state(NodeRole::Edge);
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

        let response = dispatch(
            &IpcRequest::ExposeService {
                name: "web".to_string(),
                local_addr: "/tmp/web.sock".to_string(),
                protocol: Protocol::Unix,
                health_check: None,
            },
            &state,
        )
        .await;

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
