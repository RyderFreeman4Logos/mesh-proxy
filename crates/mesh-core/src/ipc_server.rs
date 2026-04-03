use std::path::Path;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::Result;
use mesh_proto::{
    ConnectedNode, DEFAULT_SERVICE_QUOTA, IpcRequest, IpcResponse, MeshConfig, NodeInfo, NodeRole,
    ServiceStatus, StatusInfo,
};
use tokio::net::{UnixListener, UnixStream};
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
        IpcRequest::Reload => IpcResponse::Reloaded,
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

/// Handle ExposeService: add service entry to config (edge only).
async fn handle_expose_service(
    state: &SharedState,
    name: &str,
    local_addr: &str,
    _protocol: mesh_proto::Protocol,
    _health_check: Option<mesh_proto::HealthCheckConfig>,
) -> IpcResponse {
    if state.config.role != NodeRole::Edge {
        return IpcResponse::Error {
            message: "expose is only available on edge nodes".to_string(),
        };
    }

    // Validate inputs.
    if let Err(e) = mesh_proto::validate_service_name(name) {
        return IpcResponse::Error {
            message: format!("invalid service name: {e}"),
        };
    }
    if let Err(e) = mesh_proto::validate_local_addr(local_addr) {
        return IpcResponse::Error {
            message: format!("invalid local address: {e}"),
        };
    }

    // Note: In a full implementation this would also update the config file
    // on disk and trigger a re-registration with the control node.
    // For now, we acknowledge the request.
    IpcResponse::ServiceExposed {
        name: name.to_owned(),
        assigned_port: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mesh_proto::NodeRole;
    use tokio::sync::broadcast;

    fn shared_state() -> SharedState {
        let (shutdown_tx, _) = broadcast::channel(1);
        SharedState {
            shutdown_tx,
            config: MeshConfig {
                role: NodeRole::Control,
                ..MeshConfig::default()
            },
            runtime: Arc::new(RwLock::new(MeshRuntimeState::default())),
            node_state: Arc::new(tokio::sync::RwLock::new(NodeState::Uninit)),
        }
    }

    #[tokio::test]
    async fn test_status_reports_initializing_before_mesh_node_is_ready() {
        let state = shared_state();

        let response = dispatch(&IpcRequest::Status, &state).await;

        let IpcResponse::Status(info) = response else {
            panic!("expected status response");
        };
        assert_eq!(info.endpoint_id, INITIALIZING_ENDPOINT_ID);
        assert!(!info.online);
    }

    #[tokio::test]
    async fn test_status_reports_mesh_node_state_after_update() {
        let state = shared_state();
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
        let (shutdown_tx, _) = broadcast::channel(1);
        let state = SharedState {
            shutdown_tx,
            config: MeshConfig {
                role: NodeRole::Edge,
                ..MeshConfig::default()
            },
            runtime: Arc::new(RwLock::new(MeshRuntimeState::default())),
            node_state: Arc::new(tokio::sync::RwLock::new(NodeState::Uninit)),
        };

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
        let state = shared_state(); // role = Control

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
}
