use std::path::Path;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::Result;
use mesh_proto::{IpcRequest, IpcResponse, MeshConfig, StatusInfo};
use tokio::net::{UnixListener, UnixStream};
use tracing::{info, warn};

use crate::daemon::{ShutdownRx, ShutdownTx};
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

/// Shared state accessible to IPC connection handlers.
struct SharedState {
    shutdown_tx: ShutdownTx,
    config: MeshConfig,
    runtime: Arc<RwLock<MeshRuntimeState>>,
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
        });

        Ok(Self { listener, state })
    }

    /// Returns a handle for updating mesh runtime status exposed via IPC.
    pub(crate) fn status_handle(&self) -> IpcStatusHandle {
        IpcStatusHandle {
            runtime: Arc::clone(&self.state.runtime),
        }
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

/// Handle a single IPC connection: read request, dispatch, write response.
async fn handle_connection(mut stream: UnixStream, state: Arc<SharedState>) {
    let request: IpcRequest = match frame::read_json(&mut stream).await {
        Ok(req) => req,
        Err(e) => {
            warn!(error = %e, "failed to read IPC request");
            return;
        }
    };

    let response = dispatch(&request, &state);

    if let Err(e) = frame::write_json(&mut stream, &response).await {
        warn!(error = %e, "failed to write IPC response");
    }
}

/// Map an IPC request to a response.
fn dispatch(request: &IpcRequest, state: &SharedState) -> IpcResponse {
    match request {
        IpcRequest::Status => {
            let runtime = match state.runtime.read() {
                Ok(runtime) => runtime,
                Err(poisoned) => poisoned.into_inner(),
            };
            let (endpoint_id, online) = runtime.status_fields();

            IpcResponse::Status(StatusInfo {
                role: format!("{:?}", state.config.role),
                node_name: state.config.node_name.clone(),
                endpoint_id,
                online,
                connected_nodes: vec![],
                services: vec![],
                health_bind: state.config.health_bind.clone(),
                route_table_version: 0,
            })
        }
        IpcRequest::Stop => {
            let _ = state.shutdown_tx.send(());
            IpcResponse::Ok {
                message: "shutting down".to_string(),
            }
        }
        IpcRequest::Reload => IpcResponse::Ok {
            message: "reload not yet implemented".to_string(),
        },
        IpcRequest::Restart => IpcResponse::Error {
            message: "restart not yet implemented".to_string(),
        },
        IpcRequest::ExposeService { .. } => IpcResponse::Error {
            message: "expose not yet implemented".to_string(),
        },
        IpcRequest::AcceptNode { .. } => IpcResponse::Error {
            message: "accept not yet implemented".to_string(),
        },
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
        }
    }

    #[test]
    fn test_status_reports_initializing_before_mesh_node_is_ready() {
        let state = shared_state();

        let response = dispatch(&IpcRequest::Status, &state);

        let IpcResponse::Status(info) = response else {
            panic!("expected status response");
        };
        assert_eq!(info.endpoint_id, INITIALIZING_ENDPOINT_ID);
        assert!(!info.online);
    }

    #[test]
    fn test_status_reports_mesh_node_state_after_update() {
        let state = shared_state();
        let handle = IpcStatusHandle {
            runtime: Arc::clone(&state.runtime),
        };
        handle.set_mesh_node("ep-123".to_string(), true);

        let response = dispatch(&IpcRequest::Status, &state);

        let IpcResponse::Status(info) = response else {
            panic!("expected status response");
        };
        assert_eq!(info.endpoint_id, "ep-123");
        assert!(info.online);
    }
}
