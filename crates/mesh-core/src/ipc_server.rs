use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use mesh_proto::{IpcRequest, IpcResponse, MeshConfig, StatusInfo};
use tokio::net::{UnixListener, UnixStream};
use tracing::{info, warn};

use crate::daemon::{ShutdownRx, ShutdownTx};
use crate::frame;

/// Shared state accessible to IPC connection handlers.
struct SharedState {
    shutdown_tx: ShutdownTx,
    config: MeshConfig,
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
        });

        Ok(Self { listener, state })
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
        IpcRequest::Status => IpcResponse::Status(StatusInfo {
            role: format!("{:?}", state.config.role),
            node_name: state.config.node_name.clone(),
            endpoint_id: String::new(), // TODO(1.7): real endpoint id
            online: false,              // TODO(1.7): real online status
            connected_nodes: vec![],
            services: vec![],
        }),
        IpcRequest::Stop => {
            let _ = state.shutdown_tx.send(());
            IpcResponse::Ok {
                message: "shutting down".to_string(),
            }
        }
        IpcRequest::Reload => IpcResponse::Ok {
            message: "reload not yet implemented".to_string(),
        },
        IpcRequest::AcceptNode { .. } => IpcResponse::Error {
            message: "accept not yet implemented".to_string(),
        },
    }
}
