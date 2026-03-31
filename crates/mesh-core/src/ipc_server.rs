use std::path::Path;

use anyhow::Result;
use tokio::net::{UnixListener, UnixStream};
use tracing::{info, warn};

use crate::daemon::ShutdownRx;

/// Unix domain socket IPC server for CLI-daemon communication.
pub struct IpcServer {
    listener: UnixListener,
}

impl IpcServer {
    /// Bind to the given socket path, removing any stale socket first.
    pub async fn bind(socket_path: &Path) -> Result<Self> {
        if socket_path.exists() {
            std::fs::remove_file(socket_path)?;
        }

        if let Some(parent) = socket_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let listener = UnixListener::bind(socket_path)?;
        info!(path = %socket_path.display(), "IPC server listening");
        Ok(Self { listener })
    }

    /// Run the IPC accept loop until shutdown.
    pub async fn run(&self, mut shutdown_rx: ShutdownRx) -> Result<()> {
        loop {
            tokio::select! {
                result = self.listener.accept() => {
                    match result {
                        Ok((stream, _addr)) => {
                            tokio::spawn(Self::handle_connection(stream));
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

    /// Handle a single IPC connection.
    async fn handle_connection(_stream: UnixStream) {
        // TODO(1.4): implement frame read/write protocol
    }
}
