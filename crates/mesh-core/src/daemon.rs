use std::path::PathBuf;

use anyhow::Result;
use mesh_proto::MeshConfig;
use tokio::sync::broadcast;
use tracing::{error, info};

use crate::ipc_server::IpcServer;

/// Shutdown signal type.
pub type ShutdownTx = broadcast::Sender<()>;
pub type ShutdownRx = broadcast::Receiver<()>;

/// The mesh-proxy daemon engine.
pub struct Daemon {
    config: MeshConfig,
    shutdown_tx: ShutdownTx,
}

impl Daemon {
    /// Create a new daemon from the loaded configuration.
    pub fn new(config: MeshConfig) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self { config, shutdown_tx }
    }

    /// Returns a new shutdown receiver.
    pub fn shutdown_rx(&self) -> ShutdownRx {
        self.shutdown_tx.subscribe()
    }

    /// Returns the shutdown sender (for signal handlers / IPC stop command).
    pub fn shutdown_tx(&self) -> &ShutdownTx {
        &self.shutdown_tx
    }

    /// Returns the loaded configuration.
    pub fn config(&self) -> &MeshConfig {
        &self.config
    }

    /// Returns the Unix domain socket path for IPC.
    pub fn socket_path(&self) -> PathBuf {
        self.config.data_dir.join("daemon.sock")
    }

    /// Returns the PID file path.
    pub fn pid_path(&self) -> PathBuf {
        self.config.data_dir.join("daemon.pid")
    }

    /// Main daemon loop. Starts IPC server and waits for shutdown.
    pub async fn run(&self) -> Result<()> {
        info!(
            role = ?self.config.role,
            node_name = %self.config.node_name,
            "daemon starting"
        );

        // Start IPC server
        let socket_path = self.socket_path();
        let ipc_server =
            IpcServer::bind(&socket_path, self.shutdown_tx.clone(), self.config.clone()).await?;
        let ipc_shutdown_rx = self.shutdown_rx();
        let ipc_handle = tokio::spawn(async move {
            if let Err(e) = ipc_server.run(ipc_shutdown_rx).await {
                error!(error = %e, "IPC server error");
            }
        });

        // TODO(1.7): Initialize iroh endpoint

        // Wait for shutdown signal
        let mut shutdown_rx = self.shutdown_rx();
        shutdown_rx.recv().await.ok();
        info!("shutdown signal received");

        // Wait for IPC server to finish
        ipc_handle.await.ok();

        // TODO(1.6): Cleanup (remove socket, PID file)
        info!("daemon stopped");
        Ok(())
    }
}
