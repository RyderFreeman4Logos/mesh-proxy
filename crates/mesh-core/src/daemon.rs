use std::path::PathBuf;

use anyhow::Result;
use mesh_proto::MeshConfig;
use tokio::sync::broadcast;
use tracing::info;

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

    /// Main daemon loop. Starts subsystems and waits for shutdown.
    pub async fn run(&self) -> Result<()> {
        info!(
            role = ?self.config.role,
            node_name = %self.config.node_name,
            "daemon starting"
        );

        let mut shutdown_rx = self.shutdown_rx();

        // TODO(1.4): Start IPC server
        // TODO(1.7): Initialize iroh endpoint

        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("shutdown signal received");
            }
        }

        // TODO(1.6): Cleanup (remove socket, PID file)
        info!("daemon stopped");
        Ok(())
    }
}
