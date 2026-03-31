use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use mesh_proto::MeshConfig;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

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

    /// Main daemon loop. Starts IPC server, installs signal handlers, and
    /// waits for shutdown.
    pub async fn run(&self) -> Result<()> {
        info!(
            role = ?self.config.role,
            node_name = %self.config.node_name,
            "daemon starting"
        );

        let socket_path = self.socket_path();
        let pid_path = self.pid_path();

        // Start IPC server
        let ipc_server = crate::ipc_server::IpcServer::bind(
            &socket_path,
            self.shutdown_tx.clone(),
            self.config.clone(),
        )
        .await?;
        let ipc_shutdown_rx = self.shutdown_rx();
        let ipc_handle = tokio::spawn(async move {
            if let Err(e) = ipc_server.run(ipc_shutdown_rx).await {
                error!(error = %e, "IPC server error");
            }
        });

        // TODO(1.7): Initialize iroh endpoint

        // Install signal handlers — both trigger the same shutdown broadcast
        let shutdown_tx = self.shutdown_tx.clone();
        tokio::spawn(async move {
            Self::wait_for_signals(shutdown_tx).await;
        });

        // Wait for shutdown signal (from signal handler or IPC stop command)
        let mut shutdown_rx = self.shutdown_rx();
        shutdown_rx.recv().await.ok();
        info!("shutdown signal received, cleaning up...");

        // Wait for IPC server to finish
        ipc_handle.await.ok();

        // Cleanup socket and PID file
        Self::cleanup(&socket_path, &pid_path);

        info!("daemon stopped");
        Ok(())
    }

    /// Wait for SIGTERM or Ctrl-C, then broadcast shutdown.
    async fn wait_for_signals(shutdown_tx: ShutdownTx) {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");

        tokio::select! {
            _ = sigterm.recv() => {
                info!("received SIGTERM");
            }
            result = tokio::signal::ctrl_c() => {
                if let Err(e) = result {
                    error!(error = %e, "ctrl_c handler failed");
                    return;
                }
                info!("received Ctrl-C");
            }
        }

        let _ = shutdown_tx.send(());
    }

    /// Remove socket and PID file on exit.
    fn cleanup(socket_path: &PathBuf, pid_path: &PathBuf) {
        if let Err(e) = fs::remove_file(socket_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!(path = %socket_path.display(), error = %e, "failed to remove socket");
            }
        }
        if let Err(e) = fs::remove_file(pid_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!(path = %pid_path.display(), error = %e, "failed to remove PID file");
            }
        }
    }
}
