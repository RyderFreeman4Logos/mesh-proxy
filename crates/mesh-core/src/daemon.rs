use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use mesh_proto::{MeshConfig, NodeRole, ServiceEntry};
use tokio::sync::{broadcast, mpsc};
use tokio::task;
use tracing::{error, info, warn};

use crate::ConfigWatcher;
use crate::control_node::ControlNode;
use crate::edge_node::EdgeNode;
use crate::ipc_server::NodeState;
use crate::process::StartupReadyWriter;

/// Shutdown signal type.
pub type ShutdownTx = broadcast::Sender<()>;
pub type ShutdownRx = broadcast::Receiver<()>;

/// The mesh-proxy daemon engine.
pub struct Daemon {
    config: MeshConfig,
    config_path: PathBuf,
    shutdown_tx: ShutdownTx,
    startup_ready: Option<StartupReadyWriter>,
}

impl Daemon {
    /// Create a new daemon from the loaded configuration.
    pub fn new(
        config: MeshConfig,
        config_path: PathBuf,
        startup_ready: StartupReadyWriter,
    ) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            config,
            config_path,
            shutdown_tx,
            startup_ready: Some(startup_ready),
        }
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

    /// Main daemon loop. Starts IPC server, initialises node by role,
    /// installs signal handlers, and waits for shutdown.
    pub async fn run(&mut self) -> Result<()> {
        info!(
            role = ?self.config.role,
            node_name = %self.config.node_name,
            "daemon starting"
        );

        let socket_path = self.socket_path();
        let pid_path = self.pid_path();
        let (config_tx, mut config_rx) = mpsc::channel(8);

        // Start IPC server
        let ipc_server = crate::ipc_server::IpcServer::bind(
            &socket_path,
            self.shutdown_tx.clone(),
            self.config.clone(),
            self.config_path.clone(),
            config_tx.clone(),
        )
        .await?;
        let ipc_status = ipc_server.status_handle();
        let node_state_handle = ipc_server.node_state_handle();

        if let Some(mut startup_ready) = self.startup_ready.take() {
            startup_ready.signal_ready()?;
        }

        let ipc_shutdown_rx = self.shutdown_rx();
        let ipc_handle = tokio::spawn(async move {
            if let Err(e) = ipc_server.run(ipc_shutdown_rx).await {
                error!(error = %e, "IPC server error");
            }
        });

        // Initialize iroh endpoint
        let mesh_node =
            crate::mesh_node::MeshNode::new(&mut self.config, &self.config_path).await?;
        ipc_status.set_mesh_node(mesh_node.id().to_string(), true);
        info!(endpoint_id = %mesh_node.id(), "mesh node initialized");

        // Branch by role: create node state and start role-specific tasks.
        let (accept_handle, _config_watcher) = match self.config.role {
            NodeRole::Control => {
                let cn = Arc::new(tokio::sync::RwLock::new(ControlNode::new()));
                {
                    let mut ns = node_state_handle.write().await;
                    *ns = NodeState::Control(Arc::clone(&cn));
                }
                let shutdown_rx = self.shutdown_rx();
                let endpoint = mesh_node.endpoint().clone();
                let cn_clone = Arc::clone(&cn);
                (
                    Some(tokio::spawn(async move {
                        crate::control_node::run_accept_loop(cn_clone, endpoint, shutdown_rx).await;
                    })),
                    None,
                )
            }
            NodeRole::Edge => {
                let en = Arc::new(tokio::sync::RwLock::new(EdgeNode::with_endpoint(
                    mesh_node.endpoint().clone(),
                )));
                // Load persisted route cache if available.
                if let Some((routes, version)) = EdgeNode::load_route_cache(&self.config.data_dir) {
                    let mut edge = en.write().await;
                    edge.update_routes(routes, version);
                    info!(version, "loaded persisted route cache");
                }
                {
                    let mut ns = node_state_handle.write().await;
                    *ns = NodeState::Edge(Arc::clone(&en));
                }
                let config_watcher =
                    ConfigWatcher::new(self.config_path.clone(), config_tx.clone())?;
                info!(path = %self.config_path.display(), "config watcher started");
                (None, Some(config_watcher))
            }
        };

        // Install signal handlers — both trigger the same shutdown broadcast
        let shutdown_tx = self.shutdown_tx.clone();
        tokio::spawn(async move {
            Self::wait_for_signals(shutdown_tx).await;
        });

        // Wait for shutdown signal (from signal handler or IPC stop command)
        let mut shutdown_rx = self.shutdown_rx();
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    break;
                }
                maybe_reload = config_rx.recv() => {
                    let Some(()) = maybe_reload else {
                        warn!("config reload channel closed");
                        break;
                    };

                    if let Err(error) = self.reload_config().await {
                        error!(error = %error, path = %self.config_path.display(), "failed to reload config");
                    }
                }
            }
        }
        info!("shutdown signal received, cleaning up...");

        // Wait for accept loop to finish (if running).
        if let Some(handle) = accept_handle {
            handle.await.ok();
        }

        // Close iroh endpoint before cleanup
        mesh_node.close().await;

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
        if let Err(e) = fs::remove_file(socket_path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!(path = %socket_path.display(), error = %e, "failed to remove socket");
        }
        if let Err(e) = fs::remove_file(pid_path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!(path = %pid_path.display(), error = %e, "failed to remove PID file");
        }
    }

    async fn reload_config(&mut self) -> Result<()> {
        let config_path = self.config_path.clone();
        let new_config = task::spawn_blocking(move || MeshConfig::load(&config_path))
            .await
            .map_err(anyhow::Error::from)??;

        if self.config.role == NodeRole::Edge {
            let diff = ServiceDiff::between(&self.config.services, &new_config.services);
            diff.log(&self.config_path, new_config.services.len());
        } else {
            info!(path = %self.config_path.display(), "config reloaded from disk");
        }

        self.config = new_config;
        Ok(())
    }
}

#[derive(Debug)]
struct ServiceDiff {
    added: Vec<ServiceEntry>,
    removed: Vec<ServiceEntry>,
    changed: Vec<ServiceChange>,
}

impl ServiceDiff {
    fn between(previous: &[ServiceEntry], current: &[ServiceEntry]) -> Self {
        let previous_by_name = services_by_name(previous);
        let current_by_name = services_by_name(current);
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut changed = Vec::new();

        for (name, current_service) in &current_by_name {
            match previous_by_name.get(name) {
                None => added.push(current_service.clone()),
                Some(previous_service) if previous_service != current_service => {
                    changed.push(ServiceChange {
                        previous: previous_service.clone(),
                        current: current_service.clone(),
                    });
                }
                Some(_) => {}
            }
        }

        for (name, previous_service) in &previous_by_name {
            if !current_by_name.contains_key(name) {
                removed.push(previous_service.clone());
            }
        }

        Self {
            added,
            removed,
            changed,
        }
    }

    fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }

    fn log(&self, config_path: &Path, service_count: usize) {
        if self.is_empty() {
            info!(
                path = %config_path.display(),
                service_count,
                "config reloaded with no service changes"
            );
            return;
        }

        info!(
            path = %config_path.display(),
            service_count,
            added = self.added.len(),
            removed = self.removed.len(),
            changed = self.changed.len(),
            "config reloaded with service changes"
        );

        for service in &self.added {
            info!(
                name = %service.name,
                local_addr = %service.local_addr,
                protocol = ?service.protocol,
                health_check = ?service.health_check,
                "service added via config reload"
            );
        }

        for service in &self.removed {
            info!(
                name = %service.name,
                local_addr = %service.local_addr,
                protocol = ?service.protocol,
                health_check = ?service.health_check,
                "service removed via config reload"
            );
        }

        for change in &self.changed {
            info!(
                name = %change.current.name,
                old_local_addr = %change.previous.local_addr,
                new_local_addr = %change.current.local_addr,
                old_protocol = ?change.previous.protocol,
                new_protocol = ?change.current.protocol,
                old_health_check = ?change.previous.health_check,
                new_health_check = ?change.current.health_check,
                "service updated via config reload"
            );
        }
    }
}

#[derive(Debug)]
struct ServiceChange {
    previous: ServiceEntry,
    current: ServiceEntry,
}

fn services_by_name(services: &[ServiceEntry]) -> BTreeMap<String, ServiceEntry> {
    services
        .iter()
        .cloned()
        .map(|service| (service.name.clone(), service))
        .collect()
}
