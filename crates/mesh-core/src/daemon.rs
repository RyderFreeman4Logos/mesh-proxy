use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use mesh_proto::{JoinTicket, MeshConfig, NodeRole, ServiceEntry};
use tokio::sync::{broadcast, mpsc};
use tokio::task;
use tracing::{error, info, warn};

use crate::ConfigWatcher;
use crate::control_node::ControlNode;
use crate::edge_node::EdgeNode;
use crate::health_server::HealthServerState;
use crate::ipc_server::NodeState;
use crate::process::StartupReadyWriter;

/// Shutdown signal type.
pub type ShutdownTx = broadcast::Sender<()>;
pub type ShutdownRx = broadcast::Receiver<()>;

const PROXY_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const PROXY_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const JOIN_TICKET_FILE_NAME: &str = "join_ticket.txt";
const JOIN_TICKET_TTL_SECONDS: u64 = 3600;

fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn save_join_ticket(data_dir: &Path, endpoint_id: &str) -> Result<String> {
    let ticket = JoinTicket {
        endpoint_id: endpoint_id.to_owned(),
        created_at: now_epoch(),
        ttl_seconds: JOIN_TICKET_TTL_SECONDS,
        nonce: rand::random(),
    };
    let ticket_bs58 = ticket.to_bs58().context("failed to encode join ticket")?;
    write_join_ticket_file(data_dir, &ticket_bs58)?;
    Ok(ticket_bs58)
}

fn write_join_ticket_file(data_dir: &Path, ticket_bs58: &str) -> Result<()> {
    fs::create_dir_all(data_dir)
        .with_context(|| format!("failed to create data dir {}", data_dir.display()))?;

    let ticket_path = data_dir.join(JOIN_TICKET_FILE_NAME);
    let mut open_options = fs::OpenOptions::new();
    open_options.create(true).write(true).truncate(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        open_options.mode(0o600);
    }

    let mut ticket_file = open_options
        .open(&ticket_path)
        .with_context(|| format!("failed to open join ticket file {}", ticket_path.display()))?;
    ticket_file
        .write_all(ticket_bs58.as_bytes())
        .with_context(|| format!("failed to write join ticket file {}", ticket_path.display()))?;
    ticket_file
        .sync_all()
        .with_context(|| format!("failed to sync join ticket file {}", ticket_path.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(&ticket_path, fs::Permissions::from_mode(0o600)).with_context(
            || {
                format!(
                    "failed to set join ticket permissions {}",
                    ticket_path.display()
                )
            },
        )?;
    }

    Ok(())
}

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
        let endpoint_id = mesh_node.id().to_string();
        let endpoint_addr = serde_json::to_string(&mesh_node.endpoint().addr())
            .context("failed to serialize endpoint address")?;
        ipc_status.set_mesh_node(endpoint_id.clone(), Some(endpoint_addr), true);
        info!(endpoint_id = %endpoint_id, "mesh node initialized");

        if self.config.role == NodeRole::Edge {
            let ticket_bs58 = save_join_ticket(&self.config.data_dir, &endpoint_id)?;
            tracing::info!(
                "Join ticket saved. Run on control node: mesh-proxy accept {}",
                ticket_bs58
            );
        }

        // Branch by role: create node state and start role-specific tasks.
        let (accept_handle, _config_watcher, shared_config, health_state, active_connections) =
            match self.config.role {
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
                            crate::control_node::run_accept_loop(cn_clone, endpoint, shutdown_rx)
                                .await;
                        })),
                        None,
                        None,
                        Some(HealthServerState::from(Arc::clone(&cn))),
                        None,
                    )
                }
                NodeRole::Edge => {
                    let runtime_config = Arc::new(tokio::sync::RwLock::new(self.config.clone()));
                    let shared_runtime_config = Arc::clone(&runtime_config);
                    let en = Arc::new(tokio::sync::RwLock::new(EdgeNode::with_endpoint(
                        mesh_node.endpoint().clone(),
                    )));
                    let health_state = HealthServerState::from(Arc::clone(&en));
                    let edge_active_connections = Arc::new(AtomicUsize::new(0));
                    let shutdown_active_connections = Arc::clone(&edge_active_connections);
                    // Load persisted route cache if available.
                    if let Some((routes, version)) =
                        EdgeNode::load_route_cache(&self.config.data_dir)
                    {
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
                    let endpoint = mesh_node.endpoint().clone();
                    let data_dir = self.config.data_dir.clone();
                    let shutdown_rx = self.shutdown_rx();
                    let accept_handle = tokio::spawn(async move {
                        crate::edge_node::run_edge_accept_loop(
                            en,
                            endpoint,
                            runtime_config,
                            data_dir,
                            edge_active_connections,
                            shutdown_rx,
                        )
                        .await;
                    });
                    (
                        Some(accept_handle),
                        Some(config_watcher),
                        Some(shared_runtime_config),
                        Some(health_state),
                        Some(shutdown_active_connections),
                    )
                }
            };

        let health_handle = if let Some(health_bind) = self.config.health_bind.as_deref() {
            let bind_addr: SocketAddr = health_bind
                .parse()
                .map_err(|error| anyhow::anyhow!("invalid health_bind {health_bind}: {error}"))?;
            let Some(health_state) = health_state else {
                return Err(anyhow::anyhow!(
                    "health server requested before node state initialization"
                ));
            };
            let shutdown_rx = self.shutdown_rx();
            Some(tokio::spawn(async move {
                if let Err(error) = crate::health_server::run_health_server_with_state(
                    bind_addr,
                    health_state,
                    shutdown_rx,
                )
                .await
                {
                    error!(%bind_addr, error = %error, "health server error");
                }
            }))
        } else {
            None
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

                    if let Err(error) = self.reload_config(shared_config.as_ref()).await {
                        error!(error = %error, path = %self.config_path.display(), "failed to reload config");
                    }
                }
            }
        }
        info!("shutdown signal received, draining runtime state");

        // Wait for accept loop to finish (if running).
        if let Some(handle) = accept_handle
            && let Err(error) = handle.await
        {
            warn!(error = %error, "accept loop task join failed");
        }

        if let Some(active_connections) = active_connections.as_deref() {
            Self::wait_for_inflight_proxy_connections(
                active_connections,
                PROXY_DRAIN_TIMEOUT,
                PROXY_DRAIN_POLL_INTERVAL,
            )
            .await;
        }

        // Close iroh endpoint before cleanup
        mesh_node.close().await;

        if let Some(handle) = health_handle
            && let Err(error) = handle.await
        {
            warn!(error = %error, "health server task join failed");
        }

        // Wait for IPC server to finish
        if let Err(error) = ipc_handle.await {
            warn!(error = %error, "IPC server task join failed");
        }

        // Cleanup socket and PID file
        Self::cleanup(&socket_path, &pid_path);

        info!("graceful shutdown complete");
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

    async fn wait_for_inflight_proxy_connections(
        active_connections: &AtomicUsize,
        timeout: Duration,
        poll_interval: Duration,
    ) {
        let initial = active_connections.load(Ordering::Relaxed);
        if initial == 0 {
            return;
        }

        info!(
            in_flight = initial,
            timeout_secs = timeout.as_secs(),
            "waiting for in-flight proxy connections to drain"
        );

        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let in_flight = active_connections.load(Ordering::Relaxed);
            if in_flight == 0 {
                info!("in-flight proxy connections drained");
                return;
            }

            if tokio::time::Instant::now() >= deadline {
                warn!(
                    in_flight,
                    timeout_secs = timeout.as_secs(),
                    "timed out waiting for in-flight proxy connections"
                );
                return;
            }

            tokio::time::sleep(poll_interval).await;
        }
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

    async fn reload_config(
        &mut self,
        shared_config: Option<&Arc<tokio::sync::RwLock<MeshConfig>>>,
    ) -> Result<()> {
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

        if let Some(shared_config) = shared_config {
            *shared_config.write().await = new_config.clone();
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::Builder;

    #[tokio::test]
    async fn test_wait_for_inflight_proxy_connections_returns_after_drain() {
        let active_connections = Arc::new(AtomicUsize::new(1));
        let drain_counter = Arc::clone(&active_connections);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(25)).await;
            drain_counter.store(0, Ordering::Relaxed);
        });

        Daemon::wait_for_inflight_proxy_connections(
            active_connections.as_ref(),
            Duration::from_millis(250),
            Duration::from_millis(10),
        )
        .await;

        assert_eq!(active_connections.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_wait_for_inflight_proxy_connections_times_out() {
        let active_connections = AtomicUsize::new(2);

        Daemon::wait_for_inflight_proxy_connections(
            &active_connections,
            Duration::from_millis(30),
            Duration::from_millis(10),
        )
        .await;

        assert_eq!(active_connections.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_save_join_ticket_creates_valid_bs58_file() {
        let dir = Builder::new()
            .prefix("mesh-daemon-join-ticket-")
            .tempdir_in("/tmp")
            .unwrap();
        let data_dir = dir.path().join("data");
        let endpoint_id = "edge-endpoint-123";
        let before = now_epoch();

        save_join_ticket(&data_dir, endpoint_id).unwrap();

        let after = now_epoch();
        let ticket_path = data_dir.join(JOIN_TICKET_FILE_NAME);
        assert!(ticket_path.exists());

        let ticket_bs58 = fs::read_to_string(&ticket_path).unwrap();
        let ticket = JoinTicket::from_bs58(&ticket_bs58).unwrap();
        assert_eq!(ticket.endpoint_id, endpoint_id);
        assert_eq!(ticket.ttl_seconds, JOIN_TICKET_TTL_SECONDS);
        assert!(ticket.created_at >= before);
        assert!(ticket.created_at <= after);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = fs::metadata(&ticket_path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
    }
}
