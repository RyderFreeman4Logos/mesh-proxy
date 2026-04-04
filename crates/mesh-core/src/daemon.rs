use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use mesh_proto::{JoinTicket, MeshConfig, NodeRole, PortAssignment, ServiceEntry};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task;
use tracing::{error, info, warn};

use crate::ConfigWatcher;
use crate::control_node::ControlNode;
use crate::edge_node::EdgeNode;
use crate::health_server::HealthServerState;
use crate::ipc_server::NodeState;
use crate::process::StartupReadyWriter;

type SharedRuntimeConfig = Arc<tokio::sync::RwLock<MeshConfig>>;
type SharedControlNode = Arc<tokio::sync::RwLock<ControlNode>>;
type SharedLocalProxy = Arc<tokio::sync::Mutex<EdgeNode>>;

/// Shutdown signal type.
pub type ShutdownTx = broadcast::Sender<()>;
pub type ShutdownRx = broadcast::Receiver<()>;

const PROXY_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const PROXY_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const JOIN_TICKET_FILE_NAME: &str = "join_ticket.txt";
const JOIN_TICKET_TTL_SECONDS: u64 = 3600;
const PORT_ASSIGNMENT_CHANNEL_CAPACITY: usize = 16;

#[derive(Debug, Clone, Default)]
pub(crate) struct PortAssignmentNotifier {
    channels: Arc<Mutex<HashMap<String, broadcast::Sender<u16>>>>,
}

impl PortAssignmentNotifier {
    pub(crate) fn subscribe(&self, service_name: &str) -> broadcast::Receiver<u16> {
        self.sender_for(service_name).subscribe()
    }

    pub(crate) fn notify_assignments(&self, assignments: &[PortAssignment]) {
        for assignment in assignments {
            let _ = self
                .sender_for(&assignment.service_name)
                .send(assignment.assigned_port);
        }
    }

    fn sender_for(&self, service_name: &str) -> broadcast::Sender<u16> {
        let mut channels = match self.channels.lock() {
            Ok(channels) => channels,
            Err(poisoned) => poisoned.into_inner(),
        };

        channels
            .entry(service_name.to_owned())
            .or_insert_with(|| broadcast::channel(PORT_ASSIGNMENT_CHANNEL_CAPACITY).0)
            .clone()
    }
}

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
    service_change_tx: Option<watch::Sender<()>>,
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
            service_change_tx: None,
        }
    }

    /// Create a daemon without a startup-notification pipe.
    ///
    /// This is intended for in-process integration tests and embedders that do
    /// not fork before starting the runtime.
    pub fn new_without_startup_signal(config: MeshConfig, config_path: PathBuf) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            config,
            config_path,
            shutdown_tx,
            startup_ready: None,
            service_change_tx: None,
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
        let port_assignment_notifier = PortAssignmentNotifier::default();

        // Start IPC server
        let ipc_server = crate::ipc_server::IpcServer::bind_with_port_assignment_notifier(
            &socket_path,
            self.shutdown_tx.clone(),
            self.config.clone(),
            self.config_path.clone(),
            config_tx.clone(),
            port_assignment_notifier.clone(),
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
        let mesh_node = crate::mesh_node::MeshNode::new(&self.config.data_dir).await?;
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
        let (
            accept_handle,
            registration_handle,
            _config_watcher,
            shared_config,
            service_change_tx,
            health_state,
            active_connections,
            control_local_proxy_handle,
        ) = match self.config.role {
            NodeRole::Control => {
                let runtime_config = Arc::new(tokio::sync::RwLock::new(self.config.clone()));
                let cn = Arc::new(tokio::sync::RwLock::new(ControlNode::load_from_disk(
                    &self.config.data_dir,
                )));
                {
                    let mut ns = node_state_handle.write().await;
                    *ns = NodeState::Control(Arc::clone(&cn));
                }
                let shutdown_rx = self.shutdown_rx();
                let endpoint = mesh_node.endpoint().clone();
                let cn_clone = Arc::clone(&cn);
                let control_local_proxy = Self::maybe_spawn_control_local_proxy(
                    &self.config,
                    Arc::clone(&cn),
                    endpoint.clone(),
                    Arc::clone(&runtime_config),
                    self.shutdown_rx(),
                )
                .await;
                (
                    Some(tokio::spawn(async move {
                        crate::control_node::run_accept_loop(cn_clone, endpoint, shutdown_rx).await;
                    })),
                    None,
                    None,
                    Some(runtime_config),
                    None,
                    Some(HealthServerState::from(Arc::clone(&cn))),
                    None,
                    control_local_proxy.map(|(_, handle)| handle),
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
                if let Some(cache) = EdgeNode::load_route_cache(&self.config.data_dir) {
                    let mut edge = en.write().await;
                    let version = cache.version;
                    let control_endpoint_id = cache.control_endpoint_id.clone();
                    edge.restore_route_cache(cache);
                    info!(
                        version,
                        ?control_endpoint_id,
                        "loaded persisted route cache"
                    );
                }
                {
                    let mut ns = node_state_handle.write().await;
                    *ns = NodeState::Edge(Arc::clone(&en));
                }
                let config_watcher =
                    ConfigWatcher::new(self.config_path.clone(), config_tx.clone())?;
                info!(path = %self.config_path.display(), "config watcher started");
                let (svc_tx, svc_rx) = watch::channel(());
                let reg_handle = if let Some(ref control_addr) = self.config.control_addr {
                    let endpoint = mesh_node.endpoint().clone();
                    let control_addr = control_addr.clone();
                    let node_name = self.config.node_name.clone();
                    let config = Arc::clone(&runtime_config);
                    let en = Arc::clone(&en);
                    let shutdown_rx = self.shutdown_rx();
                    let port_assignment_notifier = port_assignment_notifier.clone();
                    Some(tokio::spawn(async move {
                        crate::edge_registration::run_registration_loop_with_port_assignment_notifier(
                            endpoint,
                            control_addr,
                            node_name,
                            config,
                            en,
                            crate::edge_registration::RegistrationLoopChannels {
                                port_assignment_notifier,
                                service_change_rx: svc_rx,
                                shutdown: shutdown_rx,
                            },
                        )
                        .await;
                    }))
                } else {
                    None
                };
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
                    reg_handle,
                    Some(config_watcher),
                    Some(shared_runtime_config),
                    Some(svc_tx),
                    Some(health_state),
                    Some(shutdown_active_connections),
                    None,
                )
            }
        };
        self.service_change_tx = service_change_tx;

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

        if let Some(handle) = registration_handle
            && let Err(error) = handle.await
        {
            warn!(error = %error, "registration loop task join failed");
        }

        if let Some(handle) = control_local_proxy_handle
            && let Err(error) = handle.await
        {
            warn!(error = %error, "control local proxy task join failed");
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

    async fn maybe_spawn_control_local_proxy(
        config: &MeshConfig,
        control_node: SharedControlNode,
        endpoint: iroh::Endpoint,
        runtime_config: SharedRuntimeConfig,
        shutdown_rx: ShutdownRx,
    ) -> Option<(SharedLocalProxy, tokio::task::JoinHandle<()>)> {
        if !config.enable_local_proxy {
            return None;
        }

        let route_change_rx = {
            let control_node = control_node.read().await;
            control_node.subscribe_route_changes()
        };
        let local_proxy = Arc::new(tokio::sync::Mutex::new(EdgeNode::with_endpoint(endpoint)));
        let task_local_proxy = Arc::clone(&local_proxy);
        let task_control_node = Arc::clone(&control_node);
        let task_runtime_config = Arc::clone(&runtime_config);
        let handle = tokio::spawn(async move {
            Self::run_control_local_proxy(
                task_control_node,
                task_local_proxy,
                task_runtime_config,
                route_change_rx,
                shutdown_rx,
            )
            .await;
        });
        Some((local_proxy, handle))
    }

    async fn run_control_local_proxy(
        control_node: SharedControlNode,
        local_proxy: SharedLocalProxy,
        runtime_config: SharedRuntimeConfig,
        mut route_change_rx: watch::Receiver<u64>,
        mut shutdown_rx: ShutdownRx,
    ) {
        Self::sync_control_local_proxy_routes(&control_node, &local_proxy, &runtime_config).await;

        loop {
            tokio::select! {
                change_result = route_change_rx.changed() => {
                    if change_result.is_err() {
                        break;
                    }
                    Self::sync_control_local_proxy_routes(&control_node, &local_proxy, &runtime_config).await;
                }
                recv_result = shutdown_rx.recv() => {
                    if let Err(error) = recv_result {
                        tracing::debug!(error = %error, "control local proxy shutdown channel closed");
                    }
                    break;
                }
            }
        }

        local_proxy.lock().await.purge_route_cache().await;
    }

    async fn sync_control_local_proxy_routes(
        control_node: &SharedControlNode,
        local_proxy: &SharedLocalProxy,
        runtime_config: &SharedRuntimeConfig,
    ) {
        let (routes, version) = {
            let control_node = control_node.read().await;
            (control_node.routes().clone(), control_node.route_version())
        };

        local_proxy
            .lock()
            .await
            .force_route_update(routes, version, Arc::clone(runtime_config))
            .await;
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

        // Avoid duplicate runtime work when IPC and the config watcher report
        // the same on-disk config update back-to-back.
        if new_config == self.config {
            return Ok(());
        }

        if self.config.role == NodeRole::Edge {
            let diff = ServiceDiff::between(&self.config.services, &new_config.services);
            diff.log(&self.config_path, new_config.services.len());
            let services_changed = !diff.is_empty();

            if let Some(shared_config) = shared_config {
                *shared_config.write().await = new_config.clone();
            }

            self.config = new_config;

            if services_changed && let Some(service_change_tx) = self.service_change_tx.as_ref() {
                let _ = service_change_tx.send(());
            }

            return Ok(());
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
    use iroh::endpoint::presets;
    use mesh_proto::{
        ALPN_CONTROL, ALPN_PROXY, DEFAULT_SERVICE_QUOTA, IpcRequest, IpcResponse, NodeInfo,
        Protocol, ServiceRegistration, frame,
    };
    use tempfile::Builder;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpStream;
    use tokio::net::UnixStream;
    use tokio::time::{sleep, timeout};

    fn make_node(endpoint_id: &str) -> NodeInfo {
        NodeInfo {
            endpoint_id: endpoint_id.to_string(),
            node_name: format!("node-{endpoint_id}"),
            quota_limit: DEFAULT_SERVICE_QUOTA as u16,
            quota_used: 0,
            is_online: false,
            last_heartbeat: None,
            addr: None,
        }
    }

    fn make_registration(name: &str, local_addr: &str) -> ServiceRegistration {
        ServiceRegistration {
            name: name.to_string(),
            local_addr: local_addr.to_string(),
            protocol: Protocol::Tcp,
            health_check: None,
        }
    }

    fn make_service_entry(name: &str, local_addr: &str) -> ServiceEntry {
        ServiceEntry {
            name: name.to_string(),
            local_addr: local_addr.to_string(),
            protocol: Protocol::Tcp,
            health_check: None,
        }
    }

    async fn build_control_endpoint() -> Result<iroh::Endpoint> {
        iroh::Endpoint::builder(presets::N0)
            .alpns(vec![ALPN_CONTROL.to_vec()])
            .bind()
            .await
            .context("failed to bind control endpoint")
    }

    fn make_edge_config(
        control_addr: String,
        data_dir: PathBuf,
        services: Vec<ServiceEntry>,
    ) -> MeshConfig {
        MeshConfig {
            node_name: "edge-alpha".to_string(),
            role: NodeRole::Edge,
            control_addr: Some(control_addr),
            enable_local_proxy: false,
            health_bind: None,
            services,
            data_dir,
        }
    }

    async fn send_ipc_request(socket_path: &Path, request: &IpcRequest) -> Result<IpcResponse> {
        let mut stream = UnixStream::connect(socket_path).await.with_context(|| {
            format!(
                "failed to connect to IPC server at {}",
                socket_path.display()
            )
        })?;
        frame::write_json(&mut stream, request).await?;
        let response = frame::read_json(&mut stream).await?;
        Ok(response)
    }

    async fn wait_for_control_service(
        control_node: &Arc<tokio::sync::RwLock<ControlNode>>,
        endpoint_id: &str,
        service_name: &str,
        minimum_version: u64,
    ) -> Result<()> {
        timeout(Duration::from_secs(5), async {
            loop {
                let ready = {
                    let control = control_node.read().await;
                    control.route_version() >= minimum_version
                        && control.routes().values().any(|route| {
                            route.endpoint_id == endpoint_id && route.service_name == service_name
                        })
                };

                if ready {
                    return;
                }

                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("timed out waiting for control service registration")
    }

    fn available_tcp_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    async fn assert_tcp_listener_accepts_and_closes(port: u16) {
        let mut stream = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("local proxy listener should accept connections");
        let mut buf = [0_u8; 1];
        let bytes_read = timeout(Duration::from_secs(1), stream.read(&mut buf))
            .await
            .expect("listener should respond in time")
            .expect("listener read should succeed");
        assert_eq!(bytes_read, 0);
    }

    async fn assert_tcp_listener_stopped(port: u16) {
        timeout(Duration::from_secs(1), async {
            loop {
                match std::net::TcpListener::bind(("127.0.0.1", port)) {
                    Ok(listener) => {
                        drop(listener);
                        break;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => {
                        panic!("failed to verify TCP listener shutdown on {port}: {error}")
                    }
                }
            }
        })
        .await
        .expect("listener should stop before timeout");
    }

    async fn wait_for_listener_count(local_proxy: &SharedLocalProxy, expected: usize) {
        timeout(Duration::from_secs(1), async {
            loop {
                if local_proxy.lock().await.listener_count() == expected {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("listener count should converge");
    }

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

    #[tokio::test]
    async fn test_control_local_proxy_disabled_does_not_spawn_listeners() {
        let config = MeshConfig {
            role: NodeRole::Control,
            enable_local_proxy: false,
            ..MeshConfig::default()
        };
        let runtime_config = Arc::new(tokio::sync::RwLock::new(config.clone()));
        let control_node = Arc::new(tokio::sync::RwLock::new(ControlNode::new()));

        let endpoint = iroh::Endpoint::builder(presets::N0)
            .alpns(vec![ALPN_PROXY.to_vec()])
            .bind()
            .await
            .unwrap();
        let (shutdown_tx, _) = broadcast::channel(1);
        let runtime = Daemon::maybe_spawn_control_local_proxy(
            &config,
            Arc::clone(&control_node),
            endpoint.clone(),
            runtime_config,
            shutdown_tx.subscribe(),
        )
        .await;
        assert!(runtime.is_none());

        let probe_port = available_tcp_port();
        let listener = std::net::TcpListener::bind(("127.0.0.1", probe_port))
            .expect("listener port should remain free when local proxy is disabled");
        drop(listener);

        let _ = shutdown_tx.send(());
        endpoint.close().await;
    }

    #[tokio::test]
    async fn test_control_local_proxy_tracks_route_changes() {
        let config = MeshConfig {
            role: NodeRole::Control,
            enable_local_proxy: true,
            ..MeshConfig::default()
        };
        let runtime_config = Arc::new(tokio::sync::RwLock::new(config.clone()));
        let control_node = Arc::new(tokio::sync::RwLock::new(ControlNode::new()));
        {
            let mut control = control_node.write().await;
            control.accept_node(make_node("edge-a"), 100).unwrap();
        }

        let endpoint = iroh::Endpoint::builder(presets::N0)
            .alpns(vec![ALPN_PROXY.to_vec()])
            .bind()
            .await
            .unwrap();
        let (shutdown_tx, _) = broadcast::channel(1);
        let (local_proxy, handle) = Daemon::maybe_spawn_control_local_proxy(
            &config,
            Arc::clone(&control_node),
            endpoint.clone(),
            runtime_config,
            shutdown_tx.subscribe(),
        )
        .await
        .expect("enabled local proxy should spawn a sync task");

        wait_for_listener_count(&local_proxy, 0).await;

        let assigned_port = {
            let mut control = control_node.write().await;
            control
                .register_services(
                    "edge-a",
                    "node-edge-a",
                    &[make_registration("llm-api", "127.0.0.1:3000")],
                    200,
                )
                .unwrap()[0]
                .assigned_port
        };

        wait_for_listener_count(&local_proxy, 1).await;
        assert_tcp_listener_accepts_and_closes(assigned_port).await;

        {
            let mut control = control_node.write().await;
            let no_services = Vec::<ServiceRegistration>::new();
            control
                .register_services("edge-a", "node-edge-a", &no_services, 300)
                .unwrap();
        }

        wait_for_listener_count(&local_proxy, 0).await;
        assert_tcp_listener_stopped(assigned_port).await;

        let _ = shutdown_tx.send(());
        handle.await.unwrap();
        endpoint.close().await;
    }

    #[tokio::test]
    async fn test_ipc_expose_triggers_runtime_reregistration() -> Result<()> {
        timeout(Duration::from_secs(15), async {
            let control_endpoint = build_control_endpoint().await?;
            let edge_endpoint = build_control_endpoint().await?;
            let edge_endpoint_id = edge_endpoint.id().to_string();
            let control_node = Arc::new(tokio::sync::RwLock::new(ControlNode::new()));
            {
                let mut control = control_node.write().await;
                control.add_node(make_node(&edge_endpoint_id));
            }

            let (accept_shutdown_tx, accept_shutdown_rx) = broadcast::channel(4);
            let accept_handle = tokio::spawn(crate::control_node::run_accept_loop(
                Arc::clone(&control_node),
                control_endpoint.clone(),
                accept_shutdown_rx,
            ));

            let dir = Builder::new()
                .prefix("mesh-daemon-expose-reregister-")
                .tempdir_in("/tmp")
                .context("failed to create daemon test tempdir")?;
            let config_path = dir.path().join("config.toml");
            let socket_path = dir.path().join("daemon.sock");
            let control_addr = serde_json::to_string(&control_endpoint.addr())
                .context("failed to serialize control address")?;
            let initial_config = make_edge_config(
                control_addr.clone(),
                dir.path().join("data"),
                vec![make_service_entry("echo", "127.0.0.1:18080")],
            );
            initial_config.save(&config_path)?;

            let runtime_config = Arc::new(tokio::sync::RwLock::new(initial_config.clone()));
            let edge_node = Arc::new(tokio::sync::RwLock::new(EdgeNode::with_endpoint(
                edge_endpoint.clone(),
            )));
            let (service_change_tx, service_change_rx) = watch::channel(());
            let port_assignment_notifier = PortAssignmentNotifier::default();
            let (registration_shutdown_tx, registration_shutdown_rx) = broadcast::channel(4);
            let registration_handle = tokio::spawn(
                crate::edge_registration::run_registration_loop_with_port_assignment_notifier(
                    edge_endpoint.clone(),
                    control_addr,
                    "edge-alpha".to_string(),
                    Arc::clone(&runtime_config),
                    Arc::clone(&edge_node),
                    crate::edge_registration::RegistrationLoopChannels {
                        port_assignment_notifier: port_assignment_notifier.clone(),
                        service_change_rx,
                        shutdown: registration_shutdown_rx,
                    },
                ),
            );

            let (reload_tx, mut reload_rx) = mpsc::channel(8);
            let _watcher = ConfigWatcher::new(config_path.clone(), reload_tx.clone())?;
            let ipc_shutdown_tx = broadcast::channel(4).0;
            let ipc_shutdown_rx = ipc_shutdown_tx.subscribe();
            let ipc_server = crate::ipc_server::IpcServer::bind_with_port_assignment_notifier(
                &socket_path,
                ipc_shutdown_tx.clone(),
                initial_config.clone(),
                config_path.clone(),
                reload_tx,
                port_assignment_notifier,
            )
            .await?;
            ipc_server.attach_edge_node(Arc::clone(&edge_node)).await;
            let ipc_handle = tokio::spawn(async move { ipc_server.run(ipc_shutdown_rx).await });

            let mut daemon = Daemon {
                config: initial_config,
                config_path: config_path.clone(),
                shutdown_tx: broadcast::channel(1).0,
                startup_ready: None,
                service_change_tx: Some(service_change_tx),
            };

            let test_result: Result<()> = async {
                wait_for_control_service(&control_node, &edge_endpoint_id, "echo", 1).await?;
                let initial_route_version = control_node.read().await.route_version();

                let request = IpcRequest::ExposeService {
                    name: "admin".to_string(),
                    local_addr: "127.0.0.1:19090".to_string(),
                    protocol: Protocol::Tcp,
                    health_check: None,
                };
                let process_reload = async {
                    let explicit_reload = timeout(Duration::from_millis(250), reload_rx.recv())
                        .await
                        .context("timed out waiting for explicit reload signal")?;
                    assert_eq!(explicit_reload, Some(()));

                    daemon.reload_config(Some(&runtime_config)).await?;
                    wait_for_control_service(
                        &control_node,
                        &edge_endpoint_id,
                        "admin",
                        initial_route_version + 1,
                    )
                    .await?;
                    Ok::<(), anyhow::Error>(())
                };
                let (response, reload_result) =
                    tokio::join!(send_ipc_request(&socket_path, &request), process_reload);
                let response = response?;
                reload_result?;

                let IpcResponse::ServiceExposed { assigned_port, .. } = response else {
                    anyhow::bail!("expected service exposed response");
                };
                assert!(
                    (40000..=49999).contains(&assigned_port),
                    "assigned port should be in the mesh service range"
                );

                let watcher_reload = timeout(Duration::from_secs(5), reload_rx.recv())
                    .await
                    .context("timed out waiting for config watcher reload signal")?;
                assert_eq!(watcher_reload, Some(()));

                daemon.reload_config(Some(&runtime_config)).await?;
                sleep(Duration::from_millis(200)).await;

                let control = control_node.read().await;
                assert_eq!(control.route_version(), initial_route_version + 1);
                assert!(
                    control
                        .routes()
                        .values()
                        .any(|route| route.service_name == "admin"),
                    "control route table should contain the newly exposed service"
                );
                Ok(())
            }
            .await;

            let _ = ipc_shutdown_tx.send(());
            let _ = registration_shutdown_tx.send(());
            let _ = accept_shutdown_tx.send(());
            edge_endpoint.close().await;
            control_endpoint.close().await;

            timeout(Duration::from_secs(2), ipc_handle)
                .await
                .context("IPC server task did not exit in time")?
                .context("IPC server task panicked")??;
            timeout(Duration::from_secs(2), registration_handle)
                .await
                .context("registration loop did not exit in time")?
                .context("registration loop panicked")?;
            timeout(Duration::from_secs(2), accept_handle)
                .await
                .context("accept loop did not exit in time")?
                .context("accept loop panicked")?;

            test_result
        })
        .await
        .context("timed out waiting for IPC expose re-registration test")?
    }
}
