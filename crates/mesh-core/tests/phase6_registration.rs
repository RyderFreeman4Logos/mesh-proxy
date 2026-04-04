use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use iroh::endpoint::presets;
use mesh_core::edge_registration::run_registration_loop;
use mesh_core::{ControlNode, Daemon, EdgeNode, run_accept_loop};
use mesh_proto::{
    ALPN_CONTROL, DEFAULT_SERVICE_QUOTA, IpcRequest, IpcResponse, JoinTicket, MeshConfig, NodeInfo,
    NodeRole, Protocol, RouteEntry, ServiceEntry, ServiceRegistration, ServiceStatus, StatusInfo,
    frame,
};
use tempfile::Builder;
use tokio::net::UnixStream;
use tokio::sync::{RwLock, broadcast, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

fn writable_tempdir(prefix: &str) -> tempfile::TempDir {
    Builder::new()
        .prefix(prefix)
        .tempdir_in("/tmp")
        .expect("tempdir should be created in /tmp")
}

async fn build_endpoint() -> Result<iroh::Endpoint> {
    iroh::Endpoint::builder(presets::N0)
        .alpns(vec![ALPN_CONTROL.to_vec()])
        .bind()
        .await
        .context("failed to bind iroh endpoint")
}

fn make_whitelist_node(endpoint_id: String, node_name: &str) -> NodeInfo {
    NodeInfo {
        endpoint_id,
        node_name: node_name.to_owned(),
        quota_limit: DEFAULT_SERVICE_QUOTA as u16,
        quota_used: 0,
        is_online: false,
        last_heartbeat: None,
        addr: None,
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

fn make_service_registration(name: &str, local_addr: &str) -> ServiceRegistration {
    ServiceRegistration {
        name: name.to_string(),
        local_addr: local_addr.to_string(),
        protocol: Protocol::Tcp,
        health_check: None,
    }
}

fn make_named_edge_config(
    node_name: &str,
    control_addr: String,
    data_dir: std::path::PathBuf,
    services: Vec<ServiceEntry>,
) -> MeshConfig {
    MeshConfig {
        node_name: node_name.to_string(),
        role: NodeRole::Edge,
        control_addr: Some(control_addr),
        enable_local_proxy: false,
        health_bind: None,
        services,
        data_dir,
    }
}

fn make_edge_config(
    control_addr: String,
    data_dir: std::path::PathBuf,
    services: Vec<ServiceEntry>,
) -> MeshConfig {
    make_named_edge_config("edge-alpha", control_addr, data_dir, services)
}

async fn send_ipc_request(
    socket_path: &std::path::Path,
    request: &IpcRequest,
) -> Result<IpcResponse> {
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

async fn wait_for_join_ticket(data_dir: &std::path::Path) -> Result<JoinTicket> {
    let ticket_path = data_dir.join("join_ticket.txt");
    timeout(Duration::from_secs(5), async {
        loop {
            match std::fs::read_to_string(&ticket_path) {
                Ok(ticket_bs58) => {
                    return JoinTicket::from_bs58(ticket_bs58.trim())
                        .context("failed to decode join ticket");
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    sleep(Duration::from_millis(25)).await;
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("failed to read join ticket at {}", ticket_path.display())
                    });
                }
            }
        }
    })
    .await
    .context("timed out waiting for daemon join ticket")?
}

async fn wait_for_route_cache_control_identity(
    data_dir: &std::path::Path,
    expected_control_endpoint_id: &str,
) -> Result<()> {
    let route_cache_path = data_dir.join("route_cache.json");
    timeout(Duration::from_secs(5), async {
        loop {
            let matches = std::fs::read_to_string(&route_cache_path)
                .ok()
                .and_then(|json| serde_json::from_str::<serde_json::Value>(&json).ok())
                .and_then(|cache| {
                    cache
                        .get("control_endpoint_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .is_some_and(|control_endpoint_id| {
                    control_endpoint_id == expected_control_endpoint_id
                });

            if matches {
                return;
            }

            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("timed out waiting for persisted control identity change")
}

#[derive(Clone, Debug, serde::Deserialize)]
struct RouteCacheSnapshot {
    routes: HashMap<u16, RouteEntry>,
    version: u64,
    control_endpoint_id: Option<String>,
}

fn load_route_cache_snapshot(data_dir: &std::path::Path) -> Option<RouteCacheSnapshot> {
    let route_cache_path = data_dir.join("route_cache.json");
    let route_cache_json = std::fs::read_to_string(route_cache_path).ok()?;
    serde_json::from_str(&route_cache_json).ok()
}

async fn wait_for_route_cache(
    data_dir: &std::path::Path,
    mut predicate: impl FnMut(&RouteCacheSnapshot) -> bool,
) -> Result<RouteCacheSnapshot> {
    let data_dir = data_dir.to_path_buf();
    timeout(Duration::from_secs(5), async move {
        loop {
            if let Some(route_cache) = load_route_cache_snapshot(&data_dir)
                && predicate(&route_cache)
            {
                return route_cache;
            }

            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("timed out waiting for route cache condition")
}

async fn wait_for_route_cache_service(
    data_dir: &std::path::Path,
    service_name: &str,
    minimum_version: u64,
) -> Result<(u16, RouteEntry)> {
    let route_cache = wait_for_route_cache(data_dir, |route_cache| {
        route_cache.version >= minimum_version
            && route_cache
                .routes
                .values()
                .any(|route| route.service_name == service_name)
    })
    .await?;

    let (assigned_port, route) = route_cache
        .routes
        .iter()
        .find(|(_, route)| route.service_name == service_name)
        .context("route cache should contain the requested service")?;

    Ok((*assigned_port, route.clone()))
}

async fn read_status(socket_path: &std::path::Path) -> Result<StatusInfo> {
    match send_ipc_request(socket_path, &IpcRequest::Status).await? {
        IpcResponse::Status(status) => Ok(status),
        other => anyhow::bail!("expected status response, got {other:?}"),
    }
}

async fn wait_for_status(
    socket_path: &std::path::Path,
    mut predicate: impl FnMut(&StatusInfo) -> bool,
) -> Result<StatusInfo> {
    let socket_path = socket_path.to_path_buf();
    timeout(Duration::from_secs(5), async move {
        loop {
            let status = read_status(&socket_path).await?;
            if predicate(&status) {
                return Ok(status);
            }

            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("timed out waiting for status condition")?
}

struct RegistrationHarness {
    _tempdir: tempfile::TempDir,
    control_node: Arc<RwLock<ControlNode>>,
    edge_node: Arc<RwLock<EdgeNode>>,
    control_endpoint: iroh::Endpoint,
    edge_endpoint: iroh::Endpoint,
    edge_endpoint_id: String,
    accept_shutdown_tx: broadcast::Sender<()>,
    registration_shutdown_tx: broadcast::Sender<()>,
    _service_change_tx: watch::Sender<()>,
    accept_handle: JoinHandle<()>,
    registration_handle: JoinHandle<()>,
}

impl RegistrationHarness {
    async fn start() -> Result<Self> {
        Self::start_with(vec![make_service_entry("echo", "127.0.0.1:18080")], false).await
    }

    async fn start_with(
        edge_services: Vec<ServiceEntry>,
        preload_existing_route: bool,
    ) -> Result<Self> {
        let control_endpoint = build_endpoint().await?;
        let edge_endpoint = build_endpoint().await?;
        let edge_endpoint_id = edge_endpoint.id().to_string();

        let control_node = Arc::new(RwLock::new(ControlNode::new()));
        {
            let mut control = control_node.write().await;
            if preload_existing_route {
                let seed_endpoint_id = "seed-node-001".to_string();
                control.add_node(make_whitelist_node(seed_endpoint_id.clone(), "edge-seed"));
                control
                    .register_services(
                        &seed_endpoint_id,
                        "edge-seed",
                        &[make_service_registration("seed-api", "127.0.0.1:28080")],
                        1_700_000_000,
                    )
                    .map_err(anyhow::Error::msg)
                    .context("failed to seed control route table")?;
            }
            control.add_node(make_whitelist_node(edge_endpoint_id.clone(), "edge-alpha"));
        }

        let (accept_shutdown_tx, accept_shutdown_rx) = broadcast::channel(4);
        let accept_handle = tokio::spawn(run_accept_loop(
            Arc::clone(&control_node),
            control_endpoint.clone(),
            accept_shutdown_rx,
        ));

        let tempdir = writable_tempdir("mesh-core-phase6-registration-");
        let control_addr = serde_json::to_string(&control_endpoint.addr())
            .context("failed to serialize control endpoint address")?;
        let config = Arc::new(RwLock::new(make_edge_config(
            control_addr.clone(),
            tempdir.path().join("data"),
            edge_services,
        )));

        let edge_node = Arc::new(RwLock::new(EdgeNode::with_endpoint(edge_endpoint.clone())));
        let (_control_target_tx, control_target_rx) = watch::channel(Some(control_addr.clone()));
        let (_service_change_tx, service_change_rx) = watch::channel(());
        let (registration_shutdown_tx, registration_shutdown_rx) = broadcast::channel(4);

        let registration_handle = tokio::spawn(run_registration_loop(
            edge_endpoint.clone(),
            "edge-alpha".to_string(),
            Arc::clone(&config),
            Arc::clone(&edge_node),
            control_target_rx,
            service_change_rx,
            registration_shutdown_rx,
        ));

        Ok(Self {
            _tempdir: tempdir,
            control_node,
            edge_node,
            control_endpoint,
            edge_endpoint,
            edge_endpoint_id,
            accept_shutdown_tx,
            registration_shutdown_tx,
            _service_change_tx,
            accept_handle,
            registration_handle,
        })
    }

    async fn wait_for_registration(&self) {
        loop {
            let registered = {
                let control = self.control_node.read().await;
                let is_online = control
                    .whitelist()
                    .get(&self.edge_endpoint_id)
                    .is_some_and(|node| node.is_online);
                is_online && !control.services().is_empty()
            };

            if registered {
                return;
            }

            sleep(POLL_INTERVAL).await;
        }
    }

    async fn wait_for_edge_routes(&self, expected_count: usize, minimum_version: u64) {
        loop {
            let ready = {
                let edge = self.edge_node.read().await;
                edge.cached_routes().len() >= expected_count
                    && edge.route_version() >= minimum_version
            };

            if ready {
                return;
            }

            sleep(POLL_INTERVAL).await;
        }
    }

    async fn shutdown(self) -> Result<()> {
        let _ = self.registration_shutdown_tx.send(());
        let _ = self.accept_shutdown_tx.send(());

        self.edge_endpoint.close().await;
        self.control_endpoint.close().await;

        timeout(Duration::from_secs(2), self.registration_handle)
            .await
            .context("registration loop did not exit in time")?
            .context("registration loop panicked")?;
        timeout(Duration::from_secs(2), self.accept_handle)
            .await
            .context("accept loop did not exit in time")?
            .context("accept loop panicked")?;

        Ok(())
    }
}

struct ControlRegistrationHarness {
    control_node: Arc<RwLock<ControlNode>>,
    control_endpoint: iroh::Endpoint,
    accept_shutdown_tx: broadcast::Sender<()>,
    accept_handle: JoinHandle<()>,
}

impl ControlRegistrationHarness {
    async fn start() -> Result<Self> {
        let control_endpoint = build_endpoint().await?;
        let control_node = Arc::new(RwLock::new(ControlNode::new()));
        let (accept_shutdown_tx, accept_shutdown_rx) = broadcast::channel(4);
        let accept_handle = tokio::spawn(run_accept_loop(
            Arc::clone(&control_node),
            control_endpoint.clone(),
            accept_shutdown_rx,
        ));

        Ok(Self {
            control_node,
            control_endpoint,
            accept_shutdown_tx,
            accept_handle,
        })
    }

    fn control_addr(&self) -> Result<String> {
        serde_json::to_string(&self.control_endpoint.addr())
            .context("failed to serialize control endpoint address")
    }

    async fn allow_edge(&self, endpoint_id: &str, node_name: &str) {
        let mut control = self.control_node.write().await;
        control.add_node(make_whitelist_node(endpoint_id.to_owned(), node_name));
    }

    async fn seed_services(
        &self,
        endpoint_id: &str,
        node_name: &str,
        services: &[ServiceRegistration],
        now_epoch: u64,
    ) -> Result<()> {
        let mut control = self.control_node.write().await;
        control.add_node(make_whitelist_node(endpoint_id.to_owned(), node_name));
        control
            .register_services(endpoint_id, node_name, services, now_epoch)
            .map_err(anyhow::Error::msg)
            .with_context(|| format!("failed to seed control routes for {node_name}"))?;
        Ok(())
    }

    async fn wait_for_control_route(&self, service_name: &str, minimum_version: u64) {
        loop {
            let ready = {
                let control = self.control_node.read().await;
                control.route_version() >= minimum_version
                    && control
                        .routes()
                        .values()
                        .any(|route| route.service_name == service_name)
            };

            if ready {
                return;
            }

            sleep(POLL_INTERVAL).await;
        }
    }

    async fn shutdown(self) -> Result<()> {
        let _ = self.accept_shutdown_tx.send(());
        self.control_endpoint.close().await;

        timeout(Duration::from_secs(2), self.accept_handle)
            .await
            .context("accept loop did not exit in time")?
            .context("accept loop panicked")?;

        Ok(())
    }
}

struct EdgeDaemonHarness {
    _tempdir: tempfile::TempDir,
    config_path: std::path::PathBuf,
    data_dir: std::path::PathBuf,
    socket_path: std::path::PathBuf,
    shutdown_tx: broadcast::Sender<()>,
    handle: JoinHandle<Result<()>>,
}

impl EdgeDaemonHarness {
    async fn start(
        node_name: &str,
        control_addr: String,
        services: Vec<ServiceEntry>,
    ) -> Result<Self> {
        let tempdir = writable_tempdir("mesh-core-phase6-daemon-edge-");
        let config_path = tempdir.path().join("config.toml");
        let config = make_named_edge_config(
            node_name,
            control_addr,
            tempdir.path().join("data"),
            services,
        );
        config.save(&config_path)?;

        let socket_path = config.data_dir.join("daemon.sock");
        let data_dir = config.data_dir.clone();
        let mut daemon = Daemon::new_without_startup_signal(config, config_path.clone());
        let shutdown_tx = daemon.shutdown_tx().clone();
        let handle = tokio::spawn(async move { daemon.run().await });

        Ok(Self {
            _tempdir: tempdir,
            config_path,
            data_dir,
            socket_path,
            shutdown_tx,
            handle,
        })
    }

    async fn wait_for_join_ticket(&self) -> Result<JoinTicket> {
        wait_for_join_ticket(&self.data_dir).await
    }

    async fn send_request(&self, request: &IpcRequest) -> Result<IpcResponse> {
        send_ipc_request(&self.socket_path, request).await
    }

    async fn wait_for_service_status(
        &self,
        service_name: &str,
        node_name: &str,
        minimum_version: u64,
    ) -> Result<ServiceStatus> {
        let status = wait_for_status(&self.socket_path, |status| {
            status.route_table_version >= minimum_version
                && status
                    .services
                    .iter()
                    .any(|service| service.name == service_name && service.node_name == node_name)
        })
        .await?;

        status
            .services
            .into_iter()
            .find(|service| service.name == service_name && service.node_name == node_name)
            .context("status should contain the requested service")
    }

    async fn update_control_addr_and_reload(&self, control_addr: String) -> Result<()> {
        let mut config = MeshConfig::load(&self.config_path)?;
        config.control_addr = Some(control_addr);
        config.save(&self.config_path)?;

        match self.send_request(&IpcRequest::Reload).await? {
            IpcResponse::Reloaded => Ok(()),
            other => anyhow::bail!("expected reload response, got {other:?}"),
        }
    }

    async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(());
        timeout(Duration::from_secs(10), self.handle)
            .await
            .context("daemon task did not exit in time")?
            .context("daemon task panicked")?
    }
}

struct EdgeRegistrationHandle {
    _tempdir: tempfile::TempDir,
    node_name: String,
    config: Arc<RwLock<MeshConfig>>,
    edge_node: Arc<RwLock<EdgeNode>>,
    edge_endpoint: iroh::Endpoint,
    endpoint_id: String,
    control_target_tx: watch::Sender<Option<String>>,
    service_change_tx: watch::Sender<()>,
    registration_shutdown_tx: broadcast::Sender<()>,
    registration_handle: Option<JoinHandle<()>>,
}

impl EdgeRegistrationHandle {
    async fn new(
        control_addr: String,
        node_name: &str,
        services: Vec<ServiceEntry>,
    ) -> Result<Self> {
        let edge_endpoint = build_endpoint().await?;
        let endpoint_id = edge_endpoint.id().to_string();
        let tempdir = writable_tempdir("mesh-core-phase6-registration-multi-");
        let config = Arc::new(RwLock::new(make_edge_config(
            control_addr.clone(),
            tempdir.path().join("data"),
            services,
        )));
        let edge_node = Arc::new(RwLock::new(EdgeNode::with_endpoint(edge_endpoint.clone())));
        let (control_target_tx, _) = watch::channel(Some(control_addr));
        let (service_change_tx, _) = watch::channel(());
        let (registration_shutdown_tx, _) = broadcast::channel(4);

        Ok(Self {
            _tempdir: tempdir,
            node_name: node_name.to_owned(),
            config,
            edge_node,
            edge_endpoint,
            endpoint_id,
            control_target_tx,
            service_change_tx,
            registration_shutdown_tx,
            registration_handle: None,
        })
    }

    fn start(&mut self) {
        assert!(
            self.registration_handle.is_none(),
            "edge registration loop should only be started once"
        );

        let control_target_rx = self.control_target_tx.subscribe();
        let service_change_rx = self.service_change_tx.subscribe();
        let registration_shutdown_rx = self.registration_shutdown_tx.subscribe();
        let handle = tokio::spawn(run_registration_loop(
            self.edge_endpoint.clone(),
            self.node_name.clone(),
            Arc::clone(&self.config),
            Arc::clone(&self.edge_node),
            control_target_rx,
            service_change_rx,
            registration_shutdown_rx,
        ));
        self.registration_handle = Some(handle);
    }

    async fn wait_for_registration(&self, control_node: &Arc<RwLock<ControlNode>>) {
        loop {
            let registered = {
                let control = control_node.read().await;
                let is_online = control
                    .whitelist()
                    .get(&self.endpoint_id)
                    .is_some_and(|node| node.is_online);
                let has_route = control
                    .routes()
                    .values()
                    .any(|route| route.endpoint_id == self.endpoint_id);
                is_online && has_route
            };

            if registered {
                return;
            }

            sleep(POLL_INTERVAL).await;
        }
    }

    async fn wait_for_edge_routes(&self, expected_count: usize, minimum_version: u64) {
        loop {
            let ready = {
                let edge = self.edge_node.read().await;
                edge.cached_routes().len() >= expected_count
                    && edge.route_version() >= minimum_version
            };

            if ready {
                return;
            }

            sleep(POLL_INTERVAL).await;
        }
    }

    async fn wait_for_service_route(&self, service_name: &str, minimum_version: u64) {
        loop {
            let ready = {
                let edge = self.edge_node.read().await;
                edge.route_version() >= minimum_version
                    && edge
                        .cached_routes()
                        .values()
                        .any(|route| route.service_name == service_name)
            };

            if ready {
                return;
            }

            sleep(POLL_INTERVAL).await;
        }
    }

    async fn replace_services_and_reregister(&self, services: Vec<ServiceEntry>) -> Result<()> {
        {
            let mut config = self.config.write().await;
            config.services = services;
        }

        self.service_change_tx
            .send(())
            .map_err(|_| anyhow::anyhow!("failed to trigger service change re-registration"))?;

        Ok(())
    }

    async fn shutdown(mut self) -> Result<()> {
        let _ = self.registration_shutdown_tx.send(());
        self.edge_endpoint.close().await;

        if let Some(handle) = self.registration_handle.take() {
            timeout(Duration::from_secs(2), handle)
                .await
                .context("registration loop did not exit in time")?
                .context("registration loop panicked")?;
        }

        Ok(())
    }
}

#[tokio::test]
async fn test_edge_registration_loop_connects_and_registers() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let harness = RegistrationHarness::start().await?;

        let test_result: Result<()> = async {
            harness.wait_for_registration().await;
            harness.wait_for_edge_routes(1, 1).await;

            let control = harness.control_node.read().await;
            let node = control
                .whitelist()
                .get(&harness.edge_endpoint_id)
                .context("edge node missing from control whitelist")?;
            assert!(node.is_online, "edge should be online after registration");
            assert_eq!(
                control.services().len(),
                1,
                "control should track one service"
            );

            let edge = harness.edge_node.read().await;
            assert!(
                !edge.cached_routes().is_empty(),
                "edge should cache routes after registration"
            );
            assert_eq!(edge.cached_routes().len(), 1);

            Ok(())
        }
        .await;

        let shutdown_result = harness.shutdown().await;
        shutdown_result?;
        test_result
    })
    .await
    .context("edge registration integration test timed out")?
}

#[tokio::test]
async fn test_edge_registration_receives_existing_routes_without_local_services() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let harness = RegistrationHarness::start_with(Vec::new(), true).await?;

        let test_result: Result<()> = async {
            harness.wait_for_registration().await;
            harness.wait_for_edge_routes(1, 1).await;

            let control = harness.control_node.read().await;
            assert_eq!(
                control.services().len(),
                1,
                "control should retain the preloaded service"
            );

            let edge = harness.edge_node.read().await;
            assert_eq!(
                edge.cached_routes().len(),
                1,
                "edge should receive the existing route table snapshot"
            );
            let route = edge
                .cached_routes()
                .values()
                .next()
                .context("edge route cache should contain the preloaded route")?;
            assert_eq!(route.service_name, "seed-api");
            assert_eq!(route.node_name, "edge-seed");

            Ok(())
        }
        .await;

        let shutdown_result = harness.shutdown().await;
        shutdown_result?;
        test_result
    })
    .await
    .context("edge registration existing-route integration test timed out")?
}

#[tokio::test]
async fn test_reregistration_broadcasts_route_updates_to_other_edges() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let control = ControlRegistrationHarness::start().await?;
        let control_addr = control.control_addr()?;

        let mut mele = EdgeRegistrationHandle::new(
            control_addr.clone(),
            "mele",
            vec![make_service_entry("mele-api", "127.0.0.1:18080")],
        )
        .await?;
        let mut gb10 = EdgeRegistrationHandle::new(
            control_addr,
            "gb10",
            vec![make_service_entry("gb10-api", "127.0.0.1:28080")],
        )
        .await?;

        control.allow_edge(&mele.endpoint_id, "mele").await;
        control.allow_edge(&gb10.endpoint_id, "gb10").await;

        let test_result: Result<()> = async {
            mele.start();
            mele.wait_for_registration(&control.control_node).await;
            mele.wait_for_edge_routes(1, 1).await;

            gb10.start();
            gb10.wait_for_registration(&control.control_node).await;
            gb10.wait_for_edge_routes(2, 2).await;

            let gb10_initial_version = {
                let edge = gb10.edge_node.read().await;
                edge.route_version()
            };

            mele.replace_services_and_reregister(vec![
                make_service_entry("mele-api", "127.0.0.1:18080"),
                make_service_entry("mele-admin", "127.0.0.1:18081"),
            ])
            .await?;

            control.wait_for_control_route("mele-admin", 3).await;
            mele.wait_for_service_route("mele-admin", 3).await;
            timeout(
                Duration::from_millis(750),
                gb10.wait_for_service_route("mele-admin", gb10_initial_version + 1),
            )
            .await
            .context(
                "gb10 should receive the route update before reconnect backoff can hide a missing broadcast",
            )?;

            let gb10_edge = gb10.edge_node.read().await;
            assert!(
                gb10_edge.route_version() > gb10_initial_version,
                "gb10 should observe a newer route version after mele re-registers"
            );
            assert!(
                gb10_edge.cached_routes().values().any(|route| {
                    route.endpoint_id == mele.endpoint_id && route.service_name == "mele-admin"
                }),
                "gb10 should receive mele's newly registered service"
            );

            Ok(())
        }
        .await;

        let mele_shutdown = mele.shutdown().await;
        let gb10_shutdown = gb10.shutdown().await;
        let control_shutdown = control.shutdown().await;

        mele_shutdown?;
        gb10_shutdown?;
        control_shutdown?;
        test_result
    })
    .await
    .context("edge re-registration broadcast integration test timed out")?
}

#[tokio::test]
async fn test_ipc_expose_returns_assigned_port_after_reregistration() -> Result<()> {
    timeout(Duration::from_secs(20), async {
        let control = ControlRegistrationHarness::start().await?;
        let tempdir = writable_tempdir("mesh-core-phase6-ipc-expose-");
        let config_path = tempdir.path().join("config.toml");
        let initial_config = make_edge_config(
            control.control_addr()?,
            tempdir.path().join("data"),
            vec![make_service_entry("echo", "127.0.0.1:18080")],
        );
        initial_config.save(&config_path)?;

        let socket_path = initial_config.data_dir.join("daemon.sock");
        let data_dir = initial_config.data_dir.clone();
        let mut daemon = Daemon::new_without_startup_signal(initial_config, config_path);
        let daemon_shutdown_tx = daemon.shutdown_tx().clone();
        let daemon_handle = tokio::spawn(async move { daemon.run().await });

        let test_result: Result<()> = async {
            let ticket = wait_for_join_ticket(&data_dir).await?;
            control.allow_edge(&ticket.endpoint_id, "edge-alpha").await;

            control.wait_for_control_route("echo", 1).await;
            let initial_route_version = control.control_node.read().await.route_version();

            let response = send_ipc_request(
                &socket_path,
                &IpcRequest::ExposeService {
                    name: "admin".to_string(),
                    local_addr: "127.0.0.1:19090".to_string(),
                    protocol: Protocol::Tcp,
                    health_check: None,
                },
            )
            .await?;
            let IpcResponse::ServiceExposed {
                name,
                assigned_port,
            } = response
            else {
                anyhow::bail!("expected service exposed response");
            };
            assert_eq!(name, "admin");

            timeout(
                Duration::from_millis(400),
                control.wait_for_control_route("admin", initial_route_version + 1),
            )
            .await
            .context("runtime re-registration should complete before config watcher debounce")?;

            let control_assigned_port = {
                let control_node = control.control_node.read().await;
                control_node
                    .routes()
                    .iter()
                    .find_map(|(port, route)| (route.service_name == "admin").then_some(*port))
                    .context("control should publish the newly exposed service")?
            };
            assert_eq!(assigned_port, control_assigned_port);

            sleep(Duration::from_millis(800)).await;
            let control_node = control.control_node.read().await;
            assert_eq!(control_node.route_version(), initial_route_version + 1);
            assert!(
                control_node
                    .routes()
                    .values()
                    .any(|route| route.service_name == "admin"),
                "control should retain exactly one route-table update for the exposed service"
            );
            Ok(())
        }
        .await;

        let _ = daemon_shutdown_tx.send(());
        let daemon_result = timeout(Duration::from_secs(10), daemon_handle)
            .await
            .context("daemon task did not exit in time")?
            .context("daemon task panicked")?;
        let control_shutdown = control.shutdown().await;

        daemon_result?;
        control_shutdown?;
        test_result
    })
    .await
    .context("IPC expose runtime re-registration integration test timed out")?
}

#[tokio::test]
async fn test_ipc_expose_propagates_routes_end_to_end() -> Result<()> {
    timeout(Duration::from_secs(30), async {
        let control = ControlRegistrationHarness::start().await?;
        let control_addr = control.control_addr()?;
        let mut edge_one = Some(
            EdgeDaemonHarness::start(
                "edge-one",
                control_addr.clone(),
                vec![make_service_entry("edge-one-api", "127.0.0.1:18080")],
            )
            .await?,
        );
        let mut edge_two = Some(
            EdgeDaemonHarness::start(
                "edge-two",
                control_addr,
                vec![make_service_entry("edge-two-api", "127.0.0.1:28080")],
            )
            .await?,
        );

        let test_result: Result<()> = async {
            let edge_one_daemon = edge_one.as_ref().context("edge-one daemon missing")?;
            let edge_two_daemon = edge_two.as_ref().context("edge-two daemon missing")?;

            let edge_one_ticket = edge_one_daemon.wait_for_join_ticket().await?;
            control
                .allow_edge(&edge_one_ticket.endpoint_id, "edge-one")
                .await;
            control.wait_for_control_route("edge-one-api", 1).await;
            edge_one_daemon
                .wait_for_service_status("edge-one-api", "edge-one", 1)
                .await?;

            let edge_two_ticket = edge_two_daemon.wait_for_join_ticket().await?;
            control
                .allow_edge(&edge_two_ticket.endpoint_id, "edge-two")
                .await;
            control.wait_for_control_route("edge-two-api", 2).await;
            edge_two_daemon
                .wait_for_service_status("edge-one-api", "edge-one", 2)
                .await?;

            let initial_route_version = control.control_node.read().await.route_version();

            let response = edge_one_daemon
                .send_request(&IpcRequest::ExposeService {
                    name: "edge-one-admin".to_string(),
                    local_addr: "127.0.0.1:19090".to_string(),
                    protocol: Protocol::Tcp,
                    health_check: None,
                })
                .await?;
            let IpcResponse::ServiceExposed {
                name,
                assigned_port,
            } = response
            else {
                anyhow::bail!("expected service exposed response");
            };
            assert_eq!(name, "edge-one-admin");

            control
                .wait_for_control_route("edge-one-admin", initial_route_version + 1)
                .await;

            let propagated_service = edge_two_daemon
                .wait_for_service_status("edge-one-admin", "edge-one", initial_route_version + 1)
                .await?;
            assert_eq!(propagated_service.assigned_port, Some(assigned_port));

            let control_assigned_port = {
                let control_node = control.control_node.read().await;
                control_node
                    .routes()
                    .iter()
                    .find_map(|(port, route)| {
                        (route.service_name == "edge-one-admin").then_some(*port)
                    })
                    .context("control should publish the newly exposed service")?
            };
            assert_eq!(assigned_port, control_assigned_port);

            let (cached_port, cached_route) = wait_for_route_cache_service(
                &edge_one_daemon.data_dir,
                "edge-one-admin",
                initial_route_version + 1,
            )
            .await?;
            assert_eq!(cached_port, assigned_port);
            assert_eq!(cached_route.endpoint_id, edge_one_ticket.endpoint_id);
            assert_eq!(cached_route.node_name, "edge-one");
            assert_eq!(cached_route.target_local_addr, "127.0.0.1:19090");

            Ok(())
        }
        .await;

        let edge_one_shutdown = match edge_one.take() {
            Some(edge_one_daemon) => edge_one_daemon.shutdown().await,
            None => Ok(()),
        };
        let edge_two_shutdown = match edge_two.take() {
            Some(edge_two_daemon) => edge_two_daemon.shutdown().await,
            None => Ok(()),
        };
        let control_shutdown = control.shutdown().await;

        edge_one_shutdown?;
        edge_two_shutdown?;
        control_shutdown?;
        test_result
    })
    .await
    .context("IPC expose propagation end-to-end integration test timed out")?
}

#[tokio::test]
async fn test_edge_registration_reconnects_after_control_addr_change() -> Result<()> {
    timeout(Duration::from_secs(20), async {
        let control_a = ControlRegistrationHarness::start().await?;
        let control_b = ControlRegistrationHarness::start().await?;
        let tempdir = writable_tempdir("mesh-core-phase6-control-target-");
        let config_path = tempdir.path().join("config.toml");
        let initial_config = make_edge_config(
            control_a.control_addr()?,
            tempdir.path().join("data"),
            vec![make_service_entry("echo", "127.0.0.1:18080")],
        );
        initial_config.save(&config_path)?;

        let data_dir = initial_config.data_dir.clone();
        let mut daemon = Daemon::new_without_startup_signal(initial_config, config_path.clone());
        let daemon_shutdown_tx = daemon.shutdown_tx().clone();
        let daemon_handle = tokio::spawn(async move { daemon.run().await });

        let test_result: Result<()> = async {
            let ticket = wait_for_join_ticket(&data_dir).await?;
            let edge_endpoint_id = ticket.endpoint_id.clone();
            let control_a_endpoint_id = control_a.control_endpoint.id().to_string();
            let control_b_endpoint_id = control_b.control_endpoint.id().to_string();

            control_a.allow_edge(&edge_endpoint_id, "edge-alpha").await;
            control_b.allow_edge(&edge_endpoint_id, "edge-alpha").await;

            control_a.wait_for_control_route("echo", 1).await;
            wait_for_route_cache_control_identity(&data_dir, &control_a_endpoint_id).await?;

            let mut updated_config = MeshConfig::load(&config_path)?;
            updated_config.control_addr = Some(control_b.control_addr()?);
            updated_config.save(&config_path)?;

            control_b.wait_for_control_route("echo", 1).await;
            wait_for_route_cache_control_identity(&data_dir, &control_b_endpoint_id).await?;

            let control_b_node = control_b.control_node.read().await;
            assert!(
                control_b_node
                    .routes()
                    .values()
                    .any(|route| route.service_name == "echo"
                        && route.endpoint_id == edge_endpoint_id),
                "replacement control should publish the edge route after reload"
            );
            Ok(())
        }
        .await;

        let _ = daemon_shutdown_tx.send(());
        let daemon_result = timeout(Duration::from_secs(10), daemon_handle)
            .await
            .context("daemon task did not exit in time")?
            .context("daemon task panicked")?;
        let control_a_shutdown = control_a.shutdown().await;
        let control_b_shutdown = control_b.shutdown().await;

        daemon_result?;
        control_a_shutdown?;
        control_b_shutdown?;
        test_result
    })
    .await
    .context("control target reload integration test timed out")?
}

#[tokio::test]
async fn test_runtime_control_identity_rotation_reconnects_edges() -> Result<()> {
    timeout(Duration::from_secs(30), async {
        let mut control_a = Some(ControlRegistrationHarness::start().await?);
        let initial_control_addr = control_a
            .as_ref()
            .context("initial control harness missing")?
            .control_addr()?;
        let mut control_b = None;
        let mut edge = Some(
            EdgeDaemonHarness::start(
                "edge-rotating",
                initial_control_addr,
                vec![make_service_entry("echo", "127.0.0.1:18080")],
            )
            .await?,
        );

        let test_result: Result<()> = async {
            let edge_daemon = edge.as_ref().context("edge daemon missing")?;
            let initial_control = control_a
                .as_ref()
                .context("initial control harness missing")?;
            initial_control
                .seed_services(
                    "seed-control-a",
                    "seed-a",
                    &[make_service_registration("legacy-api", "127.0.0.1:28080")],
                    1_700_000_001,
                )
                .await?;

            let edge_ticket = edge_daemon.wait_for_join_ticket().await?;
            initial_control
                .allow_edge(&edge_ticket.endpoint_id, "edge-rotating")
                .await;
            initial_control.wait_for_control_route("echo", 2).await;
            edge_daemon
                .wait_for_service_status("legacy-api", "seed-a", 2)
                .await?;

            let control_a_endpoint_id = initial_control.control_endpoint.id().to_string();
            wait_for_route_cache_control_identity(&edge_daemon.data_dir, &control_a_endpoint_id)
                .await?;

            control_a
                .take()
                .context("initial control harness missing during shutdown")?
                .shutdown()
                .await?;

            let replacement_control = ControlRegistrationHarness::start().await?;
            let control_b_endpoint_id = replacement_control.control_endpoint.id().to_string();
            replacement_control
                .seed_services(
                    "seed-control-b",
                    "seed-b",
                    &[make_service_registration("fresh-api", "127.0.0.1:38080")],
                    1_700_000_002,
                )
                .await?;
            replacement_control
                .allow_edge(&edge_ticket.endpoint_id, "edge-rotating")
                .await;
            let replacement_control_addr = replacement_control.control_addr()?;
            control_b = Some(replacement_control);

            edge_daemon
                .update_control_addr_and_reload(replacement_control_addr)
                .await?;

            let replacement_control = control_b
                .as_ref()
                .context("replacement control harness missing")?;
            replacement_control.wait_for_control_route("echo", 2).await;

            let refreshed_status = wait_for_status(&edge_daemon.socket_path, |status| {
                status.route_table_version >= 2
                    && status
                        .services
                        .iter()
                        .any(|service| service.name == "fresh-api" && service.node_name == "seed-b")
                    && status.services.iter().any(|service| {
                        service.name == "echo" && service.node_name == "edge-rotating"
                    })
                    && status
                        .services
                        .iter()
                        .all(|service| service.name != "legacy-api")
            })
            .await?;
            assert!(
                refreshed_status
                    .services
                    .iter()
                    .all(|service| service.name != "legacy-api"),
                "edge should purge legacy routes from the old control"
            );

            let refreshed_cache = wait_for_route_cache(&edge_daemon.data_dir, |route_cache| {
                route_cache.control_endpoint_id.as_deref() == Some(control_b_endpoint_id.as_str())
                    && route_cache.routes.values().any(|route| {
                        route.service_name == "fresh-api" && route.node_name == "seed-b"
                    })
                    && route_cache.routes.values().any(|route| {
                        route.service_name == "echo" && route.endpoint_id == edge_ticket.endpoint_id
                    })
                    && route_cache
                        .routes
                        .values()
                        .all(|route| route.service_name != "legacy-api")
            })
            .await?;
            assert_eq!(
                refreshed_cache.control_endpoint_id.as_deref(),
                Some(control_b_endpoint_id.as_str())
            );
            wait_for_route_cache_control_identity(&edge_daemon.data_dir, &control_b_endpoint_id)
                .await?;

            let replacement_control = replacement_control.control_node.read().await;
            assert!(
                replacement_control.routes().values().any(|route| {
                    route.service_name == "echo" && route.endpoint_id == edge_ticket.endpoint_id
                }),
                "replacement control should publish the reconnected edge route"
            );
            assert!(
                replacement_control
                    .routes()
                    .values()
                    .any(|route| route.service_name == "fresh-api" && route.node_name == "seed-b"),
                "replacement control should keep its own fresh routes"
            );

            Ok(())
        }
        .await;

        let edge_shutdown = match edge.take() {
            Some(edge_daemon) => edge_daemon.shutdown().await,
            None => Ok(()),
        };
        let control_a_shutdown = match control_a.take() {
            Some(initial_control) => initial_control.shutdown().await,
            None => Ok(()),
        };
        let control_b_shutdown = match control_b.take() {
            Some(replacement_control) => replacement_control.shutdown().await,
            None => Ok(()),
        };

        edge_shutdown?;
        control_a_shutdown?;
        control_b_shutdown?;
        test_result
    })
    .await
    .context("runtime control identity rotation integration test timed out")?
}
