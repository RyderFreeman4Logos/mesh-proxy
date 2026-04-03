use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use iroh::endpoint::presets;
use mesh_core::edge_registration::run_registration_loop;
use mesh_core::{ControlNode, EdgeNode, run_accept_loop};
use mesh_proto::{
    ALPN_CONTROL, DEFAULT_SERVICE_QUOTA, MeshConfig, NodeInfo, NodeRole, Protocol, ServiceEntry,
    ServiceRegistration,
};
use tempfile::Builder;
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

fn make_edge_config(
    control_addr: String,
    data_dir: std::path::PathBuf,
    services: Vec<ServiceEntry>,
) -> MeshConfig {
    MeshConfig {
        node_name: "edge-alpha".to_string(),
        role: NodeRole::Edge,
        secret_key: None,
        control_addr: Some(control_addr),
        health_bind: None,
        services,
        data_dir,
    }
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
        let (_service_change_tx, service_change_rx) = watch::channel(());
        let (registration_shutdown_tx, registration_shutdown_rx) = broadcast::channel(4);

        let registration_handle = tokio::spawn(run_registration_loop(
            edge_endpoint.clone(),
            control_addr,
            "edge-alpha".to_string(),
            Arc::clone(&config),
            Arc::clone(&edge_node),
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
