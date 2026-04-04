use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use mesh_proto::{
    ALPN_CONTROL, ControlMessage, DEFAULT_SERVICE_QUOTA, JoinTicket, NodeInfo, PortAssignment,
    RouteEntry, ServiceId, ServiceRecord,
};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{info, warn};

use crate::{
    persistence::{self, PersistenceError},
    port_allocator::PortAllocator,
};

/// Error returned when a node exceeds its service quota.
#[derive(Debug, thiserror::Error)]
pub enum QuotaError {
    #[error("quota exceeded: node has {current} services, limit is {limit}")]
    Exceeded { current: usize, limit: u16 },
    #[error("node not found in whitelist: {endpoint_id}")]
    NodeNotFound { endpoint_id: String },
}

/// Error returned when ticket validation fails.
#[derive(Debug, thiserror::Error)]
pub enum TicketError {
    #[error("ticket has expired")]
    Expired,
    #[error("ticket nonce already consumed (replay)")]
    AlreadyUsed,
    #[error("ticket endpoint_id does not match sender")]
    EndpointMismatch,
}

/// Read-only snapshot of the control node's core state for serialization
/// or inspection without holding a mutable reference.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ControlNodeSnapshot {
    pub whitelist: HashMap<String, NodeInfo>,
    pub services: HashMap<ServiceId, ServiceRecord>,
    pub routes: HashMap<u16, RouteEntry>,
    pub route_version: u64,
}

/// Central state container for the control node.
///
/// Manages the node whitelist, service registry, port allocator, and global
/// route table. All mutations go through dedicated methods that maintain
/// internal consistency.
#[derive(Debug)]
pub struct ControlNode {
    whitelist: HashMap<String, NodeInfo>,
    allocator: PortAllocator,
    services: HashMap<ServiceId, ServiceRecord>,
    routes: HashMap<u16, RouteEntry>,
    route_version: u64,
    accepted_at: HashMap<String, u64>,
    data_dir: Option<PathBuf>,
    /// Maps ticket nonce → expiry epoch for replay prevention with bounded growth.
    used_tickets: HashMap<[u8; 16], u64>,
    /// Epoch timestamp of the most recent pong received from each endpoint.
    last_pong: HashMap<String, u64>,
}

#[derive(Debug, Clone)]
struct ControlNodeRollbackState {
    whitelist: HashMap<String, NodeInfo>,
    allocator: PortAllocator,
    services: HashMap<ServiceId, ServiceRecord>,
    routes: HashMap<u16, RouteEntry>,
    route_version: u64,
    accepted_at: HashMap<String, u64>,
    last_pong: HashMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct PersistedWhitelistEntry {
    endpoint_id: String,
    node_name: String,
    accepted_at: u64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct PersistedRoutes {
    routes: HashMap<u16, RouteEntry>,
    version: u64,
}

#[derive(Debug, Default)]
struct ActiveControlConnections {
    by_endpoint_id: RwLock<HashMap<String, iroh::endpoint::Connection>>,
}

impl ActiveControlConnections {
    async fn insert(&self, endpoint_id: &str, connection: &iroh::endpoint::Connection) {
        self.by_endpoint_id
            .write()
            .await
            .insert(endpoint_id.to_owned(), connection.clone());
    }

    async fn remove_if_stale(
        &self,
        endpoint_id: &str,
        stale_connection: &iroh::endpoint::Connection,
    ) {
        let stale_connection_id = stale_connection.stable_id();
        let mut connections = self.by_endpoint_id.write().await;
        let should_remove = connections
            .get(endpoint_id)
            .is_some_and(|connection| connection.stable_id() == stale_connection_id);

        if should_remove {
            connections.remove(endpoint_id);
        }
    }

    async fn snapshot(
        &self,
        exclude_endpoint_id: Option<&str>,
    ) -> Vec<(String, iroh::endpoint::Connection)> {
        self.by_endpoint_id
            .read()
            .await
            .iter()
            .filter(|(endpoint_id, _)| Some(endpoint_id.as_str()) != exclude_endpoint_id)
            .map(|(endpoint_id, connection)| (endpoint_id.clone(), connection.clone()))
            .collect()
    }
}

impl Default for ControlNode {
    fn default() -> Self {
        Self::new()
    }
}

impl ControlNode {
    /// Create a new control node with empty state.
    pub fn new() -> Self {
        Self {
            whitelist: HashMap::new(),
            allocator: PortAllocator::new(),
            services: HashMap::new(),
            routes: HashMap::new(),
            route_version: 0,
            accepted_at: HashMap::new(),
            data_dir: None,
            used_tickets: HashMap::new(),
            last_pong: HashMap::new(),
        }
    }

    /// Create a control node backed by persisted disk state.
    pub fn load_from_disk(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        let mut node = Self {
            data_dir: Some(data_dir.clone()),
            ..Self::new()
        };

        let persisted_whitelist = node.load_whitelist_entries();
        node.whitelist = persisted_whitelist
            .iter()
            .map(|entry| {
                (
                    entry.endpoint_id.clone(),
                    NodeInfo {
                        endpoint_id: entry.endpoint_id.clone(),
                        node_name: entry.node_name.clone(),
                        quota_limit: DEFAULT_SERVICE_QUOTA as u16,
                        quota_used: 0,
                        is_online: false,
                        last_heartbeat: None,
                        addr: None,
                    },
                )
            })
            .collect();
        node.accepted_at = persisted_whitelist
            .into_iter()
            .map(|entry| (entry.endpoint_id, entry.accepted_at))
            .collect();

        let persisted_routes = node.load_routes();
        node.route_version = persisted_routes.version;
        node.routes = persisted_routes.routes;
        node.rebuild_runtime_state_from_routes();
        node
    }

    /// Take a read-only snapshot of the core state (whitelist, services,
    /// routes, version). Useful for persistence or status queries.
    pub fn snapshot(&self) -> ControlNodeSnapshot {
        ControlNodeSnapshot {
            whitelist: self.whitelist.clone(),
            services: self.services.clone(),
            routes: self.routes.clone(),
            route_version: self.route_version,
        }
    }

    fn rollback_state(&self) -> ControlNodeRollbackState {
        ControlNodeRollbackState {
            whitelist: self.whitelist.clone(),
            allocator: self.allocator.clone(),
            services: self.services.clone(),
            routes: self.routes.clone(),
            route_version: self.route_version,
            accepted_at: self.accepted_at.clone(),
            last_pong: self.last_pong.clone(),
        }
    }

    fn restore_rollback_state(&mut self, state: ControlNodeRollbackState) {
        self.whitelist = state.whitelist;
        self.allocator = state.allocator;
        self.services = state.services;
        self.routes = state.routes;
        self.route_version = state.route_version;
        self.accepted_at = state.accepted_at;
        self.last_pong = state.last_pong;
    }

    fn whitelist_path(&self) -> Option<PathBuf> {
        self.data_dir.as_ref().map(|dir| dir.join("whitelist.json"))
    }

    fn routes_path(&self) -> Option<PathBuf> {
        self.data_dir.as_ref().map(|dir| dir.join("routes.json"))
    }

    fn load_whitelist_entries(&self) -> Vec<PersistedWhitelistEntry> {
        let Some(path) = self.whitelist_path() else {
            return Vec::new();
        };

        match persistence::load_state::<Vec<PersistedWhitelistEntry>>(&path) {
            Ok(Some(entries)) => entries,
            Ok(None) => Vec::new(),
            Err(error) => {
                warn!(path = %path.display(), error = %error, "failed to load whitelist, starting fresh");
                Vec::new()
            }
        }
    }

    fn load_routes(&self) -> PersistedRoutes {
        let Some(path) = self.routes_path() else {
            return PersistedRoutes {
                routes: HashMap::new(),
                version: 0,
            };
        };

        match persistence::load_state::<PersistedRoutes>(&path) {
            Ok(Some(routes)) => routes,
            Ok(None) => PersistedRoutes {
                routes: HashMap::new(),
                version: 0,
            },
            Err(error) => {
                warn!(path = %path.display(), error = %error, "failed to load persisted routes, starting fresh");
                PersistedRoutes {
                    routes: HashMap::new(),
                    version: 0,
                }
            }
        }
    }

    fn persist_whitelist(&self) -> Result<(), PersistenceError> {
        let Some(path) = self.whitelist_path() else {
            return Ok(());
        };

        let mut entries = self
            .whitelist
            .values()
            .map(|node| PersistedWhitelistEntry {
                endpoint_id: node.endpoint_id.clone(),
                node_name: node.node_name.clone(),
                accepted_at: self
                    .accepted_at
                    .get(&node.endpoint_id)
                    .copied()
                    .unwrap_or(0),
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));

        persistence::save_atomic_sync(&path, &entries)
    }

    fn persist_routes(&self) -> Result<(), PersistenceError> {
        let Some(path) = self.routes_path() else {
            return Ok(());
        };

        persistence::save_atomic_sync(
            &path,
            &PersistedRoutes {
                routes: self.routes.clone(),
                version: self.route_version,
            },
        )
    }

    fn rebuild_runtime_state_from_routes(&mut self) {
        self.allocator = PortAllocator::new();
        self.services.clear();
        self.last_pong.clear();

        for node in self.whitelist.values_mut() {
            node.quota_used = 0;
            node.is_online = false;
            node.last_heartbeat = None;
            node.addr = None;
        }

        let mut sorted_routes = self.routes.iter().collect::<Vec<_>>();
        sorted_routes.sort_by_key(|(port, _)| **port);

        let mut restored_routes = HashMap::with_capacity(self.routes.len());

        for (&port, route) in sorted_routes {
            let Some(node) = self.whitelist.get_mut(&route.endpoint_id) else {
                warn!(
                    endpoint_id = %route.endpoint_id,
                    service_name = %route.service_name,
                    port,
                    "skipping persisted route for unknown whitelist entry"
                );
                continue;
            };

            let service_id = ServiceId {
                endpoint_id: route.endpoint_id.clone(),
                service_name: route.service_name.clone(),
            };
            self.allocator.restore_active(port, service_id.clone());
            self.services.insert(
                service_id.clone(),
                ServiceRecord {
                    service_id,
                    node_name: route.node_name.clone(),
                    local_addr: route.target_local_addr.clone(),
                    protocol: route.protocol,
                    published_port: Some(port),
                    health_state: mesh_proto::HealthState::Unknown,
                    last_seen: None,
                },
            );
            node.quota_used = node.quota_used.saturating_add(1);
            restored_routes.insert(port, route.clone());
        }

        self.routes = restored_routes;
    }

    /// Add a node to the whitelist. Overwrites any existing entry with the
    /// same endpoint ID.
    pub fn add_node(&mut self, info: NodeInfo) {
        self.whitelist.insert(info.endpoint_id.clone(), info);
    }

    /// Add a node to the whitelist and persist the accepted-node list.
    pub fn accept_node(
        &mut self,
        info: NodeInfo,
        accepted_at: u64,
    ) -> Result<(), PersistenceError> {
        let rollback = self.rollback_state();
        self.accepted_at
            .insert(info.endpoint_id.clone(), accepted_at);
        self.whitelist.insert(info.endpoint_id.clone(), info);

        if let Err(error) = self.persist_whitelist() {
            self.restore_rollback_state(rollback);
            return Err(error);
        }

        Ok(())
    }

    /// Remove a node from the whitelist, release all its allocated ports,
    /// and clean up heartbeat tracking.
    pub fn remove_node(&mut self, endpoint_id: &str) {
        self.whitelist.remove(endpoint_id);
        self.accepted_at.remove(endpoint_id);
        self.last_pong.remove(endpoint_id);

        // Collect service IDs belonging to this node.
        let owned_services: Vec<ServiceId> = self
            .services
            .keys()
            .filter(|sid| sid.endpoint_id == endpoint_id)
            .cloned()
            .collect();

        for sid in &owned_services {
            self.services.remove(sid);
        }

        // Release ports assigned to those services.
        let orphaned_ports: Vec<u16> = self
            .routes
            .iter()
            .filter(|(_, entry)| entry.endpoint_id == endpoint_id)
            .map(|(&port, _)| port)
            .collect();

        for port in orphaned_ports {
            self.routes.remove(&port);
            self.allocator.release(port);
        }

        if let Err(error) = self.persist_whitelist() {
            warn!(
                endpoint_id,
                error = %error,
                "failed to persist whitelist after node removal"
            );
        }
        if let Err(error) = self.persist_routes() {
            warn!(
                endpoint_id,
                error = %error,
                "failed to persist routes after node removal"
            );
        }
    }

    /// Check whether the node identified by `endpoint_id` can register
    /// another service without exceeding its quota.
    pub fn check_quota(&self, endpoint_id: &str) -> Result<(), QuotaError> {
        let node = self
            .whitelist
            .get(endpoint_id)
            .ok_or_else(|| QuotaError::NodeNotFound {
                endpoint_id: endpoint_id.to_owned(),
            })?;

        let current = self
            .services
            .keys()
            .filter(|sid| sid.endpoint_id == endpoint_id)
            .count();

        if current >= node.quota_limit as usize {
            return Err(QuotaError::Exceeded {
                current,
                limit: node.quota_limit,
            });
        }

        Ok(())
    }

    /// Returns a stable snapshot of quota usage for every whitelisted node.
    pub fn quota_snapshot(&self) -> Vec<(String, usize, usize)> {
        let mut quotas = self
            .whitelist
            .values()
            .map(|node| {
                let used = self
                    .services
                    .keys()
                    .filter(|sid| sid.endpoint_id == node.endpoint_id)
                    .count();
                (
                    node.endpoint_id.clone(),
                    used,
                    usize::from(node.quota_limit),
                )
            })
            .collect::<Vec<_>>();
        quotas.sort_by(|left, right| left.0.cmp(&right.0));
        quotas
    }

    /// Updates the configured service quota for a whitelisted node.
    pub fn set_quota_limit(&mut self, endpoint_id: &str, limit: u16) -> Result<(), QuotaError> {
        let node = self
            .whitelist
            .get_mut(endpoint_id)
            .ok_or_else(|| QuotaError::NodeNotFound {
                endpoint_id: endpoint_id.to_owned(),
            })?;
        node.quota_limit = limit;
        Ok(())
    }

    /// Returns an immutable reference to the port allocator.
    pub fn allocator(&self) -> &PortAllocator {
        &self.allocator
    }

    /// Returns a mutable reference to the port allocator.
    pub fn allocator_mut(&mut self) -> &mut PortAllocator {
        &mut self.allocator
    }

    /// Record a ticket nonce as used with the given expiry epoch.
    /// Returns `false` if already consumed.
    pub fn consume_ticket(&mut self, nonce: [u8; 16], expiry_epoch: u64) -> bool {
        use std::collections::hash_map::Entry;
        match self.used_tickets.entry(nonce) {
            Entry::Occupied(_) => false,
            Entry::Vacant(e) => {
                e.insert(expiry_epoch);
                true
            }
        }
    }

    /// Remove all ticket nonces whose expiry epoch is at or before `now_epoch`.
    pub fn prune_expired_tickets(&mut self, now_epoch: u64) {
        self.used_tickets.retain(|_, expiry| *expiry > now_epoch);
    }

    /// Returns the current route version counter.
    pub fn route_version(&self) -> u64 {
        self.route_version
    }

    /// Access the service registry.
    pub fn services(&self) -> &HashMap<ServiceId, ServiceRecord> {
        &self.services
    }

    /// Access the route table.
    pub fn routes(&self) -> &HashMap<u16, RouteEntry> {
        &self.routes
    }

    /// Access the whitelist.
    pub fn whitelist(&self) -> &HashMap<String, NodeInfo> {
        &self.whitelist
    }

    // -- Ticket validation (2B-06) --

    /// Validate a join ticket and consume its nonce if valid.
    ///
    /// Checks expiration, replay (nonce already used), and endpoint identity.
    /// On success the nonce is recorded so the same ticket cannot be reused.
    pub fn validate_ticket(
        &mut self,
        ticket: &JoinTicket,
        from_endpoint_id: &str,
        now_epoch: u64,
    ) -> Result<(), TicketError> {
        if ticket.is_expired(now_epoch) {
            return Err(TicketError::Expired);
        }
        if self.used_tickets.contains_key(&ticket.nonce) {
            return Err(TicketError::AlreadyUsed);
        }
        if ticket.endpoint_id != from_endpoint_id {
            return Err(TicketError::EndpointMismatch);
        }
        let expiry = ticket.created_at.saturating_add(ticket.ttl_seconds);
        self.used_tickets.insert(ticket.nonce, expiry);
        Ok(())
    }

    // -- Heartbeat tracking (2B-07) --

    /// Record a pong received from an edge node.
    pub fn record_pong(&mut self, endpoint_id: &str, now_epoch: u64) {
        self.last_pong.insert(endpoint_id.to_owned(), now_epoch);
        if let Some(node) = self.whitelist.get_mut(endpoint_id) {
            node.is_online = true;
            node.last_heartbeat = Some(now_epoch);
        }
    }

    /// Returns endpoint IDs of nodes whose last pong is older than
    /// `HEARTBEAT_TIMEOUT_SECS` relative to `now_epoch`.
    pub fn check_heartbeats(&self, now_epoch: u64) -> Vec<String> {
        self.last_pong
            .iter()
            .filter(|(_, last)| {
                now_epoch.saturating_sub(**last) > mesh_proto::HEARTBEAT_TIMEOUT_SECS
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    // -- Service registration (2C-02) --

    /// Register services for a node, allocating ports atomically.
    ///
    /// On success, returns port assignments and updates the whitelist, service
    /// registry, and route table. On failure (quota exceeded, pool exhausted),
    /// all tentative allocations are rolled back.
    pub fn register_services(
        &mut self,
        endpoint_id: &str,
        node_name: &str,
        registrations: &[mesh_proto::ServiceRegistration],
        now_epoch: u64,
    ) -> Result<Vec<PortAssignment>, String> {
        let quota_limit = self
            .whitelist
            .get(endpoint_id)
            .ok_or_else(|| format!("node not found in whitelist: {endpoint_id}"))?
            .quota_limit;
        let current_count = self
            .services
            .keys()
            .filter(|sid| sid.endpoint_id == endpoint_id)
            .count();

        let mut requested_services = HashMap::with_capacity(registrations.len());
        let mut requested_order = Vec::with_capacity(registrations.len());
        for reg in registrations {
            let sid = ServiceId {
                endpoint_id: endpoint_id.to_owned(),
                service_name: reg.name.clone(),
            };
            if requested_services.insert(sid.clone(), reg).is_some() {
                return Err(format!("duplicate service registration: {}", reg.name));
            }
            requested_order.push(sid);
        }

        let final_count = requested_order.len();
        if final_count > quota_limit as usize {
            return Err(format!(
                "quota exceeded: have {current_count}, want final {final_count}, limit {}",
                quota_limit
            ));
        }

        let rollback = self.rollback_state();

        let existing_services = self
            .services
            .iter()
            .filter(|(sid, _)| sid.endpoint_id == endpoint_id)
            .map(|(sid, record)| (sid.clone(), record.clone()))
            .collect::<HashMap<_, _>>();

        // Stage allocator changes first so failures do not partially mutate state.
        let mut staged_allocator = self.allocator.clone();
        for (sid, record) in &existing_services {
            if !requested_services.contains_key(sid)
                && let Some(port) = record.published_port
            {
                staged_allocator.release(port);
            }
        }

        let mut assignments = Vec::with_capacity(registrations.len());
        let mut newly_allocated_ports = Vec::new();

        for (sid, reg) in requested_order.iter().zip(registrations.iter()) {
            let assigned_port = match existing_services
                .get(sid)
                .and_then(|record| record.published_port)
            {
                Some(port) => port,
                None => match staged_allocator.allocate(sid, now_epoch) {
                    Some(port) => {
                        newly_allocated_ports.push(port);
                        port
                    }
                    None => return Err("port pool exhausted".to_owned()),
                },
            };

            assignments.push(PortAssignment {
                service_name: reg.name.clone(),
                assigned_port,
            });
        }

        for port in newly_allocated_ports {
            staged_allocator.confirm(port);
        }
        self.allocator = staged_allocator;

        let registered_service_ids = existing_services.keys().cloned().collect::<Vec<_>>();
        for sid in registered_service_ids {
            self.services.remove(&sid);
        }

        let existing_route_ports = self
            .routes
            .iter()
            .filter(|(_, entry)| entry.endpoint_id == endpoint_id)
            .map(|(&port, _)| port)
            .collect::<Vec<_>>();
        for port in existing_route_ports {
            self.routes.remove(&port);
        }

        if let Some(info) = self.whitelist.get_mut(endpoint_id) {
            info.is_online = true;
            info.last_heartbeat = Some(now_epoch);
            info.quota_used = final_count.min(u16::MAX as usize) as u16;
        }

        for ((sid, reg), assignment) in requested_order
            .iter()
            .zip(registrations.iter())
            .zip(assignments.iter())
        {
            let health_state = existing_services
                .get(sid)
                .map_or(mesh_proto::HealthState::Unknown, |record| {
                    record.health_state
                });
            let record = ServiceRecord {
                service_id: sid.clone(),
                node_name: node_name.to_owned(),
                local_addr: reg.local_addr.clone(),
                protocol: reg.protocol,
                published_port: Some(assignment.assigned_port),
                health_state,
                last_seen: Some(now_epoch),
            };
            self.services.insert(sid.clone(), record);

            // Insert route entry.
            self.routes.insert(
                assignment.assigned_port,
                RouteEntry {
                    service_name: reg.name.clone(),
                    node_name: node_name.to_owned(),
                    endpoint_id: endpoint_id.to_owned(),
                    target_local_addr: reg.local_addr.clone(),
                    protocol: reg.protocol,
                },
            );
        }

        self.route_version += 1;
        if let Err(error) = self.persist_routes() {
            self.restore_rollback_state(rollback);
            return Err(format!("failed to persist routes: {error}"));
        }
        Ok(assignments)
    }

    /// Returns a list of online nodes with their endpoint addresses for broadcasting.
    pub fn online_node_addrs(&self) -> Vec<(String, String)> {
        self.whitelist
            .values()
            .filter(|n| n.is_online)
            .filter_map(|n| n.addr.as_ref().map(|a| (n.endpoint_id.clone(), a.clone())))
            .collect()
    }

    /// Update the stored address for a node (used when accepting connections).
    pub fn set_node_addr(&mut self, endpoint_id: &str, addr: String) {
        if let Some(info) = self.whitelist.get_mut(endpoint_id) {
            info.addr = Some(addr);
        }
    }

    // -- Health aggregation (2B-08) --

    /// Update health state for services reported by an edge node.
    ///
    /// Unknown services (not in the registry) are logged and skipped
    /// rather than causing a panic.
    pub fn aggregate_health(
        &mut self,
        from_endpoint_id: &str,
        services: Vec<mesh_proto::ServiceHealthEntry>,
        now_epoch: u64,
    ) {
        if let Some(node) = self.whitelist.get_mut(from_endpoint_id) {
            node.is_online = true;
            node.last_heartbeat = Some(now_epoch);
        }

        for entry in services {
            let sid = ServiceId {
                endpoint_id: from_endpoint_id.to_owned(),
                service_name: entry.service_name.clone(),
            };
            if let Some(record) = self.services.get_mut(&sid) {
                record.health_state = entry.health_state;
                record.last_seen = Some(now_epoch);
            } else {
                tracing::warn!(
                    endpoint_id = from_endpoint_id,
                    service_name = entry.service_name,
                    "unknown service in health report, skipping"
                );
            }
        }
    }
}

// -- Network layer (2C-01, 2C-02, 2C-03, 2C-04) --

fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Accept incoming control-plane connections, verify ALPN, and dispatch messages.
///
/// Runs until a shutdown signal is received or the endpoint is closed.
pub async fn run_accept_loop(
    node: Arc<RwLock<ControlNode>>,
    endpoint: iroh::Endpoint,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    let active_connections = Arc::new(ActiveControlConnections::default());

    loop {
        tokio::select! {
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else {
                    info!("endpoint closed, stopping accept loop");
                    break;
                };
                // Accept the connection and complete the handshake.
                // ALPN is only available after handshake on Connection<HandshakeCompleted>.
                let connection = match incoming.accept() {
                    Ok(connecting) => match connecting.await {
                        Ok(conn) => conn,
                        Err(e) => {
                            warn!(error = %e, "failed to complete connection handshake");
                            continue;
                        }
                    },
                    Err(e) => {
                        warn!(error = %e, "failed to accept incoming connection");
                        continue;
                    }
                };

                // Check ALPN after handshake.
                if connection.alpn() != ALPN_CONTROL {
                    warn!(
                        alpn = ?String::from_utf8_lossy(connection.alpn()),
                        "rejecting connection with unexpected ALPN"
                    );
                    connection.close(0u32.into(), b"wrong ALPN");
                    continue;
                }

                let node = Arc::clone(&node);
                let active_connections = Arc::clone(&active_connections);
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(node, active_connections, connection).await {
                        warn!(error = %e, "control connection handler error");
                    }
                });
            }
            _ = shutdown.recv() => {
                info!("accept loop received shutdown signal");
                break;
            }
        }
    }
}

/// Handle a single control-plane connection: read messages in a loop and dispatch.
async fn handle_connection(
    node: Arc<RwLock<ControlNode>>,
    active_connections: Arc<ActiveControlConnections>,
    connection: iroh::endpoint::Connection,
) -> anyhow::Result<()> {
    let remote_id = connection.remote_id();
    let remote_id_string = remote_id.to_string();
    info!(%remote_id, "control connection established");

    // Store the endpoint ID string for route broadcasting.
    {
        let mut n = node.write().await;
        n.set_node_addr(&remote_id_string, remote_id_string.clone());
    }
    active_connections
        .insert(&remote_id_string, &connection)
        .await;

    let result: anyhow::Result<()> = async {
        loop {
            let (mut send, mut recv) = match connection.accept_bi().await {
                Ok(pair) => pair,
                Err(e) => {
                    // Connection closed or error — stop handling.
                    info!(error = %e, "control connection closed");
                    break;
                }
            };

            let msg: ControlMessage = match mesh_proto::frame::read_json(&mut recv).await {
                Ok(msg) => msg,
                Err(e) => {
                    warn!(error = %e, "failed to read control message");
                    break;
                }
            };

            match msg {
                ControlMessage::Register {
                    node_name,
                    auth_ticket,
                    services,
                } => {
                    if let Err(e) = handle_register(
                        &node,
                        &active_connections,
                        &remote_id_string,
                        &auth_ticket,
                        &node_name,
                        services,
                        &mut send,
                    )
                    .await
                    {
                        warn!(error = %e, "register handler failed");
                    }
                }
                ControlMessage::RouteTableRequest => {
                    if let Err(error) = write_full_route_table(&node, &mut send).await {
                        warn!(error = %error, "failed to respond to route table request");
                    }
                }
                ControlMessage::HealthReport {
                    endpoint_id,
                    services,
                } => {
                    let mut n = node.write().await;
                    n.aggregate_health(&endpoint_id, services, now_epoch());
                }
                ControlMessage::Ping => {
                    if let Err(e) =
                        mesh_proto::frame::write_json(&mut send, &ControlMessage::Pong).await
                    {
                        warn!(error = %e, "failed to send Pong");
                    }
                }
                ControlMessage::Pong => {
                    let mut n = node.write().await;
                    n.record_pong(&remote_id_string, now_epoch());
                }
                other => {
                    warn!(?other, "unexpected control message from edge");
                }
            }

            // Finish the stream so the peer knows this request is done.
            send.finish()
                .map_err(|e| anyhow::anyhow!("failed to finish send stream: {e}"))?;
        }

        Ok(())
    }
    .await;

    active_connections
        .remove_if_stale(&remote_id_string, &connection)
        .await;
    result
}

async fn write_full_route_table(
    node: &Arc<RwLock<ControlNode>>,
    send: &mut iroh::endpoint::SendStream,
) -> anyhow::Result<()> {
    let (routes, version) = {
        let node = node.read().await;
        (node.routes().clone(), node.route_version())
    };
    let route_update = ControlMessage::RouteTableUpdate { routes, version };
    mesh_proto::frame::write_json(send, &route_update)
        .await
        .context("failed to send full route table")
}

/// Process a Register message: validate ticket, allocate ports, respond with Ack/Nack.
async fn handle_register(
    node: &Arc<RwLock<ControlNode>>,
    active_connections: &Arc<ActiveControlConnections>,
    endpoint_id: &str,
    auth_ticket_str: &str,
    node_name: &str,
    services: Vec<mesh_proto::ServiceRegistration>,
    send: &mut iroh::endpoint::SendStream,
) -> anyhow::Result<()> {
    let now = now_epoch();

    // Decode ticket.
    let ticket = match JoinTicket::from_bs58(auth_ticket_str) {
        Ok(t) => t,
        Err(e) => {
            let reason = format!("invalid ticket: {e}");
            warn!(%reason, "register rejected");
            mesh_proto::frame::write_json(send, &ControlMessage::RegisterNack { reason })
                .await
                .context("failed to send RegisterNack")?;
            return Ok(());
        }
    };

    // Validate ticket (expiry, replay, endpoint match).
    {
        let mut n = node.write().await;
        if let Err(e) = n.validate_ticket(&ticket, endpoint_id, now) {
            let reason = format!("ticket validation failed: {e}");
            warn!(%reason, "register rejected");
            mesh_proto::frame::write_json(send, &ControlMessage::RegisterNack { reason })
                .await
                .context("failed to send RegisterNack")?;
            return Ok(());
        }
    }

    // Register services (quota check + port allocation, atomic).
    let result = {
        let mut n = node.write().await;
        n.register_services(endpoint_id, node_name, &services, now)
    };

    match result {
        Ok(assignments) => {
            info!(
                endpoint_id,
                node_name,
                count = assignments.len(),
                "node registered successfully"
            );
            mesh_proto::frame::write_json(send, &ControlMessage::RegisterAck { assignments })
                .await
                .context("failed to send RegisterAck")?;

            if let Err(error) = write_full_route_table(node, send).await {
                warn!(
                    error = %error,
                    "failed to send initial route table after registration"
                );
            }

            // TODO: Debounce this fan-out when many edges re-register at once to avoid O(N^2)
            // route broadcasts during control-plane churn.
            broadcast_routes(node, active_connections, Some(endpoint_id)).await;
        }
        Err(reason) => {
            warn!(%reason, "register rejected");
            mesh_proto::frame::write_json(send, &ControlMessage::RegisterNack { reason })
                .await
                .context("failed to send RegisterNack")?;
        }
    }

    Ok(())
}

/// Broadcast the current route table to every connected edge except the sender.
async fn broadcast_routes(
    node: &Arc<RwLock<ControlNode>>,
    active_connections: &Arc<ActiveControlConnections>,
    exclude_endpoint_id: Option<&str>,
) {
    const ROUTE_BROADCAST_TIMEOUT: Duration = Duration::from_secs(5);

    let (routes, version) = {
        let n = node.read().await;
        (n.routes().clone(), n.route_version())
    };
    let targets = active_connections.snapshot(exclude_endpoint_id).await;

    let update = Arc::new(ControlMessage::RouteTableUpdate { routes, version });
    let mut broadcasts = JoinSet::new();

    for (endpoint_id, connection) in targets {
        let update = Arc::clone(&update);
        broadcasts.spawn(async move {
            match timeout(ROUTE_BROADCAST_TIMEOUT, async {
                let (mut send, _recv) = connection.open_bi().await.context("open_bi failed")?;
                mesh_proto::frame::write_json(&mut send, update.as_ref())
                    .await
                    .context("write route update failed")?;
                send.finish()
                    .map_err(|error| anyhow::anyhow!("finish failed: {error}"))?;
                Ok::<(), anyhow::Error>(())
            })
            .await
            {
                Ok(Ok(())) => Ok::<(), (String, anyhow::Error)>(()),
                Ok(Err(error)) => Err((endpoint_id, error)),
                Err(_) => Err((
                    endpoint_id,
                    anyhow::anyhow!(
                        "route broadcast timed out after {}s",
                        ROUTE_BROADCAST_TIMEOUT.as_secs()
                    ),
                )),
            }
        });
    }

    while let Some(result) = broadcasts.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err((endpoint_id, error))) => {
                warn!(endpoint_id = %endpoint_id, error = %error, "route broadcast to node failed");
            }
            Err(error) => {
                warn!(error = %error, "route broadcast task join failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mesh_proto::DEFAULT_SERVICE_QUOTA;
    use tempfile::Builder;

    use super::*;

    fn writable_tempdir(prefix: &str) -> tempfile::TempDir {
        Builder::new().prefix(prefix).tempdir_in("/tmp").unwrap()
    }

    fn make_node(endpoint_id: &str, quota: u16) -> NodeInfo {
        NodeInfo {
            endpoint_id: endpoint_id.to_owned(),
            node_name: format!("node-{endpoint_id}"),
            quota_limit: quota,
            quota_used: 0,
            is_online: true,
            last_heartbeat: None,
            addr: None,
        }
    }

    fn make_service(endpoint_id: &str, name: &str) -> (ServiceId, ServiceRecord) {
        let sid = ServiceId {
            endpoint_id: endpoint_id.to_owned(),
            service_name: name.to_owned(),
        };
        let rec = ServiceRecord {
            service_id: sid.clone(),
            node_name: format!("node-{endpoint_id}"),
            local_addr: "127.0.0.1:8080".to_owned(),
            protocol: mesh_proto::Protocol::Tcp,
            published_port: None,
            health_state: mesh_proto::HealthState::Unknown,
            last_seen: None,
        };
        (sid, rec)
    }

    fn make_registration(name: &str, local_addr: &str) -> mesh_proto::ServiceRegistration {
        mesh_proto::ServiceRegistration {
            name: name.to_owned(),
            local_addr: local_addr.to_owned(),
            protocol: mesh_proto::Protocol::Tcp,
            health_check: None,
        }
    }

    #[test]
    fn test_whitelist_persistence_roundtrip_matches_saved_entries() {
        let dir = writable_tempdir("control-whitelist-");
        let mut cn = ControlNode::load_from_disk(dir.path());
        let accepted_at = 1_700_000_000;
        let node = make_node("edge-a", DEFAULT_SERVICE_QUOTA as u16);

        cn.accept_node(node, accepted_at).unwrap();

        let stored: Vec<PersistedWhitelistEntry> =
            crate::persistence::load_state(&dir.path().join("whitelist.json"))
                .unwrap()
                .unwrap();
        assert_eq!(
            stored,
            vec![PersistedWhitelistEntry {
                endpoint_id: "edge-a".to_owned(),
                node_name: "node-edge-a".to_owned(),
                accepted_at,
            }]
        );

        let restored = ControlNode::load_from_disk(dir.path());
        assert_eq!(restored.whitelist.len(), 1);
        assert_eq!(restored.whitelist["edge-a"].node_name, "node-edge-a");
        assert_eq!(restored.accepted_at["edge-a"], accepted_at);
    }

    #[test]
    fn test_route_table_persistence_roundtrip_matches_saved_entries() {
        let dir = writable_tempdir("control-routes-");
        let mut cn = ControlNode::load_from_disk(dir.path());
        cn.accept_node(make_node("edge-a", DEFAULT_SERVICE_QUOTA as u16), 100)
            .unwrap();

        let registrations = vec![
            make_registration("llm-api", "127.0.0.1:3000"),
            make_registration("metrics", "127.0.0.1:3001"),
        ];
        cn.register_services("edge-a", "node-edge-a", &registrations, 200)
            .unwrap();

        let stored: PersistedRoutes =
            crate::persistence::load_state(&dir.path().join("routes.json"))
                .unwrap()
                .unwrap();
        assert_eq!(stored.routes, *cn.routes());
        assert_eq!(stored.version, cn.route_version());

        let restored = ControlNode::load_from_disk(dir.path());
        assert_eq!(restored.routes(), cn.routes());
        assert_eq!(restored.route_version(), cn.route_version());
        assert_eq!(restored.services.len(), 2);
        assert_eq!(restored.whitelist["edge-a"].quota_used, 2);
    }

    #[test]
    fn test_load_from_disk_starts_empty_for_missing_or_empty_files() {
        let missing_dir = writable_tempdir("control-missing-");
        let missing = ControlNode::load_from_disk(missing_dir.path());
        assert!(missing.whitelist.is_empty());
        assert!(missing.routes.is_empty());

        let empty_dir = writable_tempdir("control-empty-");
        std::fs::write(empty_dir.path().join("whitelist.json"), "").unwrap();
        std::fs::write(empty_dir.path().join("routes.json"), "").unwrap();

        let empty = ControlNode::load_from_disk(empty_dir.path());
        assert!(empty.whitelist.is_empty());
        assert!(empty.routes.is_empty());
        assert!(empty.services.is_empty());
    }

    #[test]
    fn test_add_and_remove_node() {
        let mut cn = ControlNode::new();
        let node = make_node("aaa", DEFAULT_SERVICE_QUOTA as u16);
        cn.add_node(node);
        assert!(cn.whitelist.contains_key("aaa"));

        cn.remove_node("aaa");
        assert!(!cn.whitelist.contains_key("aaa"));
    }

    #[test]
    fn test_snapshot_clones_data() {
        let mut cn = ControlNode::new();
        cn.add_node(make_node("bbb", 3));
        let snap = cn.snapshot();
        assert_eq!(snap.whitelist.len(), 1);
        assert_eq!(snap.route_version, 0);
    }

    #[test]
    fn test_check_quota_within_limit() {
        let mut cn = ControlNode::new();
        cn.add_node(make_node("ccc", 2));

        // No services yet — should pass.
        assert!(cn.check_quota("ccc").is_ok());

        // Add one service.
        let (sid, rec) = make_service("ccc", "web");
        cn.services.insert(sid, rec);
        assert!(cn.check_quota("ccc").is_ok());
    }

    #[test]
    fn test_check_quota_exceeded() {
        let mut cn = ControlNode::new();
        cn.add_node(make_node("ddd", 1));

        let (sid, rec) = make_service("ddd", "web");
        cn.services.insert(sid, rec);

        let result = cn.check_quota("ddd");
        assert!(result.is_err());
        match result.unwrap_err() {
            QuotaError::Exceeded { current, limit } => {
                assert_eq!(current, 1);
                assert_eq!(limit, 1);
            }
            other => panic!("expected Exceeded, got {other:?}"),
        }
    }

    #[test]
    fn test_check_quota_unknown_node() {
        let cn = ControlNode::new();
        let result = cn.check_quota("nonexistent");
        assert!(matches!(result, Err(QuotaError::NodeNotFound { .. })));
    }

    #[test]
    fn test_quota_snapshot_reports_used_and_limit_per_node() {
        let mut cn = ControlNode::new();
        cn.add_node(make_node("node-a", 3));
        cn.add_node(make_node("node-b", 5));
        let (service_id, service_record) = make_service("node-a", "web");
        cn.services.insert(service_id, service_record);

        let quotas = cn.quota_snapshot();

        assert_eq!(
            quotas,
            vec![("node-a".to_string(), 1, 3), ("node-b".to_string(), 0, 5),]
        );
    }

    #[test]
    fn test_set_quota_limit_updates_existing_node() {
        let mut cn = ControlNode::new();
        cn.add_node(make_node("node-a", 3));

        cn.set_quota_limit("node-a", 9).unwrap();

        assert_eq!(cn.whitelist["node-a"].quota_limit, 9);
    }

    #[test]
    fn test_register_services_preserves_port_on_reregistration() {
        let mut cn = ControlNode::new();
        cn.add_node(make_node("node-a", 4));
        let registrations = vec![make_registration("llm-api", "127.0.0.1:3000")];

        let first_assignments = cn
            .register_services("node-a", "node-node-a", &registrations, 100)
            .expect("initial registration should succeed");
        let first_port = first_assignments[0].assigned_port;

        let second_assignments = cn
            .register_services("node-a", "node-node-a", &registrations, 200)
            .expect("re-registration should succeed");

        let service_id = ServiceId {
            endpoint_id: "node-a".to_owned(),
            service_name: "llm-api".to_owned(),
        };
        assert_eq!(second_assignments[0].assigned_port, first_port);
        assert_eq!(
            cn.services
                .get(&service_id)
                .and_then(|record| record.published_port),
            Some(first_port)
        );
        assert_eq!(cn.routes.len(), 1, "re-registration should not leak routes");
        assert!(cn.routes.contains_key(&first_port));
    }

    #[test]
    fn test_register_services_releases_removed_service() {
        let mut cn = ControlNode::new();
        cn.add_node(make_node("node-a", 5));
        let initial_registrations = vec![
            make_registration("llm-api", "127.0.0.1:3000"),
            make_registration("metrics", "127.0.0.1:3001"),
        ];

        let initial_assignments = cn
            .register_services("node-a", "node-node-a", &initial_registrations, 100)
            .expect("initial registration should succeed");
        let llm_port = initial_assignments[0].assigned_port;
        let metrics_port = initial_assignments[1].assigned_port;

        let llm_only = vec![make_registration("llm-api", "127.0.0.1:3000")];
        let retained_assignments = cn
            .register_services("node-a", "node-node-a", &llm_only, 200)
            .expect("reconcile should succeed");

        let removed_service_id = ServiceId {
            endpoint_id: "node-a".to_owned(),
            service_name: "metrics".to_owned(),
        };
        assert_eq!(retained_assignments[0].assigned_port, llm_port);
        assert!(!cn.services.contains_key(&removed_service_id));
        assert!(!cn.routes.contains_key(&metrics_port));
        assert_eq!(cn.allocator().allocated_count(), 1);

        let expanded_registrations = vec![
            make_registration("llm-api", "127.0.0.1:3000"),
            make_registration("admin", "127.0.0.1:3002"),
        ];
        let expanded_assignments = cn
            .register_services("node-a", "node-node-a", &expanded_registrations, 300)
            .expect("adding a replacement service should succeed");

        assert_eq!(expanded_assignments[1].assigned_port, metrics_port);
    }

    #[test]
    fn test_register_services_quota_reflects_final_count() {
        let mut cn = ControlNode::new();
        cn.add_node(make_node("node-a", 5));
        let initial_registrations = vec![
            make_registration("llm-api", "127.0.0.1:3000"),
            make_registration("metrics", "127.0.0.1:3001"),
        ];

        cn.register_services("node-a", "node-node-a", &initial_registrations, 100)
            .expect("initial registration should succeed");
        assert_eq!(cn.whitelist["node-a"].quota_used, 2);

        let reconciled_registrations = vec![make_registration("llm-api", "127.0.0.1:3000")];
        cn.register_services("node-a", "node-node-a", &reconciled_registrations, 200)
            .expect("re-registration should succeed");

        assert_eq!(cn.whitelist["node-a"].quota_used, 1);
    }

    #[test]
    fn test_remove_node_cleans_services_and_routes() {
        let mut cn = ControlNode::new();
        cn.add_node(make_node("eee", 5));

        let (sid, rec) = make_service("eee", "api");
        cn.services.insert(sid, rec);

        // Simulate a route entry for this node.
        cn.routes.insert(
            40000,
            RouteEntry {
                service_name: "api".to_owned(),
                node_name: "node-eee".to_owned(),
                endpoint_id: "eee".to_owned(),
                target_local_addr: "127.0.0.1:8080".to_owned(),
                protocol: mesh_proto::Protocol::Tcp,
            },
        );

        cn.remove_node("eee");
        assert!(cn.services.is_empty());
        assert!(cn.routes.is_empty());
    }

    #[test]
    fn test_consume_ticket_prevents_replay() {
        let mut cn = ControlNode::new();
        let nonce = [1u8; 16];
        assert!(cn.consume_ticket(nonce, 5000), "first use should succeed");
        assert!(!cn.consume_ticket(nonce, 5000), "replay should fail");
    }

    #[test]
    fn test_prune_expired_tickets() {
        let mut cn = ControlNode::new();
        let ticket_a = make_ticket("node-a", 1000, 100, [20u8; 16]); // expires at 1100
        let ticket_b = make_ticket("node-b", 1000, 3600, [21u8; 16]); // expires at 4600

        assert!(cn.validate_ticket(&ticket_a, "node-a", 1050).is_ok());
        assert!(cn.validate_ticket(&ticket_b, "node-b", 1050).is_ok());

        // Prune at epoch 1100 — ticket_a expiry (1100) is not > 1100, so pruned.
        cn.prune_expired_tickets(1100);

        // ticket_a's nonce should be pruned (can be re-inserted, but the ticket
        // itself is expired so validate_ticket would reject it anyway).
        assert!(!cn.used_tickets.contains_key(&[20u8; 16]));
        // ticket_b should still be tracked.
        assert!(cn.used_tickets.contains_key(&[21u8; 16]));
    }

    // -- Ticket validation tests (2B-06) --

    fn make_ticket(endpoint_id: &str, created_at: u64, ttl: u64, nonce: [u8; 16]) -> JoinTicket {
        JoinTicket {
            endpoint_id: endpoint_id.to_owned(),
            created_at,
            ttl_seconds: ttl,
            nonce,
        }
    }

    #[test]
    fn test_validate_ticket_valid() {
        let mut cn = ControlNode::new();
        let ticket = make_ticket("node-a", 1000, 3600, [10u8; 16]);
        assert!(cn.validate_ticket(&ticket, "node-a", 2000).is_ok());
    }

    #[test]
    fn test_validate_ticket_expired() {
        let mut cn = ControlNode::new();
        let ticket = make_ticket("node-a", 1000, 100, [11u8; 16]);
        // now = 1101 → expired (1101 - 1000 = 101 > 100)
        let result = cn.validate_ticket(&ticket, "node-a", 1101);
        assert!(matches!(result, Err(TicketError::Expired)));
    }

    #[test]
    fn test_validate_ticket_replay() {
        let mut cn = ControlNode::new();
        let ticket = make_ticket("node-a", 1000, 3600, [12u8; 16]);
        assert!(cn.validate_ticket(&ticket, "node-a", 2000).is_ok());
        // Same ticket again → replay
        let result = cn.validate_ticket(&ticket, "node-a", 2001);
        assert!(matches!(result, Err(TicketError::AlreadyUsed)));
    }

    #[test]
    fn test_validate_ticket_endpoint_mismatch() {
        let mut cn = ControlNode::new();
        let ticket = make_ticket("node-a", 1000, 3600, [13u8; 16]);
        let result = cn.validate_ticket(&ticket, "node-b", 2000);
        assert!(matches!(result, Err(TicketError::EndpointMismatch)));
    }

    // -- Heartbeat tests (2B-07) --

    #[test]
    fn test_heartbeat_timeout_detection() {
        let mut cn = ControlNode::new();
        cn.record_pong("node-x", 1000);
        cn.record_pong("node-y", 1050);

        // At now = 1091 → node-x is 91s behind (> 90), node-y is 41s (ok)
        let timed_out = cn.check_heartbeats(1091);
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0], "node-x");
    }

    #[test]
    fn test_heartbeat_no_timeout() {
        let mut cn = ControlNode::new();
        cn.record_pong("node-x", 1000);

        // At now = 1090 → exactly 90s (not > 90)
        let timed_out = cn.check_heartbeats(1090);
        assert!(timed_out.is_empty());
    }

    #[test]
    fn test_remove_node_cleans_last_pong() {
        let mut cn = ControlNode::new();
        cn.add_node(make_node("pong-node", 5));
        cn.record_pong("pong-node", 1000);

        // Before removal, stale pong should be reported.
        assert_eq!(cn.check_heartbeats(1200).len(), 1);

        cn.remove_node("pong-node");

        // After removal, check_heartbeats must not report the removed node.
        assert!(cn.check_heartbeats(1200).is_empty());
    }

    // -- Health aggregation tests (2B-08) --

    #[test]
    fn test_aggregate_health_updates_known_service() {
        let mut cn = ControlNode::new();
        cn.add_node(make_node("ep1", 5));
        let (sid, rec) = make_service("ep1", "web");
        cn.services.insert(sid, rec);

        cn.aggregate_health(
            "ep1",
            vec![mesh_proto::ServiceHealthEntry {
                service_name: "web".to_owned(),
                health_state: mesh_proto::HealthState::Healthy,
                last_error: None,
            }],
            1234,
        );

        let sid = ServiceId {
            endpoint_id: "ep1".to_owned(),
            service_name: "web".to_owned(),
        };
        assert_eq!(
            cn.services.get(&sid).unwrap().health_state,
            mesh_proto::HealthState::Healthy
        );
        assert_eq!(cn.services.get(&sid).unwrap().last_seen, Some(1234));
        assert_eq!(cn.whitelist["ep1"].last_heartbeat, Some(1234));
    }

    #[test]
    fn test_aggregate_health_skips_unknown_service() {
        let mut cn = ControlNode::new();

        // No services registered — should not panic, just skip.
        cn.aggregate_health(
            "ep-unknown",
            vec![mesh_proto::ServiceHealthEntry {
                service_name: "ghost".to_owned(),
                health_state: mesh_proto::HealthState::Unhealthy,
                last_error: Some("timeout".to_owned()),
            }],
            4321,
        );

        assert!(cn.services.is_empty());
        assert_eq!(cn.whitelist.get("ep-unknown"), None);
    }
}
