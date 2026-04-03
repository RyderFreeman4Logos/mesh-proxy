use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use mesh_proto::{
    ALPN_CONTROL, ControlMessage, JoinTicket, NodeInfo, PortAssignment, RouteEntry, ServiceId,
    ServiceRecord,
};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::port_allocator::PortAllocator;

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
    /// Maps ticket nonce → expiry epoch for replay prevention with bounded growth.
    used_tickets: HashMap<[u8; 16], u64>,
    /// Epoch timestamp of the most recent pong received from each endpoint.
    last_pong: HashMap<String, u64>,
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
            used_tickets: HashMap::new(),
            last_pong: HashMap::new(),
        }
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

    /// Add a node to the whitelist. Overwrites any existing entry with the
    /// same endpoint ID.
    pub fn add_node(&mut self, info: NodeInfo) {
        self.whitelist.insert(info.endpoint_id.clone(), info);
    }

    /// Remove a node from the whitelist, release all its allocated ports,
    /// and clean up heartbeat tracking.
    pub fn remove_node(&mut self, endpoint_id: &str) {
        self.whitelist.remove(endpoint_id);
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
        // Quota check: ensure enough capacity for all requested services.
        let current_count = self
            .services
            .keys()
            .filter(|sid| sid.endpoint_id == endpoint_id)
            .count();
        let node = self
            .whitelist
            .get(endpoint_id)
            .ok_or_else(|| format!("node not found in whitelist: {endpoint_id}"))?;
        if current_count + registrations.len() > node.quota_limit as usize {
            return Err(format!(
                "quota exceeded: have {current_count}, want {} more, limit {}",
                registrations.len(),
                node.quota_limit
            ));
        }

        // Allocate ports for each service (tentative).
        let mut assignments = Vec::with_capacity(registrations.len());
        let mut allocated_ports = Vec::with_capacity(registrations.len());

        for reg in registrations {
            let sid = ServiceId {
                endpoint_id: endpoint_id.to_owned(),
                service_name: reg.name.clone(),
            };
            match self.allocator.allocate(&sid, now_epoch) {
                Some(port) => {
                    allocated_ports.push(port);
                    assignments.push(PortAssignment {
                        service_name: reg.name.clone(),
                        assigned_port: port,
                    });
                }
                None => {
                    // Rollback all tentative allocations.
                    for port in &allocated_ports {
                        self.allocator.release(*port);
                    }
                    return Err("port pool exhausted".to_owned());
                }
            }
        }

        // Commit: update whitelist online status, services, and routes.
        if let Some(info) = self.whitelist.get_mut(endpoint_id) {
            info.is_online = true;
            info.last_heartbeat = Some(now_epoch);
            info.quota_used = current_count
                .saturating_add(registrations.len())
                .min(u16::MAX as usize) as u16;
        }

        for (reg, assignment) in registrations.iter().zip(assignments.iter()) {
            let sid = ServiceId {
                endpoint_id: endpoint_id.to_owned(),
                service_name: reg.name.clone(),
            };
            let record = ServiceRecord {
                service_id: sid.clone(),
                node_name: node_name.to_owned(),
                local_addr: reg.local_addr.clone(),
                protocol: reg.protocol,
                published_port: Some(assignment.assigned_port),
                health_state: mesh_proto::HealthState::Unknown,
                last_seen: Some(now_epoch),
            };
            self.services.insert(sid, record);

            // Confirm the pending port allocation.
            self.allocator.confirm(assignment.assigned_port);

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
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(node, connection).await {
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
    connection: iroh::endpoint::Connection,
) -> anyhow::Result<()> {
    let remote_id = connection.remote_id();
    info!(%remote_id, "control connection established");

    // Store the endpoint ID string for route broadcasting.
    {
        let mut n = node.write().await;
        n.set_node_addr(&remote_id.to_string(), remote_id.to_string());
    }

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
                    &remote_id.to_string(),
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
                n.record_pong(&remote_id.to_string(), now_epoch());
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

/// Process a Register message: validate ticket, allocate ports, respond with Ack/Nack.
async fn handle_register(
    node: &Arc<RwLock<ControlNode>>,
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

/// Broadcast the current route table to all online edge nodes.
///
/// Connects to each node individually, sends the update, and continues
/// on per-node failure. Uses a fresh stream per node (no connection pooling).
pub async fn broadcast_routes(node: &Arc<RwLock<ControlNode>>, endpoint: &iroh::Endpoint) {
    let (routes, version, targets) = {
        let n = node.read().await;
        (n.routes().clone(), n.route_version(), n.online_node_addrs())
    };

    let update = ControlMessage::RouteTableUpdate { routes, version };

    for (eid, addr_str) in &targets {
        // Parse the stored EndpointId string back, then connect via Into<EndpointAddr>.
        let endpoint_id: iroh::EndpointId = match addr_str.parse() {
            Ok(a) => a,
            Err(e) => {
                warn!(endpoint_id = %eid, error = %e, "cannot parse stored endpoint id, skipping broadcast");
                continue;
            }
        };

        let conn = match endpoint.connect(endpoint_id, ALPN_CONTROL).await {
            Ok(c) => c,
            Err(e) => {
                warn!(endpoint_id = %eid, error = %e, "failed to connect for route broadcast");
                continue;
            }
        };

        let result: anyhow::Result<()> = async {
            let (mut send, _recv) = conn.open_bi().await.context("open_bi failed")?;
            mesh_proto::frame::write_json(&mut send, &update)
                .await
                .context("write route update failed")?;
            send.finish()
                .map_err(|e| anyhow::anyhow!("finish failed: {e}"))?;
            Ok(())
        }
        .await;

        if let Err(e) = result {
            warn!(endpoint_id = %eid, error = %e, "route broadcast to node failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use mesh_proto::DEFAULT_SERVICE_QUOTA;

    use super::*;

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
