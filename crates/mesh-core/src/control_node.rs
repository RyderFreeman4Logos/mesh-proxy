use std::collections::HashMap;

use mesh_proto::{JoinTicket, NodeInfo, RouteEntry, ServiceId, ServiceRecord};

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

    // -- Health aggregation (2B-08) --

    /// Update health state for services reported by an edge node.
    ///
    /// Unknown services (not in the registry) are logged and skipped
    /// rather than causing a panic.
    pub fn aggregate_health(
        &mut self,
        from_endpoint_id: &str,
        services: Vec<mesh_proto::ServiceHealthEntry>,
    ) {
        for entry in services {
            let sid = ServiceId {
                endpoint_id: from_endpoint_id.to_owned(),
                service_name: entry.service_name.clone(),
            };
            if let Some(record) = self.services.get_mut(&sid) {
                record.health_state = entry.health_state;
                // We don't have a clock here — caller should set last_seen
                // via a separate method if needed, but we update what we can.
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
        );

        let sid = ServiceId {
            endpoint_id: "ep1".to_owned(),
            service_name: "web".to_owned(),
        };
        assert_eq!(
            cn.services.get(&sid).unwrap().health_state,
            mesh_proto::HealthState::Healthy
        );
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
        );

        assert!(cn.services.is_empty());
    }
}
