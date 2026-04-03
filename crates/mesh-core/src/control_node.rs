use std::collections::{HashMap, HashSet};

use mesh_proto::{NodeInfo, RouteEntry, ServiceId, ServiceRecord};

use crate::port_allocator::PortAllocator;

/// Error returned when a node exceeds its service quota.
#[derive(Debug, thiserror::Error)]
pub enum QuotaError {
    #[error("quota exceeded: node has {current} services, limit is {limit}")]
    Exceeded { current: usize, limit: u16 },
    #[error("node not found in whitelist: {endpoint_id}")]
    NodeNotFound { endpoint_id: String },
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
    used_tickets: HashSet<[u8; 16]>,
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
            used_tickets: HashSet::new(),
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

    /// Remove a node from the whitelist and release all its allocated ports.
    pub fn remove_node(&mut self, endpoint_id: &str) {
        self.whitelist.remove(endpoint_id);

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

    /// Record a ticket nonce as used. Returns `false` if already consumed.
    pub fn consume_ticket(&mut self, nonce: [u8; 16]) -> bool {
        self.used_tickets.insert(nonce)
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
        assert!(cn.consume_ticket(nonce), "first use should succeed");
        assert!(!cn.consume_ticket(nonce), "replay should fail");
    }
}
