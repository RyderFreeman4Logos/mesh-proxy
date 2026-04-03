use serde::{Deserialize, Serialize};

use crate::config::Protocol;
use crate::health::HealthState;

/// Unique identifier for a service across the mesh, composed of the hosting
/// node's endpoint and the service name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceId {
    pub endpoint_id: String,
    pub service_name: String,
}

/// Full record describing a registered service in the global service directory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceRecord {
    pub service_id: ServiceId,
    pub node_name: String,
    pub local_addr: String,
    pub protocol: Protocol,
    pub published_port: Option<u16>,
    pub health_state: HealthState,
    /// Unix timestamp (seconds) of the last heartbeat or status update.
    pub last_seen: Option<u64>,
}

/// Summary information about a node in the mesh.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub endpoint_id: String,
    pub node_name: String,
    pub quota_limit: u16,
    pub quota_used: u16,
    pub is_online: bool,
    /// Unix timestamp (seconds) of the last heartbeat.
    pub last_heartbeat: Option<u64>,
}

/// Per-service health entry reported by an edge node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceHealthEntry {
    pub service_name: String,
    pub health_state: HealthState,
    pub last_error: Option<String>,
}
