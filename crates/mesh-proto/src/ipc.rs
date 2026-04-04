use serde::{Deserialize, Serialize};

use crate::config::Protocol;
use crate::health::HealthCheckConfig;
use crate::listener::ListenerState;

/// Request from CLI client to the local daemon via Unix socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcRequest {
    /// Get daemon status (role, connected nodes, services, routes).
    Status,
    /// Show per-node service quota usage (control node only).
    QuotaShow,
    /// Update the service quota for a specific node (control node only).
    QuotaSet { endpoint_id: String, limit: usize },
    /// Gracefully stop the daemon.
    Stop,
    /// Restart the daemon (stop + start with current config).
    Restart,
    /// Reload configuration from disk.
    Reload,
    /// Expose a local service through the mesh (edge node only).
    ExposeService {
        name: String,
        local_addr: String,
        protocol: Protocol,
        health_check: Option<HealthCheckConfig>,
    },
    /// Remove a previously exposed local service (edge node only).
    UnexposeService { name: String },
    /// Accept a new edge node (control node only).
    AcceptNode {
        ticket: String,
        /// Optional friendly name assigned by the admin.
        node_name: Option<String>,
    },
}

/// Response from daemon to CLI client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcResponse {
    /// Status information.
    Status(StatusInfo),
    /// Service quota usage for all known nodes.
    QuotaInfo {
        /// Tuples of (endpoint_id, used, limit).
        quotas: Vec<(String, usize, usize)>,
    },
    /// Operation succeeded.
    Ok { message: String },
    /// A quota update was applied.
    QuotaUpdated,
    /// Configuration was reloaded from disk.
    Reloaded,
    /// Operation failed.
    Error { message: String },
    /// A service was successfully exposed and assigned a port by the control node.
    ServiceExposed { name: String, assigned_port: u16 },
    /// A service was saved locally, but the control node did not confirm a port in time.
    ServiceExposeTimedOut { name: String, timeout_seconds: u64 },
    /// A service was removed from the local config.
    ServiceUnexposed { name: String },
    /// Runtime status for a specific managed listener.
    ListenerStatus { port: u16, state: ListenerState },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusInfo {
    pub role: String,
    pub node_name: String,
    pub endpoint_id: String,
    /// Serialized `EndpointAddr` used to connect back to this node.
    pub endpoint_addr: Option<String>,
    pub online: bool,
    pub peer_count: usize,
    pub connected_nodes: Vec<ConnectedNode>,
    pub services: Vec<ServiceStatus>,
    /// Bind address for the local health query endpoint, if configured.
    pub health_bind: Option<String>,
    /// Current version of the cached route table.
    pub route_table_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectedNode {
    pub name: String,
    pub endpoint_id: String,
    pub online: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceDisplayStatus {
    Healthy,
    Degraded,
    Unhealthy,
    #[default]
    Unknown,
    Ok,
    Unreachable,
    Routed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceStatus {
    pub name: String,
    pub node_name: String,
    pub assigned_port: Option<u16>,
    pub status: ServiceDisplayStatus,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ipc_request_unexpose_service_roundtrip() {
        let request = IpcRequest::UnexposeService {
            name: "web".to_string(),
        };

        let encoded = serde_json::to_string(&request).unwrap();
        let decoded: IpcRequest = serde_json::from_str(&encoded).unwrap();

        let IpcRequest::UnexposeService { name } = decoded else {
            panic!("expected unexpose service request");
        };
        assert_eq!(name, "web");
    }

    #[test]
    fn test_ipc_response_service_unexposed_roundtrip() {
        let response = IpcResponse::ServiceUnexposed {
            name: "web".to_string(),
        };

        let encoded = serde_json::to_string(&response).unwrap();
        let decoded: IpcResponse = serde_json::from_str(&encoded).unwrap();

        let IpcResponse::ServiceUnexposed { name } = decoded else {
            panic!("expected service unexposed response");
        };
        assert_eq!(name, "web");
    }

    #[test]
    fn test_ipc_response_service_expose_timed_out_roundtrip() {
        let response = IpcResponse::ServiceExposeTimedOut {
            name: "web".to_string(),
            timeout_seconds: 15,
        };

        let encoded = serde_json::to_string(&response).unwrap();
        let decoded: IpcResponse = serde_json::from_str(&encoded).unwrap();

        let IpcResponse::ServiceExposeTimedOut {
            name,
            timeout_seconds,
        } = decoded
        else {
            panic!("expected service expose timed out response");
        };
        assert_eq!(name, "web");
        assert_eq!(timeout_seconds, 15);
    }
}
