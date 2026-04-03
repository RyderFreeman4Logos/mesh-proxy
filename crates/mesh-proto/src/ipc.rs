use serde::{Deserialize, Serialize};

use crate::config::Protocol;
use crate::health::HealthCheckConfig;

/// Request from CLI client to the local daemon via Unix socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcRequest {
    /// Get daemon status (role, connected nodes, services, routes).
    Status,
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
    /// Operation succeeded.
    Ok { message: String },
    /// Operation failed.
    Error { message: String },
    /// A service was successfully exposed and (optionally) assigned a port.
    ServiceExposed {
        name: String,
        assigned_port: Option<u16>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusInfo {
    pub role: String,
    pub node_name: String,
    pub endpoint_id: String,
    pub online: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceStatus {
    pub name: String,
    pub assigned_port: Option<u16>,
    pub status: String,
}
