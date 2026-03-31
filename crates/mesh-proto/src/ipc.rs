use serde::{Deserialize, Serialize};

/// Request from CLI client to the local daemon via Unix socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcRequest {
    /// Get daemon status (role, connected nodes, services, routes).
    Status,
    /// Gracefully stop the daemon.
    Stop,
    /// Reload configuration from disk.
    Reload,
    /// Accept a new edge node (control node only).
    AcceptNode {
        ticket: String,
        node_name: String,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusInfo {
    pub role: String,
    pub node_name: String,
    pub endpoint_id: String,
    pub online: bool,
    pub connected_nodes: Vec<ConnectedNode>,
    pub services: Vec<ServiceStatus>,
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
