use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::config::Protocol;
use crate::health::HealthCheckConfig;
use crate::service::ServiceHealthEntry;

/// Messages exchanged over the control plane (ALPN: /mesh/ctrl/1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlMessage {
    /// Edge -> Control: register this node and its services.
    Register {
        node_name: String,
        auth_ticket: String,
        services: Vec<ServiceRegistration>,
    },
    /// Control -> Edge: registration accepted, here are your port assignments.
    RegisterAck {
        assignments: Vec<PortAssignment>,
    },
    /// Control -> Edge: registration rejected.
    RegisterNack {
        reason: String,
    },
    /// Control -> All edges: updated global route table.
    RouteTableUpdate {
        routes: HashMap<u16, RouteEntry>,
        version: u64,
    },
    /// Edge -> Control: periodic health status report.
    HealthReport {
        endpoint_id: String,
        services: Vec<ServiceHealthEntry>,
    },
    /// Bidirectional heartbeat.
    Ping,
    Pong,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceRegistration {
    pub name: String,
    pub local_addr: String,
    pub protocol: Protocol,
    pub health_check: Option<HealthCheckConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortAssignment {
    pub service_name: String,
    /// The globally unique port assigned by the control node.
    pub assigned_port: u16,
}

/// An entry in the global route table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteEntry {
    pub service_name: String,
    pub node_name: String,
    /// The EndpointId (public key) of the node hosting this service, hex-encoded.
    pub endpoint_id: String,
    /// The local address on the target node.
    pub target_local_addr: String,
    pub protocol: Protocol,
}
