mod config;
pub mod frame;
pub mod health;
mod ipc;
mod message;
mod service;
mod ticket;
mod validate;

pub use config::{MeshConfig, NodeRole, Protocol, ServiceEntry};
pub use health::{HealthCheckConfig, HealthCheckMode, HealthState};
pub use ipc::{IpcRequest, IpcResponse, StatusInfo};
pub use message::{ControlMessage, PortAssignment, RouteEntry, ServiceRegistration};
pub use service::{NodeInfo, ServiceHealthEntry, ServiceId, ServiceRecord};
pub use ticket::{JoinTicket, TicketError, TicketStatus};
pub use validate::{
    MAX_LOCAL_ADDR_LEN, MAX_SERVICE_NAME_LEN, MAX_SERVICES_PER_REGISTRATION, MAX_TICKET_LEN,
    validate_local_addr, validate_service_name,
};

/// ALPN identifier for control plane communication.
pub const ALPN_CONTROL: &[u8] = b"/mesh/ctrl/1";

/// ALPN identifier for data plane proxy traffic.
pub const ALPN_PROXY: &[u8] = b"/mesh/proxy/1";

/// Service port pool range start (inclusive).
pub const SERVICE_PORT_START: u16 = 40000;

/// Service port pool range end (inclusive).
pub const SERVICE_PORT_END: u16 = 48999;

/// Reserved port range start (inclusive), for system services (health endpoint, etc.).
pub const RESERVED_PORT_START: u16 = 49000;

/// Reserved port range end (inclusive).
pub const RESERVED_PORT_END: u16 = 49999;

/// Default port for the local health query HTTP endpoint.
pub const DEFAULT_HEALTH_PORT: u16 = 49000;

/// Default maximum services per edge node.
pub const DEFAULT_SERVICE_QUOTA: usize = 5;

/// Heartbeat ping interval in seconds.
pub const HEARTBEAT_INTERVAL_SECS: u64 = 30;

/// Duration in seconds after which a node is considered timed-out if no pong
/// has been received.
pub const HEARTBEAT_TIMEOUT_SECS: u64 = 90;

/// Number of consecutive missed heartbeats before marking a node offline.
pub const MAX_MISSED_HEARTBEATS: u32 = 3;
