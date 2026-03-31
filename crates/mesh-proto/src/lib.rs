mod config;
mod ipc;
mod message;

pub use config::{MeshConfig, NodeRole, ServiceEntry};
pub use ipc::{IpcRequest, IpcResponse};
pub use message::ControlMessage;

/// ALPN identifier for control plane communication.
pub const ALPN_CONTROL: &[u8] = b"/mesh/ctrl/1";

/// ALPN identifier for data plane proxy traffic.
pub const ALPN_PROXY: &[u8] = b"/mesh/proxy/1";

/// Default port pool range start (inclusive).
pub const PORT_RANGE_START: u16 = 40000;

/// Default port pool range end (inclusive).
pub const PORT_RANGE_END: u16 = 49999;

/// Default maximum services per edge node.
pub const DEFAULT_SERVICE_QUOTA: usize = 5;
