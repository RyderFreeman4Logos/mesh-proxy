mod config_watcher;
pub mod connection_pool;
mod control_node;
mod daemon;
mod edge_node;
pub mod edge_registration;
pub mod health_prober;
pub mod health_server;
mod ipc_server;
mod mesh_node;
mod persistence;
mod port_allocator;
pub mod process;

pub use config_watcher::ConfigWatcher;
pub use connection_pool::{ConnectionPool, bridge_bidirectional, bridge_streams};
pub use control_node::{
    ControlNode, ControlNodeSnapshot, QuotaError, TicketError, broadcast_routes, run_accept_loop,
};
pub use daemon::{Daemon, ShutdownRx, ShutdownTx};
#[cfg(unix)]
pub use edge_node::spawn_unix_listener;
pub use edge_node::{
    ConnectionState, EdgeNode, ProxyConnectionPermit, ProxyHandshakeValidationError,
    TransitionError, route_diff, spawn_tcp_listener, try_reserve_proxy_connection_slot,
    validate_inbound_proxy_handshake,
};
pub use ipc_server::IpcServer;
pub use mesh_node::MeshNode;
pub use persistence::{PersistenceError, load_state, save_atomic};
pub use port_allocator::{PortAllocator, PortState};
