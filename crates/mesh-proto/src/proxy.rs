use serde::{Deserialize, Serialize};

use crate::config::Protocol;

/// Initial metadata exchanged before opening a proxied service stream.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ProxyHandshake {
    pub service_name: String,
    pub port: u16,
    pub protocol: Protocol,
}
