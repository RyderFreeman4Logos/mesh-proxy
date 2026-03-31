use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Top-level configuration file structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshConfig {
    pub node_name: String,
    pub role: NodeRole,
    /// Secret key bytes (base58-encoded Ed25519 seed), persisted across restarts.
    pub secret_key: Option<String>,
    /// For edge nodes: the control node's endpoint address (serialized).
    pub control_addr: Option<String>,
    /// Services this node exposes (edge nodes only).
    #[serde(default)]
    pub services: Vec<ServiceEntry>,
    /// Data directory for state persistence.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    Control,
    Edge,
}

/// A service exposed by an edge node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceEntry {
    /// Human-readable service name (e.g. "llama3_api").
    pub name: String,
    /// Local address the service listens on.
    /// Can be a TCP address ("127.0.0.1:8080") or Unix socket path ("/tmp/foo.sock").
    pub local_addr: String,
    /// Protocol: "tcp" or "unix".
    #[serde(default = "default_protocol")]
    pub protocol: String,
}

fn default_data_dir() -> PathBuf {
    dirs_next::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("mesh-proxy")
}

fn default_protocol() -> String {
    "tcp".to_string()
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            node_name: hostname::get()
                .map(|h| h.to_string_lossy().into_owned())
                .unwrap_or_else(|_| "unnamed".to_string()),
            role: NodeRole::Edge,
            secret_key: None,
            control_addr: None,
            services: Vec::new(),
            data_dir: default_data_dir(),
        }
    }
}
