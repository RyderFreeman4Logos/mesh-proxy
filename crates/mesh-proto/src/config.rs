use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};

/// Top-level configuration file structure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

impl MeshConfig {
    /// Returns the default config file path: `~/.config/mesh-proxy/config.toml`.
    pub fn default_config_path() -> PathBuf {
        dirs_next::config_dir()
            .unwrap_or_else(|| PathBuf::from(".config"))
            .join("mesh-proxy")
            .join("config.toml")
    }

    /// Load config from `path`. If the file does not exist, creates a default
    /// config, persists it, and returns it.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        if path.exists() {
            let content = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read config at {}", path.display()))?;
            let config: Self = toml::from_str(&content)
                .with_context(|| format!("failed to parse config at {}", path.display()))?;
            Ok(config)
        } else {
            let config = Self::default();
            config.save(path)?;
            Ok(config)
        }
    }

    /// Atomic write: serializes to a temp file, sets permissions to 0600,
    /// then renames into place.
    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create config dir {}", parent.display()))?;
        }

        let content = toml::to_string_pretty(self).context("failed to serialize config")?;

        // Write to a sibling temp file, then atomically rename.
        let tmp_path = path.with_extension("toml.tmp");
        std::fs::write(&tmp_path, content.as_bytes())
            .with_context(|| format!("failed to write temp config at {}", tmp_path.display()))?;

        // Set restrictive permissions (config may contain secret_key).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(&tmp_path, perms)
                .with_context(|| format!("failed to set permissions on {}", tmp_path.display()))?;
        }

        std::fs::rename(&tmp_path, path).with_context(|| {
            format!(
                "failed to rename {} → {}",
                tmp_path.display(),
                path.display()
            )
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> MeshConfig {
        MeshConfig {
            node_name: "test-node".to_string(),
            role: NodeRole::Control,
            secret_key: Some("5HueCGU8rMjxEXxiPuD5BDku4MkFqeZyd4dZ1jvhTVqvbTLvyTJ".to_string()),
            control_addr: None,
            services: vec![ServiceEntry {
                name: "llama3_api".to_string(),
                local_addr: "127.0.0.1:8080".to_string(),
                protocol: "tcp".to_string(),
            }],
            data_dir: PathBuf::from("/tmp/mesh-test"),
        }
    }

    #[test]
    fn test_config_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");

        let original = sample_config();
        original.save(&path).unwrap();

        let loaded = MeshConfig::load(&path).unwrap();
        assert_eq!(original, loaded);
    }

    #[test]
    fn test_config_default_creation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub").join("config.toml");

        assert!(!path.exists());
        let config = MeshConfig::load(&path).unwrap();
        assert!(path.exists());
        assert_eq!(config.role, NodeRole::Edge);
        assert!(config.secret_key.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn test_config_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");

        let config = sample_config();
        config.save(&path).unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "config file should be owner-only rw");
    }
}
