use std::path::Path;

use anyhow::{Context, Result};
use iroh::{Endpoint, EndpointId, SecretKey, endpoint::presets};
use mesh_proto::{ALPN_CONTROL, ALPN_PROXY, MeshConfig};
use tracing::info;

/// Wraps an iroh [`Endpoint`] with mesh-proxy identity management.
///
/// On first run the builder auto-generates a random secret key, which is then
/// bs58-encoded and persisted into the config file so subsequent restarts reuse
/// the same identity.
pub struct MeshNode {
    endpoint: Endpoint,
}

impl MeshNode {
    /// Initialise the iroh endpoint.
    ///
    /// * Loads an existing secret key from `config.secret_key` (bs58-encoded
    ///   32-byte Ed25519 seed) when present.
    /// * If no key exists (first run), lets the builder generate one and saves
    ///   the result back to `config` + disk via [`MeshConfig::save`].
    pub async fn new(config: &mut MeshConfig, config_path: &Path) -> Result<Self> {
        let alpns = vec![ALPN_CONTROL.to_vec(), ALPN_PROXY.to_vec()];

        let had_key = config.secret_key.is_some();

        let mut builder = Endpoint::builder(presets::N0).alpns(alpns);

        // Decode persisted key when available.
        if let Some(ref encoded) = config.secret_key {
            let raw = bs58::decode(encoded)
                .into_vec()
                .context("invalid bs58 in secret_key")?;
            let bytes: [u8; 32] = raw.try_into().map_err(|v: Vec<u8>| {
                anyhow::anyhow!("secret_key must be 32 bytes, got {}", v.len())
            })?;
            let sk = SecretKey::from_bytes(&bytes);
            builder = builder.secret_key(sk);
        }

        let endpoint = builder
            .bind()
            .await
            .context("failed to bind iroh endpoint")?;

        // Persist the auto-generated key on first run.
        if !had_key {
            let key_bytes = endpoint.secret_key().to_bytes();
            config.secret_key = Some(bs58::encode(key_bytes).into_string());
            config
                .save(config_path)
                .context("failed to save generated secret_key to config")?;
            info!("generated and persisted new node identity");
        }

        endpoint.online().await;
        info!(endpoint_id = %endpoint.id(), "iroh endpoint is online");

        Ok(Self { endpoint })
    }

    /// The public identity of this node.
    pub fn id(&self) -> EndpointId {
        self.endpoint.id()
    }

    /// Borrow the underlying iroh endpoint (for accepting / connecting).
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Gracefully shut down the iroh endpoint.
    pub async fn close(self) {
        self.endpoint.close().await;
        info!("iroh endpoint closed");
    }
}
