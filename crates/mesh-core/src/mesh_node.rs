use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use iroh::{Endpoint, EndpointId, SecretKey, endpoint::presets};
use mesh_proto::{ALPN_CONTROL, ALPN_PROXY};
use tracing::info;

const SECRET_KEY_FILE_NAME: &str = "secret_key";

fn secret_key_path(data_dir: &Path) -> PathBuf {
    data_dir.join(SECRET_KEY_FILE_NAME)
}

fn decode_secret_key_file(path: &Path, bytes: &[u8]) -> Result<SecretKey> {
    if let Ok(raw) = <[u8; 32]>::try_from(bytes) {
        return Ok(SecretKey::from_bytes(&raw));
    }

    let encoded = std::str::from_utf8(bytes)
        .with_context(|| format!("secret key file {} is not valid UTF-8", path.display()))?
        .trim();
    if encoded.is_empty() {
        bail!("secret key file {} is empty", path.display());
    }

    let raw = bs58::decode(encoded)
        .into_vec()
        .with_context(|| format!("invalid bs58 in secret key file {}", path.display()))?;
    let bytes: [u8; 32] = raw.try_into().map_err(|raw: Vec<u8>| {
        anyhow::anyhow!(
            "secret key file {} must decode to 32 bytes, got {}",
            path.display(),
            raw.len()
        )
    })?;

    Ok(SecretKey::from_bytes(&bytes))
}

fn load_secret_key(data_dir: &Path) -> Result<Option<SecretKey>> {
    let path = secret_key_path(data_dir);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read secret key file {}", path.display()));
        }
    };

    decode_secret_key_file(&path, &bytes).map(Some)
}

fn save_secret_key(data_dir: &Path, secret_key: &SecretKey) -> Result<()> {
    fs::create_dir_all(data_dir)
        .with_context(|| format!("failed to create data dir {}", data_dir.display()))?;

    let path = secret_key_path(data_dir);
    let tmp_path = path.with_extension("tmp");
    let encoded = bs58::encode(secret_key.to_bytes()).into_string();

    let mut open_options = fs::OpenOptions::new();
    open_options.create(true).write(true).truncate(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        open_options.mode(0o600);
    }

    let mut file = open_options
        .open(&tmp_path)
        .with_context(|| format!("failed to open temp secret key file {}", tmp_path.display()))?;
    file.write_all(encoded.as_bytes()).with_context(|| {
        format!(
            "failed to write temp secret key file {}",
            tmp_path.display()
        )
    })?;
    file.sync_all()
        .with_context(|| format!("failed to sync temp secret key file {}", tmp_path.display()))?;
    drop(file);

    fs::rename(&tmp_path, &path).with_context(|| {
        format!(
            "failed to rename secret key file {} -> {}",
            tmp_path.display(),
            path.display()
        )
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to set secret key permissions {}", path.display()))?;
    }

    Ok(())
}

/// Wraps an iroh [`Endpoint`] with mesh-proxy identity management.
///
/// On first run the builder auto-generates a random secret key, which is then
/// persisted to the node data directory so subsequent restarts reuse the same
/// identity.
pub struct MeshNode {
    endpoint: Endpoint,
}

impl MeshNode {
    /// Initialise the iroh endpoint.
    ///
    /// * Loads an existing secret key from `data_dir/secret_key` when present.
    /// * If no key exists (first run), lets the builder generate one and saves
    ///   it to disk with owner-only permissions.
    pub async fn new(data_dir: &Path) -> Result<Self> {
        let alpns = vec![ALPN_CONTROL.to_vec(), ALPN_PROXY.to_vec()];
        let mut builder = Endpoint::builder(presets::N0).alpns(alpns);
        let persisted_secret_key = load_secret_key(data_dir)?;
        let had_persisted_key = persisted_secret_key.is_some();

        if let Some(secret_key) = persisted_secret_key {
            builder = builder.secret_key(secret_key);
        }

        let endpoint = builder
            .bind()
            .await
            .context("failed to bind iroh endpoint")?;

        if had_persisted_key {
            info!("loaded persisted node identity");
        } else {
            save_secret_key(data_dir, endpoint.secret_key())
                .context("failed to persist generated secret key")?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::Builder;

    fn writable_tempdir() -> tempfile::TempDir {
        Builder::new()
            .prefix("mesh-node-")
            .tempdir_in("/tmp")
            .unwrap()
    }

    #[test]
    fn test_load_secret_key_returns_none_when_missing() {
        let dir = writable_tempdir();

        let loaded = load_secret_key(dir.path()).unwrap();

        assert!(loaded.is_none());
    }

    #[test]
    fn test_save_secret_key_roundtrip_matches_original_key() {
        let dir = writable_tempdir();
        let expected_bytes = [7u8; 32];
        let expected_key = SecretKey::from_bytes(&expected_bytes);

        save_secret_key(dir.path(), &expected_key).unwrap();

        let loaded = load_secret_key(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.to_bytes(), expected_key.to_bytes());
    }

    #[test]
    fn test_load_secret_key_accepts_raw_key_bytes() {
        let dir = writable_tempdir();
        let raw_bytes = [5u8; 32];
        let path = secret_key_path(dir.path());

        fs::create_dir_all(dir.path()).unwrap();
        fs::write(&path, raw_bytes).unwrap();

        let loaded = load_secret_key(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.to_bytes(), raw_bytes);
    }

    #[cfg(unix)]
    #[test]
    fn test_save_secret_key_sets_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = writable_tempdir();
        let secret_key = SecretKey::from_bytes(&[9u8; 32]);

        save_secret_key(dir.path(), &secret_key).unwrap();

        let path = secret_key_path(dir.path());
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
