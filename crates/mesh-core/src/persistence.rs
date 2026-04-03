use std::path::Path;

use serde::Serialize;
use serde::de::DeserializeOwned;

/// Errors arising from persistence operations.
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Atomically persist `data` as JSON to `path`.
///
/// Creates → writes → fsyncs → renames on the same file handle, so a crash
/// mid-write never leaves a partially-written file.
pub async fn save_atomic<T: Serialize>(path: &Path, data: &T) -> Result<(), PersistenceError> {
    use tokio::io::AsyncWriteExt;

    let tmp_path = path.with_extension("json.tmp");
    let json = serde_json::to_string_pretty(data)?;

    let mut file = tokio::fs::File::create(&tmp_path).await?;
    file.write_all(json.as_bytes()).await?;
    file.sync_all().await?;

    tokio::fs::rename(&tmp_path, path).await?;

    Ok(())
}

/// Load a JSON-serialized state file from `path`.
///
/// Returns `Ok(None)` if the file does not exist, `Ok(Some(T))` if it does,
/// and `Err` on parse or IO errors (other than not-found).
pub fn load_state<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, PersistenceError> {
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let value = serde_json::from_str(&contents)?;
            Ok(Some(value))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestData {
        name: String,
        value: u64,
    }

    #[tokio::test]
    async fn test_save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        let data = TestData {
            name: "hello".to_owned(),
            value: 42,
        };

        save_atomic(&path, &data).await.unwrap();

        let loaded: Option<TestData> = load_state(&path).unwrap();
        assert_eq!(loaded, Some(data));
    }

    #[test]
    fn test_load_missing_file_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");
        let result: Result<Option<TestData>, PersistenceError> = load_state(&path);
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_save_atomic_no_partial_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("atomic.json");
        let tmp_path = path.with_extension("json.tmp");

        let data = TestData {
            name: "atomicity".to_owned(),
            value: 99,
        };

        save_atomic(&path, &data).await.unwrap();

        // The temp file should have been renamed away.
        assert!(!tmp_path.exists(), "temp file should not remain");
        assert!(path.exists(), "target file should exist");
    }
}
