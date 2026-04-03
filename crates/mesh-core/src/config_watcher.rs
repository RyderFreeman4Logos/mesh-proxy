use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{debug, warn};

const CONFIG_DEBOUNCE: Duration = Duration::from_millis(500);

/// Watches the config file and emits debounced reload signals.
pub struct ConfigWatcher {
    _watcher: RecommendedWatcher,
    _debounce_task: tokio::task::JoinHandle<()>,
}

impl ConfigWatcher {
    /// Create a new watcher that notifies `tx` after config changes settle.
    pub fn new(config_path: PathBuf, tx: mpsc::Sender<()>) -> Result<Self> {
        let watch_path = config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let tmp_path = config_path.with_extension("toml.tmp");
        let callback_config_path = config_path.clone();
        let callback_tmp_path = tmp_path.clone();
        let (raw_tx, mut raw_rx) = mpsc::unbounded_channel();

        let mut watcher = RecommendedWatcher::new(
            move |result: notify::Result<Event>| match result {
                Ok(event)
                    if is_relevant_event(
                        &event,
                        callback_config_path.as_path(),
                        callback_tmp_path.as_path(),
                    ) =>
                {
                    if raw_tx.send(()).is_err() {
                        debug!("config watcher debounce task already stopped");
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(error = %error, "config watcher event error");
                }
            },
            Config::default().with_poll_interval(CONFIG_DEBOUNCE),
        )
        .context("failed to create config watcher")?;

        watcher
            .watch(&watch_path, RecursiveMode::NonRecursive)
            .with_context(|| format!("failed to watch config path {}", watch_path.display()))?;

        let debounce_task = tokio::spawn(async move {
            while raw_rx.recv().await.is_some() {
                let debounce_deadline = tokio::time::Instant::now() + CONFIG_DEBOUNCE;
                let debounce = tokio::time::sleep_until(debounce_deadline);
                tokio::pin!(debounce);

                loop {
                    tokio::select! {
                        _ = &mut debounce => {
                            if tx.send(()).await.is_err() {
                                debug!(path = %config_path.display(), "config reload receiver dropped");
                                return;
                            }
                            break;
                        }
                        maybe_event = raw_rx.recv() => {
                            if maybe_event.is_none() {
                                return;
                            }
                            debounce.as_mut().reset(tokio::time::Instant::now() + CONFIG_DEBOUNCE);
                        }
                    }
                }
            }
        });

        Ok(Self {
            _watcher: watcher,
            _debounce_task: debounce_task,
        })
    }
}

fn is_relevant_event(event: &Event, config_path: &Path, tmp_path: &Path) -> bool {
    event
        .paths
        .iter()
        .any(|path| path == config_path || path == tmp_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mesh_proto::MeshConfig;
    use tempfile::Builder;

    fn writable_tempdir() -> tempfile::TempDir {
        Builder::new()
            .prefix("mesh-core-")
            .tempdir_in("/tmp")
            .unwrap()
    }

    #[tokio::test]
    async fn test_config_watcher_notifies_after_debounce() {
        let dir = writable_tempdir();
        let config_path = dir.path().join("config.toml");
        MeshConfig::default().save(&config_path).unwrap();

        let (tx, mut rx) = mpsc::channel(4);
        let _watcher = ConfigWatcher::new(config_path.clone(), tx).unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut updated = MeshConfig::default();
        updated.node_name = "updated-node".to_string();
        let started_at = tokio::time::Instant::now();
        updated.save(&config_path).unwrap();

        let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for config notification");
        assert_eq!(received, Some(()));
        assert!(
            started_at.elapsed() >= CONFIG_DEBOUNCE,
            "notification should be debounced"
        );
    }
}
