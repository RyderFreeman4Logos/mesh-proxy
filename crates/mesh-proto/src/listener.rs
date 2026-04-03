use serde::{Deserialize, Serialize};

/// Runtime status of a listener managed by the daemon.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ListenerState {
    Running,
    Stopping,
    Failed { reason: String },
}
