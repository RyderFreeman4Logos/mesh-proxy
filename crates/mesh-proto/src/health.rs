use serde::{Deserialize, Serialize};

/// How the daemon probes a local service for health.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthCheckMode {
    /// Open a TCP connection to the service address, close immediately.
    TcpConnect,
    /// Open a Unix socket connection, close immediately.
    UnixConnect,
    /// Send an HTTP GET to `target` URL and expect 2xx.
    HttpGet,
}

/// Per-service health check configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    pub mode: HealthCheckMode,
    /// Override target for the probe (e.g. an HTTP URL for HttpGet mode).
    /// When `None`, the service's `local_addr` is used.
    #[serde(default)]
    pub target: Option<String>,
    /// Probe interval in seconds.
    #[serde(default = "default_interval")]
    pub interval_seconds: u64,
}

fn default_interval() -> u64 {
    10
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            mode: HealthCheckMode::TcpConnect,
            target: None,
            interval_seconds: default_interval(),
        }
    }
}

/// Aggregated health state of a service.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthState {
    Healthy,
    Degraded,
    Unhealthy,
    #[default]
    Unknown,
}
