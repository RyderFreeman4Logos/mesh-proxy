use std::collections::HashMap;
use std::fmt;
use std::path::Path;

use mesh_proto::{MAX_LISTENERS, RouteEntry};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::persistence::{self, PersistenceError};

/// Error returned when a state transition is not allowed.
#[derive(Debug, Clone, thiserror::Error)]
#[error("illegal transition from {from} to {to}")]
pub struct TransitionError {
    pub from: ConnectionState,
    pub to: ConnectionState,
}

/// Connection lifecycle states for an edge node communicating with the control plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Unauthenticated,
    Registering,
    Authenticated,
}

/// Compute the listener delta between the current and incoming route tables.
///
/// `to_spawn` contains new ports and routes whose entry changed in-place.
/// `to_remove` contains ports that no longer exist in the incoming table.
pub fn route_diff(
    old: &HashMap<u16, RouteEntry>,
    new: &HashMap<u16, RouteEntry>,
) -> (Vec<(u16, RouteEntry)>, Vec<u16>) {
    let mut to_spawn = new
        .iter()
        .filter_map(|(&port, route)| match old.get(&port) {
            Some(existing) if existing == route => None,
            _ => Some((port, route.clone())),
        })
        .collect::<Vec<_>>();
    to_spawn.sort_by_key(|(port, _)| *port);

    let mut to_remove = old
        .keys()
        .filter(|port| !new.contains_key(*port))
        .copied()
        .collect::<Vec<_>>();
    to_remove.sort_unstable();

    (to_spawn, to_remove)
}

/// Spawn a placeholder TCP listener for a dynamically assigned route port.
///
/// The listener accepts inbound connections, logs the peer address, and closes
/// the connection immediately until Phase 4 forwarding is implemented.
pub async fn spawn_tcp_listener(
    port: u16,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> std::io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;

    Ok(tokio::spawn(async move {
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            tracing::info!(port, %peer_addr, "accepted dynamic TCP listener connection");
                            drop(stream);
                        }
                        Err(error) => {
                            tracing::warn!(port, error = %error, "dynamic TCP listener accept failed");
                        }
                    }
                }
                recv_result = shutdown_rx.recv() => {
                    if let Err(error) = recv_result {
                        tracing::debug!(port, error = %error, "dynamic TCP listener shutdown channel closed");
                    }
                    break;
                }
            }
        }
    }))
}

async fn abort_listener(handle: JoinHandle<()>) {
    handle.abort();
    let _ = handle.await;
}

/// Spawn a placeholder Unix socket listener for local Phase 3 testing.
#[cfg(unix)]
pub async fn spawn_unix_listener(
    path: impl AsRef<Path>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> std::io::Result<JoinHandle<()>> {
    let path = path.as_ref().to_path_buf();

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if path.exists() {
        std::fs::remove_file(&path)?;
    }

    let listener = UnixListener::bind(&path)?;

    Ok(tokio::spawn(async move {
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, _addr)) => {
                            tracing::info!(path = %path.display(), "accepted dynamic Unix listener connection");
                            drop(stream);
                        }
                        Err(error) => {
                            tracing::warn!(path = %path.display(), error = %error, "dynamic Unix listener accept failed");
                        }
                    }
                }
                recv_result = shutdown_rx.recv() => {
                    if let Err(error) = recv_result {
                        tracing::debug!(path = %path.display(), error = %error, "dynamic Unix listener shutdown channel closed");
                    }
                    break;
                }
            }
        }

        if let Err(error) = std::fs::remove_file(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %path.display(), error = %error, "failed to remove dynamic Unix listener socket");
        }
    }))
}

impl fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// Local state container for an edge node.
///
/// Tracks the control-plane connection lifecycle, cached route table, and
/// heartbeat timing. The state machine enforces a linear progression
/// (Disconnected → Connecting → Unauthenticated → Registering → Authenticated)
/// with any-state → Disconnected allowed as an error fallback.
#[derive(Debug)]
pub struct EdgeNode {
    state: ConnectionState,
    cached_routes: HashMap<u16, RouteEntry>,
    route_version: u64,
    listener_pool: HashMap<u16, JoinHandle<()>>,
    shutdown_tx: broadcast::Sender<()>,
    /// Epoch timestamp of the last ping received from the control node.
    last_ping_from_control: Option<u64>,
}

impl Default for EdgeNode {
    fn default() -> Self {
        Self::new()
    }
}

impl EdgeNode {
    /// Create a new edge node in the Disconnected state.
    pub fn new() -> Self {
        let (shutdown_tx, _) = broadcast::channel(16);
        Self {
            state: ConnectionState::Disconnected,
            cached_routes: HashMap::new(),
            route_version: 0,
            listener_pool: HashMap::new(),
            shutdown_tx,
            last_ping_from_control: None,
        }
    }

    /// Attempt a state transition. Returns `Err` if the transition is not allowed.
    ///
    /// Legal transitions:
    /// - Disconnected → Connecting
    /// - Connecting → Unauthenticated
    /// - Unauthenticated → Registering
    /// - Registering → Authenticated
    /// - Any state → Disconnected (error fallback)
    pub fn transition_to(&mut self, new_state: ConnectionState) -> Result<(), TransitionError> {
        let allowed = matches!(
            (&self.state, &new_state),
            (_, ConnectionState::Disconnected)
                | (ConnectionState::Disconnected, ConnectionState::Connecting)
                | (
                    ConnectionState::Connecting,
                    ConnectionState::Unauthenticated
                )
                | (
                    ConnectionState::Unauthenticated,
                    ConnectionState::Registering
                )
                | (ConnectionState::Registering, ConnectionState::Authenticated)
        );

        if allowed {
            self.state = new_state;
            Ok(())
        } else {
            Err(TransitionError {
                from: self.state.clone(),
                to: new_state,
            })
        }
    }

    /// Returns the current connection state.
    pub fn current_state(&self) -> &ConnectionState {
        &self.state
    }

    /// Record that a ping was received from the control node.
    pub fn record_ping(&mut self, now_epoch: u64) {
        self.last_ping_from_control = Some(now_epoch);
    }

    /// Returns the epoch of the last ping from the control node.
    pub fn last_ping_from_control(&self) -> Option<u64> {
        self.last_ping_from_control
    }

    /// Access the cached route table.
    pub fn cached_routes(&self) -> &HashMap<u16, RouteEntry> {
        &self.cached_routes
    }

    /// Replace the cached route table with a new snapshot.
    pub fn update_routes(&mut self, routes: HashMap<u16, RouteEntry>, version: u64) {
        self.cached_routes = routes;
        self.route_version = version;
    }

    /// Returns the route table version.
    pub fn route_version(&self) -> u64 {
        self.route_version
    }

    /// Returns the number of active dynamic listeners currently tracked.
    pub fn listener_count(&self) -> usize {
        self.listener_pool.len()
    }

    /// Apply a route table update if the version is newer than the cached one.
    ///
    /// Returns `true` if the update was applied, `false` if stale.
    pub async fn apply_route_update(
        &mut self,
        routes: HashMap<u16, RouteEntry>,
        version: u64,
    ) -> bool {
        if version <= self.route_version {
            return false;
        }

        let (to_spawn, to_remove) = route_diff(&self.cached_routes, &routes);
        self.cached_routes = routes;
        self.route_version = version;

        for port in to_remove {
            if let Some(handle) = self.listener_pool.remove(&port) {
                abort_listener(handle).await;
            }
        }

        for (port, route) in to_spawn {
            if let Some(handle) = self.listener_pool.remove(&port) {
                abort_listener(handle).await;
            }

            if self.listener_pool.len() >= MAX_LISTENERS {
                tracing::warn!(
                    port,
                    service_name = %route.service_name,
                    limit = MAX_LISTENERS,
                    "skipping dynamic listener because the pool is full"
                );
                continue;
            }

            match spawn_tcp_listener(port, self.shutdown_tx.subscribe()).await {
                Ok(handle) => {
                    self.listener_pool.insert(port, handle);
                }
                Err(error) => {
                    tracing::warn!(
                        port,
                        service_name = %route.service_name,
                        error = %error,
                        "failed to spawn dynamic TCP listener"
                    );
                }
            }
        }

        true
    }

    /// Persist the cached route table and version to disk atomically.
    pub async fn save_route_cache(&self, data_dir: &Path) -> Result<(), PersistenceError> {
        let cache = RouteCache {
            routes: self.cached_routes.clone(),
            version: self.route_version,
        };
        let path = data_dir.join("route_cache.json");
        persistence::save_atomic(&path, &cache).await
    }

    /// Load a previously persisted route cache from disk.
    ///
    /// Returns `None` if the file does not exist.
    pub fn load_route_cache(data_dir: &Path) -> Option<(HashMap<u16, RouteEntry>, u64)> {
        let path = data_dir.join("route_cache.json");
        match persistence::load_state::<RouteCache>(&path) {
            Ok(Some(cache)) => Some((cache.routes, cache.version)),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(error = %e, "failed to load route cache, starting fresh");
                None
            }
        }
    }
}

/// Serializable wrapper for the route cache on disk.
#[derive(serde::Serialize, serde::Deserialize)]
struct RouteCache {
    routes: HashMap<u16, RouteEntry>,
    version: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use mesh_proto::{MAX_LISTENERS, Protocol};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpStream;
    use tokio::sync::broadcast;
    use tokio::time::{Duration, timeout};

    #[cfg(unix)]
    use tokio::net::UnixStream;

    fn test_route(service_name: &str, endpoint_id: &str) -> RouteEntry {
        RouteEntry {
            service_name: service_name.to_string(),
            node_name: "edge-a".to_string(),
            endpoint_id: endpoint_id.to_string(),
            target_local_addr: "127.0.0.1:8080".to_string(),
            protocol: Protocol::Tcp,
        }
    }

    fn available_tcp_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    async fn assert_tcp_connection_closed(port: u16) {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut buf = [0_u8; 1];
        let bytes_read = timeout(Duration::from_secs(1), stream.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bytes_read, 0);
    }

    async fn shutdown_listeners(edge: &mut EdgeNode) {
        let handles = edge
            .listener_pool
            .drain()
            .map(|(_, handle)| handle)
            .collect::<Vec<_>>();
        let _ = edge.shutdown_tx.send(());
        for handle in handles {
            let _ = handle.await;
        }
    }

    #[test]
    fn test_happy_path_through_all_states() {
        let mut edge = EdgeNode::new();
        assert_eq!(*edge.current_state(), ConnectionState::Disconnected);

        edge.transition_to(ConnectionState::Connecting).unwrap();
        assert_eq!(*edge.current_state(), ConnectionState::Connecting);

        edge.transition_to(ConnectionState::Unauthenticated)
            .unwrap();
        assert_eq!(*edge.current_state(), ConnectionState::Unauthenticated);

        edge.transition_to(ConnectionState::Registering).unwrap();
        assert_eq!(*edge.current_state(), ConnectionState::Registering);

        edge.transition_to(ConnectionState::Authenticated).unwrap();
        assert_eq!(*edge.current_state(), ConnectionState::Authenticated);
    }

    #[test]
    fn test_illegal_transition_rejected() {
        let mut edge = EdgeNode::new();

        // Disconnected → Authenticated is not allowed.
        assert!(edge.transition_to(ConnectionState::Authenticated).is_err());
        assert_eq!(*edge.current_state(), ConnectionState::Disconnected);

        // Disconnected → Registering is not allowed.
        assert!(edge.transition_to(ConnectionState::Registering).is_err());

        // Advance to Connecting, then try skipping to Authenticated.
        edge.transition_to(ConnectionState::Connecting).unwrap();
        assert!(edge.transition_to(ConnectionState::Authenticated).is_err());
        assert_eq!(*edge.current_state(), ConnectionState::Connecting);
    }

    #[test]
    fn test_fallback_to_disconnected_from_any_state() {
        let mut edge = EdgeNode::new();

        // From Disconnected
        edge.transition_to(ConnectionState::Disconnected).unwrap();
        assert_eq!(*edge.current_state(), ConnectionState::Disconnected);

        // From Connecting
        edge.transition_to(ConnectionState::Connecting).unwrap();
        edge.transition_to(ConnectionState::Disconnected).unwrap();
        assert_eq!(*edge.current_state(), ConnectionState::Disconnected);

        // From Authenticated
        edge.transition_to(ConnectionState::Connecting).unwrap();
        edge.transition_to(ConnectionState::Unauthenticated)
            .unwrap();
        edge.transition_to(ConnectionState::Registering).unwrap();
        edge.transition_to(ConnectionState::Authenticated).unwrap();
        edge.transition_to(ConnectionState::Disconnected).unwrap();
        assert_eq!(*edge.current_state(), ConnectionState::Disconnected);
    }

    #[test]
    fn test_record_ping() {
        let mut edge = EdgeNode::new();
        assert_eq!(edge.last_ping_from_control(), None);

        edge.record_ping(1000);
        assert_eq!(edge.last_ping_from_control(), Some(1000));

        edge.record_ping(2000);
        assert_eq!(edge.last_ping_from_control(), Some(2000));
    }

    #[test]
    fn test_route_diff_empty_to_some() {
        let old = HashMap::new();
        let new = HashMap::from([
            (41_000, test_route("alpha", "endpoint-a")),
            (41_001, test_route("beta", "endpoint-b")),
        ]);

        let (to_spawn, to_remove) = route_diff(&old, &new);

        assert_eq!(
            to_spawn,
            vec![
                (41_000, test_route("alpha", "endpoint-a")),
                (41_001, test_route("beta", "endpoint-b")),
            ]
        );
        assert!(to_remove.is_empty());
    }

    #[test]
    fn test_route_diff_some_to_empty() {
        let old = HashMap::from([
            (41_000, test_route("alpha", "endpoint-a")),
            (41_001, test_route("beta", "endpoint-b")),
        ]);
        let new = HashMap::new();

        let (to_spawn, to_remove) = route_diff(&old, &new);

        assert!(to_spawn.is_empty());
        assert_eq!(to_remove, vec![41_000, 41_001]);
    }

    #[test]
    fn test_route_diff_partial_overlap() {
        let old = HashMap::from([
            (41_000, test_route("alpha", "endpoint-a")),
            (41_001, test_route("beta", "endpoint-b")),
        ]);
        let new = HashMap::from([
            (41_001, test_route("beta", "endpoint-c")),
            (41_002, test_route("gamma", "endpoint-d")),
        ]);

        let (to_spawn, to_remove) = route_diff(&old, &new);

        assert_eq!(
            to_spawn,
            vec![
                (41_001, test_route("beta", "endpoint-c")),
                (41_002, test_route("gamma", "endpoint-d")),
            ]
        );
        assert_eq!(to_remove, vec![41_000]);
    }

    #[test]
    fn test_route_diff_no_change() {
        let old = HashMap::from([
            (41_000, test_route("alpha", "endpoint-a")),
            (41_001, test_route("beta", "endpoint-b")),
        ]);
        let new = old.clone();

        let (to_spawn, to_remove) = route_diff(&old, &new);

        assert!(to_spawn.is_empty());
        assert!(to_remove.is_empty());
    }

    #[test]
    fn test_route_diff_full_replace() {
        let old = HashMap::from([
            (41_000, test_route("alpha", "endpoint-a")),
            (41_001, test_route("beta", "endpoint-b")),
        ]);
        let new = HashMap::from([
            (41_010, test_route("gamma", "endpoint-c")),
            (41_011, test_route("delta", "endpoint-d")),
        ]);

        let (to_spawn, to_remove) = route_diff(&old, &new);

        assert_eq!(
            to_spawn,
            vec![
                (41_010, test_route("gamma", "endpoint-c")),
                (41_011, test_route("delta", "endpoint-d")),
            ]
        );
        assert_eq!(to_remove, vec![41_000, 41_001]);
    }

    #[tokio::test]
    async fn test_spawn_tcp_listener_accepts_and_closes_connection() {
        let port = available_tcp_port();
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let handle = spawn_tcp_listener(port, shutdown_rx).await.unwrap();

        assert_tcp_connection_closed(port).await;

        let _ = shutdown_tx.send(());
        handle.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_spawn_unix_listener_accepts_and_closes_connection() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("dynamic-listener.sock");
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let handle = spawn_unix_listener(&socket_path, shutdown_rx)
            .await
            .unwrap();

        let mut stream = UnixStream::connect(&socket_path).await.unwrap();
        let mut buf = [0_u8; 1];
        let bytes_read = timeout(Duration::from_secs(1), stream.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bytes_read, 0);

        let _ = shutdown_tx.send(());
        handle.await.unwrap();
        assert!(!socket_path.exists());
    }

    #[tokio::test]
    async fn test_apply_route_update_replaces_changed_listener() {
        let port = available_tcp_port();
        let mut edge = EdgeNode::new();

        let initial_routes = HashMap::from([(port, test_route("alpha", "endpoint-a"))]);
        assert!(edge.apply_route_update(initial_routes, 1).await);
        assert_eq!(edge.listener_pool.len(), 1);
        assert_tcp_connection_closed(port).await;

        let updated_routes = HashMap::from([(port, test_route("alpha", "endpoint-b"))]);
        assert!(edge.apply_route_update(updated_routes, 2).await);
        assert_eq!(edge.listener_pool.len(), 1);
        assert_tcp_connection_closed(port).await;

        shutdown_listeners(&mut edge).await;
    }

    #[tokio::test]
    async fn test_apply_route_update_enforces_max_listeners() {
        let mut edge = EdgeNode::new();
        for port in 0..MAX_LISTENERS {
            edge.listener_pool
                .insert(port as u16, tokio::spawn(async {}));
        }

        let new_port = available_tcp_port();
        let routes = HashMap::from([(new_port, test_route("overflow", "endpoint-overflow"))]);

        assert!(edge.apply_route_update(routes, 1).await);
        assert_eq!(edge.route_version(), 1);
        assert!(edge.cached_routes().contains_key(&new_port));
        assert_eq!(edge.listener_pool.len(), MAX_LISTENERS);
        assert!(!edge.listener_pool.contains_key(&new_port));

        for (_, handle) in edge.listener_pool.drain() {
            handle.await.unwrap();
        }
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Map an index 0..=3 to the legal next state from the corresponding
    /// position in the happy path.
    fn legal_sequence() -> Vec<ConnectionState> {
        vec![
            ConnectionState::Connecting,
            ConnectionState::Unauthenticated,
            ConnectionState::Registering,
            ConnectionState::Authenticated,
        ]
    }

    proptest! {
        /// Any prefix of the legal transition sequence succeeds, and extending
        /// to the full sequence always reaches Authenticated.
        #[test]
        fn prop_legal_sequence_reaches_authenticated(len in 1usize..=4) {
            let seq = legal_sequence();
            let mut edge = EdgeNode::new();
            for state in seq.into_iter().take(len) {
                edge.transition_to(state).expect("should be legal");
            }
            if len == 4 {
                prop_assert_eq!(edge.current_state(), &ConnectionState::Authenticated);
            }
        }

        /// Fallback to Disconnected is always possible, and after it the
        /// full happy path can be re-traversed.
        #[test]
        fn prop_reset_then_full_path(reset_at in 0usize..4) {
            let seq = legal_sequence();
            let mut edge = EdgeNode::new();
            // Advance partway.
            for state in seq.iter().take(reset_at) {
                edge.transition_to(state.clone()).unwrap();
            }
            // Reset.
            edge.transition_to(ConnectionState::Disconnected).unwrap();
            prop_assert_eq!(edge.current_state(), &ConnectionState::Disconnected);

            // Full path again.
            for state in &legal_sequence() {
                edge.transition_to(state.clone()).unwrap();
            }
            prop_assert_eq!(edge.current_state(), &ConnectionState::Authenticated);
        }
    }
}
