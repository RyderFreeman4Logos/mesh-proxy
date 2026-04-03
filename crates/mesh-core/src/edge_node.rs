use std::collections::HashMap;
use std::fmt;

use mesh_proto::RouteEntry;

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
        Self {
            state: ConnectionState::Disconnected,
            cached_routes: HashMap::new(),
            route_version: 0,
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
