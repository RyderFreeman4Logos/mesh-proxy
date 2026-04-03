use std::collections::HashMap;

use mesh_proto::{SERVICE_PORT_END, SERVICE_PORT_START, ServiceId};

/// Lifecycle state of a port in the allocator pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortState {
    Free,
    Pending {
        service_id: ServiceId,
        assigned_at_epoch: u64,
    },
    Active {
        service_id: ServiceId,
    },
    Dirty {
        since_epoch: u64,
    },
}

/// Manages the global port pool for service-to-port assignments.
///
/// Ports transition through: Free -> Pending -> Active -> Dirty -> Free.
/// The allocator prefers reusing previously-assigned ports for the same
/// service to maintain stable external addresses across restarts.
#[derive(Debug, Clone)]
pub struct PortAllocator {
    ports: HashMap<u16, PortState>,
    history: HashMap<ServiceId, u16>,
}

impl Default for PortAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl PortAllocator {
    /// Create a new allocator with all ports in the configured range set to [`PortState::Free`].
    pub fn new() -> Self {
        let mut ports =
            HashMap::with_capacity((SERVICE_PORT_END - SERVICE_PORT_START + 1) as usize);
        for port in SERVICE_PORT_START..=SERVICE_PORT_END {
            ports.insert(port, PortState::Free);
        }
        Self {
            ports,
            history: HashMap::new(),
        }
    }

    /// Allocate a port for the given service.
    ///
    /// Prefers reusing the port previously assigned to this service (if it is
    /// currently Free or Dirty). Falls back to the first available Free port.
    /// Returns `None` when the pool is exhausted.
    pub fn allocate(&mut self, service_id: &ServiceId, epoch: u64) -> Option<u16> {
        // Try history reuse first.
        if let Some(&prev_port) = self.history.get(service_id)
            && let Some(state) = self.ports.get(&prev_port)
            && matches!(state, PortState::Free | PortState::Dirty { .. })
        {
            self.ports.insert(
                prev_port,
                PortState::Pending {
                    service_id: service_id.clone(),
                    assigned_at_epoch: epoch,
                },
            );
            return Some(prev_port);
        }

        // Find the lowest-numbered free port.
        let free_port = (SERVICE_PORT_START..=SERVICE_PORT_END)
            .find(|p| matches!(self.ports.get(p), Some(PortState::Free)));

        if let Some(port) = free_port {
            self.ports.insert(
                port,
                PortState::Pending {
                    service_id: service_id.clone(),
                    assigned_at_epoch: epoch,
                },
            );
            self.history.insert(service_id.clone(), port);
            Some(port)
        } else {
            None
        }
    }

    /// Confirm a pending port allocation, transitioning it to Active.
    ///
    /// No-op if the port is not in Pending state.
    pub fn confirm(&mut self, port: u16) {
        if let Some(PortState::Pending { service_id, .. }) = self.ports.get(&port) {
            let sid = service_id.clone();
            self.ports
                .insert(port, PortState::Active { service_id: sid });
        }
    }

    /// Mark an Active or Pending port as Dirty (awaiting reclamation).
    ///
    /// No-op if the port is Free or already Dirty.
    pub fn mark_dirty(&mut self, port: u16, epoch: u64) {
        if let Some(state) = self.ports.get(&port)
            && matches!(state, PortState::Active { .. } | PortState::Pending { .. })
        {
            self.ports
                .insert(port, PortState::Dirty { since_epoch: epoch });
        }
    }

    /// Reclaim all Dirty ports whose `since_epoch` is older than `threshold_epoch`,
    /// returning them to the Free pool.
    pub fn reclaim_dirty(&mut self, threshold_epoch: u64) {
        for state in self.ports.values_mut() {
            if let PortState::Dirty { since_epoch } = *state
                && since_epoch < threshold_epoch
            {
                *state = PortState::Free;
            }
        }
    }

    /// Release a port unconditionally, returning it to Free and clearing any
    /// history association.
    pub fn release(&mut self, port: u16) {
        if let Some(state) = self.ports.get(&port) {
            // Clean up history for the service that held this port.
            let service_id = match state {
                PortState::Pending { service_id, .. } | PortState::Active { service_id } => {
                    Some(service_id.clone())
                }
                _ => None,
            };
            if let Some(sid) = service_id {
                self.history.remove(&sid);
            }
            self.ports.insert(port, PortState::Free);
        }
    }

    /// Count the number of ports that are not Free.
    pub fn allocated_count(&self) -> usize {
        self.ports
            .values()
            .filter(|s| !matches!(s, PortState::Free))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_service_id(name: &str) -> ServiceId {
        ServiceId {
            endpoint_id: "abc123".to_owned(),
            service_name: name.to_owned(),
        }
    }

    #[test]
    fn test_allocate_returns_port_in_range() {
        let mut alloc = PortAllocator::new();
        let sid = make_service_id("web");
        let port = alloc.allocate(&sid, 1).expect("should allocate");
        assert!(
            (SERVICE_PORT_START..=SERVICE_PORT_END).contains(&port),
            "port {port} out of range"
        );
        assert_eq!(alloc.allocated_count(), 1);
    }

    #[test]
    fn test_allocate_prefers_history_reuse() {
        let mut alloc = PortAllocator::new();
        let sid = make_service_id("web");

        // Allocate, confirm, then mark dirty (history stays, port reusable).
        let first_port = alloc.allocate(&sid, 1).unwrap();
        alloc.confirm(first_port);
        alloc.mark_dirty(first_port, 10);

        let reused_port = alloc.allocate(&sid, 20).unwrap();
        assert_eq!(reused_port, first_port, "should reuse the same port");
    }

    #[test]
    fn test_confirm_transitions_pending_to_active() {
        let mut alloc = PortAllocator::new();
        let sid = make_service_id("api");
        let port = alloc.allocate(&sid, 1).unwrap();

        assert!(matches!(
            alloc.ports.get(&port),
            Some(PortState::Pending { .. })
        ));

        alloc.confirm(port);

        assert!(matches!(
            alloc.ports.get(&port),
            Some(PortState::Active { .. })
        ));
    }

    #[test]
    fn test_mark_dirty_and_reclaim() {
        let mut alloc = PortAllocator::new();
        let sid = make_service_id("db");
        let port = alloc.allocate(&sid, 1).unwrap();
        alloc.confirm(port);

        alloc.mark_dirty(port, 100);
        assert!(matches!(
            alloc.ports.get(&port),
            Some(PortState::Dirty { since_epoch: 100 })
        ));

        // Reclaim with a threshold that does NOT include this port.
        alloc.reclaim_dirty(100);
        assert!(
            matches!(alloc.ports.get(&port), Some(PortState::Dirty { .. })),
            "should not reclaim when since_epoch == threshold"
        );

        // Reclaim with a threshold that DOES include this port.
        alloc.reclaim_dirty(101);
        assert!(matches!(alloc.ports.get(&port), Some(PortState::Free)));
    }

    #[test]
    fn test_pool_exhaustion_returns_none() {
        let mut alloc = PortAllocator::new();
        let total_ports = (SERVICE_PORT_END - SERVICE_PORT_START + 1) as usize;

        // Allocate all ports.
        for i in 0..total_ports {
            let sid = make_service_id(&format!("svc-{i}"));
            assert!(
                alloc.allocate(&sid, 1).is_some(),
                "should allocate port {i}"
            );
        }

        // Next allocation should fail.
        let extra = make_service_id("overflow");
        assert!(
            alloc.allocate(&extra, 1).is_none(),
            "pool should be exhausted"
        );
        assert_eq!(alloc.allocated_count(), total_ports);
    }
}
