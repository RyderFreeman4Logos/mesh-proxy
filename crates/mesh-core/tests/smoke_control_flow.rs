//! Smoke test: ControlNode complete registration and lifecycle flow.
//!
//! Validates the full control-plane logic path without any networking:
//! whitelist → ticket → register → ports → routes → quota → health → broadcast state.

use mesh_core::ControlNode;
use mesh_proto::{
    DEFAULT_SERVICE_QUOTA, HealthState, JoinTicket, NodeInfo, Protocol, SERVICE_PORT_END,
    SERVICE_PORT_START, ServiceHealthEntry, ServiceId, ServiceRegistration,
};

/// Helper to create a `NodeInfo` entry for tests.
fn make_edge_node(endpoint_id: &str, name: &str, quota: u16) -> NodeInfo {
    NodeInfo {
        endpoint_id: endpoint_id.to_owned(),
        node_name: name.to_owned(),
        quota_limit: quota,
        quota_used: 0,
        is_online: false,
        last_heartbeat: None,
        addr: None,
    }
}

/// Complete smoke test: add node → register services → verify ports and routes →
/// check quota → aggregate health → verify broadcast state.
#[tokio::test]
async fn test_smoke_complete_registration_flow() {
    let mut cn = ControlNode::new();
    let edge_id = "smoke-edge-001";

    // --- Phase 1: Add edge node to whitelist ---
    cn.add_node(make_edge_node(
        edge_id,
        "smoke-edge",
        DEFAULT_SERVICE_QUOTA as u16,
    ));
    assert!(
        cn.whitelist().contains_key(edge_id),
        "edge should be in whitelist after add_node"
    );

    // --- Phase 2: Validate join ticket ---
    let ticket = JoinTicket {
        endpoint_id: edge_id.to_owned(),
        created_at: 1_700_000_000,
        ttl_seconds: 3600,
        nonce: [77u8; 16],
    };
    let now = 1_700_001_000;
    cn.validate_ticket(&ticket, edge_id, now)
        .expect("ticket should be valid");

    // --- Phase 3: Register 2 services ---
    let registrations = vec![
        ServiceRegistration {
            name: "svc-alpha".to_owned(),
            local_addr: "127.0.0.1:3000".to_owned(),
            protocol: Protocol::Tcp,
            health_check: None,
        },
        ServiceRegistration {
            name: "svc-beta".to_owned(),
            local_addr: "127.0.0.1:3001".to_owned(),
            protocol: Protocol::Tcp,
            health_check: None,
        },
    ];

    let assignments = cn
        .register_services(edge_id, "smoke-edge", &registrations, now)
        .expect("registration should succeed");

    // --- Phase 4: Verify port assignments ---
    assert_eq!(assignments.len(), 2, "should have 2 port assignments");
    for a in &assignments {
        assert!(
            (SERVICE_PORT_START..=SERVICE_PORT_END).contains(&a.assigned_port),
            "port {} out of valid range",
            a.assigned_port
        );
    }
    assert_ne!(
        assignments[0].assigned_port, assignments[1].assigned_port,
        "ports must be unique"
    );

    // --- Phase 5: Verify route table ---
    assert_eq!(cn.routes().len(), 2, "route table should have 2 entries");

    let route_alpha = cn
        .routes()
        .get(&assignments[0].assigned_port)
        .expect("route for svc-alpha should exist");
    assert_eq!(route_alpha.service_name, "svc-alpha");
    assert_eq!(route_alpha.node_name, "smoke-edge");
    assert_eq!(route_alpha.endpoint_id, edge_id);
    assert_eq!(route_alpha.target_local_addr, "127.0.0.1:3000");

    let route_beta = cn
        .routes()
        .get(&assignments[1].assigned_port)
        .expect("route for svc-beta should exist");
    assert_eq!(route_beta.service_name, "svc-beta");

    // --- Phase 6: Verify quota enforcement ---
    // We used 2 of 5 quota slots — check_quota should still pass.
    cn.check_quota(edge_id)
        .expect("quota should not be exceeded with 2 of 5 services");

    // Re-register with the original 2 plus 3 more to hit the limit.
    let more_services: Vec<ServiceRegistration> = (0..3)
        .map(|i| ServiceRegistration {
            name: format!("svc-extra-{i}"),
            local_addr: format!("127.0.0.1:{}", 4000 + i),
            protocol: Protocol::Tcp,
            health_check: None,
        })
        .collect();
    let mut full_quota_services = registrations.clone();
    full_quota_services.extend(more_services);

    cn.register_services(edge_id, "smoke-edge", &full_quota_services, now)
        .expect("re-registering the full 5-service set should succeed");

    assert_eq!(cn.services().len(), 5, "should have 5 services total");
    assert_eq!(cn.routes().len(), 5, "route table should have 5 entries");

    // Now quota is full — re-registering with one extra service should fail.
    let mut over_quota = full_quota_services.clone();
    over_quota.push(ServiceRegistration {
        name: "svc-overflow".to_owned(),
        local_addr: "127.0.0.1:5000".to_owned(),
        protocol: Protocol::Tcp,
        health_check: None,
    });
    let overflow_result = cn.register_services(edge_id, "smoke-edge", &over_quota, now);
    assert!(
        overflow_result.is_err(),
        "registering beyond quota should fail"
    );
    assert!(
        overflow_result.unwrap_err().contains("quota exceeded"),
        "error should mention quota"
    );

    // --- Phase 7: Aggregate health ---
    cn.aggregate_health(
        edge_id,
        vec![
            ServiceHealthEntry {
                service_name: "svc-alpha".to_owned(),
                health_state: HealthState::Healthy,
                last_error: None,
            },
            ServiceHealthEntry {
                service_name: "svc-beta".to_owned(),
                health_state: HealthState::Degraded,
                last_error: Some("high latency".to_owned()),
            },
        ],
        now,
    );

    // Verify health states updated.
    let sid_alpha = ServiceId {
        endpoint_id: edge_id.to_owned(),
        service_name: "svc-alpha".to_owned(),
    };
    let sid_beta = ServiceId {
        endpoint_id: edge_id.to_owned(),
        service_name: "svc-beta".to_owned(),
    };
    assert_eq!(
        cn.services()
            .get(&sid_alpha)
            .expect("svc-alpha should exist")
            .health_state,
        HealthState::Healthy
    );
    assert_eq!(
        cn.services()
            .get(&sid_beta)
            .expect("svc-beta should exist")
            .health_state,
        HealthState::Degraded
    );

    // Unknown service health report should be silently skipped.
    cn.aggregate_health(
        edge_id,
        vec![ServiceHealthEntry {
            service_name: "nonexistent-svc".to_owned(),
            health_state: HealthState::Unhealthy,
            last_error: None,
        }],
        now,
    );
    // No panic, no new service added.
    assert_eq!(cn.services().len(), 5);

    // --- Phase 8: Heartbeat tracking ---
    cn.record_pong(edge_id, now);
    let timed_out = cn.check_heartbeats(now + 89); // 89s < 90s threshold
    assert!(
        timed_out.is_empty(),
        "node should not be timed out within threshold"
    );
    let timed_out = cn.check_heartbeats(now + 91); // 91s > 90s threshold
    assert_eq!(
        timed_out.len(),
        1,
        "node should be timed out after threshold"
    );
    assert_eq!(timed_out[0], edge_id);

    // --- Phase 9: Broadcast state verification ---
    // After registration, snapshot should reflect all state.
    let snapshot = cn.snapshot();
    assert_eq!(snapshot.whitelist.len(), 1);
    assert_eq!(snapshot.services.len(), 5);
    assert_eq!(snapshot.routes.len(), 5);
    assert!(
        snapshot.route_version >= 2,
        "should have incremented route version at least twice"
    );

    // The online_node_addrs should include our edge (it's online after registration).
    // It won't have an addr set (we didn't call set_node_addr), so it should be excluded
    // from broadcast targets.
    let broadcast_targets = cn.online_node_addrs();
    assert!(
        broadcast_targets.is_empty(),
        "no addr set, so broadcast targets should be empty"
    );

    // Set addr and verify it appears in broadcast targets.
    cn.set_node_addr(edge_id, "some-endpoint-addr".to_owned());
    let broadcast_targets = cn.online_node_addrs();
    assert_eq!(broadcast_targets.len(), 1);
    assert_eq!(broadcast_targets[0].0, edge_id);

    // --- Phase 10: Remove node and verify cleanup ---
    cn.remove_node(edge_id);
    assert!(cn.whitelist().is_empty(), "whitelist should be empty");
    assert!(cn.services().is_empty(), "services should be empty");
    assert!(cn.routes().is_empty(), "routes should be empty");
    assert!(
        cn.check_heartbeats(now + 200).is_empty(),
        "no heartbeat entries after removal"
    );
}

/// Test that multiple edge nodes can register independently.
#[tokio::test]
async fn test_smoke_multiple_edge_nodes() {
    let mut cn = ControlNode::new();
    let now = 1_700_000_000u64;

    // Add two edge nodes.
    cn.add_node(make_edge_node("edge-a", "alpha", 3));
    cn.add_node(make_edge_node("edge-b", "beta", 3));

    // Register services for edge-a.
    let assignments_a = cn
        .register_services(
            "edge-a",
            "alpha",
            &[ServiceRegistration {
                name: "web".to_owned(),
                local_addr: "127.0.0.1:80".to_owned(),
                protocol: Protocol::Tcp,
                health_check: None,
            }],
            now,
        )
        .expect("edge-a registration should succeed");

    // Register services for edge-b.
    let assignments_b = cn
        .register_services(
            "edge-b",
            "beta",
            &[ServiceRegistration {
                name: "web".to_owned(),
                local_addr: "127.0.0.1:80".to_owned(),
                protocol: Protocol::Tcp,
                health_check: None,
            }],
            now,
        )
        .expect("edge-b registration should succeed");

    // Both should get different ports even for same service name.
    assert_ne!(
        assignments_a[0].assigned_port, assignments_b[0].assigned_port,
        "different nodes should get different ports"
    );

    // Route table should have 2 entries.
    assert_eq!(cn.routes().len(), 2);
    assert_eq!(cn.services().len(), 2);

    // Removing one node should not affect the other.
    cn.remove_node("edge-a");
    assert_eq!(cn.routes().len(), 1, "only edge-b routes should remain");
    assert_eq!(cn.services().len(), 1, "only edge-b services should remain");
    assert!(cn.whitelist().contains_key("edge-b"));
    assert!(!cn.whitelist().contains_key("edge-a"));
}
