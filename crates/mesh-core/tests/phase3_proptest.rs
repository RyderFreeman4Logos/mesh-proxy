use std::collections::HashMap;
use std::sync::Arc;

use mesh_core::EdgeNode;
use mesh_proto::{MAX_LISTENERS, MeshConfig, Protocol, RouteEntry};
use proptest::collection::vec;
use proptest::prelude::*;
use tokio::sync::RwLock;

const PORT_POOL_SIZE: usize = MAX_LISTENERS + 4;
const MAX_SEQUENCE_LEN: usize = 16;

fn reserve_unused_tcp_ports(count: usize) -> Vec<u16> {
    let listeners = (0..count)
        .map(|_| {
            std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind")
        })
        .collect::<Vec<_>>();
    let ports = listeners
        .iter()
        .map(|listener| {
            listener
                .local_addr()
                .expect("listener should have a local addr")
                .port()
        })
        .collect::<Vec<_>>();
    drop(listeners);
    ports
}

fn build_routes(ports: &[u16], entries: &[(usize, u8)]) -> HashMap<u16, RouteEntry> {
    let mut routes = HashMap::new();

    for &(port_index, variant) in entries {
        let port = ports[port_index];
        routes.insert(
            port,
            RouteEntry {
                service_name: format!("svc-{port_index}-{}", variant % 5),
                node_name: format!("edge-{}", variant % 3),
                endpoint_id: format!("endpoint-{port_index}-{variant}"),
                target_local_addr: format!("127.0.0.1:{}", 20_000 + port_index),
                protocol: Protocol::Tcp,
            },
        );
    }

    routes
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]

    #[test]
    fn prop_listener_pool_never_exceeds_max(
        updates in vec(
            vec((0usize..PORT_POOL_SIZE, any::<u8>()), 0..=(PORT_POOL_SIZE + 16)),
            1..=MAX_SEQUENCE_LEN
        )
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime should build");

        let result: Result<(), TestCaseError> = runtime.block_on(async {
            let ports = reserve_unused_tcp_ports(PORT_POOL_SIZE);
            let mut edge = EdgeNode::new();
            let config = Arc::new(RwLock::new(MeshConfig::default()));

            for (index, update) in updates.iter().enumerate() {
                let routes = build_routes(&ports, update);
                let applied = edge
                    .apply_route_update(routes, (index + 1) as u64, Arc::clone(&config))
                    .await;
                prop_assert!(applied, "route update should be applied");
                prop_assert!(
                    edge.listener_count() <= MAX_LISTENERS,
                    "listener pool exceeded MAX_LISTENERS after update {index}: {} > {}",
                    edge.listener_count(),
                    MAX_LISTENERS
                );
            }

            let cleanup_version = updates.len() as u64 + 1;
            let cleaned_up = edge
                .apply_route_update(HashMap::new(), cleanup_version, config)
                .await;
            prop_assert!(cleaned_up, "cleanup update should be applied");
            prop_assert_eq!(edge.listener_count(), 0);

            Ok(())
        });

        result?;
    }
}
