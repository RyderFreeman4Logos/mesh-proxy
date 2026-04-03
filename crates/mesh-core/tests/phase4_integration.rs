use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use mesh_core::{
    ProxyHandshakeValidationError, bridge_bidirectional, try_reserve_proxy_connection_slot,
    validate_inbound_proxy_handshake,
};
use mesh_proto::{MAX_PROXY_CONNECTIONS, MeshConfig, Protocol, ProxyHandshake, ServiceEntry};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

#[tokio::test]
async fn test_proxy_handshake_roundtrip() -> Result<()> {
    let handshake = ProxyHandshake {
        service_name: "echo-service".to_string(),
        port: 42_000,
        protocol: Protocol::Tcp,
    };
    let (mut writer, mut reader) = tokio::io::duplex(1024);

    mesh_proto::frame::write_json(&mut writer, &handshake).await?;
    let decoded: ProxyHandshake = mesh_proto::frame::read_json(&mut reader).await?;

    assert_eq!(decoded.service_name, handshake.service_name);
    assert_eq!(decoded.port, handshake.port);
    assert_eq!(decoded.protocol, handshake.protocol);
    Ok(())
}

#[tokio::test]
async fn test_bridge_streams_echo() -> Result<()> {
    let (local_bridge, mut local_peer) = tokio::io::duplex(1024);
    let (remote_send_bridge, mut remote_recv_peer) = tokio::io::duplex(1024);
    let (remote_recv_bridge, mut remote_send_peer) = tokio::io::duplex(1024);

    let (mut local_read, mut local_write) = tokio::io::split(local_bridge);
    let (_, mut remote_send) = tokio::io::split(remote_send_bridge);
    let (mut remote_recv, _) = tokio::io::split(remote_recv_bridge);

    let bridge_task = tokio::spawn(async move {
        bridge_bidirectional(
            &mut local_read,
            &mut local_write,
            &mut remote_send,
            &mut remote_recv,
        )
        .await
    });

    local_peer
        .write_all(b"ping")
        .await
        .context("failed to write outbound payload")?;
    let mut outbound = [0_u8; 4];
    timeout(
        Duration::from_secs(5),
        remote_recv_peer.read_exact(&mut outbound),
    )
    .await
    .context("timed out reading outbound payload")??;
    assert_eq!(&outbound, b"ping");

    remote_send_peer
        .write_all(b"pong")
        .await
        .context("failed to write inbound payload")?;
    let mut inbound = [0_u8; 4];
    timeout(Duration::from_secs(5), local_peer.read_exact(&mut inbound))
        .await
        .context("timed out reading inbound payload")??;
    assert_eq!(&inbound, b"pong");

    local_peer.shutdown().await?;
    remote_send_peer.shutdown().await?;
    timeout(Duration::from_secs(5), bridge_task)
        .await
        .context("timed out waiting for bridge task")?
        .context("bridge task panicked")??;

    Ok(())
}

#[tokio::test]
async fn test_proxy_inbound_rejects_unknown_service() -> Result<()> {
    let config = MeshConfig {
        services: vec![ServiceEntry {
            name: "known-service".to_string(),
            local_addr: "127.0.0.1:8080".to_string(),
            protocol: Protocol::Tcp,
            health_check: None,
        }],
        ..MeshConfig::default()
    };
    let handshake = ProxyHandshake {
        service_name: "missing-service".to_string(),
        port: 42_000,
        protocol: Protocol::Tcp,
    };

    let error = validate_inbound_proxy_handshake(&config, &handshake).unwrap_err();
    assert_eq!(
        error,
        ProxyHandshakeValidationError::UnknownService {
            service_name: "missing-service".to_string(),
        }
    );
    Ok(())
}

#[tokio::test]
async fn test_connection_limit_enforcement() -> Result<()> {
    let saturated_connections = Arc::new(AtomicUsize::new(MAX_PROXY_CONNECTIONS));
    let rejected = try_reserve_proxy_connection_slot(Arc::clone(&saturated_connections));

    assert!(rejected.is_none());
    assert_eq!(
        saturated_connections.load(Ordering::Relaxed),
        MAX_PROXY_CONNECTIONS
    );

    let near_limit_connections = Arc::new(AtomicUsize::new(MAX_PROXY_CONNECTIONS - 1));
    let permit = try_reserve_proxy_connection_slot(Arc::clone(&near_limit_connections))
        .context("expected a permit just below the limit")?;
    assert_eq!(
        near_limit_connections.load(Ordering::Relaxed),
        MAX_PROXY_CONNECTIONS
    );

    drop(permit);
    assert_eq!(
        near_limit_connections.load(Ordering::Relaxed),
        MAX_PROXY_CONNECTIONS - 1
    );
    Ok(())
}
