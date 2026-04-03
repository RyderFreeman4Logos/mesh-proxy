use std::collections::HashMap;
use std::str::FromStr;

use anyhow::{Context, Result};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use mesh_proto::ALPN_PROXY;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::RwLock;

#[cfg(unix)]
use tokio::net::UnixStream;

/// Caches outbound QUIC connections keyed by the remote endpoint id.
#[derive(Debug)]
pub struct ConnectionPool {
    pool: RwLock<HashMap<String, Connection>>,
    endpoint: Endpoint,
}

impl ConnectionPool {
    /// Create a connection pool backed by the provided iroh endpoint.
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            pool: RwLock::new(HashMap::new()),
            endpoint,
        }
    }

    /// Get a cached connection or establish a new one on demand.
    pub async fn get_or_connect(&self, endpoint_id_hex: &str) -> Result<Connection> {
        if let Some(connection) = self.cached_connection(endpoint_id_hex).await {
            return Ok(connection);
        }

        self.connect_and_cache(endpoint_id_hex).await
    }

    async fn cached_connection(&self, endpoint_id_hex: &str) -> Option<Connection> {
        let cached = {
            let pool = self.pool.read().await;
            pool.get(endpoint_id_hex).cloned()
        }?;

        if let Some(error) = cached.close_reason() {
            tracing::debug!(
                endpoint_id = endpoint_id_hex,
                error = %error,
                "evicting closed cached QUIC connection"
            );
            self.pool.write().await.remove(endpoint_id_hex);
            return None;
        }

        Some(cached)
    }

    async fn connect_and_cache(&self, endpoint_id_hex: &str) -> Result<Connection> {
        let endpoint_id = EndpointId::from_str(endpoint_id_hex)
            .with_context(|| format!("invalid endpoint id: {endpoint_id_hex}"))?;
        let endpoint_addr = EndpointAddr::from(endpoint_id);

        let connection = self
            .endpoint
            .connect(endpoint_addr, ALPN_PROXY)
            .await
            .with_context(|| format!("failed to connect to endpoint {endpoint_id_hex}"))?;

        let mut pool = self.pool.write().await;
        pool.insert(endpoint_id_hex.to_string(), connection.clone());
        Ok(connection)
    }
}

/// Copy bytes bidirectionally between two pairs of split streams.
pub async fn bridge_bidirectional<LocalRead, LocalWrite, RemoteWrite, RemoteRead>(
    local_read: &mut LocalRead,
    local_write: &mut LocalWrite,
    remote_write: &mut RemoteWrite,
    remote_read: &mut RemoteRead,
) -> Result<()>
where
    LocalRead: AsyncRead + Unpin,
    LocalWrite: AsyncWrite + Unpin,
    RemoteWrite: AsyncWrite + Unpin,
    RemoteRead: AsyncRead + Unpin,
{
    let copy_out = tokio::io::copy(local_read, remote_write);
    let copy_in = tokio::io::copy(remote_read, local_write);
    tokio::try_join!(copy_out, copy_in).context("failed to bridge local and QUIC streams")?;
    Ok(())
}

/// Copy bytes between a local TCP stream and an iroh bidirectional QUIC stream pair.
pub async fn bridge_streams(
    local: TcpStream,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    let (mut local_read, mut local_write) = local.into_split();
    bridge_bidirectional(&mut local_read, &mut local_write, &mut send, &mut recv).await?;
    send.finish().context("failed to finish QUIC send stream")?;
    Ok(())
}

#[cfg(unix)]
pub(crate) async fn bridge_unix_streams(
    local: UnixStream,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    let (mut local_read, mut local_write) = tokio::io::split(local);
    bridge_bidirectional(&mut local_read, &mut local_write, &mut send, &mut recv).await?;
    send.finish().context("failed to finish QUIC send stream")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::{Context, Result};
    use mesh_proto::ALPN_PROXY;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::{Duration, timeout};

    use super::*;

    #[tokio::test]
    async fn test_connection_pool_reuses_open_connection_and_reconnects_after_close() -> Result<()>
    {
        use iroh::address_lookup::memory::MemoryLookup;
        use iroh::endpoint::presets;

        let address_lookup = MemoryLookup::new();
        let server_endpoint = iroh::Endpoint::builder(presets::N0)
            .alpns(vec![ALPN_PROXY.to_vec()])
            .bind()
            .await
            .context("failed to bind server endpoint")?;
        let client_endpoint = iroh::Endpoint::builder(presets::N0)
            .address_lookup(address_lookup.clone())
            .alpns(vec![ALPN_PROXY.to_vec()])
            .bind()
            .await
            .context("failed to bind client endpoint")?;
        let endpoint_id = server_endpoint.id().to_string();
        address_lookup.add_endpoint_info(server_endpoint.addr());

        let accept_task = tokio::spawn(async move {
            let mut accepted = Vec::new();
            for _ in 0..2 {
                let incoming = server_endpoint
                    .accept()
                    .await
                    .context("accept returned none")?;
                let connection = incoming
                    .accept()
                    .context("failed to accept incoming connection")?
                    .await
                    .context("incoming connection handshake failed")?;
                accepted.push(connection);
            }
            Result::<Vec<Connection>>::Ok(accepted)
        });

        let pool = ConnectionPool::new(client_endpoint.clone());

        let first = pool.get_or_connect(&endpoint_id).await?;
        let reused = pool.get_or_connect(&endpoint_id).await?;
        assert_eq!(first.stable_id(), reused.stable_id());

        first.close(0u32.into(), b"test close");
        let _ = timeout(Duration::from_secs(5), first.closed())
            .await
            .context("timed out waiting for cached connection to close")?;

        let reconnected = pool.get_or_connect(&endpoint_id).await?;
        assert_ne!(first.stable_id(), reconnected.stable_id());

        client_endpoint.close().await;
        let accepted = timeout(Duration::from_secs(5), accept_task)
            .await
            .context("timed out waiting for accept task")?
            .context("accept task panicked")??;
        for connection in accepted {
            connection.close(0u32.into(), b"test done");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_bridge_streams_copies_bidirectional_payloads() -> Result<()> {
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

        local_peer.write_all(b"ping").await?;
        let mut outbound = [0_u8; 4];
        timeout(
            Duration::from_secs(1),
            remote_recv_peer.read_exact(&mut outbound),
        )
        .await
        .context("timed out reading outbound payload")??;
        assert_eq!(&outbound, b"ping");

        remote_send_peer.write_all(b"pong").await?;
        let mut inbound = [0_u8; 4];
        timeout(Duration::from_secs(1), local_peer.read_exact(&mut inbound))
            .await
            .context("timed out reading inbound payload")??;
        assert_eq!(&inbound, b"pong");

        local_peer.shutdown().await?;
        remote_send_peer.shutdown().await?;

        timeout(Duration::from_secs(1), bridge_task)
            .await
            .context("timed out waiting for bridge task")?
            .context("bridge task panicked")??;

        Ok(())
    }
}
