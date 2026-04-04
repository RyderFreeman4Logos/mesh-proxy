use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use mesh_proto::ALPN_PROXY;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};

#[cfg(unix)]
use tokio::net::UnixStream;

/// Caches outbound QUIC connections keyed by the remote endpoint id.
#[derive(Debug)]
pub struct ConnectionPool {
    pool: RwLock<HashMap<String, Connection>>,
    dial_locks: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    endpoint: Endpoint,
}

impl ConnectionPool {
    /// Create a connection pool backed by the provided iroh endpoint.
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            pool: RwLock::new(HashMap::new()),
            dial_locks: RwLock::new(HashMap::new()),
            endpoint,
        }
    }

    /// Return the endpoint id used by this pool for outbound connections.
    pub fn local_endpoint_id(&self) -> String {
        self.endpoint.id().to_string()
    }

    /// Get a cached connection or establish a new one on demand.
    pub async fn get_or_connect(&self, endpoint_id_hex: &str) -> Result<Connection> {
        if let Some(connection) = self.cached_connection(endpoint_id_hex).await {
            return Ok(connection);
        }

        let dial_lock = self.dial_lock(endpoint_id_hex).await;
        let _dial_guard = dial_lock.lock().await;

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
            self.evict_if_stale(endpoint_id_hex, &cached).await;
            return None;
        }

        Some(cached)
    }

    async fn dial_lock(&self, endpoint_id_hex: &str) -> Arc<Mutex<()>> {
        if let Some(lock) = self.dial_locks.read().await.get(endpoint_id_hex).cloned() {
            return lock;
        }

        let mut dial_locks = self.dial_locks.write().await;
        Arc::clone(
            dial_locks
                .entry(endpoint_id_hex.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    async fn evict_if_stale(&self, endpoint_id_hex: &str, stale_connection: &Connection) {
        let stale_connection_id = stale_connection.stable_id();
        let mut pool = self.pool.write().await;
        let should_remove = pool
            .get(endpoint_id_hex)
            .is_some_and(|connection| connection.stable_id() == stale_connection_id);

        if should_remove {
            pool.remove(endpoint_id_hex);
        }
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
    use tokio::sync::Barrier;
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
    async fn test_connection_pool_serializes_concurrent_dials_per_endpoint() -> Result<()> {
        use iroh::address_lookup::memory::MemoryLookup;
        use iroh::endpoint::presets;

        let caller_count = 8;
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

        let accept_endpoint = server_endpoint.clone();
        let accept_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let mut accepted = Vec::new();
            loop {
                let incoming =
                    match timeout(Duration::from_millis(200), accept_endpoint.accept()).await {
                        Ok(Some(incoming)) => incoming,
                        Ok(None) | Err(_) => break,
                    };

                let connection = incoming
                    .accept()
                    .context("failed to accept incoming connection")?
                    .await
                    .context("incoming connection handshake failed")?;
                accepted.push(connection);

                if accepted.len() == caller_count {
                    break;
                }
            }

            Result::<Vec<Connection>>::Ok(accepted)
        });

        let pool = Arc::new(ConnectionPool::new(client_endpoint.clone()));
        let barrier = Arc::new(Barrier::new(caller_count));
        let mut tasks = Vec::with_capacity(caller_count);
        for _ in 0..caller_count {
            let barrier = Arc::clone(&barrier);
            let endpoint_id = endpoint_id.clone();
            let pool = Arc::clone(&pool);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                let connection = pool.get_or_connect(&endpoint_id).await?;
                Result::<_, anyhow::Error>::Ok(connection.stable_id())
            }));
        }

        let mut stable_ids = Vec::with_capacity(caller_count);
        for task in tasks {
            stable_ids.push(
                timeout(Duration::from_secs(5), task)
                    .await
                    .context("timed out waiting for concurrent dial task")?
                    .context("concurrent dial task panicked")??,
            );
        }

        assert!(
            stable_ids.windows(2).all(|pair| pair[0] == pair[1]),
            "all callers should receive the same cached connection"
        );

        let accepted = timeout(Duration::from_secs(5), accept_task)
            .await
            .context("timed out waiting for accept task")?
            .context("accept task panicked")??;
        assert_eq!(accepted.len(), 1, "expected exactly one outbound dial");

        client_endpoint.close().await;
        for connection in accepted {
            connection.close(0u32.into(), b"test done");
        }
        server_endpoint.close().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_pool_preserves_reconnected_entry_during_stale_eviction() -> Result<()>
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
        first.close(0u32.into(), b"test close");
        let _ = timeout(Duration::from_secs(5), first.closed())
            .await
            .context("timed out waiting for original cached connection to close")?;

        let replacement = pool.get_or_connect(&endpoint_id).await?;
        assert_ne!(first.stable_id(), replacement.stable_id());

        pool.evict_if_stale(&endpoint_id, &first).await;

        let cached = pool
            .cached_connection(&endpoint_id)
            .await
            .context("expected replacement connection to remain cached")?;
        assert_eq!(cached.stable_id(), replacement.stable_id());

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
