use redis::aio::MultiplexedConnection;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// A pool of Redis connections that distributes load across multiple connections.
///
/// This is useful for high-throughput scenarios where a single connection may become
/// a bottleneck. Connections are distributed using round-robin selection.
#[derive(Clone)]
pub struct ConnectionPool {
    inner: Arc<ConnectionPoolInner>,
}

struct ConnectionPoolInner {
    connections: Vec<MultiplexedConnection>,
    index: AtomicUsize,
}

impl ConnectionPool {
    /// Creates a new connection pool with the specified number of connections.
    pub async fn new(client: &redis::Client, size: usize) -> Result<Self, redis::RedisError> {
        let mut connections = Vec::with_capacity(size);
        for _ in 0..size {
            connections.push(client.get_multiplexed_async_connection().await?);
        }
        Ok(Self {
            inner: Arc::new(ConnectionPoolInner {
                connections,
                index: AtomicUsize::new(0),
            }),
        })
    }

    /// Creates a pool with default size of 10 connections.
    pub async fn with_default_size(client: &redis::Client) -> Result<Self, redis::RedisError> {
        Self::new(client, 10).await
    }

    /// Gets a connection from the pool using round-robin selection.
    pub fn get(&self) -> MultiplexedConnection {
        let idx = self.inner.index.fetch_add(1, Ordering::Relaxed) % self.inner.connections.len();
        self.inner.connections[idx].clone()
    }

    /// Returns the number of connections in the pool.
    pub fn size(&self) -> usize {
        self.inner.connections.len()
    }
}

impl From<MultiplexedConnection> for ConnectionPool {
    fn from(conn: MultiplexedConnection) -> Self {
        Self {
            inner: Arc::new(ConnectionPoolInner {
                connections: vec![conn],
                index: AtomicUsize::new(0),
            }),
        }
    }
}
