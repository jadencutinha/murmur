//! A thin client for a murmur cluster that provides exactly-once semantics.
//!
//! The clerk registers once to obtain a `client_id`, then stamps every mutation
//! with that id and a monotonically increasing `seq`. Crucially, a retried
//! operation *reuses the same `seq`*: combined with the server-side dedup table,
//! that makes each logical operation take effect exactly once even though the
//! clerk may resend it to several nodes before one commits it.
//!
//! Leader discovery is by round-robin: the clerk remembers whichever node last
//! accepted a request and tries it first, falling through the rest on a
//! not-leader / unavailable error until one succeeds or a budget elapses.

use std::time::{Duration, Instant};

use tonic::transport::Channel;
use tonic::{Code, Status};

use crate::proto::kv_client::KvClient;
use crate::proto::{AppendRequest, DeleteRequest, GetRequest, PutRequest, RegisterRequest};

/// How long a single logical operation may spend retrying across nodes.
const RETRY_BUDGET: Duration = Duration::from_secs(10);
/// Pause between retry attempts, giving an election time to settle.
const RETRY_PAUSE: Duration = Duration::from_millis(50);

pub struct Clerk {
    endpoints: Vec<String>,
    clients: Vec<Option<KvClient<Channel>>>,
    /// Index of the node we currently believe is leader; tried first.
    leader: usize,
    client_id: u64,
    seq: u64,
}

/// A not-leader or transient-unavailable error is worth retrying elsewhere;
/// anything else (e.g. an engine failure) is surfaced to the caller.
fn retryable(status: &Status) -> bool {
    matches!(status.code(), Code::FailedPrecondition | Code::Unavailable)
}

impl Clerk {
    /// Connect to a cluster given every node's KV address, and register for a
    /// client id (retrying across nodes until one answers).
    pub async fn connect(endpoints: Vec<String>) -> anyhow::Result<Self> {
        anyhow::ensure!(!endpoints.is_empty(), "clerk needs at least one endpoint");
        let clients = vec![None; endpoints.len()];
        let mut clerk = Self { endpoints, clients, leader: 0, client_id: 0, seq: 0 };

        let deadline = Instant::now() + RETRY_BUDGET;
        loop {
            if let Some(mut client) = clerk.current().await {
                if let Ok(resp) = client.register(RegisterRequest {}).await {
                    clerk.client_id = resp.into_inner().client_id;
                    return Ok(clerk);
                }
            }
            anyhow::ensure!(Instant::now() < deadline, "no node answered register");
            clerk.rotate().await;
        }
    }

    /// The id assigned to this clerk at registration.
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    /// Read `key` (routed through Raft for a linearizable result).
    pub async fn get(&mut self, key: Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
        self.retry(|mut client, _id, _seq| {
            let key = key.clone();
            async move {
                let resp = client.get(GetRequest { key }).await?.into_inner();
                Ok(resp.found.then_some(resp.value))
            }
        })
        .await
    }

    /// Write `key` = `value`.
    pub async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        self.retry(|mut client, client_id, seq| {
            let (key, value) = (key.clone(), value.clone());
            async move {
                client.put(PutRequest { key, value, client_id, seq }).await?;
                Ok(())
            }
        })
        .await
    }

    /// Append `value` to `key`, returning the value after the append.
    pub async fn append(&mut self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        self.retry(|mut client, client_id, seq| {
            let (key, value) = (key.clone(), value.clone());
            async move {
                let resp = client.append(AppendRequest { key, value, client_id, seq }).await?;
                Ok(resp.into_inner().value)
            }
        })
        .await
    }

    /// Remove `key`.
    pub async fn delete(&mut self, key: Vec<u8>) -> anyhow::Result<()> {
        self.retry(|mut client, client_id, seq| {
            let key = key.clone();
            async move {
                client.delete(DeleteRequest { key, client_id, seq }).await?;
                Ok(())
            }
        })
        .await
    }

    /// Run one logical operation with a fresh `seq`, retrying it across nodes
    /// (reusing that same seq every attempt) until it succeeds or the budget runs
    /// out. `op` builds and sends the specific RPC. The client is passed by value
    /// (it clones cheaply — a shared channel) so the returned future owns it.
    async fn retry<T, F, Fut>(&mut self, op: F) -> anyhow::Result<T>
    where
        F: Fn(KvClient<Channel>, u64, u64) -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        self.seq += 1;
        let seq = self.seq;
        let client_id = self.client_id;
        let deadline = Instant::now() + RETRY_BUDGET;
        loop {
            if let Some(client) = self.current().await {
                match op(client, client_id, seq).await {
                    Ok(value) => return Ok(value),
                    Err(status) if retryable(&status) => {}
                    Err(status) => return Err(status.into()),
                }
            }
            anyhow::ensure!(Instant::now() < deadline, "operation timed out across all nodes");
            self.rotate().await;
        }
    }

    /// A connected client for the current `leader` endpoint, reconnecting lazily.
    /// Returns `None` (and drops the cached client) if the node can't be reached.
    async fn current(&mut self) -> Option<KvClient<Channel>> {
        let idx = self.leader;
        if self.clients[idx].is_none() {
            match KvClient::connect(format!("http://{}", self.endpoints[idx])).await {
                Ok(client) => self.clients[idx] = Some(client),
                Err(_) => return None,
            }
        }
        self.clients[idx].clone()
    }

    /// Move on to the next node after a failure, pausing briefly. Cached clients
    /// are kept: tonic reconnects a broken channel on its own, and a node that was
    /// merely "not leader" is perfectly healthy.
    async fn rotate(&mut self) {
        self.leader = (self.leader + 1) % self.endpoints.len();
        tokio::time::sleep(RETRY_PAUSE).await;
    }
}
