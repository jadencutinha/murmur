//! The application layer that turns a Raft node into a replicated key/value
//! service: it owns the local Sable engine, drains committed log entries and
//! applies them, and serves the client-facing `Kv` gRPC API by routing every
//! request through Raft.
//!
//! Flow of a request: the [`KvService`] encodes it as a [`proto::Command`],
//! proposes it to the local Raft node, and blocks on a one-shot until the apply
//! loop reports that entry committed and applied — then returns the result. If
//! this node is not the leader the proposal is refused and the client is told who
//! the leader is, so it can redirect.
//!
//! Reads go through the log just like writes. That buys linearizability (a read
//! observes a definite point in the committed order) at the cost of a consensus
//! round; a lease- or read-index-based fast path is a future optimization.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::command;
use crate::proto::kv_server::Kv;
use crate::proto::{
    Command, DeleteRequest, DeleteResponse, GetRequest, GetResponse, Op, PutRequest, PutResponse,
};
use crate::raft::{
    start_node, ApplyReceiver, ClusterConfig, NodeHandle, NodeId, RaftStorage, Timing,
};
use crate::store::KvStore;

/// How long a client request waits for its entry to commit before giving up. A
/// healthy leader commits in well under this; exceeding it usually means we lost
/// leadership after proposing, so the client should retry.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(3);

/// A proposal is keyed by its `(client_id, seq)` — unique per in-flight request —
/// so the apply loop can match a committed entry back to the waiting caller.
type WaiterKey = (u64, u64);

/// The result of applying one command to the state machine.
enum Outcome {
    /// A read: the value found (or `None` if absent/deleted).
    Value(Option<Vec<u8>>),
    /// A write applied successfully.
    Done,
    /// The engine rejected the operation.
    Failed(String),
}

/// State shared between the apply loop and the request handlers. Deliberately
/// does *not* hold the [`NodeHandle`]: the apply loop keeps this alive, and if it
/// also kept the node alive the node could never shut down (its apply channel
/// would never close).
struct AppState {
    store: KvStore,
    /// Callers waiting for their proposed entry to be applied.
    waiters: Mutex<HashMap<WaiterKey, oneshot::Sender<Outcome>>>,
}

impl AppState {
    /// Apply one committed command to the local engine and, if a local caller is
    /// waiting on it, hand back the outcome. Runs on every node; only the node
    /// that proposed the command has a matching waiter.
    fn apply(&self, command: &Command) {
        let outcome = match Op::try_from(command.op) {
            Ok(Op::Get) => match self.store.get(&command.key) {
                Ok(value) => Outcome::Value(value),
                Err(e) => Outcome::Failed(e.to_string()),
            },
            Ok(Op::Put) => match self.store.put(&command.key, &command.value) {
                Ok(()) => Outcome::Done,
                Err(e) => Outcome::Failed(e.to_string()),
            },
            Ok(Op::Delete) => match self.store.delete(&command.key) {
                Ok(()) => Outcome::Done,
                Err(e) => Outcome::Failed(e.to_string()),
            },
            Err(_) => Outcome::Failed(format!("unknown op {}", command.op)),
        };
        if let Some(tx) = self.waiters.lock().unwrap().remove(&(command.client_id, command.seq)) {
            // The receiver may be gone (caller timed out); that's fine.
            let _ = tx.send(outcome);
        }
    }
}

/// Drain committed entries in order and apply each to the state machine. Ends
/// when the node stops and the apply channel closes.
async fn run_apply_loop(mut applies: ApplyReceiver, state: Arc<AppState>) {
    while let Some(entry) = applies.recv().await {
        match command::decode(&entry.command) {
            Ok(command) => state.apply(&command),
            // A malformed entry should be impossible (we encoded it), but never
            // let one wedge the apply loop — skip it and keep the log flowing.
            Err(e) => eprintln!("apply: skipping undecodable entry {}: {e}", entry.index),
        }
    }
}

/// Owns the Raft node and the per-request bookkeeping. Behind an `Arc` so the
/// gRPC layer can clone cheaply per request.
struct Inner {
    node: NodeHandle,
    node_id: NodeId,
    state: Arc<AppState>,
    /// Per-node monotonic counter making every proposal's `(client_id, seq)`
    /// unique, so committed entries map unambiguously back to their waiters.
    seq: AtomicU64,
}

impl Inner {
    async fn execute(&self, mut command: Command) -> Result<Outcome, Status> {
        let key: WaiterKey = (self.node_id, self.seq.fetch_add(1, Ordering::Relaxed));
        command.client_id = key.0;
        command.seq = key.1;

        // Register the waiter *before* proposing, so the entry can never be
        // applied before someone is listening for it.
        let (tx, rx) = oneshot::channel();
        self.state.waiters.lock().unwrap().insert(key, tx);

        if self.node.propose(command::encode(&command)).is_none() {
            self.state.waiters.lock().unwrap().remove(&key);
            return Err(self.not_leader());
        }

        match tokio::time::timeout(COMMIT_TIMEOUT, rx).await {
            Ok(Ok(outcome)) => Ok(outcome),
            // Timed out or the sender was dropped: clean up and ask for a retry.
            _ => {
                self.state.waiters.lock().unwrap().remove(&key);
                Err(Status::unavailable("command not committed in time; retry"))
            }
        }
    }

    /// A "not leader" error, annotated with the current leader's id (when known)
    /// in the `x-leader-id` metadata so a client can redirect.
    fn not_leader(&self) -> Status {
        let mut status = Status::failed_precondition("not leader");
        if let Some(leader) = self.node.leader_id() {
            if let Ok(value) = leader.to_string().parse() {
                status.metadata_mut().insert("x-leader-id", value);
            }
        }
        status
    }
}

/// The client-facing key/value service, backed by replicated Raft state. Cloning
/// is cheap (a shared `Arc`) — tonic clones it per request, and callers keep a
/// clone for observability.
#[derive(Clone)]
pub struct KvService {
    inner: Arc<Inner>,
}

impl KvService {
    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.inner.node_id
    }
    /// Whether this node currently believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.inner.node.is_leader()
    }
    /// The node this node currently thinks is leader, if any.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.inner.node.leader_id()
    }
}

/// Start a replicated KV node: launch the Raft node on `raft_listener`, spawn the
/// apply loop over `store`, and return the [`KvService`] to serve to clients.
pub async fn start(
    config: ClusterConfig,
    storage: Box<dyn RaftStorage>,
    timing: Timing,
    raft_listener: TcpListener,
    store: KvStore,
) -> anyhow::Result<KvService> {
    let node_id = config.id;
    let (node, applies) = start_node(config, storage, timing, raft_listener).await?;
    let state = Arc::new(AppState { store, waiters: Mutex::new(HashMap::new()) });

    // The apply loop holds only `AppState`, so dropping the node closes the apply
    // channel and lets the loop finish on its own.
    tokio::spawn(run_apply_loop(applies, state.clone()));

    Ok(KvService {
        inner: Arc::new(Inner { node, node_id, state, seq: AtomicU64::new(0) }),
    })
}

#[tonic::async_trait]
impl Kv for KvService {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let key = request.into_inner().key;
        match self.inner.execute(command::get(key)).await? {
            Outcome::Value(Some(value)) => Ok(Response::new(GetResponse { found: true, value })),
            Outcome::Value(None) => {
                Ok(Response::new(GetResponse { found: false, value: Vec::new() }))
            }
            Outcome::Failed(msg) => Err(Status::internal(msg)),
            // A GET never yields `Done`.
            Outcome::Done => Ok(Response::new(GetResponse { found: false, value: Vec::new() })),
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let PutRequest { key, value } = request.into_inner();
        match self.inner.execute(command::put(key, value)).await? {
            Outcome::Done => Ok(Response::new(PutResponse {})),
            Outcome::Failed(msg) => Err(Status::internal(msg)),
            Outcome::Value(_) => Ok(Response::new(PutResponse {})),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let key = request.into_inner().key;
        match self.inner.execute(command::delete(key)).await? {
            Outcome::Done => Ok(Response::new(DeleteResponse {})),
            Outcome::Failed(msg) => Err(Status::internal(msg)),
            Outcome::Value(_) => Ok(Response::new(DeleteResponse {})),
        }
    }
}
