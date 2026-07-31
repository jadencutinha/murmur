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
//!
//! Exactly-once: a client stamps every mutation with its `client_id` and a
//! monotonic `seq`, reusing the pair when it retries. The apply loop keeps a
//! per-client table of the last seq it applied and that command's result, so a
//! duplicate is recognized and its cached result replayed instead of applying the
//! mutation twice — even if the retry lands on a different leader.

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
    AppendRequest, AppendResponse, Command, DeleteRequest, DeleteResponse, GetRequest, GetResponse,
    Op, PutRequest, PutResponse, RegisterRequest, RegisterResponse,
};
use crate::raft::{
    start_node, ApplyReceiver, ClusterConfig, NodeHandle, NodeId, RaftStorage, Timing,
};
use crate::store::KvStore;

/// How long a client request waits for its entry to commit before giving up. A
/// healthy leader commits in well under this; exceeding it usually means we lost
/// leadership after proposing, so the client should retry.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(3);

/// The result of applying one command to the state machine. Cloneable so it can
/// be cached in the dedup table and replayed to a retried request.
#[derive(Clone)]
enum Outcome {
    /// A read (or the value produced by an append): the value, or `None` if absent.
    Value(Option<Vec<u8>>),
    /// A write applied successfully.
    Done,
    /// The engine rejected the operation.
    Failed(String),
}

/// The last request applied for a given client, remembered so a retry of it is
/// recognized and its result replayed rather than applied again.
struct LastApplied {
    seq: u64,
    outcome: Outcome,
}

/// State shared between the apply loop and the request handlers. Deliberately
/// does *not* hold the [`NodeHandle`]: the apply loop keeps this alive, and if it
/// also kept the node alive the node could never shut down (its apply channel
/// would never close).
struct AppState {
    store: KvStore,
    /// Callers waiting for their proposed entry to be applied, keyed by the
    /// per-proposal nonce.
    waiters: Mutex<HashMap<u64, oneshot::Sender<Outcome>>>,
    /// Per-client dedup table: the highest seq applied and its result. Volatile —
    /// rebuilt by replaying the committed log after a restart. Touched only by the
    /// single apply loop, but behind a `Mutex` since `AppState` is shared.
    dedup: Mutex<HashMap<u64, LastApplied>>,
}

impl AppState {
    /// Apply one committed command and wake the local caller waiting on it (if
    /// any — only the proposing node holds the matching waiter).
    fn apply(&self, command: &Command) {
        let outcome = self.apply_committed(command);
        if let Some(tx) = self.waiters.lock().unwrap().remove(&command.nonce) {
            let _ = tx.send(outcome); // receiver may be gone if the caller timed out
        }
    }

    /// Run a command against the engine, consulting and updating the dedup table
    /// for identified mutations so each applies exactly once.
    fn apply_committed(&self, command: &Command) -> Outcome {
        let op = Op::try_from(command.op);
        let is_mutation = matches!(op, Ok(Op::Put) | Ok(Op::Delete) | Ok(Op::Append));

        // Reads and unidentified writes bypass the dedup table entirely.
        if !is_mutation || command.client_id == 0 {
            return self.run(op, command);
        }

        let mut table = self.dedup.lock().unwrap();
        match table.get(&command.client_id) {
            // Already applied this seq (or an older one): replay the cached result
            // and do not touch the engine again.
            Some(last) if command.seq <= last.seq => last.outcome.clone(),
            _ => {
                let outcome = self.run(op, command);
                table.insert(
                    command.client_id,
                    LastApplied { seq: command.seq, outcome: outcome.clone() },
                );
                outcome
            }
        }
    }

    /// Execute a decoded operation against the Sable engine.
    fn run(&self, op: Result<Op, prost::UnknownEnumValue>, command: &Command) -> Outcome {
        match op {
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
            Ok(Op::Append) => {
                // Read-modify-write; safe without locking because the apply loop
                // is the sole writer and runs one entry at a time.
                let mut value = match self.store.get(&command.key) {
                    Ok(v) => v.unwrap_or_default(),
                    Err(e) => return Outcome::Failed(e.to_string()),
                };
                value.extend_from_slice(&command.value);
                match self.store.put(&command.key, &value) {
                    Ok(()) => Outcome::Value(Some(value)),
                    Err(e) => Outcome::Failed(e.to_string()),
                }
            }
            Err(_) => Outcome::Failed(format!("unknown op {}", command.op)),
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
    /// Per-node counter feeding proposal nonces.
    nonce: AtomicU64,
}

impl Inner {
    /// A nonce unique across the whole cluster: the node id in the high bits, a
    /// per-node counter in the low bits. Cluster-unique so an entry proposed here
    /// never wakes the wrong waiter after it replicates elsewhere.
    fn next_nonce(&self) -> u64 {
        let counter = self.nonce.fetch_add(1, Ordering::Relaxed);
        (self.node_id << 48) | (counter & ((1 << 48) - 1))
    }

    async fn execute(&self, mut command: Command) -> Result<Outcome, Status> {
        let nonce = self.next_nonce();
        command.nonce = nonce;

        // Register the waiter *before* proposing, so the entry can never be
        // applied before someone is listening for it.
        let (tx, rx) = oneshot::channel();
        self.state.waiters.lock().unwrap().insert(nonce, tx);

        if self.node.propose(command::encode(&command)).is_none() {
            self.state.waiters.lock().unwrap().remove(&nonce);
            return Err(self.not_leader());
        }

        match tokio::time::timeout(COMMIT_TIMEOUT, rx).await {
            Ok(Ok(outcome)) => Ok(outcome),
            // Timed out or the sender was dropped: clean up and ask for a retry.
            _ => {
                self.state.waiters.lock().unwrap().remove(&nonce);
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
    let state = Arc::new(AppState {
        store,
        waiters: Mutex::new(HashMap::new()),
        dedup: Mutex::new(HashMap::new()),
    });

    // The apply loop holds only `AppState`, so dropping the node closes the apply
    // channel and lets the loop finish on its own.
    tokio::spawn(run_apply_loop(applies, state.clone()));

    Ok(KvService {
        inner: Arc::new(Inner { node, node_id, state, nonce: AtomicU64::new(0) }),
    })
}

#[tonic::async_trait]
impl Kv for KvService {
    async fn register(
        &self,
        _request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        // A random nonzero id. Collision across clients is astronomically
        // unlikely, so no consensus round is needed to hand one out.
        let client_id = fastrand::u64(1..=u64::MAX);
        Ok(Response::new(RegisterResponse { client_id }))
    }

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
        let PutRequest { key, value, client_id, seq } = request.into_inner();
        match self.inner.execute(command::put(key, value, client_id, seq)).await? {
            Outcome::Done => Ok(Response::new(PutResponse {})),
            Outcome::Failed(msg) => Err(Status::internal(msg)),
            Outcome::Value(_) => Ok(Response::new(PutResponse {})),
        }
    }

    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let AppendRequest { key, value, client_id, seq } = request.into_inner();
        match self.inner.execute(command::append(key, value, client_id, seq)).await? {
            Outcome::Value(Some(value)) => Ok(Response::new(AppendResponse { value })),
            Outcome::Value(None) => Ok(Response::new(AppendResponse { value: Vec::new() })),
            Outcome::Failed(msg) => Err(Status::internal(msg)),
            Outcome::Done => Ok(Response::new(AppendResponse { value: Vec::new() })),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let DeleteRequest { key, client_id, seq } = request.into_inner();
        match self.inner.execute(command::delete(key, client_id, seq)).await? {
            Outcome::Done => Ok(Response::new(DeleteResponse {})),
            Outcome::Failed(msg) => Err(Status::internal(msg)),
            Outcome::Value(_) => Ok(Response::new(DeleteResponse {})),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> AppState {
        let dir = tempfile::tempdir().unwrap();
        AppState {
            store: KvStore::open(dir.path()).unwrap(),
            waiters: Mutex::new(HashMap::new()),
            dedup: Mutex::new(HashMap::new()),
        }
    }

    fn value(outcome: Outcome) -> Option<Vec<u8>> {
        match outcome {
            Outcome::Value(v) => v,
            other => panic!("expected a value outcome, got {}", label(&other)),
        }
    }

    fn label(o: &Outcome) -> &'static str {
        match o {
            Outcome::Value(_) => "Value",
            Outcome::Done => "Done",
            Outcome::Failed(_) => "Failed",
        }
    }

    #[test]
    fn a_retried_mutation_applies_exactly_once() {
        let state = state();
        let client = 42;

        // First append: "a".
        let first = command::append(b"k".to_vec(), b"a".to_vec(), client, 1);
        assert_eq!(value(state.apply_committed(&first)), Some(b"a".to_vec()));

        // The *same* (client, seq) again — a retry. It must not append a second
        // time, and must replay the original result.
        let retry = command::append(b"k".to_vec(), b"a".to_vec(), client, 1);
        assert_eq!(value(state.apply_committed(&retry)), Some(b"a".to_vec()));
        assert_eq!(state.store.get(b"k").unwrap(), Some(b"a".to_vec())); // still just "a"

        // A new seq is a genuine new operation and does append.
        let next = command::append(b"k".to_vec(), b"b".to_vec(), client, 2);
        assert_eq!(value(state.apply_committed(&next)), Some(b"ab".to_vec()));
    }

    #[test]
    fn different_clients_are_tracked_independently() {
        let state = state();
        // Same seq number, different clients: both apply (dedup is per-client).
        state.apply_committed(&command::append(b"k".to_vec(), b"x".to_vec(), 1, 1));
        state.apply_committed(&command::append(b"k".to_vec(), b"y".to_vec(), 2, 1));
        assert_eq!(state.store.get(b"k").unwrap(), Some(b"xy".to_vec()));
    }

    #[test]
    fn unidentified_writes_are_not_deduplicated() {
        let state = state();
        // client_id 0 means "no identity": every apply takes effect.
        let cmd = command::append(b"k".to_vec(), b"z".to_vec(), 0, 0);
        state.apply_committed(&cmd);
        state.apply_committed(&cmd);
        assert_eq!(state.store.get(b"k").unwrap(), Some(b"zz".to_vec()));
    }
}
