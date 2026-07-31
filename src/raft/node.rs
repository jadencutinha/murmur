//! The async driver that turns the synchronous [`ConsensusModule`] into a live
//! node: it serves the Raft gRPC service and runs the periodic tick loop that
//! fires elections and heartbeats.
//!
//! The concurrency rule throughout: lock the core only to read or mutate state
//! and to *build* the RPCs to send, then release the lock before any `await`.
//! Replies are folded back in under a fresh lock. That keeps the fast
//! `std::sync::Mutex` from ever being held across a network round trip.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::proto::raft_server::RaftServer;

use super::config::{ClusterConfig, Timing};
use super::consensus::ConsensusModule;
use super::rpc::{AppendEntriesArgs, InstallSnapshotArgs, RequestVoteArgs};
use super::storage::RaftStorage;
use super::transport::{PeerClients, RaftService};
use super::types::{Apply, LogIndex, NodeId, Role, Term};

/// How often the driver wakes to check timers. Must be well under the heartbeat
/// interval so heartbeats and election checks fire promptly.
const TICK: Duration = Duration::from_millis(15);

/// The receiving end of a node's apply stream: committed commands and installed
/// snapshots arrive here in strict log order for the state machine to apply. The
/// caller owns it (a raw KV node drains it into Sable; tests drain it to observe
/// replication).
pub type ApplyReceiver = mpsc::UnboundedReceiver<Apply>;

/// A running node: a shared handle onto its consensus state plus the background
/// tasks (gRPC server + driver). Dropping or [`kill`](NodeHandle::kill)ing it
/// stops the node — the latter simulates a crash in tests.
pub struct NodeHandle {
    pub id: NodeId,
    core: Arc<Mutex<ConsensusModule>>,
    running: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<()>>,
}

impl NodeHandle {
    pub fn role(&self) -> Role {
        self.core.lock().unwrap().role()
    }
    pub fn current_term(&self) -> Term {
        self.core.lock().unwrap().current_term()
    }
    pub fn leader_id(&self) -> Option<NodeId> {
        self.core.lock().unwrap().leader_id()
    }
    pub fn is_leader(&self) -> bool {
        self.core.lock().unwrap().is_leader()
    }
    pub fn commit_index(&self) -> LogIndex {
        self.core.lock().unwrap().commit_index()
    }
    pub fn last_applied(&self) -> LogIndex {
        self.core.lock().unwrap().last_applied()
    }
    pub fn last_log_index(&self) -> LogIndex {
        self.core.lock().unwrap().last_log_index()
    }
    /// Entries physically held in the log tail (past any snapshot).
    pub fn raft_log_len(&self) -> usize {
        self.core.lock().unwrap().raft_log_len()
    }

    /// A detached, cloneable handle onto the consensus core for the state machine:
    /// it can hand snapshots back to Raft and read the log's size without holding
    /// any of the node's background tasks, so keeping it never blocks shutdown.
    pub fn control(&self) -> RaftControl {
        RaftControl { core: self.core.clone() }
    }

    /// Append a client command to the log if this node is the leader, returning
    /// the index it was assigned. `None` means "not the leader" — the caller must
    /// redirect to whoever [`leader_id`](Self::leader_id) reports. Commitment and
    /// application happen asynchronously; watch [`commit_index`](Self::commit_index)
    /// or the apply stream to learn when the entry takes effect.
    pub fn propose(&self, command: Vec<u8>) -> Option<LogIndex> {
        self.core.lock().unwrap().submit_command(command)
    }

    /// Simulate a crash: stop the driver and drop the gRPC server so the node
    /// goes silent. Its peers will notice the missing heartbeats and re-elect.
    pub fn kill(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Drop for NodeHandle {
    fn drop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// The state machine's window onto Raft for snapshotting. Deliberately holds only
/// the shared consensus core (not the [`NodeHandle`]'s tasks), so the apply loop
/// can retain it without keeping the node alive — mirroring why `AppState` keeps
/// no `NodeHandle`.
#[derive(Clone)]
pub struct RaftControl {
    core: Arc<Mutex<ConsensusModule>>,
}

impl RaftControl {
    /// Entries physically held in the log tail — watched to decide when to snapshot.
    pub fn raft_log_len(&self) -> usize {
        self.core.lock().unwrap().raft_log_len()
    }

    /// Hand Raft a state-machine snapshot capturing everything through `index`, so
    /// it can compact the log up to that point. A no-op if `index` is stale or not
    /// yet applied (see [`ConsensusModule::snapshot`]).
    pub fn snapshot(&self, index: LogIndex, data: Vec<u8>) {
        self.core.lock().unwrap().snapshot(index, data);
    }
}

/// Start a node: bind its consensus state to `listener`'s gRPC server and launch
/// the tick loop. `listener` must already be bound to this node's address (so
/// callers can use port 0 in tests); `config.peers` gives everyone else.
///
/// Returns the node handle and the [`ApplyReceiver`] carrying committed entries in
/// log order. The caller must drain the receiver (the state machine consumes it);
/// if it is dropped, applied entries are simply discarded.
pub async fn start_node(
    config: ClusterConfig,
    storage: Box<dyn RaftStorage>,
    timing: Timing,
    listener: TcpListener,
) -> anyhow::Result<(NodeHandle, ApplyReceiver)> {
    let now = Instant::now();
    let peer_ids: Vec<NodeId> = config.peers.iter().map(|p| p.id).collect();
    let core = Arc::new(Mutex::new(ConsensusModule::new(
        config.id,
        peer_ids.clone(),
        storage,
        timing,
        now,
    )?));

    let peer_addrs: Vec<(NodeId, String)> =
        config.peers.iter().map(|p| (p.id, p.addr.clone())).collect();
    let peers = PeerClients::connect(&peer_addrs)?;
    let running = Arc::new(AtomicBool::new(true));
    let (apply_tx, apply_rx) = mpsc::unbounded_channel();

    // Serve the Raft RPCs into the shared core.
    let service = RaftService::new(core.clone());
    let server_task = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(RaftServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });

    let driver_task = spawn_driver(core.clone(), peers, peer_ids, timing, running.clone(), apply_tx);

    let handle = NodeHandle {
        id: config.id,
        core,
        running,
        tasks: vec![server_task, driver_task],
    };
    Ok((handle, apply_rx))
}

/// What to send a peer this tick. Most ticks are AppendEntries (heartbeat, fresh
/// entries, or a repair probe); a peer whose next entry we have already compacted
/// gets an InstallSnapshot instead.
enum Outgoing {
    Append(AppendEntriesArgs),
    Snapshot(InstallSnapshotArgs),
}

/// One tick's worth of work, decided under the lock and executed after release.
enum Step {
    /// A PreVote probe to broadcast before committing to a real election (§9.6).
    StartPreElection(RequestVoteArgs),
    StartElection(RequestVoteArgs),
    /// Messages to send peers this tick (per-peer AppendEntries or InstallSnapshot).
    Replicate(Vec<(NodeId, Outgoing)>),
    Idle,
}

fn spawn_driver(
    core: Arc<Mutex<ConsensusModule>>,
    peers: PeerClients,
    peer_ids: Vec<NodeId>,
    timing: Timing,
    running: Arc<AtomicBool>,
    apply_tx: mpsc::UnboundedSender<Apply>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Arm the first heartbeat to fire immediately once we become leader.
        let mut last_heartbeat = Instant::now() - timing.heartbeat;

        while running.load(Ordering::SeqCst) {
            tokio::time::sleep(TICK).await;
            let now = Instant::now();

            let step = {
                let mut core = core.lock().unwrap();
                if core.is_leader() {
                    // Send to a peer when a heartbeat is due, or whenever we hold
                    // entries it is missing — the latter replicates a fresh command
                    // within one tick instead of waiting for the heartbeat clock.
                    let heartbeat_due = now.duration_since(last_heartbeat) >= timing.heartbeat;
                    let messages: Vec<(NodeId, Outgoing)> = peer_ids
                        .iter()
                        .filter_map(|&peer| {
                            // A peer whose next entry we've compacted can only be
                            // caught up by shipping the snapshot; do it on heartbeat
                            // ticks so stale ones don't spam the wire.
                            if core.needs_snapshot(peer) {
                                return heartbeat_due.then(|| {
                                    (peer, Outgoing::Snapshot(core.install_snapshot_args(peer)))
                                });
                            }
                            let args = core.append_args_for(peer);
                            (heartbeat_due || !args.entries.is_empty())
                                .then_some((peer, Outgoing::Append(args)))
                        })
                        .collect();
                    if messages.is_empty() {
                        Step::Idle
                    } else {
                        if heartbeat_due {
                            last_heartbeat = now;
                        }
                        Step::Replicate(messages)
                    }
                } else if core.pre_vote_succeeded() {
                    // A pre-vote majority pledged support: promote to a real
                    // election (bump the term, self-vote) and campaign for real.
                    Step::StartElection(core.start_election(now))
                } else if core.election_timed_out(now) {
                    // Probe with a pre-vote before disturbing anyone's term.
                    Step::StartPreElection(core.start_pre_election(now))
                } else {
                    Step::Idle
                }
            };

            match step {
                Step::StartPreElection(args) => {
                    // Fan out the pre-vote probe; grants are tallied independently
                    // and never change anyone's term.
                    for &peer in &peer_ids {
                        let core = core.clone();
                        let peers = peers.clone();
                        let args = args.clone();
                        tokio::spawn(async move {
                            if let Ok(reply) = peers.request_vote(peer, args).await {
                                core.lock().unwrap().record_pre_vote_reply(peer, reply);
                            }
                        });
                    }
                }
                Step::StartElection(args) => {
                    // Fan out RequestVote; each reply is folded back independently.
                    for &peer in &peer_ids {
                        let core = core.clone();
                        let peers = peers.clone();
                        let args = args.clone();
                        tokio::spawn(async move {
                            if let Ok(reply) = peers.request_vote(peer, args).await {
                                core.lock().unwrap().record_vote_reply(peer, reply);
                            }
                        });
                    }
                }
                Step::Replicate(messages) => {
                    for (peer, outgoing) in messages {
                        let core = core.clone();
                        let peers = peers.clone();
                        match outgoing {
                            Outgoing::Append(args) => {
                                // How far this message advances the peer's log if
                                // accepted; handed to `handle_append_reply` to set
                                // `match_index`.
                                let match_hint =
                                    args.prev_log_index + args.entries.len() as u64;
                                tokio::spawn(async move {
                                    if let Ok(reply) = peers.append_entries(peer, args).await {
                                        core.lock()
                                            .unwrap()
                                            .handle_append_reply(peer, reply, match_hint);
                                    }
                                });
                            }
                            Outgoing::Snapshot(args) => {
                                // Success advances the peer to the snapshot boundary.
                                let last = args.last_included_index;
                                tokio::spawn(async move {
                                    if let Ok(reply) = peers.install_snapshot(peer, args).await {
                                        core.lock()
                                            .unwrap()
                                            .handle_install_snapshot_reply(peer, reply, last);
                                    }
                                });
                            }
                        }
                    }
                }
                Step::Idle => {}
            }

            // Single drain site for the apply stream: whether commitment advanced
            // via our own replication replies (leader) or a leader's `leader_commit`
            // (follower), newly committed commands — and any installed snapshot —
            // are shipped here in log order.
            let applied = core.lock().unwrap().take_applies();
            for item in applied {
                // A dropped receiver just means nobody is consuming; entries are
                // already durable in the log, so discarding the notification is safe.
                let _ = apply_tx.send(item);
            }
        }
    })
}
