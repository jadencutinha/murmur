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
use super::rpc::{AppendEntriesArgs, RequestVoteArgs};
use super::storage::RaftStorage;
use super::transport::{PeerClients, RaftService};
use super::types::{Applied, LogIndex, NodeId, Role, Term};

/// How often the driver wakes to check timers. Must be well under the heartbeat
/// interval so heartbeats and election checks fire promptly.
const TICK: Duration = Duration::from_millis(15);

/// The receiving end of a node's apply stream: committed entries arrive here in
/// strict log order for the state machine to apply. The caller owns it (a raw KV
/// node drains it into Sable; tests drain it to observe replication).
pub type ApplyReceiver = mpsc::UnboundedReceiver<Applied>;

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

/// One tick's worth of work, decided under the lock and executed after release.
enum Step {
    StartElection(RequestVoteArgs),
    /// AppendEntries to send this tick: a heartbeat, freshly appended entries, or
    /// a repair attempt for a lagging peer — the message shape covers all three.
    Replicate(Vec<(NodeId, AppendEntriesArgs)>),
    Idle,
}

fn spawn_driver(
    core: Arc<Mutex<ConsensusModule>>,
    peers: PeerClients,
    peer_ids: Vec<NodeId>,
    timing: Timing,
    running: Arc<AtomicBool>,
    apply_tx: mpsc::UnboundedSender<Applied>,
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
                    let messages: Vec<(NodeId, AppendEntriesArgs)> = peer_ids
                        .iter()
                        .filter_map(|&peer| {
                            let args = core.append_args_for(peer);
                            (heartbeat_due || !args.entries.is_empty()).then_some((peer, args))
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
                } else if core.election_timed_out(now) {
                    Step::StartElection(core.start_election(now))
                } else {
                    Step::Idle
                }
            };

            match step {
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
                    for (peer, args) in messages {
                        let core = core.clone();
                        let peers = peers.clone();
                        // How far this message advances the peer's log if accepted;
                        // handed back to `handle_append_reply` to set `match_index`.
                        let match_hint = args.prev_log_index + args.entries.len() as u64;
                        tokio::spawn(async move {
                            if let Ok(reply) = peers.append_entries(peer, args).await {
                                core.lock().unwrap().handle_append_reply(peer, reply, match_hint);
                            }
                        });
                    }
                }
                Step::Idle => {}
            }

            // Single drain site for the apply cursor: whether commitment advanced
            // via our own replication replies (leader) or a leader's `leader_commit`
            // (follower), newly committed entries are shipped here in log order.
            let applied = core.lock().unwrap().take_applies();
            for entry in applied {
                // A dropped receiver just means nobody is consuming; entries are
                // already durable in the log, so discarding the notification is safe.
                let _ = apply_tx.send(entry);
            }
        }
    })
}
