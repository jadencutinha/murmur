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
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::proto::raft_server::RaftServer;

use super::config::{ClusterConfig, Timing};
use super::consensus::ConsensusModule;
use super::rpc::{AppendEntriesArgs, RequestVoteArgs};
use super::storage::RaftStorage;
use super::transport::{PeerClients, RaftService};
use super::types::{NodeId, Role, Term};

/// How often the driver wakes to check timers. Must be well under the heartbeat
/// interval so heartbeats and election checks fire promptly.
const TICK: Duration = Duration::from_millis(15);

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
pub async fn start_node(
    config: ClusterConfig,
    storage: Box<dyn RaftStorage>,
    timing: Timing,
    listener: TcpListener,
) -> anyhow::Result<NodeHandle> {
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

    // Serve the Raft RPCs into the shared core.
    let service = RaftService::new(core.clone());
    let server_task = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(RaftServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });

    let driver_task = spawn_driver(core.clone(), peers, peer_ids, timing, running.clone());

    Ok(NodeHandle {
        id: config.id,
        core,
        running,
        tasks: vec![server_task, driver_task],
    })
}

/// One tick's worth of work, decided under the lock and executed after release.
enum Step {
    StartElection(RequestVoteArgs),
    Heartbeat,
    Idle,
}

fn spawn_driver(
    core: Arc<Mutex<ConsensusModule>>,
    peers: PeerClients,
    peer_ids: Vec<NodeId>,
    timing: Timing,
    running: Arc<AtomicBool>,
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
                    if now.duration_since(last_heartbeat) >= timing.heartbeat {
                        Step::Heartbeat
                    } else {
                        Step::Idle
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
                Step::Heartbeat => {
                    last_heartbeat = now;
                    // Build each peer's message under the lock, then send unlocked.
                    let messages: Vec<(NodeId, AppendEntriesArgs)> = {
                        let core = core.lock().unwrap();
                        peer_ids.iter().map(|&p| (p, core.append_args_for(p))).collect()
                    };
                    for (peer, args) in messages {
                        let core = core.clone();
                        let peers = peers.clone();
                        // How far this message would advance the peer's log if it
                        // succeeds — used for match_index at the replication checkpoint.
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
        }
    })
}
