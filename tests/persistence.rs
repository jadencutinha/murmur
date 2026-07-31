//! Integration test for crash recovery over real gRPC: the entire cluster is
//! killed and restarted on the same addresses and data directories, then must
//! reload its logs from disk, re-elect, and resume committing.
//!
//! Restarting every node together (rather than one into a live cluster) is
//! deliberate: a single node returning with a stale, shorter log can't win an
//! election yet keeps deposing the leader with ever-higher terms — the
//! disruptive-restart liveness gap that PreVote closes, which belongs to the
//! chaos checkpoint. Repairing an individual lagging log is covered
//! deterministically by the Figure-7 convergence test.
//!
//! The test is churn-tolerant: nothing pins a leader (`FileStorage` fsyncs perturb
//! timing enough to move leadership around), so it always proposes to whoever is
//! currently leader and asserts on commit-index progress.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use murmur::raft::{
    start_node, ApplyReceiver, ClusterConfig, FileStorage, NodeHandle, Peer, Timing,
};
use tempfile::TempDir;

/// A cluster member we can crash and restart. Owns its data dir (kept alive for
/// the whole test) and its fixed loopback address so a restart can rebind it.
struct Node {
    id: u64,
    addr: String,
    dir: TempDir,
    peers: Vec<Peer>,
    handle: NodeHandle,
    _apply_rx: ApplyReceiver,
}

impl Node {
    fn state_path(&self) -> std::path::PathBuf {
        self.dir.path().join("raft-state")
    }
}

/// Monotonic command generator, so every proposal is distinct (values are opaque
/// to Raft; we only care that the log grows and commits advance).
static SEQ: AtomicU64 = AtomicU64::new(0);
fn next_command() -> Vec<u8> {
    format!("k{}", SEQ.fetch_add(1, Ordering::Relaxed)).into_bytes()
}

/// Bind `addr`, retrying briefly since a just-killed node may not have released
/// the port yet.
async fn rebind(addr: &str) -> tokio::net::TcpListener {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => return l,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => panic!("could not rebind {addr}: {e}"),
        }
    }
}

async fn spawn_cluster(n: u64) -> Vec<Node> {
    let mut listeners = Vec::new();
    let mut addrs = Vec::new();
    for id in 1..=n {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push((id, listener.local_addr().unwrap().to_string()));
        listeners.push((id, listener));
    }

    let mut nodes = Vec::new();
    for (id, listener) in listeners {
        let peers: Vec<Peer> = addrs
            .iter()
            .filter(|(pid, _)| *pid != id)
            .map(|(pid, addr)| Peer { id: *pid, addr: addr.clone() })
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let config = ClusterConfig { id, peers: peers.clone() };
        let (handle, apply_rx) = start_node(
            config,
            Box::new(FileStorage::new(dir.path().join("raft-state"))),
            Timing::default(),
            listener,
        )
        .await
        .unwrap();
        let addr = addrs.iter().find(|(pid, _)| *pid == id).unwrap().1.clone();
        nodes.push(Node { id, addr, dir, peers, handle, _apply_rx: apply_rx });
    }
    nodes
}

fn min_commit(nodes: &[Node]) -> u64 {
    nodes.iter().map(|n| n.handle.commit_index()).min().unwrap_or(0)
}

/// Keep proposing fresh commands to whichever node is leader until the *minimum*
/// commit index across the cluster reaches `target` — i.e. every node has
/// committed at least that far. Tolerates leadership churn and dropped proposals.
async fn drive_all_to_commit(nodes: &[Node], target: u64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if min_commit(nodes) >= target {
            return;
        }
        if let Some(leader) = nodes.iter().find(|n| n.handle.is_leader()) {
            leader.handle.propose(next_command());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let got: Vec<u64> = nodes.iter().map(|n| n.handle.commit_index()).collect();
    panic!("cluster never reached min commit {target}; got {got:?}");
}

/// Wait until every node has replicated (and so persisted) at least `target`
/// entries.
async fn await_all_logs(nodes: &[Node], target: u64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if nodes.iter().all(|n| n.handle.last_log_index() >= target) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let got: Vec<u64> = nodes.iter().map(|n| n.handle.last_log_index()).collect();
    panic!("not all nodes replicated {target} entries; got {got:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_whole_cluster_recovers_its_log_after_restart() {
    let mut nodes = spawn_cluster(3).await;

    // Commit a batch and make sure every node has persisted it to disk.
    drive_all_to_commit(&nodes, 3, Duration::from_secs(10)).await;
    await_all_logs(&nodes, 3, Duration::from_secs(10)).await;
    let persisted = min_commit(&nodes);
    assert!(persisted >= 3);

    // Crash the entire cluster.
    for node in &mut nodes {
        node.handle.kill();
    }

    // Restart every node on its original address and data directory. Each reloads
    // its term, vote, and log from disk before serving.
    for i in 0..nodes.len() {
        let listener = rebind(&nodes[i].addr).await;
        let config = ClusterConfig { id: nodes[i].id, peers: nodes[i].peers.clone() };
        let (handle, apply_rx) = start_node(
            config,
            Box::new(FileStorage::new(nodes[i].state_path())),
            Timing::default(),
            listener,
        )
        .await
        .unwrap();
        nodes[i].handle = handle;
        nodes[i]._apply_rx = apply_rx;
    }

    // Persistence: every node's log came back from disk, even though volatile
    // commit/apply state reset to zero.
    for node in &nodes {
        assert!(
            node.handle.last_log_index() >= persisted,
            "node {} lost its log across the restart",
            node.id
        );
    }

    // Recovery: the restarted cluster re-elects and, with writes flowing again,
    // commits fresh entries on top of the reloaded log on *every* node.
    drive_all_to_commit(&nodes, persisted + 2, Duration::from_secs(15)).await;
}
