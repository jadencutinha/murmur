//! Integration test for log compaction and InstallSnapshot over real gRPC.
//!
//! A follower is crashed, then the live majority commits far past it and compacts
//! its log — discarding the very entries the crashed node still needs. When the
//! node restarts, AppendEntries can no longer catch it up (those entries are
//! gone), so the leader must ship its snapshot instead. The node installs it,
//! jumps to the snapshot boundary, and rejoins.
//!
//! Like `persistence.rs`, this drives raw Raft (no KV app layer) and pins no
//! leader: it snapshots whichever nodes are live and proposes to whoever leads, so
//! the assertions ride on commit/snapshot progress rather than a fixed leader.

use std::time::{Duration, Instant};

use murmur::raft::{
    start_node, Apply, ApplyReceiver, ClusterConfig, FileStorage, NodeHandle, Peer, Timing,
};
use tempfile::TempDir;

/// A cluster member we can crash, compact, and restart. Owns its data dir and its
/// fixed loopback address so a restart can rebind it.
struct Node {
    id: u64,
    addr: String,
    dir: TempDir,
    peers: Vec<Peer>,
    handle: NodeHandle,
    apply_rx: ApplyReceiver,
}

impl Node {
    fn state_path(&self) -> std::path::PathBuf {
        self.dir.path().join("raft-state")
    }

    /// Pull everything queued on the apply stream so the channel doesn't grow
    /// unbounded and so we can confirm a snapshot was delivered on install.
    fn drain(&mut self) -> Vec<Apply> {
        let mut out = Vec::new();
        while let Ok(item) = self.apply_rx.try_recv() {
            out.push(item);
        }
        out
    }
}

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
        nodes.push(Node { id, addr, dir, peers, handle, apply_rx });
    }
    nodes
}

/// Keep proposing fresh commands to whichever live node is leader until the
/// minimum commit index across `live` reaches `target`. Tolerates leadership churn.
async fn drive_live_to_commit(nodes: &mut [Node], live: &[usize], target: u64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let mut seq = 0u64;
    while Instant::now() < deadline {
        let min = live.iter().map(|&i| nodes[i].handle.commit_index()).min().unwrap_or(0);
        if min >= target {
            for &i in live {
                nodes[i].drain();
            }
            return;
        }
        if let Some(&i) = live.iter().find(|&&i| nodes[i].handle.is_leader()) {
            nodes[i].handle.propose(format!("cmd-{seq}").into_bytes());
            seq += 1;
        }
        for &i in live {
            nodes[i].drain();
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    let got: Vec<u64> = live.iter().map(|&i| nodes[i].handle.commit_index()).collect();
    panic!("live nodes never reached commit {target}; got {got:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lagging_node_is_caught_up_by_an_installed_snapshot() {
    let mut nodes = spawn_cluster(3).await;

    // Crash node index 2 immediately, before it takes on any real log. Nodes 0 and
    // 1 are still a majority and can make progress without it.
    nodes[2].handle.kill();
    let laggard_last_log = nodes[2].handle.last_log_index();
    let live = [0usize, 1usize];

    // The live majority commits well past the crashed node.
    drive_live_to_commit(&mut nodes, &live, 80, Duration::from_secs(20)).await;

    // Compact both live nodes' logs against a snapshot, discarding the prefix the
    // crashed node still needs. (The KV app layer does this automatically past a
    // threshold; here we drive raw Raft, so we trigger it explicitly.)
    for &i in &live {
        let at = nodes[i].handle.last_applied();
        assert!(at > laggard_last_log, "must compact past the laggard's log");
        nodes[i].handle.control().snapshot(at, b"state-machine-image".to_vec());
        assert!(nodes[i].handle.raft_log_len() < 80); // tail is now short
    }
    // A boundary every live node has snapshotted at or beyond.
    let compacted_to = live.iter().map(|&i| snap_index(&nodes[i])).min().unwrap();
    assert!(compacted_to > laggard_last_log);

    // Restart the crashed node on its original address and data dir. Its short log
    // is now unreachable by AppendEntries, so the leader must InstallSnapshot it.
    let listener = rebind(&nodes[2].addr).await;
    let config = ClusterConfig { id: nodes[2].id, peers: nodes[2].peers.clone() };
    let (handle, apply_rx) = start_node(
        config,
        Box::new(FileStorage::new(nodes[2].state_path())),
        Timing::default(),
        listener,
    )
    .await
    .unwrap();
    nodes[2].handle = handle;
    nodes[2].apply_rx = apply_rx;

    // Keep the cluster live and wait for the restarted node to catch up. It catches
    // up only by installing a snapshot: the entries it lacks are gone from the log.
    let deadline = Instant::now() + Duration::from_secs(25);
    let mut installed_snapshot = false;
    loop {
        // Drive progress on whoever leads (all three may participate now).
        if let Some(i) = (0..3).find(|&i| nodes[i].handle.is_leader()) {
            nodes[i].handle.propose(b"keepalive".to_vec());
        }
        // A snapshot delivered on the apply stream is the smoking gun.
        for item in nodes[2].drain() {
            if matches!(item, Apply::Snapshot(_)) {
                installed_snapshot = true;
            }
        }
        for i in [0, 1] {
            nodes[i].drain();
        }

        if installed_snapshot && snap_index(&nodes[2]) >= compacted_to {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "laggard never installed the snapshot: snapshot_index={}, target={compacted_to}",
            snap_index(&nodes[2]),
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    // The reborn node jumped its log origin to the snapshot boundary and committed
    // at least that far — it is back in sync with the cluster.
    assert!(installed_snapshot, "expected a snapshot to be delivered to the laggard");
    assert!(snap_index(&nodes[2]) >= compacted_to);
    assert!(nodes[2].handle.last_log_index() >= compacted_to);
}

/// The index a node's log has been compacted through.
fn snap_index(node: &Node) -> u64 {
    // `last_log_index - raft_log_len` is exactly the snapshot boundary.
    node.handle.last_log_index() - node.handle.raft_log_len() as u64
}
