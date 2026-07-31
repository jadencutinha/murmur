//! Chaos test: safety and liveness under repeated leader failure.
//!
//! The two properties Raft must never violate, exercised by killing leaders out
//! from under a running cluster:
//!
//! - **Safety** — every node applies the same commands in the same order, and a
//!   committed entry is never lost. We check the survivors' apply streams agree on
//!   their common prefix after each failure.
//! - **Liveness** — after each leader dies the cluster elects a new one and keeps
//!   committing. PreVote (checkpoint 11) keeps that failover clean: a node that
//!   can't win never inflates its term, so it can't stall the survivors.

use std::time::{Duration, Instant};

use murmur::raft::{
    start_node, Apply, ApplyReceiver, ClusterConfig, InMemoryStorage, NodeHandle, Peer, Timing,
};

/// A running node that accumulates the commands it has applied, so tests can
/// compare apply order across the cluster.
struct Node {
    id: u64,
    alive: bool,
    handle: NodeHandle,
    apply_rx: ApplyReceiver,
    applied: Vec<Vec<u8>>,
}

impl Node {
    fn drain(&mut self) {
        while let Ok(item) = self.apply_rx.try_recv() {
            if let Apply::Command(entry) = item {
                self.applied.push(entry.command);
            }
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
        let peers = addrs
            .iter()
            .filter(|(pid, _)| *pid != id)
            .map(|(pid, addr)| Peer { id: *pid, addr: addr.clone() })
            .collect();
        let config = ClusterConfig { id, peers };
        let (handle, apply_rx) =
            start_node(config, Box::new(InMemoryStorage::new()), Timing::default(), listener)
                .await
                .unwrap();
        nodes.push(Node { id, alive: true, handle, apply_rx, applied: Vec::new() });
    }
    nodes
}

fn live(nodes: &[Node]) -> Vec<usize> {
    (0..nodes.len()).filter(|&i| nodes[i].alive).collect()
}

/// Poll until exactly one *live* node is leader, returning its index.
async fn await_leader(nodes: &mut [Node], timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        for n in nodes.iter_mut() {
            n.drain();
        }
        let leaders: Vec<usize> =
            live(nodes).into_iter().filter(|&i| nodes[i].handle.is_leader()).collect();
        if let [i] = leaders[..] {
            return i;
        }
        assert!(Instant::now() < deadline, "no single live leader within {timeout:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Propose fresh commands to whichever live node leads until every live node's
/// commit index advances by at least `count` beyond where it started.
async fn commit_more(nodes: &mut [Node], count: u64, timeout: Duration) {
    let base: u64 = live(nodes).into_iter().map(|i| nodes[i].handle.commit_index()).min().unwrap();
    let target = base + count;
    let deadline = Instant::now() + timeout;
    let mut seq = 0u64;
    loop {
        for n in nodes.iter_mut() {
            n.drain();
        }
        let min = live(nodes).into_iter().map(|i| nodes[i].handle.commit_index()).min().unwrap();
        if min >= target {
            return;
        }
        if let Some(i) = live(nodes).into_iter().find(|&i| nodes[i].handle.is_leader()) {
            nodes[i].handle.propose(format!("v{}-{seq}", nodes[i].id).into_bytes());
            seq += 1;
        }
        assert!(Instant::now() < deadline, "commit did not advance by {count} within {timeout:?}");
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

/// Safety check: for every pair of live nodes, one apply log is a prefix of the
/// other — i.e. they never disagree on a command at the same position.
fn assert_apply_logs_consistent(nodes: &[Node]) {
    let logs: Vec<&Vec<Vec<u8>>> = live(nodes).iter().map(|&i| &nodes[i].applied).collect();
    for a in &logs {
        for b in &logs {
            let common = a.len().min(b.len());
            assert_eq!(a[..common], b[..common], "apply logs diverged");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_data_survives_repeated_leader_kills() {
    let mut nodes = spawn_cluster(5).await;

    // Establish a leader and commit an initial batch across the whole cluster.
    await_leader(&mut nodes, Duration::from_secs(5)).await;
    commit_more(&mut nodes, 5, Duration::from_secs(10)).await;

    // Kill the leader twice (5 → 4 → 3, always keeping the quorum of 3). Each time,
    // the cluster must re-elect and keep committing without losing applied history.
    for _ in 0..2 {
        let leader = await_leader(&mut nodes, Duration::from_secs(10)).await;
        // Snapshot the applied history the survivors already agree on.
        for n in nodes.iter_mut() {
            n.drain();
        }
        assert_apply_logs_consistent(&nodes);

        // Kill the leader.
        nodes[leader].handle.kill();
        nodes[leader].alive = false;

        // A new leader emerges among the survivors and fresh entries commit — the
        // committed prefix from before the failure is never rolled back.
        await_leader(&mut nodes, Duration::from_secs(10)).await;
        commit_more(&mut nodes, 3, Duration::from_secs(10)).await;
        assert_apply_logs_consistent(&nodes);
    }

    // Final state: three survivors, all still committing and mutually consistent.
    assert_eq!(live(&nodes).len(), 3);
    for n in nodes.iter_mut() {
        n.drain();
    }
    assert_apply_logs_consistent(&nodes);
}
