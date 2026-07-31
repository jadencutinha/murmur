//! Integration tests for Raft log replication over real gRPC: commands proposed
//! to the leader replicate to every node, commit on a majority, and apply in the
//! same order everywhere.

use std::time::{Duration, Instant};

use murmur::raft::{
    start_node, Applied, ApplyReceiver, ClusterConfig, InMemoryStorage, NodeHandle, Peer, Timing,
};

/// A running node paired with the applies it has emitted so far. The apply
/// receiver is drained into `applied` on demand so tests can inspect it.
struct Member {
    handle: NodeHandle,
    apply_rx: ApplyReceiver,
    applied: Vec<Applied>,
}

impl Member {
    /// Pull everything currently queued on the apply stream into `applied`.
    fn drain(&mut self) {
        while let Ok(entry) = self.apply_rx.try_recv() {
            self.applied.push(entry);
        }
    }
}

/// Bring up an `n`-node cluster on ephemeral loopback ports. Node ids are `1..=n`.
async fn spawn_cluster(n: u64) -> Vec<Member> {
    let mut listeners = Vec::new();
    let mut addrs = Vec::new();
    for id in 1..=n {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push((id, listener.local_addr().unwrap().to_string()));
        listeners.push((id, listener));
    }

    let mut members = Vec::new();
    for (id, listener) in listeners {
        let peers = addrs
            .iter()
            .filter(|(pid, _)| *pid != id)
            .map(|(pid, addr)| Peer { id: *pid, addr: addr.clone() })
            .collect();
        let config = ClusterConfig { id, peers };
        let (handle, apply_rx) = start_node(
            config,
            Box::new(InMemoryStorage::new()),
            Timing::default(),
            listener,
        )
        .await
        .unwrap();
        members.push(Member { handle, apply_rx, applied: Vec::new() });
    }
    members
}

/// Poll until exactly one node is leader, returning its index in `members`.
async fn await_leader(members: &[Member], timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let leaders: Vec<usize> = members
            .iter()
            .enumerate()
            .filter(|(_, m)| m.handle.is_leader())
            .map(|(i, _)| i)
            .collect();
        if let [i] = leaders[..] {
            return i;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no single leader within {timeout:?}");
}

/// Wait until every node's commit index reaches `target`, or panic.
async fn await_commit(members: &[Member], target: u64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if members.iter().all(|m| m.handle.commit_index() >= target) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let got: Vec<u64> = members.iter().map(|m| m.handle.commit_index()).collect();
    panic!("commit index never reached {target} on all nodes; got {got:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commands_replicate_commit_and_apply_everywhere() {
    let mut members = spawn_cluster(3).await;
    let leader = await_leader(&members, Duration::from_secs(5)).await;

    // Propose three commands to the leader.
    let commands: Vec<Vec<u8>> = vec![b"set a=1".to_vec(), b"set b=2".to_vec(), b"del a".to_vec()];
    for (i, cmd) in commands.iter().enumerate() {
        let index = members[leader].handle.propose(cmd.clone());
        assert_eq!(index, Some(i as u64 + 1), "leader should assign sequential indices");
    }

    // All three commit on every node...
    await_commit(&members, 3, Duration::from_secs(5)).await;

    // ...and apply in the same order, with identical bytes, on every node.
    // Give the apply stream a moment to flush past the commit.
    tokio::time::sleep(Duration::from_millis(100)).await;
    for m in &mut members {
        m.drain();
        let applied: Vec<&[u8]> = m.applied.iter().map(|a| a.command.as_slice()).collect();
        let expected: Vec<&[u8]> = commands.iter().map(|c| c.as_slice()).collect();
        assert_eq!(applied, expected, "node {} applied wrong sequence", m.handle.id);
        // Indices are dense and 1-based.
        let indices: Vec<u64> = m.applied.iter().map(|a| a.index).collect();
        assert_eq!(indices, vec![1, 2, 3]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_cannot_commit_commands() {
    let members = spawn_cluster(3).await;
    let leader = await_leader(&members, Duration::from_secs(5)).await;
    let follower = (0..members.len()).find(|&i| i != leader).unwrap();

    // Proposing to a follower is rejected outright — no index handed back.
    assert_eq!(members[follower].handle.propose(b"nope".to_vec()), None);
}
