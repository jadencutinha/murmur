//! Integration tests for Raft leader election over real gRPC: a cluster of
//! nodes on loopback ports elects exactly one leader, and re-elects when the
//! leader crashes.

use std::time::{Duration, Instant};

use murmur::raft::{start_node, ClusterConfig, InMemoryStorage, NodeHandle, Peer, Timing};

/// Bring up an `n`-node cluster on ephemeral loopback ports. Node ids are `1..=n`.
async fn spawn_cluster(n: u64) -> Vec<NodeHandle> {
    // Bind every listener first so each node's config can name all its peers.
    let mut listeners = Vec::new();
    let mut addrs = Vec::new();
    for id in 1..=n {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push((id, listener.local_addr().unwrap().to_string()));
        listeners.push((id, listener));
    }

    let mut handles = Vec::new();
    for (id, listener) in listeners {
        let peers = addrs
            .iter()
            .filter(|(pid, _)| *pid != id)
            .map(|(pid, addr)| Peer { id: *pid, addr: addr.clone() })
            .collect();
        let config = ClusterConfig { id, peers };
        let handle = start_node(
            config,
            Box::new(InMemoryStorage::new()),
            Timing::default(),
            listener,
        )
        .await
        .unwrap();
        handles.push(handle);
    }
    handles
}

fn leaders(handles: &[NodeHandle]) -> Vec<&NodeHandle> {
    handles.iter().filter(|h| h.is_leader()).collect()
}

/// Poll until exactly one node reports itself leader, or panic on timeout.
/// Returns that leader's id and term.
async fn await_stable_leader(handles: &[NodeHandle], timeout: Duration) -> (u64, u64) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let current = leaders(handles);
        if current.len() == 1 {
            let leader = current[0];
            let term = leader.current_term();
            // Two nodes must never be leader in the same term (election safety).
            let same_term_leaders = handles
                .iter()
                .filter(|h| h.is_leader() && h.current_term() == term)
                .count();
            assert_eq!(same_term_leaders, 1, "more than one leader in term {term}");
            return (leader.id, term);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no single leader elected within {timeout:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_elects_a_single_leader() {
    let handles = spawn_cluster(3).await;
    let (leader_id, term) = await_stable_leader(&handles, Duration::from_secs(5)).await;
    assert!((1..=3).contains(&leader_id));
    assert!(term >= 1);

    // The two followers should agree on who the leader is.
    let followers_agreeing = handles
        .iter()
        .filter(|h| h.id != leader_id && h.leader_id() == Some(leader_id))
        .count();
    assert_eq!(followers_agreeing, 2, "followers did not converge on the leader");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_re_elects_after_leader_crash() {
    let mut handles = spawn_cluster(3).await;
    let (first_leader, first_term) = await_stable_leader(&handles, Duration::from_secs(5)).await;

    // Crash the leader.
    let idx = handles.iter().position(|h| h.id == first_leader).unwrap();
    handles[idx].kill();

    // A surviving node must win a later term.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline, "no re-election after leader crash");
        let survivors: Vec<&NodeHandle> =
            handles.iter().filter(|h| h.id != first_leader && h.is_leader()).collect();
        if let [leader] = survivors[..] {
            assert!(leader.current_term() > first_term, "new leader must be in a later term");
            assert_ne!(leader.id, first_leader);
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
