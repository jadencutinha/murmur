//! End-to-end test of the replicated KV service: a client drives Put/Get/Delete
//! over gRPC through Raft, the operations commit on the leader and apply on every
//! node's Sable engine, and a follower correctly refuses and redirects writes.

use std::time::{Duration, Instant};

use murmur::app::{self, KvService};
use murmur::proto::kv_client::KvClient;
use murmur::proto::kv_server::KvServer;
use murmur::proto::{DeleteRequest, GetRequest, PutRequest};
use murmur::raft::{ClusterConfig, InMemoryStorage, Peer, Timing};
use murmur::store::KvStore;
use tempfile::TempDir;
use tonic::transport::{Channel, Server};

use tokio_stream::wrappers::TcpListenerStream;

/// One node under test: its client, a store handle sharing the node's engine (for
/// verifying replication directly), and its service handle (for leadership).
struct KvNode {
    id: u64,
    client: KvClient<Channel>,
    store: KvStore,
    service: KvService,
    _dir: TempDir,
}

async fn spawn_cluster(n: u64) -> Vec<KvNode> {
    // Bind Raft and KV listeners up front so every node's config can name its peers.
    let mut raft_listeners = Vec::new();
    let mut raft_addrs = Vec::new();
    let mut kv_listeners = Vec::new();
    let mut kv_addrs = Vec::new();
    for id in 1..=n {
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        raft_addrs.push((id, rl.local_addr().unwrap().to_string()));
        raft_listeners.push((id, rl));
        let kl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        kv_addrs.push((id, kl.local_addr().unwrap().to_string()));
        kv_listeners.push(kl);
    }

    let mut nodes = Vec::new();
    for ((id, raft_listener), kv_listener) in raft_listeners.into_iter().zip(kv_listeners) {
        let peers: Vec<Peer> = raft_addrs
            .iter()
            .filter(|(pid, _)| *pid != id)
            .map(|(pid, addr)| Peer { id: *pid, addr: addr.clone() })
            .collect();

        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        let store_handle = store.clone(); // shares the same engine for verification

        let config = ClusterConfig { id, peers };
        let service = app::start(
            config,
            Box::new(InMemoryStorage::new()),
            Timing::default(),
            raft_listener,
            store,
        )
        .await
        .unwrap();

        // Serve the KV API for this node.
        let serve = service.clone();
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(KvServer::new(serve))
                .serve_with_incoming(TcpListenerStream::new(kv_listener))
                .await;
        });

        let kv_addr = kv_addrs.iter().find(|(pid, _)| *pid == id).unwrap().1.clone();
        let client = connect(&kv_addr).await;
        nodes.push(KvNode { id, client, store: store_handle, service, _dir: dir });
    }
    nodes
}

async fn connect(addr: &str) -> KvClient<Channel> {
    for _ in 0..100 {
        if let Ok(client) = KvClient::connect(format!("http://{addr}")).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("KV server never became reachable at {addr}");
}

/// Poll until exactly one node is leader; return its index.
async fn await_leader(nodes: &[KvNode], timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let leaders: Vec<usize> = nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.service.is_leader())
            .map(|(i, _)| i)
            .collect();
        if let [i] = leaders[..] {
            return i;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no single leader within {timeout:?}");
}

/// Wait until every node's local engine reports `key` = `expected` (None = absent).
async fn await_replicated(nodes: &[KvNode], key: &[u8], expected: Option<&[u8]>, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let all = nodes
            .iter()
            .all(|n| n.store.get(key).unwrap().as_deref() == expected);
        if all {
            return;
        }
        assert!(Instant::now() < deadline, "engines did not converge on {key:?} = {expected:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_replicate_and_reads_are_served_through_raft() {
    let mut nodes = spawn_cluster(3).await;
    let leader = await_leader(&nodes, Duration::from_secs(5)).await;

    // Put through the leader.
    nodes[leader]
        .client
        .put(PutRequest { key: b"color".to_vec(), value: b"amber".to_vec() })
        .await
        .expect("leader accepts the write");

    // A linearizable read through the leader sees it.
    let resp = nodes[leader]
        .client
        .get(GetRequest { key: b"color".to_vec() })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.found);
    assert_eq!(resp.value, b"amber");

    // The command applied on *every* node's engine, not just the leader's.
    await_replicated(&nodes, b"color", Some(b"amber"), Duration::from_secs(5)).await;

    // Delete through the leader; it too replicates everywhere.
    nodes[leader]
        .client
        .delete(DeleteRequest { key: b"color".to_vec() })
        .await
        .expect("leader accepts the delete");
    await_replicated(&nodes, b"color", None, Duration::from_secs(5)).await;

    let resp = nodes[leader]
        .client
        .get(GetRequest { key: b"color".to_vec() })
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.found);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_refuses_and_redirects_writes() {
    let mut nodes = spawn_cluster(3).await;
    let leader = await_leader(&nodes, Duration::from_secs(5)).await;
    let follower = (0..nodes.len()).find(|&i| i != leader).unwrap();
    let leader_id = nodes[leader].id;

    // Wait until the follower has heard from the leader, so it can name it in the
    // redirect hint (it learns the leader only on receiving a heartbeat).
    let deadline = Instant::now() + Duration::from_secs(5);
    while nodes[follower].service.leader_id() != Some(leader_id) {
        assert!(Instant::now() < deadline, "follower never learned the leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // A write to a follower is refused with a not-leader error...
    let err = nodes[follower]
        .client
        .put(PutRequest { key: b"k".to_vec(), value: b"v".to_vec() })
        .await
        .expect_err("follower must refuse writes");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    // ...and points the client at the current leader so it can redirect.
    let hint = err
        .metadata()
        .get("x-leader-id")
        .expect("redirect hint present")
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(hint, leader_id);
}
