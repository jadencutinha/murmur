//! Exactly-once semantics: a mutation carrying the same `(client_id, seq)` as a
//! previous one is recognized as a duplicate and applied only once, and the
//! high-level `Clerk` drives ordinary traffic correctly.

use std::time::{Duration, Instant};

use murmur::app::{self, KvService};
use murmur::clerk::Clerk;
use murmur::proto::kv_client::KvClient;
use murmur::proto::kv_server::KvServer;
use murmur::proto::AppendRequest;
use murmur::raft::{ClusterConfig, InMemoryStorage, Peer, Timing};
use murmur::store::KvStore;
use tempfile::TempDir;
use tonic::transport::{Channel, Server};

use tokio_stream::wrappers::TcpListenerStream;

struct Node {
    client: KvClient<Channel>,
    store: KvStore,
    service: KvService,
    kv_addr: String,
    _dir: TempDir,
}

async fn spawn_cluster(n: u64) -> Vec<Node> {
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
        let store_handle = store.clone();

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

        let serve = service.clone();
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(KvServer::new(serve))
                .serve_with_incoming(TcpListenerStream::new(kv_listener))
                .await;
        });

        let kv_addr = kv_addrs.iter().find(|(pid, _)| *pid == id).unwrap().1.clone();
        let client = connect(&kv_addr).await;
        nodes.push(Node { client, store: store_handle, service, kv_addr, _dir: dir });
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
    panic!("KV server never reachable at {addr}");
}

async fn await_leader(nodes: &[Node], timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let leaders: Vec<usize> =
            (0..nodes.len()).filter(|&i| nodes[i].service.is_leader()).collect();
        if let [i] = leaders[..] {
            return i;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no single leader within {timeout:?}");
}

async fn await_replicated(nodes: &[Node], key: &[u8], expected: &[u8], timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if nodes.iter().all(|n| n.store.get(key).unwrap().as_deref() == Some(expected)) {
            return;
        }
        assert!(Instant::now() < deadline, "engines did not converge on {key:?}={expected:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicate_append_applies_once() {
    let mut nodes = spawn_cluster(3).await;
    let leader = await_leader(&nodes, Duration::from_secs(5)).await;

    // Simulate a client whose first append actually succeeded but whose ack was
    // lost, so it resends the *identical* request (same client_id + seq).
    let dup = AppendRequest {
        key: b"log".to_vec(),
        value: b"X".to_vec(),
        client_id: 777,
        seq: 1,
    };

    let first = nodes[leader].client.append(dup.clone()).await.unwrap().into_inner();
    let second = nodes[leader].client.append(dup.clone()).await.unwrap().into_inner();

    // Both calls report the same post-append value...
    assert_eq!(first.value, b"X");
    assert_eq!(second.value, b"X");

    // ...and the engine holds a single "X" on every node, not "XX".
    await_replicated(&nodes, b"log", b"X", Duration::from_secs(5)).await;

    // A genuinely new seq does append again.
    let next = nodes[leader]
        .client
        .append(AppendRequest { key: b"log".to_vec(), value: b"Y".to_vec(), client_id: 777, seq: 2 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(next.value, b"XY");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clerk_drives_ordinary_traffic() {
    let nodes = spawn_cluster(3).await;
    await_leader(&nodes, Duration::from_secs(5)).await;

    let endpoints: Vec<String> = nodes.iter().map(|n| n.kv_addr.clone()).collect();
    let mut clerk = Clerk::connect(endpoints).await.unwrap();
    assert_ne!(clerk.client_id(), 0, "clerk registered for an id");

    clerk.put(b"greeting".to_vec(), b"he".to_vec()).await.unwrap();
    let after = clerk.append(b"greeting".to_vec(), b"llo".to_vec()).await.unwrap();
    assert_eq!(after, b"hello");

    let read = clerk.get(b"greeting".to_vec()).await.unwrap();
    assert_eq!(read.as_deref(), Some(&b"hello"[..]));

    clerk.delete(b"greeting".to_vec()).await.unwrap();
    assert_eq!(clerk.get(b"greeting".to_vec()).await.unwrap(), None);
}
