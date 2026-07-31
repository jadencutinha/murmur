//! End-to-end test of the single-node KV gRPC service: a real tonic client
//! driving a real server over a real (loopback) socket.

use std::time::Duration;

use murmur::proto::kv_client::KvClient;
use murmur::proto::{DeleteRequest, GetRequest, PutRequest};
use murmur::store::KvStore;

/// Spawn a KV server on an ephemeral port and return a connected client.
async fn spawn_node() -> KvClient<tonic::transport::Channel> {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = KvStore::open(dir.path()).expect("open store");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    // Keep the tempdir alive for the lifetime of the server task.
    tokio::spawn(async move {
        let _dir = dir;
        murmur::server::serve(store, listener).await.expect("serve");
    });

    // The server task may not have started accepting yet; retry the dial.
    for _ in 0..50 {
        if let Ok(client) = KvClient::connect(format!("http://{addr}")).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server never became reachable at {addr}");
}

#[tokio::test]
async fn put_get_delete_over_grpc() {
    let mut client = spawn_node().await;

    // Missing key -> not found.
    let resp = client
        .get(GetRequest { key: b"color".to_vec() })
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.found);

    // Put then read it back.
    client
        .put(PutRequest {
            key: b"color".to_vec(),
            value: b"amber".to_vec(),
        })
        .await
        .unwrap();
    let resp = client
        .get(GetRequest { key: b"color".to_vec() })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.found);
    assert_eq!(resp.value, b"amber");

    // Delete then confirm gone.
    client
        .delete(DeleteRequest { key: b"color".to_vec() })
        .await
        .unwrap();
    let resp = client
        .get(GetRequest { key: b"color".to_vec() })
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.found);
}

#[tokio::test]
async fn empty_value_is_found() {
    let mut client = spawn_node().await;
    client
        .put(PutRequest {
            key: b"k".to_vec(),
            value: Vec::new(),
        })
        .await
        .unwrap();
    let resp = client
        .get(GetRequest { key: b"k".to_vec() })
        .await
        .unwrap()
        .into_inner();
    // A present key with an empty value must report found = true.
    assert!(resp.found);
    assert!(resp.value.is_empty());
}
