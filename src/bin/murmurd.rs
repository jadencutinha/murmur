//! `murmurd` — the murmur node daemon.
//!
//! A node runs two gRPC servers: the internal Raft service (node-to-node, on
//! `--raft-listen`) and the client-facing KV service (on `--kv-listen`). Every KV
//! write and read is routed through Raft before touching the local Sable engine,
//! so a cluster of murmurd processes keeps one replicated key/value store.
//!
//! Usage:
//!   murmurd --id 1 --raft-listen 127.0.0.1:6001 --kv-listen 127.0.0.1:5001 \
//!           --data data/node1 --peer 2@127.0.0.1:6002 --peer 3@127.0.0.1:6003
//!
//! With no `--peer` flags it runs as a single-node cluster (elects itself and
//! serves immediately) — handy for a quick local smoke test.

use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use murmur::app;
use murmur::proto::kv_server::KvServer;
use murmur::raft::{ClusterConfig, FileStorage, Peer, Timing};
use murmur::store::KvStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut id: u64 = 1;
    let mut raft_listen = "127.0.0.1:6001".to_string();
    let mut kv_listen = "127.0.0.1:5001".to_string();
    let mut data = "data/node1".to_string();
    let mut peers: Vec<Peer> = Vec::new();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--id" => id = args.next().and_then(|v| v.parse().ok()).unwrap_or(id),
            "--raft-listen" => raft_listen = args.next().unwrap_or(raft_listen),
            "--kv-listen" => kv_listen = args.next().unwrap_or(kv_listen),
            "--data" => data = args.next().unwrap_or(data),
            "--peer" => {
                let spec = args.next().unwrap_or_default();
                peers.push(parse_peer(&spec)?);
            }
            "-h" | "--help" => {
                eprintln!(
                    "usage: murmurd --id <n> --raft-listen <addr> --kv-listen <addr> \\\n\
                     \x20              --data <dir> [--peer <id@addr> ...]"
                );
                return Ok(());
            }
            other => anyhow::bail!("unknown argument: {other}"),
        }
    }

    let raft_addr: SocketAddr = raft_listen.parse()?;
    let kv_addr: SocketAddr = kv_listen.parse()?;

    // Split the data directory: Raft's durable state vs. the KV engine's data.
    let storage = Box::new(FileStorage::new(format!("{data}/raft-state")));
    let store = KvStore::open(format!("{data}/kv"))?;

    let peer_count = peers.len();
    let raft_listener = TcpListener::bind(raft_addr).await?;
    let config = ClusterConfig { id, peers };
    let service = app::start(config, storage, Timing::default(), raft_listener, store).await?;

    println!(
        "murmurd[{id}]: raft on {raft_addr}, KV on {kv_addr}, data dir `{data}`, {peer_count} peer(s)"
    );

    let kv_listener = TcpListener::bind(kv_addr).await?;
    Server::builder()
        .add_service(KvServer::new(service))
        .serve_with_incoming(TcpListenerStream::new(kv_listener))
        .await?;
    Ok(())
}

/// Parse a `--peer` spec of the form `id@host:port`.
fn parse_peer(spec: &str) -> anyhow::Result<Peer> {
    let (id, addr) = spec
        .split_once('@')
        .ok_or_else(|| anyhow::anyhow!("bad --peer `{spec}`, expected id@host:port"))?;
    Ok(Peer { id: id.parse()?, addr: addr.to_string() })
}
