//! `murmurd` — the murmur node daemon.
//!
//! At this checkpoint it runs a single node: open a Sable-backed store and serve
//! the KV gRPC API. Cluster flags (peers, node id) arrive with the Raft
//! checkpoints; for now it takes just a listen address and a data directory.
//!
//! Usage: `murmurd --listen 127.0.0.1:50051 --data data/node1`

use std::net::SocketAddr;

use murmur::store::KvStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut listen = "127.0.0.1:50051".to_string();
    let mut data = "data/node".to_string();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => listen = args.next().unwrap_or(listen),
            "--data" => data = args.next().unwrap_or(data),
            "-h" | "--help" => {
                eprintln!("usage: murmurd --listen <addr> --data <dir>");
                return Ok(());
            }
            other => anyhow::bail!("unknown argument: {other}"),
        }
    }

    let addr: SocketAddr = listen.parse()?;
    let store = KvStore::open(&data)?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("murmurd: serving KV on {addr}, data dir `{data}`");

    murmur::server::serve(store, listener).await
}
