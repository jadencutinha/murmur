//! `murmur-bench` — a throughput / latency benchmark for a running cluster.
//!
//! It spins up `--clients` concurrent [`Clerk`](murmur::clerk::Clerk)s, each
//! issuing `--ops` operations, and reports aggregate throughput and the latency
//! distribution. Every write is a full Raft round (propose → replicate to a
//! majority → commit → apply), and reads are linearized through the log too, so
//! the numbers reflect end-to-end replicated-consensus latency, not a local store.
//!
//! Point it at a running cluster (see `demo.sh` or `murmurd`):
//!   murmur-bench --peers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 \
//!                --clients 8 --ops 200 --value-size 64

use std::time::{Duration, Instant};

use murmur::clerk::Clerk;

const DEFAULT_PEERS: &str = "127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut peers = DEFAULT_PEERS.to_string();
    let mut clients: usize = 8;
    let mut ops: usize = 200;
    let mut value_size: usize = 64;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--peers" => peers = args.next().unwrap_or(peers),
            "--clients" => clients = args.next().and_then(|v| v.parse().ok()).unwrap_or(clients),
            "--ops" => ops = args.next().and_then(|v| v.parse().ok()).unwrap_or(ops),
            "--value-size" => {
                value_size = args.next().and_then(|v| v.parse().ok()).unwrap_or(value_size)
            }
            "-h" | "--help" => {
                eprintln!(
                    "usage: murmur-bench [--peers a,b,c] [--clients N] [--ops M] [--value-size B]"
                );
                return Ok(());
            }
            other => anyhow::bail!("unknown argument: {other}"),
        }
    }

    let endpoints: Vec<String> = peers.split(',').map(|s| s.trim().to_string()).collect();
    let value = vec![b'x'; value_size];
    println!(
        "murmur-bench: {clients} clients × {ops} ops = {} total, {value_size}-byte values",
        clients * ops
    );
    println!("cluster: {}\n", endpoints.join(", "));

    report("PUT", run(&endpoints, clients, ops, Workload::Put(value.clone())).await?);
    // GET the keys the PUT phase just wrote, so every read hits.
    report("GET", run(&endpoints, clients, ops, Workload::Get).await?);
    Ok(())
}

enum Workload {
    Put(Vec<u8>),
    Get,
}

/// Run one phase: `clients` tasks each performing `ops` operations, returning the
/// wall-clock elapsed and every individual operation latency.
async fn run(
    endpoints: &[String],
    clients: usize,
    ops: usize,
    workload: Workload,
) -> anyhow::Result<(Duration, Vec<Duration>)> {
    let value = match &workload {
        Workload::Put(v) => Some(v.clone()),
        Workload::Get => None,
    };

    let start = Instant::now();
    let mut tasks = Vec::new();
    for c in 0..clients {
        let endpoints = endpoints.to_vec();
        let value = value.clone();
        tasks.push(tokio::spawn(async move {
            let mut clerk = Clerk::connect(endpoints).await?;
            let mut latencies = Vec::with_capacity(ops);
            for i in 0..ops {
                let key = format!("c{c}-k{i}").into_bytes();
                let op_start = Instant::now();
                match &value {
                    Some(v) => clerk.put(key, v.clone()).await?,
                    None => {
                        clerk.get(key).await?;
                    }
                }
                latencies.push(op_start.elapsed());
            }
            Ok::<_, anyhow::Error>(latencies)
        }));
    }

    let mut all = Vec::with_capacity(clients * ops);
    for task in tasks {
        all.extend(task.await??);
    }
    Ok((start.elapsed(), all))
}

/// Print throughput and latency percentiles for a phase.
fn report(name: &str, (elapsed, mut latencies): (Duration, Vec<Duration>)) {
    latencies.sort_unstable();
    let n = latencies.len();
    let throughput = n as f64 / elapsed.as_secs_f64();
    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    let pct = |p: f64| latencies[((n as f64 * p) as usize).min(n - 1)];
    let mean = latencies.iter().sum::<Duration>() / n as u32;

    println!("{name}: {n} ops in {:.2}s → {throughput:.0} ops/s", elapsed.as_secs_f64());
    println!(
        "  latency (ms): mean {:.2}  p50 {:.2}  p95 {:.2}  p99 {:.2}  max {:.2}\n",
        ms(mean),
        ms(pct(0.50)),
        ms(pct(0.95)),
        ms(pct(0.99)),
        ms(latencies[n - 1]),
    );
}
