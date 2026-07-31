# murmur

A distributed, strongly-consistent key/value store in Rust. murmur takes
[**Sable**](https://github.com/jadencutinha/sable) — a from-scratch LSM-tree
storage engine — wraps it behind a gRPC service, and replicates it across a
cluster with a **hand-written implementation of the Raft consensus algorithm**.

Every write goes through Raft: the leader appends it to a replicated log, waits
for a majority to store it, then applies it to each node's local Sable engine —
so all replicas hold identical data and a committed write survives the loss of a
minority of nodes. Reads are linearized through the same log. Transport is
**gRPC** (tonic + prost); the structure follows MIT's 6.5840 labs.

```
        client (Clerk)
           │  Put / Get / Append / Delete   (retries to find the leader)
           ▼
   ┌──────────────────┐   AppendEntries / RequestVote / InstallSnapshot   ┌────────┐
   │   leader node    │◀────────────────────────────────────────────────▶│follower│
   │                  │                                                   │        │
   │  KV gRPC service │   propose → replicate to a majority → commit      │  ...   │
   │       │          │                                                   │        │
   │   Raft log  ─────┼──▶ apply in log order ──▶ Sable (LSM engine)      │ Sable  │
   └──────────────────┘                                                   └────────┘
         node 1                        node 2                              node 3
```

## What's implemented

A fairly complete Raft, built and tested in stages:

- **Leader election** with randomized timeouts, plus **PreVote** so a stale or
  partitioned node can't disrupt a healthy leader.
- **Log replication** with the consistency check and fast conflict-backtracking
  repair (rewind past a whole divergent term in one round trip).
- **Persistence & crash recovery** — term, vote, and log are fsync'd durably;
  a restarted node reloads and catches up.
- **Exactly-once client semantics** — client ids + per-client sequence numbers
  with a server-side dedup table, so a retried mutation is applied once even
  across a leader change.
- **Log compaction / snapshots** — the log is truncated against a state-machine
  snapshot, and `InstallSnapshot` catches up a follower whose entries were
  compacted away.
- **Linearizable reads** — reads are routed through the log too.

## Layout

| Path | What lives there |
|------|------------------|
| `src/store.rs` | `KvStore` — the thin wrapper over the embedded Sable engine. |
| `src/app.rs` | The replicated `Kv` service: routes requests through Raft, runs the apply loop, dedup table, and snapshotting. |
| `src/clerk.rs` | `Clerk` — the client: registers once, stamps mutations for exactly-once, discovers the leader by round-robin. |
| `src/command.rs` | Encoding of KV operations into (and out of) opaque Raft log-entry bytes. |
| `src/server.rs` | The original single-node (unreplicated) KV service, kept for a direct smoke test. |
| `src/raft/types.rs` | Core values: terms, the `Log` (with its snapshot offset and all index arithmetic), roles, apply messages. |
| `src/raft/rpc.rs` | The logical RPCs — RequestVote / AppendEntries / InstallSnapshot — free of any wire format. |
| `src/raft/consensus.rs` | The **synchronous, I/O-free consensus state machine**: elections, PreVote, replication, commit, snapshots. |
| `src/raft/storage.rs` | `PersistentState` and the durable, crash-safe `FileStorage`. |
| `src/raft/transport.rs` | The gRPC boundary: peer clients, the inbound service, wire ↔ logical conversions. |
| `src/raft/node.rs` | The async driver: serves RPCs and runs the tick loop that fires elections and heartbeats. |
| `src/raft/config.rs` | Cluster membership and timing knobs. |
| `src/bin/murmurd.rs` | The node daemon. |
| `src/bin/murmur.rs` | The client CLI. |
| `src/bin/murmur-bench.rs` | A throughput / latency benchmark. |
| `proto/` | `kv.proto` (client-facing) and `raft.proto` (internal, node-to-node). |

The design keeps consensus **pure**: `consensus.rs` mutates in-process state and
returns values with no timers or networking, so Raft's tricky invariants live in
one place that's unit-testable without a runtime. `node.rs` owns all the async
machinery and only ever holds the fast `std::sync::Mutex` around the core to read
state or build the next RPC — never across an `await`.

## Build & test

```sh
cargo build
cargo test          # unit tests + integration tests (election, replication,
                    # persistence, exactly-once, snapshots, chaos, …)
```

## Run a cluster

Start three nodes (each runs an internal Raft server and a client-facing KV
server). In three terminals, or backgrounded:

```sh
murmurd --id 1 --raft-listen 127.0.0.1:6001 --kv-listen 127.0.0.1:5001 \
        --data data/n1 --peer 2@127.0.0.1:6002 --peer 3@127.0.0.1:6003
murmurd --id 2 --raft-listen 127.0.0.1:6002 --kv-listen 127.0.0.1:5002 \
        --data data/n2 --peer 1@127.0.0.1:6001 --peer 3@127.0.0.1:6003
murmurd --id 3 --raft-listen 127.0.0.1:6003 --kv-listen 127.0.0.1:5003 \
        --data data/n3 --peer 1@127.0.0.1:6001 --peer 2@127.0.0.1:6002
```

Then drive it with the CLI (point it at any/all nodes — it finds the leader):

```sh
murmur put color amber
murmur get color                       # -> amber
murmur append log "hello "
murmur append log "world"              # -> hello world
murmur del color
murmur get color                       # -> (not found), exit 1

# any subset of nodes works; the clerk redirects to the leader
murmur --peers 127.0.0.1:5002,127.0.0.1:5003 get log
```

## Demo

`demo.sh` does all of the above automatically — builds, launches a 3-node
cluster in a temp dir, runs a scripted sequence of CLI commands, and tears
everything down:

```sh
./demo.sh            # scripted demo
./demo.sh --bench    # ...followed by a short benchmark
```

## Benchmark

Against a running cluster, `murmur-bench` drives concurrent clients and reports
throughput and the latency distribution. Every operation is a full consensus
round (propose → replicate to a majority → commit → apply), reads included:

```sh
murmur-bench --peers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 \
             --clients 8 --ops 200 --value-size 64
```

```
PUT: 1600 ops in 13.22s → 121 ops/s
  latency (ms): mean 66.10  p50 64.85  p95 83.11  p99 91.69  max 116.59
GET: 1600 ops in 14.56s → 110 ops/s
  latency (ms): mean 72.78  p50 68.93  p95 95.32  p99 129.27  max 154.32
```

Per-op latency is dominated by the replication round trip (a write can't return
until a majority has stored it); throughput scales with client concurrency.

## Design notes

- **Language:** Rust; Sable is a git dependency (`sable`). Its `Db` is cheaply
  cloneable — clones share the same underlying storage.
- **Sable API used:** `Db::open(dir)`, `put/get/delete(&[u8])`,
  `get -> Option<Vec<u8>>`, `snapshot()`, `write(WriteBatch, WriteOptions)`.
- **Snapshots.** The `Log` carries a snapshot offset, so after compaction entries
  begin at `snapshot_index + 1` while callers keep addressing them by absolute
  index — all the offset math stays centralized in `types::Log`. A state-machine
  snapshot serializes the dedup table plus a full key/value image. Sable exposes
  no way to *enumerate* its keys, so `app` keeps an in-memory **key directory**
  (keys only — values stay in Sable) purely to build a transferable snapshot. The
  app snapshots every `SNAPSHOT_THRESHOLD` applied entries through a detached
  `RaftControl` handle, and `InstallSnapshot` ships the image to a follower whose
  needed entries have been compacted away.
- **PreVote.** Before incrementing its term, a node runs a *pre-election*: it asks
  "would you vote for me?" and only campaigns for real on a majority of pre-votes.
  A voter grants a pre-vote only after a full election timeout with no leader
  contact (tracked separately from its own campaign timer, so a pre-candidate
  still helps elect a better one). This closes the disruptive-restart gap — a node
  that can't win never inflates its term or deposes a healthy leader.
- **Known gap (app-layer restart).** A node that restarts reloads its snapshot and
  re-applies the committed tail on top of Sable's already-durable data. Because
  the reset only restores *the snapshot's* keys, a key whose first-ever operation
  after the snapshot is a bare `Append` (created entirely within the post-snapshot
  tail) could double-apply across a crash. It's narrow, and this class of gap is
  scoped out of the raw-Raft persistence test. A durable applied-index written in
  the same Sable `WriteBatch` as each mutation would close it.

## How it was built

murmur was built in twelve checkpoints, each a self-contained, tested step:

| # | Checkpoint |
|---|-----------|
| 1 | Scaffold + Sable integration (embedded store wrapper, smoke test) |
| 2 | Network layer, single node (gRPC KV service: Get/Put/Del) |
| 3 | Raft core types + persistent state (terms, log, roles, RPC messages) |
| 4 | Leader election (3-node cluster elects; re-elects on leader death) |
| 5 | Log replication, happy path (append, replicate, commit, apply) |
| 6 | Log consistency / conflict repair (consistency check, nextIndex backtrack) |
| 7 | Persistence + crash recovery (state + log survive restart, catch up) |
| 8 | Wire Raft → Sable KV over network (client → leader → log → apply) |
| 9 | Client dedup / exactly-once (client IDs + seq nums, retry-safe) |
| 10 | Log compaction / snapshots (snapshot state, truncate log, InstallSnapshot) |
| 11 | Failure recovery + chaos tests (PreVote; kill/partition safety + liveness) |
| 12 | Demo, CLI, docs, benchmark |

## License

MIT.
