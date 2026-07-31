# murmur — build checkpoints

A distributed key-value store built on top of [Sable](https://github.com/jadencutinha/sable)
(a from-scratch LSM-tree engine in Rust). murmur wraps Sable behind a gRPC network
layer and replicates it across a cluster with a hand-written implementation of the
**Raft** consensus algorithm.

Transport: **gRPC (tonic + prost)**. Structure follows MIT 6.5840 (formerly 6.824).

Legend: ✅ done · 🔨 in progress · ⬜ not started

| # | Checkpoint | Status |
|---|-----------|--------|
| 1 | Scaffold + Sable integration (embedded store wrapper, smoke test) | ✅ |
| 2 | Network layer, single node (gRPC KV service: Get/Put/Del) | ✅ |
| 3 | Raft core types + persistent state (terms, log, roles, RPC messages) | ✅ |
| 4 | Leader election (3-node cluster elects; re-elects on leader death) | ✅ |
| 5 | Log replication, happy path (append, replicate, commit, apply) | ✅ |
| 6 | Log consistency / conflict repair (consistency check, nextIndex backtrack) | ✅ |
| 7 | Persistence + crash recovery (state + log survive restart, catch up) | ✅ |
| 8 | Wire Raft → Sable KV over network (client → leader → log → apply) | ⬜ |
| 9 | Client dedup / exactly-once (client IDs + seq nums, retry-safe) | ⬜ |
| 10 | Log compaction / snapshots (snapshot state, truncate log, InstallSnapshot) | ⬜ |
| 11 | Failure recovery + chaos tests (kill/partition, verify safety + liveness) | ⬜ |
| 12 | Demo, CLI, docs, benchmark | ⬜ |

## Notes / decisions
- **Language:** Rust (Sable is a Rust crate we build against; `sable` added as a git dependency).
- **Known gap (for checkpoint 11):** no PreVote yet. A node restarted with a *stale* log into an otherwise-healthy cluster can't win a vote but keeps incrementing its term, disrupting the leader (a livelock until timings line up). Standard Raft has this; PreVote / leader-stickiness fixes it. The persistence test therefore restarts the whole cluster at once rather than one stale node into a live cluster.
- **Sable API used:** `Db::open(dir)`, `put/get/delete(&[u8])`, `get -> Option<Vec<u8>>`, `snapshot()`, `write(WriteBatch, WriteOptions)`. `Db` is cheaply cloneable (clones share storage).
