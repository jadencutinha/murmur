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
| 8 | Wire Raft → Sable KV over network (client → leader → log → apply) | ✅ |
| 9 | Client dedup / exactly-once (client IDs + seq nums, retry-safe) | ✅ |
| 10 | Log compaction / snapshots (snapshot state, truncate log, InstallSnapshot) | ✅ |
| 11 | Failure recovery + chaos tests (kill/partition, verify safety + liveness) | ✅ |
| 12 | Demo, CLI, docs, benchmark | ⬜ |

## Notes / decisions
- **Language:** Rust (Sable is a Rust crate we build against; `sable` added as a git dependency).
- **PreVote (checkpoint 11):** a node now runs a pre-election before bumping its term — it asks "would you vote for me?" and only campaigns for real on a majority of *pre-votes*. A voter grants a pre-vote only after a full election timeout with no leader contact (tracked separately from its own campaign timer, so a pre-candidate still helps elect a better one). This closes the disruptive-restart gap: a stale or partitioned node can't win pre-votes, so it never inflates its term or deposes a healthy leader. `tests/chaos.rs` exercises safety (survivors' apply logs stay a consistent prefix) and liveness (clean failover) across repeated leader kills.
- **Sable API used:** `Db::open(dir)`, `put/get/delete(&[u8])`, `get -> Option<Vec<u8>>`, `snapshot()`, `write(WriteBatch, WriteOptions)`. `Db` is cheaply cloneable (clones share storage).
- **Snapshot design (checkpoint 10):** the `Log` carries a snapshot offset so entries begin at `snapshot_index + 1`; all index math stays centralized in `types::Log`. The state-machine snapshot serializes the dedup table plus a full key/value image. Sable exposes no way to *enumerate* keys, so `app::AppState` keeps an in-memory **key directory** (keys only — values stay in Sable) purely so a snapshot can be built and shipped. The app snapshots every `SNAPSHOT_THRESHOLD` applied entries via a detached `RaftControl` handle; `InstallSnapshot` catches up a follower whose needed entries were compacted away.
- **Known gap (app-layer restart):** a node that restarts reloads its snapshot and re-applies the committed tail on top of Sable's already-durable data. Because the reset only restores *the snapshot's* keys, a key whose first-ever op after the snapshot is a bare `Append` (created entirely within the post-snapshot tail) could double-apply across a crash. Narrow, and the same class of app-restart gap predates this checkpoint (the raw-Raft persistence test doesn't drive the KV app). A durable applied-index in Sable's `WriteBatch` would close it.
