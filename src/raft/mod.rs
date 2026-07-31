//! murmur's Raft implementation.
//!
//! Raft keeps a replicated log identical across the cluster; applying that log
//! in order to each node's Sable engine is what keeps the key/value data
//! consistent. This checkpoint lays down the *data model* only — the terms, the
//! log and its index arithmetic, the node roles, the logical RPC messages, and
//! the persistent state — all as plain synchronous Rust so it can be unit-tested
//! without any timers or sockets. Behavior (elections, replication) is layered
//! on top in later checkpoints.
//!
//! Index convention: log indices are **1-based** to match the Raft paper. Index
//! 0 is the synthetic position "before the first entry" and always has term 0.

pub mod rpc;
pub mod storage;
pub mod types;

pub use rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
pub use storage::{InMemoryStorage, PersistentState, RaftStorage};
pub use types::{Log, LogEntry, LogIndex, NodeId, Role, Term};
