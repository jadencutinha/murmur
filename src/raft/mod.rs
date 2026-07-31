//! murmur's Raft implementation.
//!
//! Raft keeps a replicated log identical across the cluster; applying that log
//! in order to each node's Sable engine is what keeps the key/value data
//! consistent. The pieces:
//!
//! - [`types`] — terms, the log and its index arithmetic, roles.
//! - [`rpc`] — the logical RequestVote / AppendEntries messages.
//! - [`storage`] — the persistent state and its storage trait.
//! - [`consensus`] — the synchronous consensus state machine (elections now;
//!   replication and repair in later checkpoints).
//! - [`transport`] — the gRPC boundary and wire conversions.
//! - [`node`] — the async driver: serves RPCs and runs the tick loop.
//!
//! Index convention: log indices are **1-based** to match the Raft paper. Index
//! 0 is the synthetic position "before the first entry" and always has term 0.

pub mod config;
pub mod consensus;
pub mod node;
pub mod rpc;
pub mod storage;
pub mod transport;
pub mod types;

pub use config::{ClusterConfig, Peer, Timing};
pub use consensus::ConsensusModule;
pub use node::{start_node, ApplyReceiver, NodeHandle};
pub use rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
pub use storage::{FileStorage, InMemoryStorage, PersistentState, RaftStorage};
pub use types::{Applied, Log, LogEntry, LogIndex, NodeId, Role, Term};
