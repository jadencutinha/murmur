//! murmur — a distributed key-value store.
//!
//! murmur takes [Sable](https://github.com/jadencutinha/sable), a single-node
//! LSM-tree storage engine, and turns it into a replicated cluster: Sable is
//! wrapped behind a gRPC network layer and kept consistent across nodes with a
//! hand-written implementation of the Raft consensus algorithm.
//!
//! The build is staged across checkpoints (see `CHECKPOINTS.md`). This module
//! tree grows one checkpoint at a time.

/// Generated gRPC types: prost messages and tonic client/server stubs for every
/// service declared under the `murmur` proto package.
pub mod proto {
    tonic::include_proto!("murmur");
}

pub mod app;
pub mod command;
pub mod raft;
pub mod server;
pub mod store;

pub use store::{KvStore, StoreError};
