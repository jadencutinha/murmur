//! Raft's persistent state and the storage abstraction behind it.
//!
//! Raft requires three pieces of state to survive crashes and be flushed to
//! stable storage *before* a node responds to any RPC that depends on them:
//! `current_term`, `voted_for`, and the `log`. Everything else (commit index,
//! role, leader hints) is volatile and rebuilt after a restart.
//!
//! This checkpoint defines the state and a [`RaftStorage`] trait with an
//! in-memory implementation for tests. A durable, disk-backed implementation is
//! added at the persistence checkpoint — the trait is the seam that lets it drop
//! in without touching the consensus logic.

use std::sync::Mutex;

use super::types::{Log, NodeId, Term};

/// The three fields Raft must persist. Saved as a unit; see [`RaftStorage`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PersistentState {
    /// Latest term this node has seen (starts at 0, monotonically increasing).
    pub current_term: Term,
    /// Candidate voted for in the current term, if any. Reset each new term.
    pub voted_for: Option<NodeId>,
    /// The replicated log.
    pub log: Log,
}

impl PersistentState {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Durable storage for [`PersistentState`].
///
/// The whole state is saved at once (as the Raft paper and the 6.5840 labs do):
/// simple, always consistent, and fine at lab scale since the hot writes are
/// small. A production system would use an append-only log with incremental
/// writes — a later optimization that this trait leaves room for.
pub trait RaftStorage: Send + Sync + 'static {
    /// Load persisted state, returning defaults on first-ever startup.
    fn load(&self) -> anyhow::Result<PersistentState>;
    /// Durably persist `state`. Must not return until the data is stable.
    fn save(&self, state: &PersistentState) -> anyhow::Result<()>;
}

/// A volatile, in-process [`RaftStorage`] for unit and integration tests.
#[derive(Default)]
pub struct InMemoryStorage {
    state: Mutex<PersistentState>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RaftStorage for InMemoryStorage {
    fn load(&self) -> anyhow::Result<PersistentState> {
        Ok(self.state.lock().unwrap().clone())
    }

    fn save(&self, state: &PersistentState) -> anyhow::Result<()> {
        *self.state.lock().unwrap() = state.clone();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::LogEntry;

    #[test]
    fn save_then_load_roundtrips() {
        let storage = InMemoryStorage::new();
        assert_eq!(storage.load().unwrap(), PersistentState::default());

        let mut log = Log::new();
        log.append(LogEntry::new(1, b"set x=1".to_vec()));
        let state = PersistentState {
            current_term: 3,
            voted_for: Some(2),
            log,
        };
        storage.save(&state).unwrap();

        assert_eq!(storage.load().unwrap(), state);
    }
}
