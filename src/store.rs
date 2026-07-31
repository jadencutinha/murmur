//! The local storage layer: a thin wrapper around the embedded Sable engine.
//!
//! Everything above this module (the gRPC service, the Raft state machine) talks
//! to storage exclusively through [`KvStore`]. Keeping Sable behind one small
//! interface means the rest of murmur never depends on Sable's concrete types,
//! and it gives us one place to later apply Raft-committed commands.

use std::path::Path;

use sable::Db;

/// Errors surfaced by the local store.
///
/// Sable's own error type is collapsed into a string here so callers (the gRPC
/// layer, the Raft apply loop) depend only on murmur types, not on Sable's.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sable engine error: {0}")]
    Engine(String),
}

/// A local, single-node key/value store backed by Sable's LSM-tree engine.
///
/// [`KvStore`] is cheaply cloneable — like [`sable::Db`], every clone shares the
/// same underlying storage, so it is safe to hand a clone to each request
/// handler or to the Raft apply task.
#[derive(Clone)]
pub struct KvStore {
    db: Db,
}

impl KvStore {
    /// Open (creating if necessary) a store rooted at `dir`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, StoreError> {
        let db = Db::open(dir).map_err(|e| StoreError::Engine(e.to_string()))?;
        Ok(Self { db })
    }

    /// Insert or overwrite `key` -> `value`.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.db
            .put(key, value)
            .map_err(|e| StoreError::Engine(e.to_string()))
    }

    /// Look up `key`, returning `None` if it is absent or deleted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.db.get(key).map_err(|e| StoreError::Engine(e.to_string()))
    }

    /// Remove `key`. Deleting an absent key is not an error.
    pub fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.db
            .delete(key)
            .map_err(|e| StoreError::Engine(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke test: the full put / get / overwrite / delete lifecycle through the
    /// Sable-backed store, proving murmur links and drives the engine correctly.
    #[test]
    fn put_get_overwrite_delete() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = KvStore::open(dir.path()).expect("open store");

        // Absent key reads as None.
        assert_eq!(store.get(b"alpha").unwrap(), None);

        // Put then get.
        store.put(b"alpha", b"one").unwrap();
        assert_eq!(store.get(b"alpha").unwrap().as_deref(), Some(&b"one"[..]));

        // Overwrite returns the newest value.
        store.put(b"alpha", b"two").unwrap();
        assert_eq!(store.get(b"alpha").unwrap().as_deref(), Some(&b"two"[..]));

        // Delete makes it absent again.
        store.delete(b"alpha").unwrap();
        assert_eq!(store.get(b"alpha").unwrap(), None);
    }

    /// A reopened store must see previously written data — durability across the
    /// handle lifetime, which every replicated node depends on after a restart.
    #[test]
    fn data_survives_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let store = KvStore::open(dir.path()).unwrap();
            store.put(b"persist", b"yes").unwrap();
        }
        let store = KvStore::open(dir.path()).unwrap();
        assert_eq!(store.get(b"persist").unwrap().as_deref(), Some(&b"yes"[..]));
    }
}
