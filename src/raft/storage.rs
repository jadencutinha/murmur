//! Raft's persistent state and the storage abstraction behind it.
//!
//! Raft requires three pieces of state to survive crashes and be flushed to
//! stable storage *before* a node responds to any RPC that depends on them:
//! `current_term`, `voted_for`, and the `log`. Everything else (commit index,
//! role, leader hints) is volatile and rebuilt after a restart.
//!
//! Two implementations live here: an in-memory one for unit and integration
//! tests, and [`FileStorage`], a durable disk-backed one used by real nodes. The
//! [`RaftStorage`] trait is the seam that lets either drop in without the
//! consensus logic knowing which is in play.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use super::types::{Log, LogEntry, NodeId, Term};

/// The state Raft must persist. Saved as a unit; see [`RaftStorage`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PersistentState {
    /// Latest term this node has seen (starts at 0, monotonically increasing).
    pub current_term: Term,
    /// Candidate voted for in the current term, if any. Reset each new term.
    pub voted_for: Option<NodeId>,
    /// The replicated log. Its snapshot boundary (`snapshot_index`/`snapshot_term`)
    /// is stored alongside the entries so a reloaded log keeps the right origin.
    pub log: Log,
    /// The opaque state-machine snapshot the log was compacted against, if any.
    /// Persisted with the log so a restarted node can still serve it to a lagging
    /// peer and restore its own volatile application state.
    pub snapshot: Option<Vec<u8>>,
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

// ---- durable, disk-backed storage ----

/// Magic + version prefixing every state file, so a stray or stale file is
/// rejected rather than misread. Version 2 added the snapshot block.
const MAGIC: &[u8; 4] = b"MRMR";
const VERSION: u8 = 2;

/// A crash-safe [`RaftStorage`] that keeps the whole state in one file.
///
/// Every [`save`](RaftStorage::save) writes to a sibling temp file, fsyncs it,
/// then atomically renames it over the real file (and fsyncs the directory).
/// A crash can therefore leave only the *old* complete state or the *new*
/// complete state — never a torn mix — which is exactly the durability Raft
/// depends on to never lose a vote or a committed entry across a restart.
pub struct FileStorage {
    path: PathBuf,
    tmp: PathBuf,
}

impl FileStorage {
    /// Store the state at `path` (created on first save; parent dirs are made as
    /// needed). Loading a non-existent path yields default state — a fresh node.
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        // Append (not replace) an extension so the temp never collides with a
        // caller's own filename that happens to have one.
        let mut tmp = path.clone().into_os_string();
        tmp.push(".tmp");
        Self { path, tmp: PathBuf::from(tmp) }
    }
}

impl RaftStorage for FileStorage {
    fn load(&self) -> anyhow::Result<PersistentState> {
        match fs::read(&self.path) {
            Ok(bytes) => decode(&bytes),
            // First-ever startup: nothing persisted yet.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(PersistentState::default()),
            Err(e) => Err(e.into()),
        }
    }

    fn save(&self, state: &PersistentState) -> anyhow::Result<()> {
        if let Some(dir) = self.path.parent() {
            if !dir.as_os_str().is_empty() {
                fs::create_dir_all(dir)?;
            }
        }
        // Write the full new image to the temp file and flush it to disk...
        let mut file = fs::File::create(&self.tmp)?;
        file.write_all(&encode(state))?;
        file.sync_all()?;
        // ...then swap it in atomically. On POSIX, rename over an existing path
        // is atomic, so a reader never sees a half-written file.
        fs::rename(&self.tmp, &self.path)?;
        // Persist the directory entry too, so the rename survives a crash.
        if let Some(dir) = self.path.parent() {
            let dir = if dir.as_os_str().is_empty() { Path::new(".") } else { dir };
            if let Ok(handle) = fs::File::open(dir) {
                let _ = handle.sync_all();
            }
        }
        Ok(())
    }
}

/// Serialize state to the on-disk layout: `MAGIC | VERSION | current_term |
/// vote-flag | vote-id | snapshot-flag | snapshot-index | snapshot-term |
/// snapshot-len | snapshot-bytes | entry-count | (term | cmd-len | cmd-bytes)*`,
/// all integers little-endian. The snapshot block is present (flag 1) once the
/// log has been compacted; before that the flag is 0 and the log begins at index 1.
fn encode(state: &PersistentState) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
    buf.extend_from_slice(&state.current_term.to_le_bytes());
    let (flag, id) = match state.voted_for {
        Some(id) => (1u8, id),
        None => (0u8, 0),
    };
    buf.push(flag);
    buf.extend_from_slice(&id.to_le_bytes());
    // Snapshot block: the log's compaction boundary and the opaque bytes it stands
    // for. Written even when absent (flag 0) so the layout stays fixed-shape.
    match &state.snapshot {
        Some(data) => {
            buf.push(1u8);
            buf.extend_from_slice(&state.log.snapshot_index().to_le_bytes());
            buf.extend_from_slice(&state.log.snapshot_term().to_le_bytes());
            buf.extend_from_slice(&(data.len() as u64).to_le_bytes());
            buf.extend_from_slice(data);
        }
        None => {
            buf.push(0u8);
            buf.extend_from_slice(&0u64.to_le_bytes()); // index
            buf.extend_from_slice(&0u64.to_le_bytes()); // term
            buf.extend_from_slice(&0u64.to_le_bytes()); // len
        }
    }
    let entries = state.log.entries();
    buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for entry in entries {
        buf.extend_from_slice(&entry.term.to_le_bytes());
        buf.extend_from_slice(&(entry.command.len() as u64).to_le_bytes());
        buf.extend_from_slice(&entry.command);
    }
    buf
}

/// Inverse of [`encode`]. Strict about length: any truncation or trailing byte
/// is an error rather than a silently accepted partial state.
fn decode(bytes: &[u8]) -> anyhow::Result<PersistentState> {
    let mut r = Reader { buf: bytes, pos: 0 };
    if r.take(4)? != MAGIC {
        anyhow::bail!("raft state file: bad magic");
    }
    let version = r.u8()?;
    if version != VERSION {
        anyhow::bail!("raft state file: unsupported version {version}");
    }
    let current_term = r.u64()?;
    let voted_for = match r.u8()? {
        0 => {
            r.u64()?; // consume the (unused) id slot
            None
        }
        1 => Some(r.u64()?),
        other => anyhow::bail!("raft state file: bad vote flag {other}"),
    };
    let snapshot_flag = r.u8()?;
    let snapshot_index = r.u64()?;
    let snapshot_term = r.u64()?;
    let snapshot_len = r.u64()? as usize;
    let snapshot = match snapshot_flag {
        0 => None,
        1 => Some(r.take(snapshot_len)?.to_vec()),
        other => anyhow::bail!("raft state file: bad snapshot flag {other}"),
    };
    let count = r.u64()?;
    // Don't pre-allocate from an untrusted count; each entry consumes ≥16 bytes,
    // so a bogus count simply runs `take` out of input and errors cleanly.
    let mut entries = Vec::new();
    for _ in 0..count {
        let term = r.u64()?;
        let len = r.u64()? as usize;
        let command = r.take(len)?.to_vec();
        entries.push(LogEntry::new(term, command));
    }
    if r.pos != r.buf.len() {
        anyhow::bail!("raft state file: {} trailing bytes", r.buf.len() - r.pos);
    }
    Ok(PersistentState {
        current_term,
        voted_for,
        log: Log::from_parts(snapshot_index, snapshot_term, entries),
        snapshot,
    })
}

/// A bounds-checked forward cursor over the state file's bytes.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> anyhow::Result<&'a [u8]> {
        let end = self.pos.checked_add(n).filter(|&e| e <= self.buf.len());
        match end {
            Some(end) => {
                let slice = &self.buf[self.pos..end];
                self.pos = end;
                Ok(slice)
            }
            None => anyhow::bail!("raft state file: truncated (wanted {n} more bytes)"),
        }
    }

    fn u8(&mut self) -> anyhow::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> anyhow::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    use crate::raft::config::Timing;
    use crate::raft::consensus::ConsensusModule;
    use crate::raft::types::LogEntry;

    fn sample_state() -> PersistentState {
        let mut log = Log::new();
        log.append(LogEntry::new(1, b"set x=1".to_vec()));
        log.append(LogEntry::new(2, Vec::new())); // empty-command entry
        PersistentState { current_term: 3, voted_for: Some(2), log, snapshot: None }
    }

    #[test]
    fn save_then_load_roundtrips() {
        let storage = InMemoryStorage::new();
        assert_eq!(storage.load().unwrap(), PersistentState::default());
        let state = sample_state();
        storage.save(&state).unwrap();
        assert_eq!(storage.load().unwrap(), state);
    }

    #[test]
    fn file_storage_survives_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft-state");

        // Missing file -> default (a fresh node).
        assert_eq!(FileStorage::new(&path).load().unwrap(), PersistentState::default());

        let state = sample_state();
        FileStorage::new(&path).save(&state).unwrap();

        // A brand-new handle on the same path (a "restart") sees the saved state.
        assert_eq!(FileStorage::new(&path).load().unwrap(), state);
    }

    #[test]
    fn file_storage_overwrites_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft-state");
        let storage = FileStorage::new(&path);

        storage.save(&sample_state()).unwrap();
        let newer = PersistentState {
            current_term: 9,
            voted_for: None,
            log: Log::new(),
            snapshot: None,
        };
        storage.save(&newer).unwrap();
        assert_eq!(storage.load().unwrap(), newer); // last write wins, no leftovers
    }

    #[test]
    fn snapshot_and_compacted_log_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft-state");
        let storage = FileStorage::new(&path);

        // A log compacted through index 5 (term 3), with a tail of two entries and
        // opaque snapshot bytes standing in for the discarded prefix.
        let log = Log::from_parts(
            5,
            3,
            vec![LogEntry::new(3, b"tail-1".to_vec()), LogEntry::new(4, b"tail-2".to_vec())],
        );
        let state = PersistentState {
            current_term: 4,
            voted_for: Some(1),
            log,
            snapshot: Some(b"opaque-state-machine-image".to_vec()),
        };
        storage.save(&state).unwrap();

        // A fresh handle recovers the boundary, the tail, and the snapshot bytes.
        let loaded = FileStorage::new(&path).load().unwrap();
        assert_eq!(loaded, state);
        assert_eq!(loaded.log.snapshot_index(), 5);
        assert_eq!(loaded.log.snapshot_term(), 3);
        assert_eq!(loaded.log.last_index(), 7);
    }

    #[test]
    fn corrupt_file_is_rejected_not_misread() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft-state");
        std::fs::write(&path, b"not a raft state file").unwrap();
        assert!(FileStorage::new(&path).load().is_err());

        // A truncated-but-plausible file also fails rather than loading garbage.
        let mut bytes = encode(&sample_state());
        bytes.truncate(bytes.len() - 3);
        std::fs::write(&path, &bytes).unwrap();
        assert!(FileStorage::new(&path).load().is_err());
    }

    /// A node's term, vote, and log must survive a crash so Raft's safety
    /// invariants hold across restarts. Drive some state through one module,
    /// then reconstruct a fresh module from the same file and check it recovered.
    #[test]
    fn a_restarted_node_recovers_term_vote_and_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft-state");

        // Single-node cluster: starting an election wins leadership immediately,
        // so we can append committed log entries and exercise every persisted field.
        {
            let storage = Box::new(FileStorage::new(&path));
            let mut cm = ConsensusModule::new(1, vec![], storage, Timing::default(), Instant::now()).unwrap();
            cm.start_election(Instant::now()); // term 1, votes for itself
            assert!(cm.is_leader());
            cm.submit_command(b"a".to_vec());
            cm.submit_command(b"b".to_vec());
        } // module dropped — simulates the crash

        // Restart: a new module loads the persisted state from disk.
        let storage = Box::new(FileStorage::new(&path));
        let recovered =
            ConsensusModule::new(1, vec![], storage, Timing::default(), Instant::now()).unwrap();
        assert_eq!(recovered.current_term(), 1);
        assert_eq!(recovered.last_log_index(), 2);
        assert_eq!(recovered.log_entries()[0].command, b"a");

        // The persisted vote still binds: a *different* candidate in the same term
        // is refused, so a restart can never cause a double-vote.
        let mut recovered = recovered;
        let reply = recovered.handle_request_vote(
            crate::raft::rpc::RequestVoteArgs {
                term: 1,
                candidate_id: 2,
                last_log_index: 2,
                last_log_term: 1,
                pre_vote: false,
            },
            Instant::now(),
        );
        assert!(!reply.vote_granted, "restarted node must honor its persisted vote");
    }
}
