//! Core Raft value types: terms, node ids, the log, and node roles.

/// A Raft term — a logical clock that increases across elections. Terms let
/// nodes detect stale leaders and reject out-of-date messages.
pub type Term = u64;

/// A 1-based position in the replicated log. Index 0 is the synthetic origin.
pub type LogIndex = u64;

/// Identifies a node in the cluster. Mapped to a network address by the cluster
/// config (introduced with the election checkpoint).
pub type NodeId = u64;

/// A node's role in the current term. Every node is exactly one of these at a
/// time and transitions between them as terms advance and votes are exchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Passive: responds to leaders and candidates. The default on startup.
    Follower,
    /// Actively soliciting votes to become leader for a new term.
    Candidate,
    /// The single node driving replication for the term.
    Leader,
}

/// One entry in the replicated log: a state-machine command tagged with the
/// term in which the leader created it. The entry's log index is implicit —
/// it is its position within [`Log`], not a stored field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    /// Term of the leader that first appended this entry. Used by the log
    /// consistency check to detect divergence.
    pub term: Term,
    /// Opaque bytes handed to the state machine when the entry commits. murmur
    /// fills this with a serialized KV operation in a later checkpoint; Raft
    /// itself never inspects it.
    pub command: Vec<u8>,
}

impl LogEntry {
    pub fn new(term: Term, command: Vec<u8>) -> Self {
        Self { term, command }
    }
}

/// A committed log entry, in log order, ready to hand to the state machine.
///
/// Produced by [`ConsensusModule::take_applies`](crate::raft::ConsensusModule::take_applies)
/// once an entry is both committed and reached by the apply cursor, then delivered
/// over the node's apply channel. Carrying the index and term (not just the bytes)
/// lets the state machine deduplicate and correlate replies to specific entries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Applied {
    pub index: LogIndex,
    pub term: Term,
    pub command: Vec<u8>,
}

/// A state-machine snapshot handed up to the application: the opaque `data` that
/// captures everything through `last_included_index`. Delivered when a follower
/// installs a leader's snapshot, and once at startup so a restarted node restores
/// the volatile state (dedup table, key set) its compacted log can no longer replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub data: Vec<u8>,
}

/// One item delivered over a node's apply channel, in strict log order: either the
/// next committed command to apply, or a whole snapshot to install in place of the
/// prefix it replaces. Keeping both on one ordered channel means the state machine
/// never applies a command out of order relative to a snapshot that supersedes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Apply {
    Command(Applied),
    Snapshot(Snapshot),
}

/// The replicated log: an ordered list of [`LogEntry`] values addressed by
/// 1-based index.
///
/// All index arithmetic — "what term is at index i", "everything after i",
/// "erase the conflicting suffix" — is centralized here so the higher-level
/// election and replication logic never does raw `Vec` indexing (a rich source
/// of off-by-one bugs in Raft implementations).
///
/// **Compaction / snapshots.** Once the state machine has been captured in a
/// snapshot, the entries it covers are discarded and the log no longer begins at
/// index 1 but at `snapshot_index + 1`. `snapshot_index`/`snapshot_term` record
/// the last entry the snapshot absorbed (the synthetic "entry before the tail"),
/// exactly as index 0 / term 0 stood in for the origin before any compaction —
/// which is just this pair equal to `(0, 0)`. Every accessor accounts for the
/// offset, so callers keep addressing entries by their absolute log index.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Log {
    /// In-memory tail: the entries not yet folded into a snapshot. `entries[i]`
    /// is at absolute index `snapshot_index + 1 + i`.
    entries: Vec<LogEntry>,
    /// Index of the last entry the snapshot covers; the tail starts just past it.
    /// `0` means "no snapshot" (the log still begins at index 1).
    snapshot_index: LogIndex,
    /// Term of the entry at `snapshot_index` — its term is retained even though
    /// the entry itself is gone, so the consistency check still works at the seam.
    snapshot_term: Term,
}

impl Log {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reconstruct a log from stored entries (used when loading persistent state).
    pub fn from_entries(entries: Vec<LogEntry>) -> Self {
        Self { entries, snapshot_index: 0, snapshot_term: 0 }
    }

    /// Reconstruct a compacted log: a tail of `entries` sitting on top of a
    /// snapshot that absorbed everything through `(snapshot_index, snapshot_term)`.
    /// Used when reloading a node that had already snapshotted.
    pub fn from_parts(snapshot_index: LogIndex, snapshot_term: Term, entries: Vec<LogEntry>) -> Self {
        Self { entries, snapshot_index, snapshot_term }
    }

    /// Borrow the raw tail entries (those past the snapshot), e.g. to serialize
    /// for persistence.
    pub fn entries(&self) -> &[LogEntry] {
        &self.entries
    }

    /// Number of entries physically held in memory (the tail). This is what the
    /// state machine watches to decide when the log has grown enough to snapshot.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Index the snapshot covers up to (`0` if none): the last compacted entry.
    pub fn snapshot_index(&self) -> LogIndex {
        self.snapshot_index
    }

    /// Term of the entry at [`snapshot_index`](Self::snapshot_index).
    pub fn snapshot_term(&self) -> Term {
        self.snapshot_term
    }

    /// Index of the last entry, or `snapshot_index` if the tail is empty (which is
    /// `0` on an uncompacted, empty log).
    pub fn last_index(&self) -> LogIndex {
        self.snapshot_index + self.entries.len() as LogIndex
    }

    /// Term of the last entry: the tail's last term, or the snapshot's term if the
    /// tail is empty (term 0 on a fresh log).
    pub fn last_term(&self) -> Term {
        self.entries.last().map_or(self.snapshot_term, |e| e.term)
    }

    /// Term of the entry at `index`. The snapshot boundary yields the snapshot's
    /// term (index 0 on a fresh log yields the origin term 0); an index inside the
    /// compacted range or past the end yields `None` — the caller holds no entry
    /// it can match there.
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == self.snapshot_index {
            Some(self.snapshot_term)
        } else if index > self.snapshot_index && index <= self.last_index() {
            Some(self.entries[(index - self.snapshot_index - 1) as usize].term)
        } else {
            None
        }
    }

    /// Index of the *first* entry with term `term`, or `None` if the tail holds
    /// none. Entries of a term are contiguous in a valid Raft log, so this marks
    /// where that term's run begins — the conflict hint a follower reports so the
    /// leader can rewind past a whole divergent term in one round trip.
    pub fn first_index_of_term(&self, term: Term) -> Option<LogIndex> {
        self.entries
            .iter()
            .position(|e| e.term == term)
            .map(|i| self.snapshot_index + i as LogIndex + 1)
    }

    /// Index of the *last* entry with term `term`, or `None` if absent. The leader
    /// uses this to resume replication just past its own copy of a term it shares
    /// with a lagging follower.
    pub fn last_index_of_term(&self, term: Term) -> Option<LogIndex> {
        self.entries
            .iter()
            .rposition(|e| e.term == term)
            .map(|i| self.snapshot_index + i as LogIndex + 1)
    }

    /// The entry at `index`, or `None` if it is at/below the snapshot boundary or
    /// out of range.
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        if index <= self.snapshot_index || index > self.last_index() {
            None
        } else {
            Some(&self.entries[(index - self.snapshot_index - 1) as usize])
        }
    }

    /// Append one entry, returning its new index.
    pub fn append(&mut self, entry: LogEntry) -> LogIndex {
        self.entries.push(entry);
        self.last_index()
    }

    /// Entries strictly after `index` (indices `index+1 ..= last`). This is the
    /// payload a leader ships in AppendEntries once it knows a follower's
    /// `prev_log_index`. An `index` at or before the snapshot yields the whole
    /// tail; at or beyond the end, an empty slice.
    pub fn entries_after(&self, index: LogIndex) -> &[LogEntry] {
        let start = index
            .saturating_sub(self.snapshot_index)
            .min(self.entries.len() as LogIndex) as usize;
        &self.entries[start..]
    }

    /// Drop every entry after `index`, keeping indices `snapshot_index+1 ..= index`.
    /// Used to erase a conflicting suffix during log repair. An `index` at or below
    /// the snapshot clears the whole tail.
    pub fn truncate_after(&mut self, index: LogIndex) {
        let keep = index.saturating_sub(self.snapshot_index) as usize;
        self.entries.truncate(keep);
    }

    /// Compact the log through `last_index`: discard the entries the snapshot now
    /// covers and advance the snapshot boundary to `(last_index, last_term)`.
    ///
    /// If our tail still holds `last_index` at the matching term, its suffix is
    /// retained (a routine snapshot, or a leader's snapshot of a prefix we share).
    /// Otherwise the snapshot supersedes a log we can't reconcile with it — a
    /// far-behind follower installing the leader's snapshot — so the whole tail is
    /// dropped. A `last_index` at or below the current snapshot is stale and ignored.
    pub fn compact(&mut self, last_index: LogIndex, last_term: Term) {
        if last_index <= self.snapshot_index {
            return;
        }
        if last_index <= self.last_index() && self.term_at(last_index) == Some(last_term) {
            let drop = (last_index - self.snapshot_index) as usize;
            self.entries.drain(..drop);
        } else {
            self.entries.clear();
        }
        self.snapshot_index = last_index;
        self.snapshot_term = last_term;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_with_terms(terms: &[Term]) -> Log {
        Log::from_entries(terms.iter().map(|&t| LogEntry::new(t, vec![])).collect())
    }

    #[test]
    fn empty_log_origin() {
        let log = Log::new();
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert_eq!(log.term_at(0), Some(0)); // synthetic origin
        assert_eq!(log.term_at(1), None);
        assert!(log.get(0).is_none());
    }

    #[test]
    fn term_and_get_by_index() {
        let log = log_with_terms(&[1, 1, 2]);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(2), Some(1));
        assert_eq!(log.term_at(3), Some(2));
        assert_eq!(log.term_at(4), None);
        assert_eq!(log.get(2).unwrap().term, 1);
    }

    #[test]
    fn entries_after_slices_correctly() {
        let log = log_with_terms(&[1, 2, 3]);
        assert_eq!(log.entries_after(0).len(), 3); // everything
        assert_eq!(log.entries_after(1).len(), 2); // indices 2,3
        assert_eq!(log.entries_after(3).len(), 0); // nothing past the end
        assert_eq!(log.entries_after(9).len(), 0); // clamped
        assert_eq!(log.entries_after(1)[0].term, 2);
    }

    #[test]
    fn term_boundaries_locate_a_terms_run() {
        let log = log_with_terms(&[1, 1, 4, 4, 4, 6]);
        assert_eq!(log.first_index_of_term(1), Some(1));
        assert_eq!(log.first_index_of_term(4), Some(3));
        assert_eq!(log.last_index_of_term(4), Some(5));
        assert_eq!(log.first_index_of_term(6), Some(6));
        assert_eq!(log.last_index_of_term(6), Some(6));
        assert_eq!(log.first_index_of_term(2), None); // absent term
        assert_eq!(log.last_index_of_term(9), None);
    }

    #[test]
    fn truncate_erases_suffix() {
        let mut log = log_with_terms(&[1, 2, 3]);
        log.truncate_after(1);
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.last_term(), 1);
        log.truncate_after(0);
        assert!(log.is_empty());
    }

    #[test]
    fn append_returns_new_index() {
        let mut log = Log::new();
        assert_eq!(log.append(LogEntry::new(1, vec![])), 1);
        assert_eq!(log.append(LogEntry::new(1, vec![])), 2);
    }

    // ---- compaction / snapshot offset (checkpoint 10) ----

    #[test]
    fn compact_keeps_the_suffix_and_shifts_the_origin() {
        // Terms 1,1,2,3,3 at indices 1..=5; snapshot through index 3.
        let mut log = log_with_terms(&[1, 1, 2, 3, 3]);
        log.compact(3, 2);
        // The tail now begins at index 4, but indices stay absolute.
        assert_eq!(log.snapshot_index(), 3);
        assert_eq!(log.snapshot_term(), 2);
        assert_eq!(log.len(), 2);
        assert_eq!(log.last_index(), 5);
        assert_eq!(log.last_term(), 3);
        // The boundary reports the snapshot term; compacted indices are gone.
        assert_eq!(log.term_at(3), Some(2));
        assert_eq!(log.term_at(2), None);
        assert!(log.get(3).is_none());
        assert_eq!(log.get(4).unwrap().term, 3);
        assert_eq!(log.term_at(5), Some(3));
    }

    #[test]
    fn appends_and_truncation_respect_the_offset() {
        let mut log = log_with_terms(&[1, 1, 2, 2]);
        log.compact(2, 1); // origin now at index 2
        assert_eq!(log.append(LogEntry::new(4, vec![])), 5);
        assert_eq!(log.entries_after(2).len(), 3); // indices 3,4,5 — the whole tail
        assert_eq!(log.entries_after(3).len(), 2); // indices 4,5
        log.truncate_after(3); // erase indices 4,5
        assert_eq!(log.last_index(), 3);
        log.truncate_after(2); // at the snapshot boundary — clears the tail
        assert!(log.is_empty());
        assert_eq!(log.last_index(), 2); // but the origin holds
        assert_eq!(log.last_term(), 1);
    }

    #[test]
    fn installing_a_snapshot_beyond_the_log_drops_the_tail() {
        // A far-behind follower whose short log the leader's snapshot supersedes.
        let mut log = log_with_terms(&[1, 1]);
        log.compact(9, 5); // snapshot index 9 is past our last index (2)
        assert!(log.is_empty());
        assert_eq!(log.snapshot_index(), 9);
        assert_eq!(log.last_index(), 9);
        assert_eq!(log.term_at(9), Some(5));
        assert_eq!(log.append(LogEntry::new(5, vec![])), 10);
    }

    #[test]
    fn a_stale_compaction_is_ignored() {
        let mut log = log_with_terms(&[1, 2, 3]);
        log.compact(2, 2);
        log.compact(1, 1); // older than the current snapshot — no-op
        assert_eq!(log.snapshot_index(), 2);
        assert_eq!(log.last_index(), 3);
    }
}
