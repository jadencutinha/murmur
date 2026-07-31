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

/// The replicated log: an ordered list of [`LogEntry`] values addressed by
/// 1-based index.
///
/// All index arithmetic — "what term is at index i", "everything after i",
/// "erase the conflicting suffix" — is centralized here so the higher-level
/// election and replication logic never does raw `Vec` indexing (a rich source
/// of off-by-one bugs in Raft implementations).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reconstruct a log from stored entries (used when loading persistent state).
    pub fn from_entries(entries: Vec<LogEntry>) -> Self {
        Self { entries }
    }

    /// Borrow the raw entries, e.g. to serialize for persistence.
    pub fn entries(&self) -> &[LogEntry] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Index of the last entry, or 0 if the log is empty.
    pub fn last_index(&self) -> LogIndex {
        self.entries.len() as LogIndex
    }

    /// Term of the last entry, or 0 if the log is empty.
    pub fn last_term(&self) -> Term {
        self.entries.last().map_or(0, |e| e.term)
    }

    /// Term of the entry at `index`. Index 0 yields the origin term 0; an index
    /// past the end yields `None` (the caller has no matching entry there).
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        match index {
            0 => Some(0),
            i if i <= self.last_index() => Some(self.entries[(i - 1) as usize].term),
            _ => None,
        }
    }

    /// Index of the *first* entry with term `term`, or `None` if the log holds
    /// none. Entries of a term are contiguous in a valid Raft log, so this marks
    /// where that term's run begins — the conflict hint a follower reports so the
    /// leader can rewind past a whole divergent term in one round trip.
    pub fn first_index_of_term(&self, term: Term) -> Option<LogIndex> {
        self.entries
            .iter()
            .position(|e| e.term == term)
            .map(|i| i as LogIndex + 1)
    }

    /// Index of the *last* entry with term `term`, or `None` if absent. The leader
    /// uses this to resume replication just past its own copy of a term it shares
    /// with a lagging follower.
    pub fn last_index_of_term(&self, term: Term) -> Option<LogIndex> {
        self.entries
            .iter()
            .rposition(|e| e.term == term)
            .map(|i| i as LogIndex + 1)
    }

    /// The entry at `index`, or `None` for index 0 or out-of-range.
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        if index == 0 || index > self.last_index() {
            None
        } else {
            Some(&self.entries[(index - 1) as usize])
        }
    }

    /// Append one entry, returning its new index.
    pub fn append(&mut self, entry: LogEntry) -> LogIndex {
        self.entries.push(entry);
        self.last_index()
    }

    /// Entries strictly after `index` (indices `index+1 ..= last`). This is the
    /// payload a leader ships in AppendEntries once it knows a follower's
    /// `prev_log_index`. An `index` at or beyond the end yields an empty slice.
    pub fn entries_after(&self, index: LogIndex) -> &[LogEntry] {
        let start = index.min(self.last_index()) as usize;
        &self.entries[start..]
    }

    /// Drop every entry after `index`, keeping indices `1..=index`. Used to erase
    /// a conflicting suffix during log repair. `index` 0 clears the whole log.
    pub fn truncate_after(&mut self, index: LogIndex) {
        self.entries.truncate(index as usize);
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
}
