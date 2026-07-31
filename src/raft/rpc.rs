//! The two core Raft RPCs as plain Rust values.
//!
//! These are the *logical* messages the consensus code reasons about. They are
//! deliberately independent of any wire format: a later checkpoint defines a
//! `raft.proto` gRPC service and converts to/from these structs at the network
//! boundary, keeping the transport swappable and the core logic transport-free.
//! (InstallSnapshot joins them at the snapshotting checkpoint.)

use super::types::{LogEntry, LogIndex, NodeId, Term};

/// Sent by a candidate to gather votes for a new term (Raft §5.2), and — with
/// `pre_vote` set — as the pre-election probe that guards against disruption (§9.6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestVoteArgs {
    /// Candidate's term. For a pre-vote this is the term the candidate *would*
    /// campaign in (its current term + 1), which it has not actually adopted.
    pub term: Term,
    /// Candidate requesting the vote.
    pub candidate_id: NodeId,
    /// Index of the candidate's last log entry — part of the up-to-date check.
    pub last_log_index: LogIndex,
    /// Term of the candidate's last log entry — the primary up-to-date check.
    pub last_log_term: Term,
    /// True for a PreVote probe: the voter must answer without changing its term,
    /// vote, or election timer, so the ask alone can never disrupt the cluster.
    pub pre_vote: bool,
}

/// A voter's response to [`RequestVoteArgs`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestVoteReply {
    /// Voter's current term, so a stale candidate can step down.
    pub term: Term,
    /// Whether the candidate received this node's vote.
    pub vote_granted: bool,
}

/// Sent by the leader to replicate entries and as a heartbeat (Raft §5.3).
/// An empty `entries` list is a pure heartbeat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendEntriesArgs {
    /// Leader's term.
    pub term: Term,
    /// So followers can redirect clients to the current leader.
    pub leader_id: NodeId,
    /// Index immediately preceding the new entries; the follower must already
    /// hold a matching entry here (the log-consistency check).
    pub prev_log_index: LogIndex,
    /// Term of the entry at `prev_log_index`.
    pub prev_log_term: Term,
    /// New entries to store (empty for a heartbeat).
    pub entries: Vec<LogEntry>,
    /// Leader's commit index, letting the follower advance its own.
    pub leader_commit: LogIndex,
}

/// A follower's response to [`AppendEntriesArgs`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendEntriesReply {
    /// Follower's current term, so a stale leader can step down.
    pub term: Term,
    /// True if the follower's log matched `prev_log_*` and it accepted the entries.
    pub success: bool,
    /// Fast-backtracking hint used when `success` is false, so the leader can
    /// skip a whole conflicting term in one round trip instead of decrementing
    /// `next_index` by one at a time. Populated by the log-repair checkpoint;
    /// `None` here means "no hint, fall back to a single decrement".
    pub conflict_index: Option<LogIndex>,
    /// The conflicting term at `conflict_index`, paired with it for the hint.
    pub conflict_term: Option<Term>,
}

impl AppendEntriesReply {
    /// A plain success carrying the responder's term.
    pub fn success(term: Term) -> Self {
        Self {
            term,
            success: true,
            conflict_index: None,
            conflict_term: None,
        }
    }

    /// A rejection with no backtracking hint (e.g. stale term).
    pub fn rejected(term: Term) -> Self {
        Self {
            term,
            success: false,
            conflict_index: None,
            conflict_term: None,
        }
    }
}

/// Sent by the leader to a follower whose next needed entry the leader has
/// already compacted away (Raft §7). Rather than a stream of chunks, murmur ships
/// the whole snapshot in one message — simple and fine at lab scale.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstallSnapshotArgs {
    /// Leader's term.
    pub term: Term,
    /// So the follower can record who the current leader is.
    pub leader_id: NodeId,
    /// The snapshot replaces every log entry up to and including this index.
    pub last_included_index: LogIndex,
    /// Term of the entry at `last_included_index` — the new log origin's term.
    pub last_included_term: Term,
    /// The opaque state-machine snapshot bytes (the application decodes them).
    pub data: Vec<u8>,
}

/// A follower's response to [`InstallSnapshotArgs`]: just its term, so a stale
/// leader learns it has been superseded and steps down.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstallSnapshotReply {
    pub term: Term,
}
