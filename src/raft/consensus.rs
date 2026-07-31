//! The Raft consensus state machine — synchronous, single-threaded, and free of
//! any I/O.
//!
//! Everything here mutates in-process state and returns values; the async
//! [`super::node`] driver owns all timers and networking and calls into this
//! module under a mutex. Keeping consensus pure this way means the tricky
//! invariants (term monotonicity, "step down on higher term", the vote rules)
//! live in one place that can be reasoned about — and tested — without a runtime.
//!
//! This checkpoint implements leader election. The AppendEntries *receiver* is
//! already written in full (consistency check + append) so heartbeats work and
//! later checkpoints have less to add, but the leader side only sends heartbeats
//! — no client commands flow yet, so logs stay empty.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use super::config::Timing;
use super::rpc::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply,
};
use super::storage::{PersistentState, RaftStorage};
use super::types::{Apply, Applied, Log, LogEntry, LogIndex, NodeId, Role, Snapshot, Term};

pub struct ConsensusModule {
    // Identity / membership.
    id: NodeId,
    peers: Vec<NodeId>,
    quorum: usize,
    storage: Box<dyn RaftStorage>,
    timing: Timing,

    // Persistent state (mirrored to `storage` via `persist`).
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Log,
    // The opaque state-machine snapshot the log was last compacted against, kept
    // in memory so the leader can ship it to a lagging peer without a disk read.
    // Its (index, term) live on `log` as the snapshot boundary.
    snapshot: Option<Vec<u8>>,

    // Volatile state on all nodes.
    role: Role,
    leader_id: Option<NodeId>,
    commit_index: LogIndex,
    // How far the apply cursor has advanced. Volatile: rebuilt by re-applying the
    // committed log after a restart. Always `<= commit_index`.
    last_applied: LogIndex,

    // Volatile state on the leader (reset on each election win).
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,

    // Volatile state on a candidate.
    votes: HashSet<NodeId>,

    // Volatile state while pre-campaigning (PreVote, §9.6). `pre_candidate` is set
    // between "election timer fired" and "won a pre-vote majority"; `pre_votes`
    // tallies the grants. Neither touches the term, so a pre-election that fails —
    // the disruptive-restart case — leaves the rest of the cluster undisturbed.
    pre_candidate: bool,
    pre_votes: HashSet<NodeId>,

    // When we last heard from a valid leader. Distinct from `election_deadline`
    // (which our own campaigns reset): this drives the PreVote grant rule — we
    // pledge a pre-vote only after a full election timeout with no leader, so a
    // node still hearing heartbeats never helps a disruptor unseat its leader.
    last_leader_contact: Instant,

    // A snapshot waiting to be handed to the state machine on the next
    // `take_applies`: set when we install a leader's snapshot, and once at startup
    // so the application restores the volatile state its compacted log can't replay.
    pending_snapshot: Option<Snapshot>,

    // When the current election timeout expires (meaningful off the leader).
    election_deadline: Instant,
}

impl ConsensusModule {
    /// Build a node, restoring any persisted term/vote/log from `storage` and
    /// arming a fresh randomized election timeout relative to `now`.
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        storage: Box<dyn RaftStorage>,
        timing: Timing,
        now: Instant,
    ) -> anyhow::Result<Self> {
        let persisted = storage.load()?;
        let quorum = (peers.len() + 1) / 2 + 1;
        // Everything the snapshot covers is, by definition, already committed and
        // applied — so the commit and apply cursors start at the snapshot boundary
        // (0 on a fresh log) rather than replaying a prefix that no longer exists.
        let base = persisted.log.snapshot_index();
        // If a snapshot was loaded, queue it for delivery: the state machine must
        // restore its volatile state (dedup table, key set) from it, since the
        // compacted log can no longer be replayed to rebuild that state.
        let pending_snapshot = persisted.snapshot.as_ref().map(|data| Snapshot {
            last_included_index: persisted.log.snapshot_index(),
            last_included_term: persisted.log.snapshot_term(),
            data: data.clone(),
        });
        let mut cm = Self {
            id,
            peers,
            quorum,
            storage,
            timing,
            current_term: persisted.current_term,
            voted_for: persisted.voted_for,
            log: persisted.log,
            snapshot: persisted.snapshot,
            role: Role::Follower,
            leader_id: None,
            commit_index: base,
            last_applied: base,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes: HashSet::new(),
            pre_candidate: false,
            pre_votes: HashSet::new(),
            last_leader_contact: now,
            pending_snapshot,
            election_deadline: now,
        };
        cm.reset_election_deadline(now);
        Ok(cm)
    }

    // ---- observation (used by the driver and tests) ----

    pub fn role(&self) -> Role {
        self.role
    }
    pub fn current_term(&self) -> Term {
        self.current_term
    }
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
    pub fn last_log_index(&self) -> LogIndex {
        self.log.last_index()
    }
    /// Borrow the log entries — for observability, debugging, and tests that
    /// assert two nodes' logs have converged.
    pub fn log_entries(&self) -> &[LogEntry] {
        self.log.entries()
    }
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    pub fn peer_ids(&self) -> &[NodeId] {
        &self.peers
    }
    /// Highest index the state machine has captured in a snapshot (`0` if none).
    pub fn snapshot_index(&self) -> LogIndex {
        self.log.snapshot_index()
    }
    /// Entries physically held in the log tail — what the application watches to
    /// decide the log has grown large enough to warrant a fresh snapshot.
    pub fn raft_log_len(&self) -> usize {
        self.log.len()
    }
    /// Whether replicating to `peer` requires an InstallSnapshot rather than an
    /// AppendEntries: true once the peer's next needed entry has been compacted
    /// away, so we no longer hold the `prev_log_*` an AppendEntries would need.
    pub fn needs_snapshot(&self, peer: NodeId) -> bool {
        self.role == Role::Leader
            && self.snapshot.is_some()
            && self.next_index.get(&peer).copied().unwrap_or(1) <= self.log.snapshot_index()
    }

    // ---- persistence ----

    fn persist(&self) {
        let state = PersistentState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            log: self.log.clone(),
            snapshot: self.snapshot.clone(),
        };
        if let Err(e) = self.storage.save(&state) {
            // A failed persist means we may violate safety after a crash; in a
            // real system this is fatal. At lab scale we surface it loudly.
            eprintln!("raft[{}]: persist failed: {e}", self.id);
        }
    }

    // ---- election timer ----

    fn reset_election_deadline(&mut self, now: Instant) {
        self.election_deadline = now + self.timing.random_election_timeout();
    }

    /// True when a non-leader has heard nothing from a leader/candidate in time
    /// and should start its own election.
    pub fn election_timed_out(&self, now: Instant) -> bool {
        self.role != Role::Leader && now >= self.election_deadline
    }

    // ---- role transitions ----

    /// Revert to follower at `term`. If `term` is strictly newer, adopt it and
    /// forget any vote (a new term means a fresh election).
    fn step_down(&mut self, term: Term) {
        let newer = term > self.current_term;
        if newer {
            self.current_term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
        self.votes.clear();
        self.abandon_pre_election();
        if newer {
            self.persist();
        }
    }

    /// Drop any in-flight pre-election. Called whenever we adopt a leader or a new
    /// term, so a stale pre-campaign never lingers.
    fn abandon_pre_election(&mut self) {
        self.pre_candidate = false;
        self.pre_votes.clear();
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.abandon_pre_election();
        // Optimistically assume peers match our log, then let AppendEntries
        // failures walk `next_index` back (log-repair checkpoint).
        let next = self.log.last_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        for &peer in &self.peers {
            self.next_index.insert(peer, next);
            self.match_index.insert(peer, 0);
        }
    }

    fn maybe_become_leader(&mut self) {
        if self.role == Role::Candidate && self.votes.len() >= self.quorum {
            self.become_leader();
        }
    }

    // ---- candidate side ----

    /// Begin a *pre-election* (Raft §9.6): probe whether a majority would vote for
    /// us in the next term, without adopting it. Arms a fresh election timeout so a
    /// failed pre-election retries, and self-grants (enough to win a single-node
    /// cluster). Critically, nothing here is persisted or increments the term —
    /// asking cannot disturb a healthy leader.
    pub fn start_pre_election(&mut self, now: Instant) -> RequestVoteArgs {
        self.pre_candidate = true;
        self.pre_votes.clear();
        self.pre_votes.insert(self.id);
        self.reset_election_deadline(now);
        RequestVoteArgs {
            // The term we *would* run in, so voters compare against it.
            term: self.current_term + 1,
            candidate_id: self.id,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
            pre_vote: true,
        }
    }

    /// Fold a pre-vote reply into pre-candidate state. A higher term means a real
    /// leadership exists we must yield to; otherwise a grant is tallied. Winning is
    /// observed by the driver via [`pre_vote_succeeded`](Self::pre_vote_succeeded),
    /// which then promotes us to a real election.
    pub fn record_pre_vote_reply(&mut self, from: NodeId, reply: RequestVoteReply) {
        if reply.term > self.current_term {
            self.step_down(reply.term);
            self.leader_id = None;
            return;
        }
        if self.pre_candidate && reply.vote_granted {
            self.pre_votes.insert(from);
        }
    }

    /// Whether we are pre-campaigning and a majority has pledged a pre-vote.
    pub fn pre_vote_succeeded(&self) -> bool {
        self.pre_candidate && self.pre_votes.len() >= self.quorum
    }

    /// Transition to candidate for a new term and produce the RequestVote to
    /// broadcast. Votes for self immediately (which is enough to win a
    /// single-node cluster). Normally reached only after a pre-vote majority.
    pub fn start_election(&mut self, now: Instant) -> RequestVoteArgs {
        self.abandon_pre_election();
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.id);
        self.reset_election_deadline(now);
        self.persist();
        self.maybe_become_leader();
        RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
            pre_vote: false,
        }
    }

    /// Fold a RequestVote reply into candidate state, possibly winning the
    /// election or stepping down on a newer term.
    pub fn record_vote_reply(&mut self, from: NodeId, reply: RequestVoteReply) {
        if reply.term > self.current_term {
            self.step_down(reply.term);
            self.leader_id = None;
            return;
        }
        // Ignore late replies from an election we've already left.
        if self.role != Role::Candidate || reply.term != self.current_term {
            return;
        }
        if reply.vote_granted {
            self.votes.insert(from);
            self.maybe_become_leader();
        }
    }

    // ---- voter side ----

    /// Raft §5.4.1: is a candidate's log at least as up-to-date as ours? Compare
    /// the last entry's term first, then length. This is what stops a node missing
    /// committed entries from winning an election and clobbering them.
    fn log_is_up_to_date(&self, last_log_index: LogIndex, last_log_term: Term) -> bool {
        last_log_term > self.log.last_term()
            || (last_log_term == self.log.last_term() && last_log_index >= self.log.last_index())
    }

    pub fn handle_request_vote(&mut self, args: RequestVoteArgs, now: Instant) -> RequestVoteReply {
        // PreVote (§9.6): answer the probe without touching term, vote, or timer.
        // Grant only if we have heard from no leader for a full election timeout —
        // so we are not shielding a leader we still hear from — the proposed term
        // isn't stale, and the candidate's log is at least as up-to-date. A leader,
        // and any follower still getting heartbeats, therefore deny, which is what
        // denies a disruptor its pre-vote majority. (Unlike a real vote, a pre-vote
        // carries no once-per-term restriction — it is a non-binding promise.)
        if args.pre_vote {
            let leader_is_live = self.role == Role::Leader
                || now.saturating_duration_since(self.last_leader_contact) < self.timing.election_min;
            let grant = args.term >= self.current_term
                && !leader_is_live
                && self.log_is_up_to_date(args.last_log_index, args.last_log_term);
            return RequestVoteReply { term: self.current_term, vote_granted: grant };
        }

        if args.term < self.current_term {
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }
        if args.term > self.current_term {
            self.step_down(args.term);
        }

        let up_to_date = self.log_is_up_to_date(args.last_log_index, args.last_log_term);
        let free_to_vote =
            self.voted_for.is_none() || self.voted_for == Some(args.candidate_id);

        if free_to_vote && up_to_date {
            self.voted_for = Some(args.candidate_id);
            self.reset_election_deadline(now); // granting a vote is "hearing from" a candidate
            self.persist();
            RequestVoteReply {
                term: self.current_term,
                vote_granted: true,
            }
        } else {
            RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            }
        }
    }

    // ---- follower side ----

    pub fn handle_append_entries(
        &mut self,
        args: AppendEntriesArgs,
        now: Instant,
    ) -> AppendEntriesReply {
        if args.term < self.current_term {
            return AppendEntriesReply::rejected(self.current_term);
        }
        // A valid leader for this term: adopt its term if newer, and in any case
        // yield to it (a candidate that sees a current-term leader steps down).
        if args.term > self.current_term {
            self.step_down(args.term);
        } else {
            self.role = Role::Follower;
            self.votes.clear();
            self.abandon_pre_election();
        }
        self.leader_id = Some(args.leader_id);
        self.last_leader_contact = now;
        self.reset_election_deadline(now);

        // Log-consistency check: we must already hold prev_log_index at the same
        // term, else our logs diverge and we reject — with a hint that tells the
        // leader where our disagreement begins so it can rewind in one step. A
        // prev_log_index at or below our snapshot boundary is already covered by
        // the snapshot, so it matches by construction and skips the check.
        if args.prev_log_index > self.log.snapshot_index() {
            match self.log.term_at(args.prev_log_index) {
                Some(term) if term == args.prev_log_term => {}
                _ => return self.append_conflict(args.prev_log_index),
            }
        }

        // Splice in the leader's entries: skip any prefix already absorbed by our
        // snapshot or that we already agree on, truncate the first conflict and
        // everything after it, then append.
        let had_entries = !args.entries.is_empty();
        let mut index = args.prev_log_index;
        for entry in args.entries {
            index += 1;
            if index <= self.log.snapshot_index() {
                continue; // already covered by our snapshot
            }
            match self.log.term_at(index) {
                Some(term) if term == entry.term => {} // already present and consistent
                Some(_) => {
                    self.log.truncate_after(index - 1);
                    self.log.append(entry);
                }
                None => {
                    self.log.append(entry);
                }
            }
        }
        if had_entries {
            self.persist();
        }

        if args.leader_commit > self.commit_index {
            self.commit_index = args.leader_commit.min(self.log.last_index());
        }

        AppendEntriesReply::success(self.current_term)
    }

    /// Build the rejection for a failed consistency check, carrying a fast-
    /// backtracking hint (Raft §5.3, "students' guide" optimization):
    ///
    /// - **Our log is too short** (we have nothing at `prev_log_index`): point the
    ///   leader at the first index we are missing, `last_index + 1`, with no term.
    /// - **Term mismatch**: report the conflicting term and the first index at
    ///   which it appears, so the leader can skip the entire term in one round trip
    ///   instead of decrementing one index at a time.
    fn append_conflict(&self, prev_log_index: LogIndex) -> AppendEntriesReply {
        match self.log.term_at(prev_log_index) {
            None => AppendEntriesReply {
                term: self.current_term,
                success: false,
                conflict_index: Some(self.log.last_index() + 1),
                conflict_term: None,
            },
            Some(conflict_term) => AppendEntriesReply {
                term: self.current_term,
                success: false,
                // The term is present, so its first index always exists.
                conflict_index: self.log.first_index_of_term(conflict_term),
                conflict_term: Some(conflict_term),
            },
        }
    }

    // ---- leader side ----

    /// Build the AppendEntries to send `peer`, based on where we believe the
    /// peer's log ends (`next_index`). In this checkpoint the log is always
    /// empty so this is a pure heartbeat; the machinery is ready for real
    /// entries at the replication checkpoint.
    pub fn append_args_for(&self, peer: NodeId) -> AppendEntriesArgs {
        let next = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or(self.log.last_index() + 1);
        let prev_log_index = next - 1;
        let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);
        AppendEntriesArgs {
            term: self.current_term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries: self.log.entries_after(prev_log_index).to_vec(),
            leader_commit: self.commit_index,
        }
    }

    /// Append a client command to the leader's log and return its index, or
    /// `None` if this node is not the leader (the caller must redirect). The entry
    /// is stamped with the current term and persisted before we return, so a crash
    /// right after acknowledging can never lose it. Replication to peers is driven
    /// separately by the tick loop via [`append_args_for`](Self::append_args_for).
    pub fn submit_command(&mut self, command: Vec<u8>) -> Option<LogIndex> {
        if self.role != Role::Leader {
            return None;
        }
        let index = self.log.append(LogEntry::new(self.current_term, command));
        self.persist();
        // Covers a single-node cluster (quorum of one): the entry is already
        // "replicated to a majority" the moment it lands, so commit right away.
        self.maybe_advance_commit();
        Some(index)
    }

    /// Handle an AppendEntries reply. Enforces the universal "step down on higher
    /// term" rule, then on success advances our view of the peer's log
    /// (`match_index`/`next_index`) and recomputes the commit index; on rejection
    /// it backs `next_index` up so the next attempt reaches further into the past.
    ///
    /// `match_hint` is how far *this* message would have advanced the peer if
    /// accepted (`prev_log_index + entries.len()`), captured by the driver before
    /// the send so a reply can update `match_index` without re-deriving it.
    pub fn handle_append_reply(
        &mut self,
        peer: NodeId,
        reply: AppendEntriesReply,
        match_hint: LogIndex,
    ) {
        if reply.term > self.current_term {
            self.step_down(reply.term);
            self.leader_id = None;
            return;
        }
        // Ignore replies to a message from a term we've since left (we may have
        // stepped down and come back, or the reply is simply stale).
        if self.role != Role::Leader || reply.term != self.current_term {
            return;
        }

        if reply.success {
            // `match_index` only ever moves forward — a delayed reply carrying a
            // smaller hint must not rewind it.
            let matched = self.match_index.get(&peer).copied().unwrap_or(0).max(match_hint);
            self.match_index.insert(peer, matched);
            self.next_index.insert(peer, matched + 1);
            self.maybe_advance_commit();
        } else {
            // Rejection: the consistency check failed. Use the follower's conflict
            // hint to rewind `next_index` past the whole divergent region in one
            // round trip, falling back to a single decrement if no hint is present.
            let next = self.next_index_after_conflict(peer, &reply);
            self.next_index.insert(peer, next.max(1));
        }
    }

    /// Where to resume replicating to a follower that rejected us, decoded from
    /// its conflict hint (see [`append_conflict`](Self::append_conflict)):
    ///
    /// - **Term hint present**: if *we* also hold that term, resume just past our
    ///   last entry of it (our prefix of the term is authoritative and agrees);
    ///   otherwise we have nothing from that term, so drop back to the follower's
    ///   first index of it.
    /// - **Index-only hint** (follower's log was too short): jump straight there.
    /// - **No hint**: single decrement (a peer from before this checkpoint).
    fn next_index_after_conflict(&self, peer: NodeId, reply: &AppendEntriesReply) -> LogIndex {
        match (reply.conflict_term, reply.conflict_index) {
            (Some(term), Some(conflict_index)) => match self.log.last_index_of_term(term) {
                Some(last) => last + 1,
                None => conflict_index,
            },
            (None, Some(conflict_index)) => conflict_index,
            // No hint: decrement one step from where we currently stand.
            _ => self.next_index.get(&peer).copied().unwrap_or(1).saturating_sub(1),
        }
    }

    // ---- snapshotting / log compaction ----

    /// Compact the log against a state-machine snapshot the application has
    /// captured through `index`. Discards the entries the snapshot now covers and
    /// records `data` as the snapshot to persist and to ship to lagging peers.
    ///
    /// The application only ever snapshots state it has applied, so `index` is
    /// always `<= last_applied` and still present in the log; a stale or
    /// not-yet-applied `index` is ignored. Persisted before returning: the compacted
    /// log and its snapshot must hit disk together to stay mutually consistent.
    pub fn snapshot(&mut self, index: LogIndex, data: Vec<u8>) {
        if index <= self.log.snapshot_index() || index > self.last_applied {
            return;
        }
        // The entry is applied, hence still in the log, so its term is present.
        let term = self
            .log
            .term_at(index)
            .expect("cannot snapshot past an entry we no longer hold");
        self.log.compact(index, term);
        self.snapshot = Some(data);
        self.persist();
    }

    /// Build the InstallSnapshot to catch `peer` up. Only meaningful when
    /// [`needs_snapshot`](Self::needs_snapshot) is true, so a snapshot exists.
    pub fn install_snapshot_args(&self, _peer: NodeId) -> InstallSnapshotArgs {
        InstallSnapshotArgs {
            term: self.current_term,
            leader_id: self.id,
            last_included_index: self.log.snapshot_index(),
            last_included_term: self.log.snapshot_term(),
            data: self.snapshot.clone().unwrap_or_default(),
        }
    }

    /// Receive an InstallSnapshot from the leader (Raft §7). Adopt the leader's
    /// term, replace our log prefix with the snapshot, and queue it for the state
    /// machine — unless it is stale (we have already applied at least that far).
    pub fn handle_install_snapshot(
        &mut self,
        args: InstallSnapshotArgs,
        now: Instant,
    ) -> InstallSnapshotReply {
        if args.term < self.current_term {
            return InstallSnapshotReply { term: self.current_term };
        }
        if args.term > self.current_term {
            self.step_down(args.term);
        } else {
            self.role = Role::Follower;
            self.votes.clear();
            self.abandon_pre_election();
        }
        self.leader_id = Some(args.leader_id);
        self.last_leader_contact = now;
        self.reset_election_deadline(now);

        // Stale: we already hold (and have applied) everything this snapshot
        // covers, so installing it would throw away newer committed state.
        if args.last_included_index <= self.commit_index {
            return InstallSnapshotReply { term: self.current_term };
        }

        // Install: compact the log to the snapshot boundary (retaining a matching
        // tail if we have one, else dropping the log wholesale) and record it.
        self.log.compact(args.last_included_index, args.last_included_term);
        self.snapshot = Some(args.data.clone());
        self.commit_index = args.last_included_index;
        self.last_applied = args.last_included_index;
        self.persist();

        // Hand the snapshot to the state machine on the next drain; the apply
        // cursor is already at its boundary, so no command precedes it.
        self.pending_snapshot = Some(Snapshot {
            last_included_index: args.last_included_index,
            last_included_term: args.last_included_term,
            data: args.data,
        });
        InstallSnapshotReply { term: self.current_term }
    }

    /// Fold an InstallSnapshot reply into leader state: step down on a higher term,
    /// otherwise advance our view of the peer to the snapshot we just shipped.
    pub fn handle_install_snapshot_reply(
        &mut self,
        peer: NodeId,
        reply: InstallSnapshotReply,
        last_included_index: LogIndex,
    ) {
        if reply.term > self.current_term {
            self.step_down(reply.term);
            self.leader_id = None;
            return;
        }
        if self.role != Role::Leader || reply.term != self.current_term {
            return;
        }
        let matched = self
            .match_index
            .get(&peer)
            .copied()
            .unwrap_or(0)
            .max(last_included_index);
        self.match_index.insert(peer, matched);
        self.next_index.insert(peer, matched + 1);
        self.maybe_advance_commit();
    }

    /// Advance `commit_index` to the highest log index replicated on a majority,
    /// subject to the current-term restriction (Raft §5.4.2): a leader may only
    /// commit an entry from an *earlier* term as a side effect of committing one
    /// of its own. Counting replicas of a stale entry directly would be unsafe, so
    /// we require `log[N].term == current_term`.
    fn maybe_advance_commit(&mut self) {
        // Walk down from the tip; the first index meeting both conditions is the
        // highest committable one.
        let mut n = self.log.last_index();
        while n > self.commit_index {
            if self.log.term_at(n) == Some(self.current_term) && self.is_replicated_on_majority(n) {
                self.commit_index = n;
                return;
            }
            n -= 1;
        }
    }

    /// True if index `n` is stored on a majority of the cluster (this leader plus
    /// every peer whose `match_index` has reached `n`).
    fn is_replicated_on_majority(&self, n: LogIndex) -> bool {
        let replicas = 1 // the leader holds it by definition
            + self
                .peers
                .iter()
                .filter(|p| self.match_index.get(p).copied().unwrap_or(0) >= n)
                .count();
        replicas >= self.quorum
    }

    /// Drain everything the state machine should apply next, in strict order: a
    /// pending snapshot first (it resets the machine to its boundary), then every
    /// entry that has become committed but not yet applied, advancing the apply
    /// cursor. The driver calls this from a single site, so items are never
    /// delivered out of order or twice.
    pub fn take_applies(&mut self) -> Vec<Apply> {
        let mut out = Vec::new();
        // A snapshot supersedes the prefix it covers; deliver it before any command
        // so the machine never applies past state on top of a reset it hasn't seen.
        if let Some(snapshot) = self.pending_snapshot.take() {
            out.push(Apply::Snapshot(snapshot));
        }
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            // Committed entries are always present in the log, so this never fails.
            let entry = self
                .log
                .get(self.last_applied)
                .expect("committed entry must exist in the log");
            out.push(Apply::Command(Applied {
                index: self.last_applied,
                term: entry.term,
                command: entry.command.clone(),
            }));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::storage::InMemoryStorage;

    fn module(id: NodeId, peers: Vec<NodeId>) -> ConsensusModule {
        ConsensusModule::new(
            id,
            peers,
            Box::new(InMemoryStorage::new()),
            Timing::default(),
            Instant::now(),
        )
        .unwrap()
    }

    /// Keep only the command applies (dropping any snapshot deliveries), so tests
    /// that care about committed entries can address them positionally.
    fn commands(applies: Vec<Apply>) -> Vec<Applied> {
        applies
            .into_iter()
            .filter_map(|a| match a {
                Apply::Command(c) => Some(c),
                Apply::Snapshot(_) => None,
            })
            .collect()
    }

    #[test]
    fn starting_an_election_bumps_term_and_self_votes() {
        let mut cm = module(1, vec![2, 3]);
        let args = cm.start_election(Instant::now());
        assert_eq!(args.term, 1);
        assert_eq!(cm.role(), Role::Candidate);
        assert_eq!(cm.current_term(), 1);
        // Self-vote alone is not a majority of 3.
        assert!(!cm.is_leader());
    }

    #[test]
    fn majority_votes_win_leadership() {
        let mut cm = module(1, vec![2, 3]);
        cm.start_election(Instant::now());
        cm.record_vote_reply(2, RequestVoteReply { term: 1, vote_granted: true });
        // Two of three (self + node 2) is a majority.
        assert!(cm.is_leader());
        assert_eq!(cm.leader_id(), Some(1));
    }

    #[test]
    fn a_node_votes_once_per_term() {
        let mut cm = module(1, vec![2, 3]);
        let now = Instant::now();
        let granted = cm.handle_request_vote(
            RequestVoteArgs { term: 5, candidate_id: 2, last_log_index: 0, last_log_term: 0, pre_vote: false },
            now,
        );
        assert!(granted.vote_granted);
        // A different candidate in the same term is refused.
        let refused = cm.handle_request_vote(
            RequestVoteArgs { term: 5, candidate_id: 3, last_log_index: 0, last_log_term: 0, pre_vote: false },
            now,
        );
        assert!(!refused.vote_granted);
    }

    #[test]
    fn higher_term_makes_a_leader_step_down() {
        let mut cm = module(1, vec![2, 3]);
        cm.start_election(Instant::now());
        cm.record_vote_reply(2, RequestVoteReply { term: 1, vote_granted: true });
        assert!(cm.is_leader());

        // A heartbeat from a newer term forces us back to follower.
        let reply = cm.handle_append_entries(
            AppendEntriesArgs {
                term: 2,
                leader_id: 3,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
            Instant::now(),
        );
        assert!(reply.success);
        assert_eq!(cm.role(), Role::Follower);
        assert_eq!(cm.current_term(), 2);
        assert_eq!(cm.leader_id(), Some(3));
    }

    /// Drive `cm` to leadership of a 3-node cluster (self + one granted vote).
    fn elect_leader(cm: &mut ConsensusModule) {
        cm.start_election(Instant::now());
        cm.record_vote_reply(2, RequestVoteReply { term: cm.current_term(), vote_granted: true });
        assert!(cm.is_leader());
    }

    /// Build a module whose persisted log already holds `terms`, at `current_term`.
    fn module_with_log(id: NodeId, peers: Vec<NodeId>, current_term: Term, terms: &[Term]) -> ConsensusModule {
        let storage = InMemoryStorage::new();
        let log = Log::from_entries(terms.iter().map(|&t| LogEntry::new(t, vec![])).collect());
        storage
            .save(&PersistentState { current_term, voted_for: None, log, snapshot: None })
            .unwrap();
        ConsensusModule::new(id, peers, Box::new(storage), Timing::default(), Instant::now()).unwrap()
    }

    #[test]
    fn only_the_leader_accepts_commands() {
        let mut cm = module(1, vec![2, 3]);
        assert_eq!(cm.submit_command(b"noop".to_vec()), None); // follower rejects
        elect_leader(&mut cm);
        assert_eq!(cm.submit_command(b"set x=1".to_vec()), Some(1));
        assert_eq!(cm.last_log_index(), 1);
    }

    #[test]
    fn commit_advances_once_a_majority_matches() {
        let mut cm = module(1, vec![2, 3]);
        elect_leader(&mut cm);
        cm.submit_command(b"a".to_vec());
        // Only the leader holds it: no majority yet.
        assert_eq!(cm.commit_index(), 0);

        // One peer acknowledges -> two of three hold it -> committed.
        cm.handle_append_reply(2, AppendEntriesReply::success(cm.current_term()), 1);
        assert_eq!(cm.commit_index(), 1);

        // The committed entry drains exactly once, in order.
        let applied = commands(cm.take_applies());
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].index, 1);
        assert_eq!(applied[0].command, b"a");
        assert!(cm.take_applies().is_empty());
    }

    #[test]
    fn a_leader_never_commits_a_prior_term_by_count_alone() {
        // Log holds one entry from term 1; we win term 2 with it already present.
        let mut cm = module_with_log(1, vec![2, 3], 1, &[1]);
        elect_leader(&mut cm); // now term 2, leader, log = [t1]

        // A majority replicates the old entry — but §5.4.2 forbids committing it
        // on replica count alone, so commit stays put.
        cm.handle_append_reply(2, AppendEntriesReply::success(cm.current_term()), 1);
        assert_eq!(cm.commit_index(), 0);

        // Appending and committing a current-term entry carries the old one with it.
        cm.submit_command(b"t2".to_vec()); // index 2, term 2
        cm.handle_append_reply(2, AppendEntriesReply::success(cm.current_term()), 2);
        assert_eq!(cm.commit_index(), 2);
        assert_eq!(commands(cm.take_applies()).len(), 2); // indices 1 and 2 both apply now
    }

    #[test]
    fn rejected_append_backs_off_next_index() {
        // Two pre-existing entries, so on election next_index[peer] starts at 3.
        let mut cm = module_with_log(1, vec![2, 3], 5, &[5, 5]);
        elect_leader(&mut cm);
        assert_eq!(cm.append_args_for(2).prev_log_index, 2); // next_index[2] == 3

        let rejection = AppendEntriesReply::rejected(cm.current_term());
        cm.handle_append_reply(2, rejection.clone(), 0);
        // One decrement per rejection; floors at 1 (prev_log_index 0), never below.
        assert_eq!(cm.append_args_for(2).prev_log_index, 1);
        cm.handle_append_reply(2, rejection.clone(), 0);
        assert_eq!(cm.append_args_for(2).prev_log_index, 0);
        cm.handle_append_reply(2, rejection, 0);
        assert_eq!(cm.append_args_for(2).prev_log_index, 0); // stays at the floor
    }

    // ---- log-repair (checkpoint 6) ----

    #[test]
    fn follower_hints_its_length_when_log_is_too_short() {
        // Follower holds one entry; leader probes an index beyond it.
        let mut f = module_with_log(2, vec![1, 3], 1, &[1]);
        let reply = f.handle_append_entries(
            AppendEntriesArgs {
                term: 1,
                leader_id: 1,
                prev_log_index: 3,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 0,
            },
            Instant::now(),
        );
        assert!(!reply.success);
        assert_eq!(reply.conflict_index, Some(2)); // first index we lack
        assert_eq!(reply.conflict_term, None); // length hint carries no term
    }

    #[test]
    fn follower_hints_the_whole_conflicting_term() {
        // Terms 1,1,2,2 — a probe that mismatches at index 4 should rewind the
        // leader to the *start* of the conflicting term (index 3), not index 4.
        let mut f = module_with_log(2, vec![1, 3], 2, &[1, 1, 2, 2]);
        let reply = f.handle_append_entries(
            AppendEntriesArgs {
                term: 5,
                leader_id: 1,
                prev_log_index: 4,
                prev_log_term: 9, // we have term 2 there, not 9
                entries: vec![],
                leader_commit: 0,
            },
            Instant::now(),
        );
        assert!(!reply.success);
        assert_eq!(reply.conflict_term, Some(2));
        assert_eq!(reply.conflict_index, Some(3)); // first index of term 2
    }

    #[test]
    fn leader_skips_past_its_own_copy_of_a_shared_term() {
        // Leader log terms 4,4,5: it shares term 4 with the follower.
        let mut cm = module_with_log(1, vec![2, 3], 5, &[4, 4, 5]);
        elect_leader(&mut cm);
        let term = cm.current_term();
        // Follower reports a conflict on term 4 starting at index 2.
        cm.handle_append_reply(
            2,
            AppendEntriesReply {
                term,
                success: false,
                conflict_index: Some(2),
                conflict_term: Some(4),
            },
            0,
        );
        // We hold term 4 through index 2, so resume at index 3 (prev_log_index 2).
        assert_eq!(cm.append_args_for(2).prev_log_index, 2);
    }

    #[test]
    fn leader_adopts_follower_index_for_a_term_it_lacks() {
        let mut cm = module_with_log(1, vec![2, 3], 5, &[4, 4, 5]);
        elect_leader(&mut cm);
        let term = cm.current_term();
        // Conflict on term 9, which the leader has never seen.
        cm.handle_append_reply(
            2,
            AppendEntriesReply {
                term,
                success: false,
                conflict_index: Some(2),
                conflict_term: Some(9),
            },
            0,
        );
        // Fall all the way back to the follower's first index of that term.
        assert_eq!(cm.append_args_for(2).prev_log_index, 1); // next_index == 2
    }

    /// Replay one leader↔follower AppendEntries exchange at a time until the
    /// follower accepts, mirroring what the driver does over the wire.
    fn repair_until_success(leader: &mut ConsensusModule, follower: &mut ConsensusModule, follower_id: NodeId) {
        for _ in 0..64 {
            let args = leader.append_args_for(follower_id);
            let match_hint = args.prev_log_index + args.entries.len() as u64;
            let reply = follower.handle_append_entries(args, Instant::now());
            let success = reply.success;
            leader.handle_append_reply(follower_id, reply, match_hint);
            if success {
                return;
            }
        }
        panic!("logs did not converge within the round budget");
    }

    #[test]
    fn a_divergent_follower_converges_to_the_leaders_log() {
        // Raft paper, Figure 7, case (f): the follower has a long divergent tail
        // (terms 2 and 3) that must be erased and replaced by the leader's.
        let mut leader = module_with_log(1, vec![2], 6, &[1, 1, 1, 4, 4, 5, 5, 6, 6, 6]);
        elect_leader(&mut leader);
        let mut follower = module_with_log(2, vec![1], 3, &[1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3]);

        repair_until_success(&mut leader, &mut follower, 2);

        // Byte-for-byte identical logs: the conflicting suffix is gone and the
        // leader's entries (now at the leader's election term) are in place.
        assert_eq!(follower.log_entries(), leader.log_entries());
    }

    #[test]
    fn stale_term_rpcs_are_rejected() {
        let mut cm = module(1, vec![2, 3]);
        cm.start_election(Instant::now()); // now at term 1
        let vote = cm.handle_request_vote(
            RequestVoteArgs { term: 0, candidate_id: 2, last_log_index: 0, last_log_term: 0, pre_vote: false },
            Instant::now(),
        );
        assert!(!vote.vote_granted);
        let append = cm.handle_append_entries(
            AppendEntriesArgs {
                term: 0,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
            Instant::now(),
        );
        assert!(!append.success);
    }

    // ---- snapshotting / compaction (checkpoint 10) ----

    #[test]
    fn snapshotting_compacts_the_log_and_keeps_serving() {
        let mut cm = module(1, vec![2, 3]);
        elect_leader(&mut cm); // term 1
        for cmd in [b"a", b"b", b"c"] {
            cm.submit_command(cmd.to_vec());
        }
        // A majority replicates through index 3, committing and applying it.
        cm.handle_append_reply(2, AppendEntriesReply::success(cm.current_term()), 3);
        assert_eq!(cm.commit_index(), 3);
        assert_eq!(commands(cm.take_applies()).len(), 3);

        // Snapshot through index 2: the tail shrinks but absolute indexing holds.
        cm.snapshot(2, b"snap@2".to_vec());
        assert_eq!(cm.snapshot_index(), 2);
        assert_eq!(cm.raft_log_len(), 1); // only index 3 survives in the tail
        assert_eq!(cm.last_log_index(), 3);

        // The leader keeps appending and committing on top of the compacted log.
        cm.submit_command(b"d".to_vec()); // index 4
        cm.handle_append_reply(2, AppendEntriesReply::success(cm.current_term()), 4);
        assert_eq!(cm.commit_index(), 4);
        assert_eq!(commands(cm.take_applies()).len(), 1); // just index 4
    }

    #[test]
    fn snapshotting_an_unapplied_or_stale_index_is_ignored() {
        let mut cm = module(1, vec![2, 3]);
        elect_leader(&mut cm);
        cm.submit_command(b"a".to_vec()); // index 1, not yet applied
        cm.snapshot(1, b"x".to_vec()); // index 1 > last_applied (0): ignored
        assert_eq!(cm.snapshot_index(), 0);
        assert_eq!(cm.raft_log_len(), 1);
    }

    #[test]
    fn a_follower_installs_a_snapshot_and_queues_it_for_apply() {
        // Follower holds a short two-entry log the leader has long since compacted.
        let mut f = module_with_log(2, vec![1, 3], 4, &[1, 1]);
        let reply = f.handle_install_snapshot(
            InstallSnapshotArgs {
                term: 5,
                leader_id: 1,
                last_included_index: 6,
                last_included_term: 4,
                data: b"leader-state".to_vec(),
            },
            Instant::now(),
        );
        assert_eq!(reply.term, 5); // adopted the leader's newer term
        assert_eq!(f.current_term(), 5);
        assert_eq!(f.leader_id(), Some(1));
        // The snapshot supersedes the whole short log; cursors jump to its boundary.
        assert_eq!(f.snapshot_index(), 6);
        assert_eq!(f.last_log_index(), 6);
        assert_eq!(f.commit_index(), 6);
        assert_eq!(f.last_applied(), 6);

        // It is delivered exactly once, with no stale command trailing it.
        let applies = f.take_applies();
        assert_eq!(applies.len(), 1);
        match &applies[0] {
            Apply::Snapshot(s) => {
                assert_eq!(s.last_included_index, 6);
                assert_eq!(s.data, b"leader-state");
            }
            other => panic!("expected a snapshot delivery, got {other:?}"),
        }
    }

    #[test]
    fn a_stale_snapshot_is_rejected_without_regressing() {
        // We have already committed and applied through index 3.
        let mut cm = module(1, vec![2, 3]);
        elect_leader(&mut cm);
        for cmd in [b"a", b"b", b"c"] {
            cm.submit_command(cmd.to_vec());
        }
        cm.handle_append_reply(2, AppendEntriesReply::success(cm.current_term()), 3);
        commands(cm.take_applies());
        assert_eq!(cm.commit_index(), 3);

        // A snapshot only reaching index 2 must not roll our applied state back.
        cm.handle_install_snapshot(
            InstallSnapshotArgs {
                term: cm.current_term(),
                leader_id: 1,
                last_included_index: 2,
                last_included_term: 1,
                data: b"old".to_vec(),
            },
            Instant::now(),
        );
        assert_eq!(cm.commit_index(), 3);
        assert_eq!(cm.snapshot_index(), 0); // nothing compacted
        assert!(cm.take_applies().is_empty()); // no spurious re-delivery
    }

    #[test]
    fn install_snapshot_reply_advances_the_peer_view() {
        let mut cm = module(1, vec![2, 3]);
        elect_leader(&mut cm);
        // We shipped peer 2 a snapshot through index 7; the reply moves its cursor.
        cm.handle_install_snapshot_reply(2, InstallSnapshotReply { term: cm.current_term() }, 7);
        assert_eq!(cm.append_args_for(2).prev_log_index, 7); // next_index[2] == 8
    }

    #[test]
    fn a_restart_with_a_snapshot_replays_no_prefix_and_delivers_it() {
        let storage = InMemoryStorage::new();
        let log = Log::from_parts(5, 3, vec![LogEntry::new(3, b"tail".to_vec())]);
        storage
            .save(&PersistentState {
                current_term: 4,
                voted_for: Some(1),
                log,
                snapshot: Some(b"restored-image".to_vec()),
            })
            .unwrap();
        let mut cm =
            ConsensusModule::new(2, vec![1, 3], Box::new(storage), Timing::default(), Instant::now())
                .unwrap();

        // Cursors resume at the snapshot boundary — the compacted prefix, which no
        // longer exists, is never replayed.
        assert_eq!(cm.snapshot_index(), 5);
        assert_eq!(cm.commit_index(), 5);
        assert_eq!(cm.last_applied(), 5);
        assert_eq!(cm.last_log_index(), 6);

        // The loaded snapshot is delivered so the app can restore volatile state.
        let applies = cm.take_applies();
        assert_eq!(applies.len(), 1);
        assert!(matches!(applies[0], Apply::Snapshot(_)));
    }

    // ---- pre-vote / anti-disruption (checkpoint 11) ----

    fn pre_vote(term: Term, candidate: NodeId, last_index: LogIndex, last_term: Term) -> RequestVoteArgs {
        RequestVoteArgs {
            term,
            candidate_id: candidate,
            last_log_index: last_index,
            last_log_term: last_term,
            pre_vote: true,
        }
    }

    #[test]
    fn a_pre_vote_is_denied_while_a_leader_is_live_and_changes_nothing() {
        let mut cm = module(1, vec![2, 3]);
        // A heartbeat from leader 2 resets our election timer: we hear a leader.
        cm.handle_append_entries(
            AppendEntriesArgs {
                term: 4,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
            Instant::now(),
        );
        assert_eq!(cm.current_term(), 4);

        // A pre-vote arriving now is refused (our timer hasn't expired), and it
        // must not perturb our term, vote, or role.
        let reply = cm.handle_request_vote(pre_vote(5, 3, 0, 0), Instant::now());
        assert!(!reply.vote_granted);
        assert_eq!(reply.term, 4);
        assert_eq!(cm.current_term(), 4); // no term inflation from the probe
        assert_eq!(cm.role(), Role::Follower);
    }

    #[test]
    fn a_pre_vote_is_granted_once_the_leader_is_gone() {
        let mut cm = module(1, vec![2, 3]);
        // Simulate the election timer having expired by asking with a future `now`.
        let later = Instant::now() + std::time::Duration::from_secs(10);
        let reply = cm.handle_request_vote(pre_vote(1, 2, 0, 0), later);
        assert!(reply.vote_granted);
        assert_eq!(cm.current_term(), 0); // still no state change — a grant is a promise
        assert_eq!(cm.voted_for, None);
    }

    #[test]
    fn a_pre_vote_from_a_stale_log_is_denied() {
        // We hold two entries; a pre-candidate with an empty log is not up-to-date,
        // so it is denied even though our timer has expired.
        let mut cm = module_with_log(1, vec![2, 3], 5, &[5, 5]);
        let later = Instant::now() + std::time::Duration::from_secs(10);
        let reply = cm.handle_request_vote(pre_vote(6, 2, 0, 0), later);
        assert!(!reply.vote_granted);
    }

    #[test]
    fn a_pre_vote_majority_promotes_to_a_real_election() {
        let mut cm = module(1, vec![2, 3]);
        cm.start_pre_election(Instant::now());
        assert!(!cm.pre_vote_succeeded()); // only our own pre-vote so far
        assert_eq!(cm.current_term(), 0); // pre-campaigning never bumps the term

        cm.record_pre_vote_reply(2, RequestVoteReply { term: 0, vote_granted: true });
        assert!(cm.pre_vote_succeeded()); // self + node 2 is a majority of three

        // Only the real election that follows advances the term and self-votes.
        let args = cm.start_election(Instant::now());
        assert_eq!(args.term, 1);
        assert!(!args.pre_vote);
        assert_eq!(cm.current_term(), 1);
        assert_eq!(cm.role(), Role::Candidate);
    }

    #[test]
    fn a_pre_candidate_yields_to_a_higher_term() {
        let mut cm = module(1, vec![2, 3]);
        cm.start_pre_election(Instant::now());
        // A reply revealing a newer term means real leadership exists elsewhere.
        cm.record_pre_vote_reply(2, RequestVoteReply { term: 7, vote_granted: false });
        assert_eq!(cm.current_term(), 7);
        assert!(!cm.pre_vote_succeeded());
        assert_eq!(cm.role(), Role::Follower);
    }
}
