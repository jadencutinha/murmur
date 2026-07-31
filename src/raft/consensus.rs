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
use super::rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
use super::storage::{PersistentState, RaftStorage};
use super::types::{Applied, Log, LogEntry, LogIndex, NodeId, Role, Term};

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
        let mut cm = Self {
            id,
            peers,
            quorum,
            storage,
            timing,
            current_term: persisted.current_term,
            voted_for: persisted.voted_for,
            log: persisted.log,
            role: Role::Follower,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes: HashSet::new(),
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
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    pub fn peer_ids(&self) -> &[NodeId] {
        &self.peers
    }

    // ---- persistence ----

    fn persist(&self) {
        let state = PersistentState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            log: self.log.clone(),
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
        if newer {
            self.persist();
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
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

    /// Transition to candidate for a new term and produce the RequestVote to
    /// broadcast. Votes for self immediately (which is enough to win a
    /// single-node cluster).
    pub fn start_election(&mut self, now: Instant) -> RequestVoteArgs {
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

    pub fn handle_request_vote(&mut self, args: RequestVoteArgs, now: Instant) -> RequestVoteReply {
        if args.term < self.current_term {
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }
        if args.term > self.current_term {
            self.step_down(args.term);
        }

        // "At least as up-to-date" (Raft §5.4.1): compare last log term first,
        // then length. This is what stops a stale node from becoming leader and
        // clobbering committed entries.
        let up_to_date = args.last_log_term > self.log.last_term()
            || (args.last_log_term == self.log.last_term()
                && args.last_log_index >= self.log.last_index());
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
        }
        self.leader_id = Some(args.leader_id);
        self.reset_election_deadline(now);

        // Log-consistency check: we must already hold prev_log_index at the same
        // term, else our logs diverge and we reject. (The fast conflict hint is
        // added at the log-repair checkpoint.)
        match self.log.term_at(args.prev_log_index) {
            Some(term) if term == args.prev_log_term => {}
            _ => return AppendEntriesReply::rejected(self.current_term),
        }

        // Splice in the leader's entries: skip any prefix we already agree on,
        // truncate the first conflict and everything after it, then append.
        let had_entries = !args.entries.is_empty();
        let mut index = args.prev_log_index;
        for entry in args.entries {
            index += 1;
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
            // Rejection: the consistency check failed. Back up one and retry.
            // (The log-repair checkpoint replaces this with the conflict-hint
            // fast path so a whole divergent term is skipped in one round trip.)
            let next = self.next_index.get(&peer).copied().unwrap_or(1);
            self.next_index.insert(peer, next.saturating_sub(1).max(1));
        }
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

    /// Drain every entry that has become committed but not yet applied, advancing
    /// the apply cursor. Returned in strict index order so the state machine can
    /// apply them one after another. The driver calls this from a single site, so
    /// entries are never delivered out of order or twice.
    pub fn take_applies(&mut self) -> Vec<Applied> {
        let mut out = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            // Committed entries are always present in the log, so this never fails.
            let entry = self
                .log
                .get(self.last_applied)
                .expect("committed entry must exist in the log");
            out.push(Applied {
                index: self.last_applied,
                term: entry.term,
                command: entry.command.clone(),
            });
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
            RequestVoteArgs { term: 5, candidate_id: 2, last_log_index: 0, last_log_term: 0 },
            now,
        );
        assert!(granted.vote_granted);
        // A different candidate in the same term is refused.
        let refused = cm.handle_request_vote(
            RequestVoteArgs { term: 5, candidate_id: 3, last_log_index: 0, last_log_term: 0 },
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
            .save(&PersistentState { current_term, voted_for: None, log })
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
        let applied = cm.take_applies();
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
        assert_eq!(cm.take_applies().len(), 2); // indices 1 and 2 both apply now
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

    #[test]
    fn stale_term_rpcs_are_rejected() {
        let mut cm = module(1, vec![2, 3]);
        cm.start_election(Instant::now()); // now at term 1
        let vote = cm.handle_request_vote(
            RequestVoteArgs { term: 0, candidate_id: 2, last_log_index: 0, last_log_term: 0 },
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
}
