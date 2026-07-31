//! The gRPC boundary for Raft: outbound clients to peers, the inbound service
//! implementation, and the conversions between wire (`proto`) and logical
//! (`super::rpc`) message types. Isolating conversions here keeps
//! [`super::consensus`] free of any protobuf types.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use crate::proto;
use crate::proto::raft_client::RaftClient;
use crate::proto::raft_server::Raft;

use super::consensus::ConsensusModule;
use super::rpc::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply,
};
use super::types::{LogEntry, NodeId};

// ---- wire <-> logical conversions ----

fn entries_to_pb(entries: &[LogEntry]) -> Vec<proto::LogEntry> {
    entries
        .iter()
        .map(|e| proto::LogEntry {
            term: e.term,
            command: e.command.clone(),
        })
        .collect()
}

fn entries_from_pb(entries: Vec<proto::LogEntry>) -> Vec<LogEntry> {
    entries
        .into_iter()
        .map(|e| LogEntry {
            term: e.term,
            command: e.command,
        })
        .collect()
}

fn vote_req_to_pb(a: &RequestVoteArgs) -> proto::RequestVoteRequest {
    proto::RequestVoteRequest {
        term: a.term,
        candidate_id: a.candidate_id,
        last_log_index: a.last_log_index,
        last_log_term: a.last_log_term,
    }
}

fn vote_req_from_pb(r: proto::RequestVoteRequest) -> RequestVoteArgs {
    RequestVoteArgs {
        term: r.term,
        candidate_id: r.candidate_id,
        last_log_index: r.last_log_index,
        last_log_term: r.last_log_term,
    }
}

fn vote_reply_to_pb(r: &RequestVoteReply) -> proto::RequestVoteResponse {
    proto::RequestVoteResponse {
        term: r.term,
        vote_granted: r.vote_granted,
    }
}

fn vote_reply_from_pb(r: proto::RequestVoteResponse) -> RequestVoteReply {
    RequestVoteReply {
        term: r.term,
        vote_granted: r.vote_granted,
    }
}

fn append_req_to_pb(a: &AppendEntriesArgs) -> proto::AppendEntriesRequest {
    proto::AppendEntriesRequest {
        term: a.term,
        leader_id: a.leader_id,
        prev_log_index: a.prev_log_index,
        prev_log_term: a.prev_log_term,
        entries: entries_to_pb(&a.entries),
        leader_commit: a.leader_commit,
    }
}

fn append_req_from_pb(r: proto::AppendEntriesRequest) -> AppendEntriesArgs {
    AppendEntriesArgs {
        term: r.term,
        leader_id: r.leader_id,
        prev_log_index: r.prev_log_index,
        prev_log_term: r.prev_log_term,
        entries: entries_from_pb(r.entries),
        leader_commit: r.leader_commit,
    }
}

fn append_reply_to_pb(r: &AppendEntriesReply) -> proto::AppendEntriesResponse {
    proto::AppendEntriesResponse {
        term: r.term,
        success: r.success,
        has_conflict: r.conflict_index.is_some(),
        conflict_index: r.conflict_index.unwrap_or(0),
        conflict_term: r.conflict_term.unwrap_or(0),
    }
}

fn append_reply_from_pb(r: proto::AppendEntriesResponse) -> AppendEntriesReply {
    AppendEntriesReply {
        term: r.term,
        success: r.success,
        conflict_index: r.has_conflict.then_some(r.conflict_index),
        conflict_term: r.has_conflict.then_some(r.conflict_term),
    }
}

fn install_req_to_pb(a: &InstallSnapshotArgs) -> proto::InstallSnapshotRequest {
    proto::InstallSnapshotRequest {
        term: a.term,
        leader_id: a.leader_id,
        last_included_index: a.last_included_index,
        last_included_term: a.last_included_term,
        data: a.data.clone(),
    }
}

fn install_req_from_pb(r: proto::InstallSnapshotRequest) -> InstallSnapshotArgs {
    InstallSnapshotArgs {
        term: r.term,
        leader_id: r.leader_id,
        last_included_index: r.last_included_index,
        last_included_term: r.last_included_term,
        data: r.data,
    }
}

fn install_reply_to_pb(r: &InstallSnapshotReply) -> proto::InstallSnapshotResponse {
    proto::InstallSnapshotResponse { term: r.term }
}

fn install_reply_from_pb(r: proto::InstallSnapshotResponse) -> InstallSnapshotReply {
    InstallSnapshotReply { term: r.term }
}

// ---- outbound: one lazily-connected client per peer ----

/// Holds a gRPC client for every peer. Connections are lazy, so nodes can be
/// started in any order and reconnect automatically after a peer restarts.
#[derive(Clone)]
pub struct PeerClients {
    clients: HashMap<NodeId, RaftClient<Channel>>,
}

impl PeerClients {
    pub fn connect(peers: &[(NodeId, String)]) -> anyhow::Result<Self> {
        let mut clients = HashMap::new();
        for (id, addr) in peers {
            let endpoint = Endpoint::from_shared(format!("http://{addr}"))?;
            clients.insert(*id, RaftClient::new(endpoint.connect_lazy()));
        }
        Ok(Self { clients })
    }

    pub async fn request_vote(
        &self,
        peer: NodeId,
        args: RequestVoteArgs,
    ) -> anyhow::Result<RequestVoteReply> {
        let mut client = self
            .clients
            .get(&peer)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown peer {peer}"))?;
        let resp = client.request_vote(vote_req_to_pb(&args)).await?;
        Ok(vote_reply_from_pb(resp.into_inner()))
    }

    pub async fn append_entries(
        &self,
        peer: NodeId,
        args: AppendEntriesArgs,
    ) -> anyhow::Result<AppendEntriesReply> {
        let mut client = self
            .clients
            .get(&peer)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown peer {peer}"))?;
        let resp = client.append_entries(append_req_to_pb(&args)).await?;
        Ok(append_reply_from_pb(resp.into_inner()))
    }

    pub async fn install_snapshot(
        &self,
        peer: NodeId,
        args: InstallSnapshotArgs,
    ) -> anyhow::Result<InstallSnapshotReply> {
        let mut client = self
            .clients
            .get(&peer)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown peer {peer}"))?;
        let resp = client.install_snapshot(install_req_to_pb(&args)).await?;
        Ok(install_reply_from_pb(resp.into_inner()))
    }
}

// ---- inbound: serve the Raft RPCs into the shared consensus module ----

/// The tonic service: every incoming RPC locks the shared [`ConsensusModule`],
/// applies it synchronously, and returns the reply. Handlers never await while
/// holding the lock, so a `std::sync::Mutex` is the right (and fastest) choice.
pub struct RaftService {
    core: Arc<Mutex<ConsensusModule>>,
}

impl RaftService {
    pub fn new(core: Arc<Mutex<ConsensusModule>>) -> Self {
        Self { core }
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn request_vote(
        &self,
        request: Request<proto::RequestVoteRequest>,
    ) -> Result<Response<proto::RequestVoteResponse>, Status> {
        let args = vote_req_from_pb(request.into_inner());
        let reply = {
            let mut core = self.core.lock().unwrap();
            core.handle_request_vote(args, Instant::now())
        };
        Ok(Response::new(vote_reply_to_pb(&reply)))
    }

    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesResponse>, Status> {
        let args = append_req_from_pb(request.into_inner());
        let reply = {
            let mut core = self.core.lock().unwrap();
            core.handle_append_entries(args, Instant::now())
        };
        Ok(Response::new(append_reply_to_pb(&reply)))
    }

    async fn install_snapshot(
        &self,
        request: Request<proto::InstallSnapshotRequest>,
    ) -> Result<Response<proto::InstallSnapshotResponse>, Status> {
        let args = install_req_from_pb(request.into_inner());
        let reply = {
            let mut core = self.core.lock().unwrap();
            core.handle_install_snapshot(args, Instant::now())
        };
        Ok(Response::new(install_reply_to_pb(&reply)))
    }
}
