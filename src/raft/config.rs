//! Cluster membership and timing configuration for a Raft node.

use std::time::Duration;

use super::types::NodeId;

/// A peer node: its id and the `host:port` its gRPC server listens on.
#[derive(Clone, Debug)]
pub struct Peer {
    pub id: NodeId,
    pub addr: String,
}

/// Static cluster view from one node's perspective: its own id plus every
/// *other* node. Membership is fixed for now; dynamic changes are out of scope.
#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub id: NodeId,
    pub peers: Vec<Peer>,
}

impl ClusterConfig {
    /// Total nodes in the cluster (peers + self).
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }
}

/// Raft's timing knobs. The election timeout is randomized per-node within
/// `[election_min, election_max]` so nodes rarely start elections simultaneously
/// (the key trick that prevents perpetual split votes). The heartbeat interval
/// must be comfortably shorter than the election minimum so a healthy leader
/// keeps followers from ever timing out.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    pub election_min: Duration,
    pub election_max: Duration,
    pub heartbeat: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            election_min: Duration::from_millis(150),
            election_max: Duration::from_millis(300),
            heartbeat: Duration::from_millis(50),
        }
    }
}

impl Timing {
    /// A fresh randomized election timeout in `[election_min, election_max]`.
    pub fn random_election_timeout(&self) -> Duration {
        let lo = self.election_min.as_millis() as u64;
        let hi = self.election_max.as_millis() as u64;
        Duration::from_millis(fastrand::u64(lo..=hi))
    }
}
