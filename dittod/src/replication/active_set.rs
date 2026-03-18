use ditto_protocol::{NodeInfo, NodeStatus};
use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};
use uuid::Uuid;

/// Manages cluster membership and primary election.
///
/// A node is considered active when its `NodeStatus == Active`.
/// Primary is normally the active node with the lexicographically smallest UUID,
/// but can be overridden by a `ForcePrimary` admin command.
pub struct ActiveSet {
    local_id:       Uuid,
    local_addr:     SocketAddr,
    cluster_port:   u16,
    nodes:          HashMap<Uuid, NodeState>,
    dead_after:     Duration,
    max_nodes:      usize,
    /// Force-elected primary override; None = automatic (smallest UUID).
    pinned_primary: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct NodeState {
    pub info:      NodeInfo,
    pub last_seen: Instant,
}

impl ActiveSet {
    pub fn new(
        local_id:     Uuid,
        local_addr:   SocketAddr,
        cluster_port: u16,
        dead_after_ms: u64,
        max_nodes:    usize,
    ) -> Self {
        let mut set = Self {
            local_id,
            local_addr,
            cluster_port,
            nodes: HashMap::new(),
            dead_after: Duration::from_millis(dead_after_ms),
            max_nodes,
            pinned_primary: None,
        };
        // Register ourselves immediately.
        set.upsert(NodeInfo {
            id:           local_id,
            addr:         local_addr,
            cluster_port,
            status:       NodeStatus::Active,
            last_applied: 0,
        });
        set
    }

    /// Update or insert a node's info (from gossip heartbeat).
    /// Returns false and ignores the node if the cluster is already full
    /// and this is an *unknown* node trying to join.
    pub fn upsert(&mut self, info: NodeInfo) -> bool {
        let is_known = self.nodes.contains_key(&info.id);
        if !is_known && self.nodes.len() >= self.max_nodes {
            tracing::warn!(
                "Cluster is at max capacity ({} nodes). Ignoring join from {}.",
                self.max_nodes, info.id
            );
            return false;
        }
        self.nodes.insert(info.id, NodeState {
            info,
            last_seen: Instant::now(),
        });
        true
    }

    /// Mark nodes as Offline if they haven't sent a heartbeat recently.
    pub fn reap_dead(&mut self) {
        let now = Instant::now();
        for state in self.nodes.values_mut() {
            if state.info.id == self.local_id {
                continue; // never mark ourselves dead
            }
            if state.info.status != NodeStatus::Offline
                && now.duration_since(state.last_seen) > self.dead_after
            {
                tracing::warn!(
                    "Node {} marked OFFLINE (last seen {:?} ago)",
                    state.info.id,
                    now.duration_since(state.last_seen)
                );
                state.info.status = NodeStatus::Offline;
            }
        }
    }

    /// Set the local node's status (e.g. Active ↔ Inactive ↔ Syncing).
    pub fn set_local_status(&mut self, status: NodeStatus) {
        if let Some(state) = self.nodes.get_mut(&self.local_id) {
            state.info.status = status;
        }
    }

    /// Update the local node's `last_applied` index.
    pub fn set_local_applied(&mut self, index: u64) {
        if let Some(state) = self.nodes.get_mut(&self.local_id) {
            state.info.last_applied = index;
        }
    }

    /// All nodes currently considered Active.
    pub fn active_nodes(&self) -> Vec<&NodeInfo> {
        self.nodes
            .values()
            .filter(|s| s.info.status == NodeStatus::Active)
            .map(|s| &s.info)
            .collect()
    }

    /// Active nodes excluding self (for broadcasting writes).
    pub fn active_peers(&self) -> Vec<&NodeInfo> {
        self.active_nodes()
            .into_iter()
            .filter(|n| n.id != self.local_id)
            .collect()
    }

    /// All known nodes (any status), for gossip broadcasting.
    pub fn all_nodes(&self) -> Vec<&NodeInfo> {
        self.nodes.values().map(|s| &s.info).collect()
    }

    /// Primary is normally the Active node with the smallest UUID, but can be
    /// overridden by `set_pinned_primary()`.  If the pinned node is no longer
    /// active, falls back to UUID-based automatic election.
    pub fn primary_id(&self) -> Option<Uuid> {
        if let Some(pinned) = self.pinned_primary {
            if self.active_nodes().iter().any(|n| n.id == pinned) {
                return Some(pinned);
            }
        }
        self.active_nodes()
            .into_iter()
            .map(|n| n.id)
            .min()
    }

    pub fn is_primary(&self) -> bool {
        self.primary_id() == Some(self.local_id)
    }

    /// Override the automatic primary election with a specific node.
    /// Pass the UUID of the node that should become primary.
    pub fn set_pinned_primary(&mut self, id: Uuid) {
        self.pinned_primary = Some(id);
    }

    /// Clear the primary override; restores UUID-based automatic election.
    pub fn clear_pinned_primary(&mut self) {
        self.pinned_primary = None;
    }

    pub fn local_id(&self) -> Uuid {
        self.local_id
    }

    pub fn local_status(&self) -> NodeStatus {
        self.nodes
            .get(&self.local_id)
            .map(|s| s.info.status.clone())
            .unwrap_or(NodeStatus::Offline)
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn max_nodes_reached(&self) -> bool {
        self.nodes.len() >= self.max_nodes
    }

    /// The minimum `last_applied` index across all active nodes.
    /// Used for safe log compaction.
    pub fn min_active_applied(&self) -> u64 {
        self.active_nodes()
            .iter()
            .map(|n| n.last_applied)
            .min()
            .unwrap_or(0)
    }

    /// Snapshot for gossip broadcast.
    pub fn snapshot(&self) -> Vec<NodeInfo> {
        self.nodes.values().map(|s| s.info.clone()).collect()
    }
}
