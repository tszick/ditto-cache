use super::{background, NodeHandle};
use std::sync::Arc;

impl NodeHandle {
    /// Run the startup recovery pass.
    ///
    /// Called once after all server tasks are launched. Waits briefly for gossip
    /// to discover peers, then fetches any missing log entries from the primary node
    /// (falling back to the peer with the highest committed index if the primary is
    /// not yet known). Transitions through Syncing to Active on success, or sets
    /// the node to Inactive if the sync fails.
    ///
    /// If no peers are found and no seeds are configured, the node starts active immediately.
    /// In seeded clusters, waits a short grace window so gossip can discover peers first.
    pub async fn run_recovery(self: Arc<Self>) {
        background::run_recovery(self).await;
    }

    /// Re-sync from peers after a runtime Inactive -> Active transition.
    ///
    /// Re-sync from the primary after a runtime Inactive to Active transition,
    /// after a backup, or when the version-check detects lag.
    ///
    /// Always syncs from the primary node. On success: sets active = true and
    /// NodeStatus::Active. On failure: sets active = false and NodeStatus::Inactive.
    pub async fn run_resync(self: Arc<Self>) {
        background::run_resync(self).await;
    }

    /// Spawn a background task that periodically compacts the write log.
    ///
    /// The safe index is the minimum `last_applied` across every known node.
    /// Offline nodes still need entries they missed while away, so compaction
    /// must retain those entries until the node has recovered. Runs every 30 seconds.
    pub fn start_log_compaction(self: Arc<Self>) {
        background::start_log_compaction(self);
    }

    /// Spawn a background task that periodically checks whether this node's
    /// committed index lags behind the primary's.
    ///
    /// If lag is detected, run_resync is triggered to re-synchronise from the
    /// primary. The check interval is read from config on every iteration so that
    /// runtime changes via SetProperty version-check-interval take effect
    /// without a restart. A value of 0 disables the check entirely.
    pub fn start_version_check(self: Arc<Self>) {
        background::start_version_check(self);
    }

    /// Spawn a periodic anti-entropy task.
    ///
    /// 1) lag-threshold check against primary index
    /// 2) optional key/version sample reconciliation against primary
    /// 3) optional bounded full keyspace reconciliation against primary
    pub fn start_anti_entropy(self: Arc<Self>) {
        background::start_anti_entropy(self);
    }

    /// Periodic rolling-upgrade compatibility probe.
    ///
    /// Queries peer nodes for `protocol-version` via admin RPC and tracks mixed
    /// version observations without affecting write/read paths.
    pub fn start_mixed_version_probe(self: Arc<Self>) {
        background::start_mixed_version_probe(self);
    }
}
