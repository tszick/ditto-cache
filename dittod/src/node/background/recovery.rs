use super::{
    broadcast_synced, mark_active_and_get_index, set_inactive, set_syncing,
    should_wait_for_recovery_peer_discovery, sync_from_peer,
};
use crate::node::NodeHandle;
use std::{
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

pub async fn run_recovery(node: Arc<NodeHandle>) {
    tokio::time::sleep(Duration::from_millis(600)).await;

    let configured_seed_count = {
        let cfg = node.config.lock().unwrap();
        cfg.cluster
            .seeds
            .iter()
            .filter(|s| !s.trim().is_empty())
            .count()
    };

    let mut peers: Vec<SocketAddr> = {
        let set = node.active_set.lock().await;
        set.all_nodes()
            .into_iter()
            .filter(|n| n.id != set.local_id())
            .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port))
            .collect()
    };

    if should_wait_for_recovery_peer_discovery(peers.len(), configured_seed_count) {
        tracing::warn!(
            "Recovery: no peers discovered yet after initial delay; waiting for gossip bootstrap (configured_seeds={}).",
            configured_seed_count
        );
        let wait_until = Instant::now() + Duration::from_millis(3000);
        while peers.is_empty() && Instant::now() < wait_until {
            tokio::time::sleep(Duration::from_millis(250)).await;
            peers = {
                let set = node.active_set.lock().await;
                set.all_nodes()
                    .into_iter()
                    .filter(|n| n.id != set.local_id())
                    .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port))
                    .collect()
            };
        }
        if peers.is_empty() {
            tracing::warn!(
                "Recovery: no peers discovered after bootstrap wait; continuing as standalone for this startup pass."
            );
        } else {
            tracing::info!(
                "Recovery: discovered {} peer(s) after bootstrap wait.",
                peers.len()
            );
        }
    }

    if peers.is_empty() {
        tracing::info!("Recovery: no peers found, assuming single-node cluster.");
        return;
    }

    let our_index = node.write_log.lock().await.committed_index();

    let sync_addr = {
        let set = node.active_set.lock().await;
        let primary_id = set.primary_id();
        let local_id = set.local_id();

        let primary_addr = primary_id.filter(|pid| *pid != local_id).and_then(|pid| {
            set.all_nodes()
                .into_iter()
                .find(|n| n.id == pid)
                .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port))
        });

        let fallback_addr = set
            .all_nodes()
            .into_iter()
            .filter(|n| n.id != local_id)
            .max_by_key(|n| n.last_applied)
            .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port));

        if primary_addr.is_none() && fallback_addr.is_some() {
            tracing::info!(
                "Recovery: local node currently elected primary; probing peer catch-up source."
            );
        }

        primary_addr.or(fallback_addr)
    };

    let peer_addr = match sync_addr {
        Some(a) => a,
        None => {
            tracing::info!("Recovery: already up-to-date (index={}).", our_index);
            return;
        }
    };

    tracing::info!(
        "Recovery: syncing from {} (our index={}).",
        peer_addr,
        our_index
    );

    set_syncing(&node).await;

    match sync_from_peer(&node, peer_addr, our_index).await {
        Ok(count) => {
            tracing::info!("Recovery: applied {} entries.", count);
        }
        Err(e) => {
            tracing::error!(
                "Recovery: failed to sync from {}: {}. Setting node Inactive.",
                peer_addr,
                e
            );
            set_inactive(&node).await;
            return;
        }
    }

    let final_index = mark_active_and_get_index(&node).await;
    tracing::info!("Recovery complete. committed_index={}", final_index);
    broadcast_synced(&node, &peers, final_index).await;
}

pub async fn run_resync(node: Arc<NodeHandle>) {
    let our_index = node.write_log.lock().await.committed_index();

    set_syncing(&node).await;

    let peers: Vec<SocketAddr> = {
        let set = node.active_set.lock().await;
        set.all_nodes()
            .into_iter()
            .filter(|n| n.id != set.local_id())
            .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port))
            .collect()
    };

    let primary_addr = {
        let set = node.active_set.lock().await;
        let primary_id = match set.primary_id() {
            Some(id) if id == set.local_id() => {
                tracing::info!("Resync: we are the primary, rejoining active set.");
                node.active.store(true, Ordering::Relaxed);
                drop(set);
                let final_index = mark_active_and_get_index(&node).await;
                broadcast_synced(&node, &peers, final_index).await;
                return;
            }
            Some(id) => id,
            None => {
                tracing::warn!("Resync: no primary elected. Setting node Inactive.");
                drop(set);
                set_inactive(&node).await;
                return;
            }
        };
        set.all_nodes()
            .into_iter()
            .find(|n| n.id == primary_id)
            .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port))
    };

    let peer_addr = match primary_addr {
        Some(a) => a,
        None => {
            tracing::warn!("Resync: primary address unknown. Setting node Inactive.");
            set_inactive(&node).await;
            return;
        }
    };

    tracing::info!(
        "Resync: at index={}, fetching missed entries from primary {}.",
        our_index,
        peer_addr
    );

    match sync_from_peer(&node, peer_addr, our_index).await {
        Ok(count) => {
            tracing::info!("Resync: applied {} entries.", count);
        }
        Err(e) => {
            tracing::warn!(
                "Resync: failed to sync from primary {}: {}. Setting node Inactive.",
                peer_addr,
                e
            );
            set_inactive(&node).await;
            return;
        }
    }

    node.active.store(true, Ordering::Relaxed);
    let final_index = mark_active_and_get_index(&node).await;
    tracing::info!(
        "Resync complete. Node is Active. committed_index={}",
        final_index
    );
    broadcast_synced(&node, &peers, final_index).await;
}
