use super::{
    admin_request_to_addr, anti_entropy_budget_remaining_checks, should_run_full_reconcile,
    should_throttle_anti_entropy_repair, NodeHandle,
};
use ditto_protocol::{AdminRequest, AdminResponse, NodeStatus};
use std::{
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

struct AntiEntropyConfig {
    enabled: bool,
    interval_ms: u64,
    min_repair_interval_ms: u64,
    lag_threshold: u64,
    key_sample_size: usize,
    full_reconcile_every: u64,
    full_reconcile_max_keys: usize,
    budget_max_checks_per_run: usize,
    budget_max_duration_ms: u64,
}

struct AntiEntropyPeerState {
    status: NodeStatus,
    am_primary: bool,
    our_index: u64,
    primary_index: u64,
    primary_addr: SocketAddr,
}

fn load_anti_entropy_config(node: &Arc<NodeHandle>) -> AntiEntropyConfig {
    let cfg = node.config.lock().unwrap();
    AntiEntropyConfig {
        enabled: cfg.replication.anti_entropy_enabled,
        interval_ms: cfg.replication.anti_entropy_interval_ms.max(1),
        min_repair_interval_ms: cfg.replication.anti_entropy_min_repair_interval_ms.max(1),
        lag_threshold: cfg.replication.anti_entropy_lag_threshold.max(1),
        key_sample_size: cfg.replication.anti_entropy_key_sample_size,
        full_reconcile_every: cfg.replication.anti_entropy_full_reconcile_every,
        full_reconcile_max_keys: cfg.replication.anti_entropy_full_reconcile_max_keys,
        budget_max_checks_per_run: cfg.replication.anti_entropy_budget_max_checks_per_run,
        budget_max_duration_ms: cfg.replication.anti_entropy_budget_max_duration_ms,
    }
}

async fn load_anti_entropy_peer_state(node: &Arc<NodeHandle>) -> AntiEntropyPeerState {
    let set = node.active_set.lock().await;
    let status = set.local_status();
    let am_primary = set.is_primary();
    let our_index = set
        .all_nodes()
        .iter()
        .find(|n| n.id == set.local_id())
        .map(|n| n.last_applied)
        .unwrap_or(0);
    let primary = {
        let all = set.all_nodes();
        set.primary_id()
            .and_then(|pid| all.into_iter().find(|n| n.id == pid))
            .map(|n| (n.last_applied, SocketAddr::new(n.addr.ip(), n.cluster_port)))
    };
    let (primary_index, primary_addr) = primary.unwrap_or((0, "0.0.0.0:0".parse().unwrap()));
    AntiEntropyPeerState {
        status,
        am_primary,
        our_index,
        primary_index,
        primary_addr,
    }
}

fn mark_anti_entropy_budget_exhausted(node: &Arc<NodeHandle>) {
    node.anti_entropy_budget_exhausted_total
        .fetch_add(1, Ordering::Relaxed);
}

fn anti_entropy_budget_exhausted(
    node: &Arc<NodeHandle>,
    run_started_at: Instant,
    consumed_checks: usize,
    budget_max_checks_per_run: usize,
    budget_max_duration_ms: u64,
) -> bool {
    let exhausted = super::anti_entropy_budget_exhausted(
        run_started_at,
        consumed_checks,
        budget_max_checks_per_run,
        budget_max_duration_ms,
    );
    if exhausted {
        mark_anti_entropy_budget_exhausted(node);
    }
    exhausted
}

async fn trigger_anti_entropy_repair(
    node: &Arc<NodeHandle>,
    min_repair_interval_ms: u64,
    throttled_message: &str,
    trigger_message: String,
) {
    let now_ms = NodeHandle::now_millis();
    let last_ms = node
        .last_anti_entropy_repair_trigger_ms
        .load(Ordering::Relaxed);
    if should_throttle_anti_entropy_repair(now_ms, last_ms, min_repair_interval_ms) {
        node.anti_entropy_repair_throttled_total
            .fetch_add(1, Ordering::Relaxed);
        tracing::debug!("{}", throttled_message);
        return;
    }

    node.last_anti_entropy_repair_trigger_ms
        .store(now_ms, Ordering::Relaxed);
    node.anti_entropy_repair_trigger_total
        .fetch_add(1, Ordering::Relaxed);
    tracing::warn!("{}", trigger_message);
    Arc::clone(node).run_resync().await;
}

pub fn start_anti_entropy(node: Arc<NodeHandle>) {
    tokio::spawn(async move {
        loop {
            let cfg = load_anti_entropy_config(&node);

            if !cfg.enabled {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            tokio::time::sleep(Duration::from_millis(cfg.interval_ms)).await;
            let run_no = node
                .anti_entropy_runs_total
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1);
            let run_started_at = Instant::now();
            let mut consumed_checks = 0usize;

            let peer_state = load_anti_entropy_peer_state(&node).await;

            if peer_state.status != NodeStatus::Active
                || peer_state.am_primary
                || peer_state.primary_addr.port() == 0
            {
                continue;
            }

            let lag = peer_state
                .primary_index
                .saturating_sub(peer_state.our_index);
            node.anti_entropy_last_detected_lag
                .store(lag, Ordering::Relaxed);
            if lag >= cfg.lag_threshold {
                trigger_anti_entropy_repair(
                    &node,
                    cfg.min_repair_interval_ms,
                    &format!(
                        "Anti-entropy: lag={} detected but repair throttled (min_interval_ms={}).",
                        lag, cfg.min_repair_interval_ms
                    ),
                    format!(
                        "Anti-entropy: lag={} (threshold={}) detected; triggering resync.",
                        lag, cfg.lag_threshold
                    ),
                )
                .await;
                continue;
            }

            if anti_entropy_budget_exhausted(
                &node,
                run_started_at,
                consumed_checks,
                cfg.budget_max_checks_per_run,
                cfg.budget_max_duration_ms,
            ) {
                continue;
            }

            if should_run_full_reconcile(run_no, cfg.full_reconcile_every) {
                node.anti_entropy_full_reconcile_runs_total
                    .fetch_add(1, Ordering::Relaxed);
                if let Some((checked, mismatches, budget_exhausted)) =
                    anti_entropy_full_reconcile_once(
                        &node,
                        peer_state.primary_addr,
                        cfg.full_reconcile_max_keys,
                        anti_entropy_budget_remaining_checks(
                            consumed_checks,
                            cfg.budget_max_checks_per_run,
                        ),
                        run_started_at,
                        cfg.budget_max_duration_ms,
                    )
                    .await
                {
                    node.anti_entropy_full_reconcile_key_checks_total
                        .fetch_add(checked, Ordering::Relaxed);
                    consumed_checks = consumed_checks.saturating_add(checked as usize);
                    if budget_exhausted {
                        mark_anti_entropy_budget_exhausted(&node);
                    }
                    if mismatches > 0 {
                        node.anti_entropy_full_reconcile_mismatch_total
                            .fetch_add(mismatches, Ordering::Relaxed);
                        trigger_anti_entropy_repair(
                            &node,
                            cfg.min_repair_interval_ms,
                            &format!(
                                "Anti-entropy: full reconcile mismatch detected but repair throttled (min_interval_ms={}).",
                                cfg.min_repair_interval_ms
                            ),
                            format!(
                                "Anti-entropy: full reconcile found {} mismatches (checked={}, run_no={}); triggering resync.",
                                mismatches, checked, run_no
                            ),
                        )
                        .await;
                        continue;
                    }
                }
            }

            if cfg.key_sample_size == 0 {
                continue;
            }

            if anti_entropy_budget_exhausted(
                &node,
                run_started_at,
                consumed_checks,
                cfg.budget_max_checks_per_run,
                cfg.budget_max_duration_ms,
            ) {
                continue;
            }

            let keys = match admin_request_to_addr(
                &node,
                peer_state.primary_addr,
                AdminRequest::ListKeys { pattern: None },
            )
            .await
            {
                Some(AdminResponse::Keys(keys)) => keys,
                _ => continue,
            };

            let mut checked = 0u64;
            let mut mismatches = 0u64;
            for key in keys.into_iter().take(cfg.key_sample_size) {
                if anti_entropy_budget_exhausted(
                    &node,
                    run_started_at,
                    consumed_checks.saturating_add(checked as usize),
                    cfg.budget_max_checks_per_run,
                    cfg.budget_max_duration_ms,
                ) {
                    break;
                }
                checked = checked.saturating_add(1);
                let local_version = node.store.get(&key).map(|e| e.version);
                let primary_version = match admin_request_to_addr(
                    &node,
                    peer_state.primary_addr,
                    AdminRequest::GetKeyInfo { key: key.clone() },
                )
                .await
                {
                    Some(AdminResponse::KeyInfo { version, .. }) => Some(version),
                    Some(AdminResponse::NotFound) => None,
                    _ => continue,
                };
                if local_version != primary_version {
                    mismatches = mismatches.saturating_add(1);
                }
            }

            node.anti_entropy_key_checks_total
                .fetch_add(checked, Ordering::Relaxed);
            if mismatches > 0 {
                node.anti_entropy_key_mismatch_total
                    .fetch_add(mismatches, Ordering::Relaxed);
                trigger_anti_entropy_repair(
                    &node,
                    cfg.min_repair_interval_ms,
                    &format!(
                        "Anti-entropy: sample mismatch detected but repair throttled (min_interval_ms={}).",
                        cfg.min_repair_interval_ms
                    ),
                    format!(
                        "Anti-entropy: detected {} key-version mismatches in sample (checked={}); triggering resync.",
                        mismatches, checked
                    ),
                )
                .await;
            }
        }
    });
}

async fn anti_entropy_full_reconcile_once(
    node: &Arc<NodeHandle>,
    primary_addr: SocketAddr,
    max_keys: usize,
    max_checks: usize,
    started_at: Instant,
    max_duration_ms: u64,
) -> Option<(u64, u64, bool)> {
    let primary_keys =
        match admin_request_to_addr(node, primary_addr, AdminRequest::ListKeys { pattern: None })
            .await
        {
            Some(AdminResponse::Keys(mut keys)) => {
                keys.sort_unstable();
                keys
            }
            _ => return None,
        };
    let mut local_keys = node.store.keys(None);
    local_keys.sort_unstable();

    let mut i = 0usize;
    let mut j = 0usize;
    let mut checked = 0u64;
    let mut mismatches = 0u64;
    let mut budget_exhausted = false;

    while i < local_keys.len() || j < primary_keys.len() {
        if max_keys > 0 && checked >= max_keys as u64 {
            break;
        }
        if checked >= max_checks as u64
            || (max_duration_ms > 0
                && started_at.elapsed() >= Duration::from_millis(max_duration_ms))
        {
            budget_exhausted = true;
            break;
        }

        match (local_keys.get(i), primary_keys.get(j)) {
            (Some(lk), Some(pk)) if lk == pk => {
                checked = checked.saturating_add(1);
                let local_version = node.store.get(lk).map(|e| e.version);
                let primary_version = match admin_request_to_addr(
                    node,
                    primary_addr,
                    AdminRequest::GetKeyInfo { key: lk.clone() },
                )
                .await
                {
                    Some(AdminResponse::KeyInfo { version, .. }) => Some(version),
                    Some(AdminResponse::NotFound) => None,
                    _ => None,
                };
                if local_version != primary_version {
                    mismatches = mismatches.saturating_add(1);
                }
                i += 1;
                j += 1;
            }
            (Some(lk), Some(pk)) => {
                checked = checked.saturating_add(1);
                if lk < pk {
                    mismatches = mismatches.saturating_add(1);
                    i += 1;
                } else {
                    mismatches = mismatches.saturating_add(1);
                    j += 1;
                }
            }
            (Some(_), None) => {
                checked = checked.saturating_add(1);
                mismatches = mismatches.saturating_add(1);
                i += 1;
            }
            (None, Some(_)) => {
                checked = checked.saturating_add(1);
                mismatches = mismatches.saturating_add(1);
                j += 1;
            }
            (None, None) => break,
        }
    }

    Some((checked, mismatches, budget_exhausted))
}
