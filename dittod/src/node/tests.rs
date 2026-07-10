use super::{
    background, observability, util::stable_node_uuid, CircuitState, ClientRequestSource,
    GetFlight, NodeHandle, TokenBucket, HOT_KEY_STATE_MAX, NAMESPACE_LATENCY_STATE_MAX,
    PROTOCOL_VERSION,
};
use crate::config::{Config, WriteQuorumMode};
use crate::store::kv_store::ExportEntry;
use crate::store::KvStore;
use bytes::Bytes;
use ditto_protocol::{
    AdminRequest, AdminResponse, ClientRequest, ClientResponse, ClusterMessage, ErrorCode,
    LogEntry, NamespaceQuotaUsage, NodeStatus,
};
use sha2::{Digest, Sha256};
use std::sync::{atomic::Ordering, Arc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn test_node(mut cfg: Config) -> Arc<NodeHandle> {
    cfg.node.id = "test-node".into();
    test_node_with_config_path(cfg, "test-node.toml".into())
}

fn test_node_with_config_path(mut cfg: Config, config_path: String) -> Arc<NodeHandle> {
    cfg.node.id = "test-node".into();
    let store = Arc::new(KvStore::new(
        cfg.cache.max_memory_mb,
        cfg.cache.default_ttl_secs,
        cfg.cache.value_size_limit_bytes,
        cfg.cache.max_keys,
        cfg.compression.enabled,
        cfg.compression.threshold_bytes,
    ));
    NodeHandle::new(cfg, config_path, store, None, "127.0.0.1".into())
}

fn temp_backup_dir(tag: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut path = std::env::temp_dir();
    path.push(format!("ditto-{}-{}-{}", tag, std::process::id(), nanos));
    std::fs::create_dir_all(&path).expect("create temp backup dir");
    path.to_string_lossy().into_owned()
}

fn write_snapshot_with_checksum(path: &std::path::Path, data: &[u8]) {
    std::fs::write(path, data).expect("write snapshot file");
    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hex::encode(hasher.finalize());
    let mut sidecar = path.to_path_buf();
    let sidecar_ext = match sidecar.extension().and_then(|e| e.to_str()) {
        Some(ext) if !ext.is_empty() => format!("{ext}.sha256"),
        _ => "sha256".to_string(),
    };
    sidecar.set_extension(sidecar_ext);
    std::fs::write(sidecar, format!("{digest}\n")).expect("write snapshot checksum sidecar");
}

#[test]
fn node_identity_is_stable_for_configured_node_id() {
    let node_a = stable_node_uuid("node-2");
    let node_b = stable_node_uuid("node-2");
    let node_c = stable_node_uuid("node-3");

    assert_eq!(node_a, node_b);
    assert_ne!(node_a, node_c);
}

#[test]
fn persistence_states_default_is_fully_disabled() {
    let cfg = Config::default();
    let (platform, runtime, enabled, backup, export, import) = NodeHandle::persistence_states(&cfg);
    assert!(!platform);
    assert!(!runtime);
    assert!(!enabled);
    assert!(!backup);
    assert!(!export);
    assert!(!import);
}

#[test]
fn persistence_states_require_platform_and_runtime() {
    let mut cfg = Config::default();
    cfg.persistence.platform_allowed = true;
    cfg.persistence.runtime_enabled = false;
    cfg.persistence.backup_allowed = true;
    cfg.persistence.export_allowed = true;
    cfg.persistence.import_allowed = true;

    let (_platform, _runtime, enabled, backup, export, import) =
        NodeHandle::persistence_states(&cfg);
    assert!(!enabled);
    assert!(!backup);
    assert!(!export);
    assert!(!import);
}

#[test]
fn persistence_states_apply_feature_flags_after_global_enable() {
    let mut cfg = Config::default();
    cfg.persistence.platform_allowed = true;
    cfg.persistence.runtime_enabled = true;
    cfg.persistence.backup_allowed = true;
    cfg.persistence.export_allowed = false;
    cfg.persistence.import_allowed = true;

    let (_platform, _runtime, enabled, backup, export, import) =
        NodeHandle::persistence_states(&cfg);
    assert!(enabled);
    assert!(backup);
    assert!(!export);
    assert!(import);
}

#[test]
fn anti_entropy_full_reconcile_schedule_helper() {
    assert!(!background::should_run_full_reconcile(0, 10));
    assert!(!background::should_run_full_reconcile(9, 10));
    assert!(background::should_run_full_reconcile(10, 10));
    assert!(background::should_run_full_reconcile(20, 10));
    assert!(!background::should_run_full_reconcile(20, 0));
}

#[test]
fn anti_entropy_repair_throttle_helper() {
    assert!(background::should_throttle_anti_entropy_repair(
        10_000, 9_500, 1_000
    ));
    assert!(!background::should_throttle_anti_entropy_repair(
        10_000, 8_000, 1_000
    ));
}

#[test]
fn read_repair_budget_helper_limits_per_minute() {
    let cfg = Config::default();
    let node = test_node(cfg);
    let now = 1_000_000_u64;
    assert!(node.try_consume_read_repair_budget(now, 2));
    assert!(node.try_consume_read_repair_budget(now + 100, 2));
    assert!(!node.try_consume_read_repair_budget(now + 200, 2));
    assert!(node.try_consume_read_repair_budget(now + 61_000, 2));
}

#[test]
fn anti_entropy_budget_helper_respects_checks_and_duration() {
    let started = Instant::now();
    assert!(!background::anti_entropy_budget_exhausted(started, 1, 2, 0));
    assert!(background::anti_entropy_budget_exhausted(started, 2, 2, 0));

    let started_old = Instant::now() - Duration::from_millis(20);
    assert!(background::anti_entropy_budget_exhausted(
        started_old,
        0,
        0,
        10
    ));
}

#[test]
fn mixed_version_response_classifier() {
    let ok = Some(AdminResponse::Properties(vec![(
        "protocol-version".into(),
        PROTOCOL_VERSION.to_string(),
    )]));
    assert_eq!(
        background::classify_mixed_version_response(ok),
        (false, false)
    );

    let mismatch = Some(AdminResponse::Properties(vec![(
        "protocol-version".into(),
        (PROTOCOL_VERSION + 1).to_string(),
    )]));
    assert_eq!(
        background::classify_mixed_version_response(mismatch),
        (true, false)
    );

    let missing = Some(AdminResponse::Properties(vec![(
        "other".into(),
        "x".into(),
    )]));
    assert_eq!(
        background::classify_mixed_version_response(missing),
        (true, false)
    );

    let error = Some(AdminResponse::Error {
        message: "boom".into(),
    });
    assert_eq!(
        background::classify_mixed_version_response(error),
        (false, true)
    );
}

#[tokio::test]
async fn restore_snapshot_is_blocked_when_import_gate_is_disabled() {
    let backup_dir = temp_backup_dir("restore-gate");
    let mut cfg = Config::default();
    cfg.backup.path = backup_dir.clone();
    cfg.persistence.platform_allowed = true;
    cfg.persistence.runtime_enabled = true;
    cfg.persistence.import_allowed = false;
    let node = test_node(cfg);

    let resp = Arc::clone(&node)
        .handle_admin(AdminRequest::RestoreLatestSnapshot)
        .await;
    match resp {
        AdminResponse::Error { message } => {
            assert!(message.contains("Restore is disabled by persistence policy"));
        }
        other => panic!("expected AdminResponse::Error, got {:?}", other),
    }
    let stats = node.stats().await;
    assert_eq!(stats.snapshot_restore_attempt_total, 1);
    assert_eq!(stats.snapshot_restore_success_total, 0);
    assert_eq!(stats.snapshot_restore_failure_total, 1);
    assert_eq!(stats.snapshot_restore_not_found_total, 0);
    assert_eq!(stats.snapshot_restore_policy_block_total, 1);

    let _ = std::fs::remove_dir_all(backup_dir);
}

#[tokio::test]
async fn restore_snapshot_loads_entries_and_updates_stats() {
    let backup_dir = temp_backup_dir("restore-success");
    let mut cfg = Config::default();
    cfg.backup.path = backup_dir.clone();
    cfg.persistence.platform_allowed = true;
    cfg.persistence.runtime_enabled = true;
    cfg.persistence.import_allowed = true;
    let node = test_node(cfg);

    let snapshot_entries = vec![(
        "snap:key".to_string(),
        ExportEntry {
            value: b"snap-value".to_vec(),
            version: 42,
            expires_at_ms: None,
        },
    )];
    let data = serde_json::to_vec(&snapshot_entries).expect("serialize snapshot");
    let file =
        std::path::Path::new(&backup_dir).join("test-node_backup_2099.01.01_00-00-00_UTC.json");
    write_snapshot_with_checksum(&file, &data);

    let resp = Arc::clone(&node)
        .handle_admin(AdminRequest::RestoreLatestSnapshot)
        .await;
    match resp {
        AdminResponse::RestoreResult { entries, .. } => assert_eq!(entries, 1),
        other => panic!("expected AdminResponse::RestoreResult, got {:?}", other),
    }

    let restored = node.store.get("snap:key").expect("restored key missing");
    assert_eq!(restored.value, Bytes::from_static(b"snap-value"));
    assert_eq!(restored.version, 42);

    let stats = node.stats().await;
    assert_eq!(stats.snapshot_last_load_entries, 1);
    assert!(stats.snapshot_last_load_path.is_some());
    assert!(stats.snapshot_last_load_age_secs.is_some());
    assert_eq!(stats.snapshot_restore_attempt_total, 1);
    assert_eq!(stats.snapshot_restore_success_total, 1);
    assert_eq!(stats.snapshot_restore_failure_total, 0);
    assert_eq!(stats.snapshot_restore_not_found_total, 0);
    assert_eq!(stats.snapshot_restore_policy_block_total, 0);

    let _ = std::fs::remove_dir_all(backup_dir);
}

#[tokio::test]
async fn restore_snapshot_rejects_file_above_size_limit() {
    let backup_dir = temp_backup_dir("restore-size-limit");
    let mut cfg = Config::default();
    cfg.backup.path = backup_dir.clone();
    cfg.backup.max_snapshot_bytes = 4;
    cfg.persistence.platform_allowed = true;
    cfg.persistence.runtime_enabled = true;
    cfg.persistence.import_allowed = true;
    let node = test_node(cfg);

    let snapshot_entries = vec![(
        "snap:key".to_string(),
        ExportEntry {
            value: b"snap-value".to_vec(),
            version: 42,
            expires_at_ms: None,
        },
    )];
    let data = serde_json::to_vec(&snapshot_entries).expect("serialize snapshot");
    let file =
        std::path::Path::new(&backup_dir).join("test-node_backup_2099.01.01_00-00-00_UTC.json");
    write_snapshot_with_checksum(&file, &data);

    let resp = Arc::clone(&node)
        .handle_admin(AdminRequest::RestoreLatestSnapshot)
        .await;
    match resp {
        AdminResponse::Error { message } => {
            assert!(message.contains("max_snapshot_bytes"));
        }
        other => panic!("expected AdminResponse::Error, got {:?}", other),
    }

    let stats = node.stats().await;
    assert_eq!(stats.snapshot_restore_failure_total, 1);

    let _ = std::fs::remove_dir_all(backup_dir);
}

#[tokio::test]
async fn restore_snapshot_rejects_entry_count_above_limit() {
    let backup_dir = temp_backup_dir("restore-entry-limit");
    let mut cfg = Config::default();
    cfg.backup.path = backup_dir.clone();
    cfg.backup.max_restore_entries = 1;
    cfg.persistence.platform_allowed = true;
    cfg.persistence.runtime_enabled = true;
    cfg.persistence.import_allowed = true;
    let node = test_node(cfg);

    let snapshot_entries = vec![
        (
            "snap:a".to_string(),
            ExportEntry {
                value: b"a".to_vec(),
                version: 1,
                expires_at_ms: None,
            },
        ),
        (
            "snap:b".to_string(),
            ExportEntry {
                value: b"b".to_vec(),
                version: 2,
                expires_at_ms: None,
            },
        ),
    ];
    let data = serde_json::to_vec(&snapshot_entries).expect("serialize snapshot");
    let file =
        std::path::Path::new(&backup_dir).join("test-node_backup_2099.01.01_00-00-00_UTC.json");
    write_snapshot_with_checksum(&file, &data);

    let resp = Arc::clone(&node)
        .handle_admin(AdminRequest::RestoreLatestSnapshot)
        .await;
    match resp {
        AdminResponse::Error { message } => {
            assert!(message.contains("max_restore_entries"));
        }
        other => panic!("expected AdminResponse::Error, got {:?}", other),
    }
    assert!(node.store.get("snap:a").is_none());
    assert!(node.store.get("snap:b").is_none());

    let _ = std::fs::remove_dir_all(backup_dir);
}

#[tokio::test]
async fn restore_snapshot_invalid_file_increments_failure_counter() {
    let backup_dir = temp_backup_dir("restore-invalid");
    let mut cfg = Config::default();
    cfg.backup.path = backup_dir.clone();
    cfg.persistence.platform_allowed = true;
    cfg.persistence.runtime_enabled = true;
    cfg.persistence.import_allowed = true;
    let node = test_node(cfg);

    let file =
        std::path::Path::new(&backup_dir).join("test-node_backup_2099.01.01_00-00-00_UTC.bin");
    write_snapshot_with_checksum(&file, b"not-protobuf");

    let resp = Arc::clone(&node)
        .handle_admin(AdminRequest::RestoreLatestSnapshot)
        .await;
    match resp {
        AdminResponse::Error { .. } => {}
        other => panic!("expected AdminResponse::Error, got {:?}", other),
    }

    let stats = node.stats().await;
    assert_eq!(stats.snapshot_restore_attempt_total, 1);
    assert_eq!(stats.snapshot_restore_success_total, 0);
    assert_eq!(stats.snapshot_restore_failure_total, 1);
    assert_eq!(stats.snapshot_restore_not_found_total, 0);
    assert_eq!(stats.snapshot_restore_policy_block_total, 0);

    let _ = std::fs::remove_dir_all(backup_dir);
}

#[tokio::test]
async fn restore_snapshot_missing_checksum_sidecar_increments_failure_counter() {
    let backup_dir = temp_backup_dir("restore-missing-checksum");
    let mut cfg = Config::default();
    cfg.backup.path = backup_dir.clone();
    cfg.persistence.platform_allowed = true;
    cfg.persistence.runtime_enabled = true;
    cfg.persistence.import_allowed = true;
    let node = test_node(cfg);

    let file =
        std::path::Path::new(&backup_dir).join("test-node_backup_2099.01.01_00-00-00_UTC.bin");
    std::fs::write(&file, b"opaque-backup-payload").expect("write snapshot without checksum");

    let resp = Arc::clone(&node)
        .handle_admin(AdminRequest::RestoreLatestSnapshot)
        .await;
    match resp {
        AdminResponse::Error { message } => {
            assert!(message.contains("missing backup checksum sidecar"));
        }
        other => panic!("expected AdminResponse::Error, got {:?}", other),
    }

    let stats = node.stats().await;
    assert_eq!(stats.snapshot_restore_attempt_total, 1);
    assert_eq!(stats.snapshot_restore_success_total, 0);
    assert_eq!(stats.snapshot_restore_failure_total, 1);
    assert_eq!(stats.snapshot_restore_not_found_total, 0);
    assert_eq!(stats.snapshot_restore_policy_block_total, 0);

    let _ = std::fs::remove_dir_all(backup_dir);
}

#[test]
fn token_bucket_limits_and_refills() {
    let mut bucket = TokenBucket::new(2, 2);
    assert!(bucket.try_take());
    assert!(bucket.try_take());
    assert!(!bucket.try_take());
    std::thread::sleep(Duration::from_millis(600));
    assert!(bucket.try_take());
}

#[test]
fn token_bucket_burst_cap_under_load() {
    let mut bucket = TokenBucket::new(10, 10);
    let mut allowed = 0usize;
    for _ in 0..100 {
        if bucket.try_take() {
            allowed += 1;
        }
    }
    assert_eq!(allowed, 10);
}

#[test]
fn write_quorum_peer_ack_requirement_all_active() {
    assert_eq!(
        NodeHandle::required_prepare_peer_acks(WriteQuorumMode::AllActive, 0),
        0
    );
    assert_eq!(
        NodeHandle::required_prepare_peer_acks(WriteQuorumMode::AllActive, 3),
        3
    );
}

#[test]
fn write_quorum_peer_ack_requirement_majority() {
    // total active = peers + primary(local)
    assert_eq!(
        NodeHandle::required_prepare_peer_acks(WriteQuorumMode::Majority, 0),
        0
    ); // 1/1
    assert_eq!(
        NodeHandle::required_prepare_peer_acks(WriteQuorumMode::Majority, 1),
        1
    ); // 2/2
    assert_eq!(
        NodeHandle::required_prepare_peer_acks(WriteQuorumMode::Majority, 2),
        1
    ); // 2/3
    assert_eq!(
        NodeHandle::required_prepare_peer_acks(WriteQuorumMode::Majority, 3),
        2
    ); // 3/4
    assert_eq!(
        NodeHandle::required_prepare_peer_acks(WriteQuorumMode::Majority, 4),
        2
    ); // 3/5
}

#[test]
fn recovery_waits_for_peer_discovery_only_in_seeded_clusters() {
    assert!(background::should_wait_for_recovery_peer_discovery(0, 1));
    assert!(background::should_wait_for_recovery_peer_discovery(0, 3));
    assert!(!background::should_wait_for_recovery_peer_discovery(1, 3));
    assert!(!background::should_wait_for_recovery_peer_discovery(0, 0));
}

#[test]
fn hot_key_adaptive_waiter_limit_scales_down_on_timeout_and_back_on_success() {
    let mut cfg = Config::default();
    cfg.hot_key.max_waiters = 16;
    cfg.hot_key.adaptive_waiters_enabled = true;
    cfg.hot_key.adaptive_min_waiters = 2;
    cfg.hot_key.adaptive_success_threshold = 2;
    cfg.hot_key.adaptive_state_max_keys = 64;
    let node = test_node(cfg);

    let initial = node.hot_key_adaptive_waiter_limit("k1", true, 16, 2, 64);
    assert_eq!(initial, 16);

    node.hot_key_adaptive_on_timeout("k1", true, 16, 2);
    assert_eq!(node.hot_key_adaptive_waiter_limit("k1", true, 16, 2, 64), 8);

    node.hot_key_adaptive_on_timeout("k1", true, 16, 2);
    assert_eq!(node.hot_key_adaptive_waiter_limit("k1", true, 16, 2, 64), 4);

    node.hot_key_adaptive_on_timeout("k1", true, 16, 2);
    assert_eq!(node.hot_key_adaptive_waiter_limit("k1", true, 16, 2, 64), 2);

    node.hot_key_adaptive_on_success("k1", true, 16, 2, 2);
    assert_eq!(node.hot_key_adaptive_waiter_limit("k1", true, 16, 2, 64), 2);
    node.hot_key_adaptive_on_success("k1", true, 16, 2, 2);
    assert_eq!(node.hot_key_adaptive_waiter_limit("k1", true, 16, 2, 64), 3);

    assert!(
        node.hot_key_adaptive_limit_decrease_total
            .load(Ordering::Relaxed)
            >= 3
    );
    assert!(
        node.hot_key_adaptive_limit_increase_total
            .load(Ordering::Relaxed)
            >= 1
    );
}

#[tokio::test]
async fn client_observability_counters_track_latency_and_error_categories() {
    let node = test_node(Config::default());

    node.observe_client_response_metrics(
        ClientRequestSource::Tcp,
        Duration::from_millis(1),
        &ClientResponse::Pong,
        Some("tenant-a"),
        Some("tenant-a::k1"),
    );
    node.observe_client_response_metrics(
        ClientRequestSource::Http,
        Duration::from_millis(12),
        &ClientResponse::Error {
            code: ErrorCode::RateLimited,
            message: "synthetic".into(),
        },
        Some("tenant-a"),
        Some("tenant-a::k1"),
    );
    node.observe_client_response_metrics(
        ClientRequestSource::Internal,
        Duration::from_millis(250),
        &ClientResponse::Error {
            code: ErrorCode::NoQuorum,
            message: "synthetic".into(),
        },
        Some("tenant-b"),
        Some("tenant-b::k2"),
    );
    node.observe_client_response_metrics(
        ClientRequestSource::Http,
        Duration::from_millis(750),
        &ClientResponse::Error {
            code: ErrorCode::InternalError,
            message: "synthetic".into(),
        },
        None,
        None,
    );

    let stats = node.stats().await;
    assert_eq!(stats.client_requests_total, 4);
    assert_eq!(stats.client_requests_tcp_total, 1);
    assert_eq!(stats.client_requests_http_total, 2);
    assert_eq!(stats.client_requests_internal_total, 1);
    assert_eq!(stats.client_request_latency_le_1ms_total, 1);
    assert_eq!(stats.client_request_latency_le_20ms_total, 1);
    assert_eq!(stats.client_request_latency_le_500ms_total, 1);
    assert_eq!(stats.client_request_latency_gt_500ms_total, 1);
    assert_eq!(stats.client_latency_p50_estimate_ms, Some(20));
    assert_eq!(stats.client_latency_p90_estimate_ms, Some(1000));
    assert_eq!(stats.client_error_total, 3);
    assert_eq!(stats.client_errors_tcp_total, 0);
    assert_eq!(stats.client_errors_http_total, 2);
    assert_eq!(stats.client_errors_internal_total, 1);
    assert_eq!(stats.client_error_throttle_total, 1);
    assert_eq!(stats.client_error_availability_total, 1);
    assert_eq!(stats.client_error_internal_total, 1);
    assert_eq!(stats.namespace_latency_top.len(), 2);
    assert_eq!(stats.namespace_latency_top[0].namespace, "tenant-a");
    assert_eq!(stats.namespace_latency_top[0].request_total, 2);
    assert_eq!(stats.hot_key_top_usage.len(), 2);
    assert_eq!(stats.hot_key_top_usage[0].key, "tenant-a::k1");
    assert_eq!(stats.hot_key_top_usage[0].request_total, 2);
}

#[test]
fn rate_limiter_denials_increment_counter() {
    let mut cfg = Config::default();
    cfg.rate_limit.enabled = true;
    cfg.rate_limit.burst = 1;
    cfg.rate_limit.requests_per_sec = 1;
    let node = test_node(cfg);

    assert!(node.allow_by_rate_limit());
    assert!(!node.allow_by_rate_limit());
    assert_eq!(node.rate_limited_requests_total.load(Ordering::Relaxed), 1);
}

#[test]
fn circuit_breaker_opens_on_threshold_and_rejects_while_open() {
    let mut cfg = Config::default();
    cfg.circuit_breaker.enabled = true;
    cfg.circuit_breaker.failure_threshold = 2;
    cfg.circuit_breaker.open_ms = 60_000;
    cfg.circuit_breaker.half_open_max_requests = 1;
    let node = test_node(cfg);

    node.record_circuit_result(&ClientResponse::Error {
        code: ErrorCode::NoQuorum,
        message: "synthetic".into(),
    });
    node.record_circuit_result(&ClientResponse::Error {
        code: ErrorCode::WriteTimeout,
        message: "synthetic".into(),
    });

    {
        let c = node.circuit.lock().unwrap();
        assert_eq!(c.state, CircuitState::Open);
    }
    assert_eq!(node.circuit_breaker_open_total.load(Ordering::Relaxed), 1);
    assert!(!node.allow_by_circuit_breaker());
    assert_eq!(node.circuit_breaker_reject_total.load(Ordering::Relaxed), 1);
}

#[test]
fn circuit_breaker_half_open_closes_after_success_quota() {
    let mut cfg = Config::default();
    cfg.circuit_breaker.enabled = true;
    cfg.circuit_breaker.failure_threshold = 1;
    cfg.circuit_breaker.open_ms = 1;
    cfg.circuit_breaker.half_open_max_requests = 2;
    let node = test_node(cfg);

    {
        let mut c = node.circuit.lock().unwrap();
        c.state = CircuitState::Open;
        c.open_until_ms = NodeHandle::now_millis().saturating_sub(1);
        c.half_open_successes = 0;
        c.consecutive_failures = 1;
    }

    assert!(node.allow_by_circuit_breaker());
    {
        let c = node.circuit.lock().unwrap();
        assert_eq!(c.state, CircuitState::HalfOpen);
        assert_eq!(c.half_open_successes, 0);
    }

    node.record_circuit_result(&ClientResponse::Pong);
    {
        let c = node.circuit.lock().unwrap();
        assert_eq!(c.state, CircuitState::HalfOpen);
        assert_eq!(c.half_open_successes, 1);
    }

    node.record_circuit_result(&ClientResponse::Pong);
    {
        let c = node.circuit.lock().unwrap();
        assert_eq!(c.state, CircuitState::Closed);
        assert_eq!(c.consecutive_failures, 0);
        assert_eq!(c.half_open_successes, 0);
    }
}

#[tokio::test]
async fn hot_key_waiter_uses_coalesced_response() {
    let mut cfg = Config::default();
    cfg.hot_key.enabled = true;
    cfg.hot_key.max_waiters = 8;
    cfg.hot_key.follower_wait_timeout_ms = 1_000;
    let node = test_node(cfg);
    let key = "hot:key".to_string();

    let flight = Arc::new(GetFlight::new());
    {
        let mut flights = node.get_flights.lock().await;
        flights.insert(key.clone(), Arc::clone(&flight));
    }

    let flight_for_sender = Arc::clone(&flight);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1)).await;
        let _ = flight_for_sender.tx.send(Some(ClientResponse::Value {
            key: "hot:key".into(),
            value: Bytes::from("v1"),
            version: 7,
        }));
    });

    let resp = node.handle_get_with_single_flight(key.clone(), key).await;
    assert!(matches!(resp, ClientResponse::Value { version: 7, .. }));
    assert_eq!(node.hot_key_coalesced_hits_total.load(Ordering::Relaxed), 1);
    assert_eq!(node.hot_key_fallback_exec_total.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn hot_key_waiter_cap_triggers_fallback() {
    let mut cfg = Config::default();
    cfg.hot_key.enabled = true;
    cfg.hot_key.max_waiters = 1;
    let node = test_node(cfg);
    let key = "hot:cap".to_string();

    let flight = Arc::new(GetFlight::new());
    flight.waiters.store(1, Ordering::Relaxed);
    {
        let mut flights = node.get_flights.lock().await;
        flights.insert(key.clone(), Arc::clone(&flight));
    }

    let resp = node.handle_get_with_single_flight(key.clone(), key).await;
    assert!(matches!(resp, ClientResponse::NotFound));
    assert_eq!(node.hot_key_fallback_exec_total.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn hot_key_follower_timeout_serves_stale() {
    let mut cfg = Config::default();
    cfg.hot_key.enabled = true;
    cfg.hot_key.max_waiters = 2;
    cfg.hot_key.follower_wait_timeout_ms = 1;
    cfg.hot_key.stale_ttl_ms = 1_000;
    cfg.hot_key.stale_max_entries = 16;
    let node = test_node(cfg);

    let _ = node
        .handle_client(ClientRequest::Set {
            key: "hot-timeout".into(),
            value: Bytes::from_static(b"value-1"),
            ttl_secs: None,
            namespace: None,
        })
        .await;
    let _ = node
        .handle_client(ClientRequest::Get {
            key: "hot-timeout".into(),
            namespace: None,
        })
        .await;

    {
        let mut flights = node.get_flights.lock().await;
        flights.insert("hot-timeout".into(), Arc::new(GetFlight::new()));
    }

    let resp = node
        .handle_get_with_single_flight("hot-timeout".into(), "hot-timeout".into())
        .await;
    match resp {
        ClientResponse::Value { value, .. } => {
            assert_eq!(value, Bytes::from_static(b"value-1"))
        }
        other => panic!("expected stale value response, got {:?}", other),
    }
    assert_eq!(node.hot_key_wait_timeout_total.load(Ordering::Relaxed), 1);
    assert_eq!(node.hot_key_stale_served_total.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn hot_key_waiter_cap_can_serve_stale_without_fallback_exec() {
    let mut cfg = Config::default();
    cfg.hot_key.enabled = true;
    cfg.hot_key.max_waiters = 1;
    cfg.hot_key.stale_ttl_ms = 1_000;
    cfg.hot_key.stale_max_entries = 16;
    let node = test_node(cfg);

    let _ = node
        .handle_client(ClientRequest::Set {
            key: "hot-cap".into(),
            value: Bytes::from_static(b"value-cap"),
            ttl_secs: None,
            namespace: None,
        })
        .await;
    let _ = node
        .handle_client(ClientRequest::Get {
            key: "hot-cap".into(),
            namespace: None,
        })
        .await;

    let flight = Arc::new(GetFlight::new());
    flight.waiters.store(1, Ordering::Relaxed);
    {
        let mut flights = node.get_flights.lock().await;
        flights.insert("hot-cap".into(), Arc::clone(&flight));
    }

    let resp = node
        .handle_get_with_single_flight("hot-cap".into(), "hot-cap".into())
        .await;
    match resp {
        ClientResponse::Value { value, .. } => {
            assert_eq!(value, Bytes::from_static(b"value-cap"))
        }
        other => panic!("expected stale value response, got {:?}", other),
    }

    assert_eq!(node.hot_key_stale_served_total.load(Ordering::Relaxed), 1);
    assert_eq!(node.hot_key_fallback_exec_total.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn namespace_quota_top_usage_empty_when_quota_disabled() {
    let mut cfg = Config::default();
    cfg.tenancy.enabled = true;
    cfg.tenancy.max_keys_per_namespace = 0;
    let node = test_node(cfg);

    let stats = node.stats().await;
    assert!(stats.namespace_quota_top_usage.is_empty());
    assert_eq!(stats.namespace_quota_reject_rate_per_min, 0);
    assert_eq!(stats.namespace_quota_reject_trend, "steady");
}

#[tokio::test]
async fn namespace_quota_top_usage_partial_pressure_sorted() {
    let mut cfg = Config::default();
    cfg.tenancy.enabled = true;
    cfg.tenancy.max_keys_per_namespace = 10;
    let node = test_node(cfg);

    let _ = node
        .handle_client(ClientRequest::Set {
            key: "k1".into(),
            value: Bytes::from("v"),
            ttl_secs: None,
            namespace: Some("tenant-a".into()),
        })
        .await;
    let _ = node
        .handle_client(ClientRequest::Set {
            key: "k1".into(),
            value: Bytes::from("v"),
            ttl_secs: None,
            namespace: Some("tenant-b".into()),
        })
        .await;
    let _ = node
        .handle_client(ClientRequest::Set {
            key: "k2".into(),
            value: Bytes::from("v"),
            ttl_secs: None,
            namespace: Some("tenant-b".into()),
        })
        .await;

    let stats = node.stats().await;
    assert_eq!(stats.namespace_quota_top_usage.len(), 2);
    assert_eq!(stats.namespace_quota_top_usage[0].namespace, "tenant-b");
    assert_eq!(stats.namespace_quota_top_usage[0].key_count, 2);
    assert_eq!(stats.namespace_quota_top_usage[0].usage_pct, 20);
    assert_eq!(stats.namespace_quota_top_usage[1].namespace, "tenant-a");
    assert_eq!(stats.namespace_quota_top_usage[1].key_count, 1);
    assert_eq!(stats.namespace_quota_top_usage[1].usage_pct, 10);
}

#[tokio::test]
async fn namespace_quota_top_usage_full_pressure_and_reject_trend() {
    let mut cfg = Config::default();
    cfg.tenancy.enabled = true;
    cfg.tenancy.max_keys_per_namespace = 2;
    let node = test_node(cfg);

    let _ = node
        .handle_client(ClientRequest::Set {
            key: "k1".into(),
            value: Bytes::from("v"),
            ttl_secs: None,
            namespace: Some("tenant-full".into()),
        })
        .await;
    let _ = node
        .handle_client(ClientRequest::Set {
            key: "k2".into(),
            value: Bytes::from("v"),
            ttl_secs: None,
            namespace: Some("tenant-full".into()),
        })
        .await;

    let blocked = node
        .handle_client(ClientRequest::Set {
            key: "k3".into(),
            value: Bytes::from("v"),
            ttl_secs: None,
            namespace: Some("tenant-full".into()),
        })
        .await;
    assert!(matches!(blocked, ClientResponse::Error { .. }));

    node.namespace_quota_reject_last_total
        .store(0, Ordering::Relaxed);
    node.namespace_quota_reject_last_ts_ms.store(
        NodeHandle::now_millis().saturating_sub(1_000),
        Ordering::Relaxed,
    );

    let stats = node.stats().await;
    let top = &stats.namespace_quota_top_usage[0];
    assert_eq!(top.namespace, "tenant-full");
    assert_eq!(top.key_count, 2);
    assert_eq!(top.usage_pct, 100);
    assert_eq!(top.remaining_keys, 0);
    assert!(stats.namespace_quota_reject_rate_per_min >= 1);
    assert!(matches!(
        stats.namespace_quota_reject_trend.as_str(),
        "rising" | "surging"
    ));
}

#[tokio::test]
async fn latency_and_hot_key_top_helpers_format_ranked_summaries() {
    let node = test_node(Config::default());

    for _ in 0..3 {
        node.observe_namespace_latency(Some("tenant-a"), 0);
        node.observe_hot_key_usage(Some("tenant-a::alpha"));
    }
    node.observe_namespace_latency(Some("tenant-b"), 5);
    node.observe_hot_key_usage(Some("tenant-b::beta"));
    node.observe_namespace_latency(None, 2);
    node.observe_hot_key_usage(None);

    assert_eq!(NodeHandle::latency_bucket_index(0), 0);
    assert_eq!(NodeHandle::latency_bucket_index(5), 1);
    assert_eq!(NodeHandle::latency_bucket_index(20), 2);
    assert_eq!(NodeHandle::latency_bucket_index(100), 3);
    assert_eq!(NodeHandle::latency_bucket_index(500), 4);
    assert_eq!(NodeHandle::latency_bucket_index(501), 5);

    let get = ClientRequest::Get {
        key: "alpha".into(),
        namespace: None,
    };
    assert_eq!(
        node.request_namespace_label(&get).as_deref(),
        Some("default")
    );
    assert_eq!(
        node.request_hot_key_label(&get, Some("tenant-a"))
            .as_deref(),
        Some("tenant-a::alpha")
    );
    assert_eq!(node.request_namespace_label(&ClientRequest::Ping), None);
    assert_eq!(
        node.request_hot_key_label(
            &ClientRequest::DeleteByPattern {
                pattern: "*".into(),
                namespace: Some("tenant-a".into())
            },
            Some("tenant-a")
        ),
        None
    );

    let latency = node.namespace_latency_top(5);
    assert_eq!(latency[0].namespace, "tenant-a");
    assert_eq!(latency[0].request_total, 3);
    assert_eq!(latency[0].latency_p95_estimate_ms, Some(1));
    assert!(
        observability::format_namespace_latency_top(&latency).contains("tenant-a:3(p95=1,p99=1)")
    );
    assert_eq!(observability::format_namespace_latency_top(&[]), "-");

    let hot_keys = node.hot_key_top_usage(10);
    assert_eq!(hot_keys[0].key, "tenant-a::alpha");
    assert_eq!(hot_keys[0].request_total, 3);
    assert_eq!(
        observability::format_hot_key_top_usage(&hot_keys),
        "tenant-a::alpha:3,tenant-b::beta:1"
    );
    assert_eq!(observability::format_hot_key_top_usage(&[]), "-");

    let quota_rows = vec![NamespaceQuotaUsage {
        namespace: "tenant-a".into(),
        key_count: 3,
        quota_limit: 10,
        usage_pct: 30,
        remaining_keys: 7,
    }];
    assert_eq!(
        observability::format_namespace_quota_top_usage(&quota_rows),
        "tenant-a:3/10(30%)"
    );
    assert_eq!(observability::format_namespace_quota_top_usage(&[]), "-");

    for idx in 0..=NAMESPACE_LATENCY_STATE_MAX {
        node.observe_namespace_latency(Some(&format!("ns-{idx}")), 0);
    }
    assert_eq!(
        node.namespace_latency_runtime.lock().unwrap().len(),
        NAMESPACE_LATENCY_STATE_MAX
    );
    for idx in 0..=HOT_KEY_STATE_MAX {
        node.observe_hot_key_usage(Some(&format!("key-{idx}")));
    }
    assert_eq!(
        node.hot_key_usage_runtime.lock().unwrap().len(),
        HOT_KEY_STATE_MAX
    );

    assert_eq!(
        observability::estimated_latency_percentile_ms([0; 6], 95),
        None
    );
    assert_eq!(
        observability::estimated_latency_percentile_ms([1, 1, 1, 1, 1, 1], 0),
        None
    );
    assert_eq!(
        observability::estimated_latency_percentile_ms([1, 1, 1, 1, 1, 1], 101),
        None
    );
    assert_eq!(
        observability::estimated_latency_percentile_ms([0, 0, 0, 0, 0, 2], 50),
        Some(1000)
    );

    let stats = node.stats().await;
    assert_eq!(
        observability::format_request_latency_buckets(&stats),
        "<=1ms:0;<=5ms:0;<=20ms:0;<=100ms:0;<=500ms:0;>500ms:0"
    );
}

#[tokio::test]
async fn cluster_prepare_commit_request_log_and_log_entries_apply_runtime_state() {
    let node = test_node(Config::default());

    let prepare = Arc::clone(&node)
        .handle_cluster(ClusterMessage::Prepare {
            log_index: 7,
            key: "cluster-key".into(),
            value: Some(Bytes::from_static(b"cluster-value")),
            ttl_secs: Some(60),
        })
        .await;
    assert!(matches!(
        prepare,
        Some(ClusterMessage::PrepareAck {
            log_index: 7,
            node_id
        }) if node_id == node.id
    ));

    let commit = Arc::clone(&node)
        .handle_cluster(ClusterMessage::Commit { log_index: 7 })
        .await;
    assert!(matches!(
        commit,
        Some(ClusterMessage::CommitAck {
            log_index: 7,
            node_id
        }) if node_id == node.id
    ));
    assert_eq!(
        node.store.get("cluster-key").unwrap().value,
        Bytes::from_static(b"cluster-value")
    );

    let log_entries = Arc::clone(&node)
        .handle_cluster(ClusterMessage::RequestLog { from_index: 6 })
        .await;
    assert!(matches!(
        log_entries,
        Some(ClusterMessage::LogEntries { ref entries })
            if entries.len() == 1 && entries[0].key == "cluster-key"
    ));

    let applied = Arc::clone(&node)
        .handle_cluster(ClusterMessage::LogEntries {
            entries: vec![LogEntry {
                index: 8,
                key: "remote-key".into(),
                value: Some(Bytes::from_static(b"remote-value")),
                ttl_secs: None,
                ts_ms: 123,
            }],
        })
        .await;
    assert!(applied.is_none());
    assert_eq!(
        node.store.get("remote-key").unwrap().value,
        Bytes::from_static(b"remote-value")
    );

    let force = Arc::clone(&node)
        .handle_cluster(ClusterMessage::ForcePrimary { node_id: node.id })
        .await;
    assert!(force.is_none());
    assert!(node.active_set.lock().await.is_primary());
}

#[tokio::test]
async fn admin_key_info_property_ttl_and_flush_paths_update_state() {
    let node = test_node(Config::default());

    let set = node
        .handle_client(ClientRequest::Set {
            key: "admin-key".into(),
            value: Bytes::from_static(b"admin-value"),
            ttl_secs: None,
            namespace: None,
        })
        .await;
    assert!(matches!(set, ClientResponse::Ok { .. }));

    let key_info = Arc::clone(&node)
        .handle_admin(AdminRequest::GetKeyInfo {
            key: "admin-key".into(),
        })
        .await;
    assert!(matches!(
        key_info,
        AdminResponse::KeyInfo {
            ref key,
            ref value,
            compressed: false,
            ..
        } if key == "admin-key" && value == &Bytes::from_static(b"admin-value")
    ));

    let property = Arc::clone(&node)
        .handle_admin(AdminRequest::SetKeyProperty {
            key: "admin-key".into(),
            name: "compressed".into(),
            value: "true".into(),
        })
        .await;
    assert!(matches!(property, AdminResponse::KeyPropertyUpdated));

    let ttl = Arc::clone(&node)
        .handle_admin(AdminRequest::SetKeysTtl {
            pattern: "admin-*".into(),
            ttl_secs: Some(30),
        })
        .await;
    assert!(matches!(ttl, AdminResponse::TtlUpdated { updated: 1 }));

    let keys = Arc::clone(&node)
        .handle_admin(AdminRequest::ListKeys {
            pattern: Some("admin-*".into()),
        })
        .await;
    assert!(matches!(
        keys,
        AdminResponse::Keys(ref keys) if keys == &vec!["admin-key".to_string()]
    ));

    let active_flush = Arc::clone(&node)
        .handle_admin(AdminRequest::FlushCache)
        .await;
    assert!(matches!(active_flush, AdminResponse::Error { .. }));

    let inactive = Arc::clone(&node)
        .handle_admin(AdminRequest::SetProperty {
            name: "active".into(),
            value: "false".into(),
        })
        .await;
    assert!(matches!(inactive, AdminResponse::Ok));

    let flushed = Arc::clone(&node)
        .handle_admin(AdminRequest::FlushCache)
        .await;
    assert!(matches!(flushed, AdminResponse::Flushed));
    assert!(node.store.keys(None).is_empty());
}

#[tokio::test]
async fn admin_set_property_updates_runtime_tuning_and_config_fields() {
    let mut config_path = std::path::PathBuf::from(temp_backup_dir("node-config"));
    config_path.push("node.toml");
    let node = test_node_with_config_path(
        Config::default(),
        config_path.to_string_lossy().into_owned(),
    );

    for (name, value) in [
        ("status", "false"),
        ("bind-addr", "127.0.0.2"),
        ("cluster-bind-addr", "127.0.0.3"),
        ("client-port", "17777"),
        ("http-port", "17778"),
        ("cluster-port", "17779"),
        ("gossip-port", "17780"),
        ("primary", "false"),
        ("max-memory", "7mb"),
        ("default-ttl", "33"),
        ("value-size-limit", "99"),
        ("max-keys", "44"),
        ("compression-enabled", "false"),
        ("compression-threshold", "8192"),
        ("write-timeout-ms", "1234"),
        ("write-quorum-mode", "majority"),
        ("version-check-interval", "2222"),
        ("read-repair-on-miss-enabled", "true"),
        ("read-repair-min-interval-ms", "3333"),
        ("read-repair-max-per-minute", "7"),
        ("anti-entropy-enabled", "true"),
        ("anti-entropy-interval-ms", "4444"),
        ("anti-entropy-min-repair-interval-ms", "5555"),
        ("anti-entropy-lag-threshold", "66"),
        ("anti-entropy-key-sample-size", "77"),
        ("anti-entropy-full-reconcile-every", "8"),
        ("anti-entropy-full-reconcile-max-keys", "88"),
        ("anti-entropy-budget-max-checks-per-run", "99"),
        ("anti-entropy-budget-max-duration-ms", "111"),
        ("mixed-version-probe-enabled", "false"),
        ("mixed-version-probe-interval-ms", "6666"),
        ("persistence-runtime-enabled", "true"),
        ("tenancy-enabled", "true"),
        ("tenancy-default-namespace", "tenant-default"),
        ("tenancy-max-keys-per-namespace", "12"),
        ("rate-limit-enabled", "true"),
        ("rate-limit-requests-per-sec", "21"),
        ("rate-limit-burst", "22"),
        ("hot-key-enabled", "true"),
        ("hot-key-max-waiters", "23"),
        ("hot-key-follower-wait-timeout-ms", "24"),
        ("hot-key-stale-ttl-ms", "25"),
        ("hot-key-stale-max-entries", "26"),
        ("hot-key-adaptive-waiters-enabled", "true"),
        ("hot-key-adaptive-min-waiters", "27"),
        ("hot-key-adaptive-success-threshold", "28"),
        ("hot-key-adaptive-state-max-keys", "29"),
        ("circuit-breaker-enabled", "true"),
        ("circuit-breaker-failure-threshold", "30"),
        ("circuit-breaker-open-ms", "31"),
        ("circuit-breaker-half-open-max-requests", "32"),
        ("unknown-property", "ignored"),
    ] {
        let resp = Arc::clone(&node)
            .handle_admin(AdminRequest::SetProperty {
                name: name.into(),
                value: value.into(),
            })
            .await;
        assert!(
            matches!(resp, AdminResponse::Ok),
            "{name} returned {resp:?}"
        );
    }

    let threshold_err = Arc::clone(&node)
        .handle_admin(AdminRequest::SetProperty {
            name: "compression-threshold".into(),
            value: "4096".into(),
        })
        .await;
    assert!(matches!(threshold_err, AdminResponse::Error { .. }));

    let stats = node.stats().await;
    assert_eq!(stats.status, NodeStatus::Inactive);
    assert_eq!(stats.memory_max_bytes, 7 * 1024 * 1024);
    assert_eq!(stats.value_size_limit_bytes, 99);
    assert_eq!(stats.max_keys_limit, 44);
    assert!(!stats.compression_enabled);
    assert_eq!(stats.compression_threshold_bytes, 8192);
    assert!(stats.tenancy_enabled);
    assert_eq!(stats.tenancy_default_namespace, "tenant-default");
    assert_eq!(stats.tenancy_max_keys_per_namespace, 12);
    assert!(stats.rate_limit_enabled);
    assert!(stats.circuit_breaker_enabled);
    assert!(stats.hot_key_enabled);
    assert!(stats.hot_key_adaptive_waiters_enabled);
    assert!(stats.read_repair_enabled);

    let cfg = node.config.lock().unwrap();
    assert_eq!(cfg.node.bind_addr, "127.0.0.2");
    assert_eq!(cfg.node.cluster_bind_addr, "127.0.0.3");
    assert_eq!(cfg.node.client_port, 17777);
    assert_eq!(cfg.node.http_port, 17778);
    assert_eq!(cfg.node.cluster_port, 17779);
    assert_eq!(cfg.node.gossip_port, 17780);
    assert_eq!(cfg.cache.default_ttl_secs, 33);
    assert_eq!(cfg.replication.write_timeout_ms, 1234);
    assert_eq!(cfg.replication.write_quorum_mode, WriteQuorumMode::Majority);
    assert_eq!(cfg.replication.version_check_interval_ms, 2222);
    assert_eq!(cfg.replication.read_repair_min_interval_ms, 3333);
    assert_eq!(cfg.replication.read_repair_max_per_minute, 7);
    assert!(cfg.replication.anti_entropy_enabled);
    assert_eq!(cfg.replication.anti_entropy_interval_ms, 4444);
    assert_eq!(cfg.replication.anti_entropy_min_repair_interval_ms, 5555);
    assert_eq!(cfg.replication.anti_entropy_lag_threshold, 66);
    assert_eq!(cfg.replication.anti_entropy_key_sample_size, 77);
    assert_eq!(cfg.replication.anti_entropy_full_reconcile_every, 8);
    assert_eq!(cfg.replication.anti_entropy_full_reconcile_max_keys, 88);
    assert_eq!(cfg.replication.anti_entropy_budget_max_checks_per_run, 99);
    assert_eq!(cfg.replication.anti_entropy_budget_max_duration_ms, 111);
    assert!(!cfg.replication.mixed_version_probe_enabled);
    assert_eq!(cfg.replication.mixed_version_probe_interval_ms, 6666);
    assert!(cfg.persistence.runtime_enabled);
    assert_eq!(cfg.rate_limit.requests_per_sec, 21);
    assert_eq!(cfg.rate_limit.burst, 22);
    assert_eq!(cfg.hot_key.max_waiters, 23);
    assert_eq!(cfg.hot_key.follower_wait_timeout_ms, 24);
    assert_eq!(cfg.hot_key.stale_ttl_ms, 25);
    assert_eq!(cfg.hot_key.stale_max_entries, 26);
    assert_eq!(cfg.hot_key.adaptive_min_waiters, 27);
    assert_eq!(cfg.hot_key.adaptive_success_threshold, 28);
    assert_eq!(cfg.hot_key.adaptive_state_max_keys, 29);
    assert_eq!(cfg.circuit_breaker.failure_threshold, 30);
    assert_eq!(cfg.circuit_breaker.open_ms, 31);
    assert_eq!(cfg.circuit_breaker.half_open_max_requests, 32);
}

#[tokio::test]
async fn client_error_and_pattern_paths_cover_runtime_guards() {
    let mut inactive_cfg = Config::default();
    inactive_cfg.node.active = false;
    let inactive = test_node(inactive_cfg);
    let inactive_resp = inactive.handle_client(ClientRequest::Ping).await;
    assert!(matches!(
        inactive_resp,
        ClientResponse::Error {
            code: ErrorCode::NodeInactive,
            ..
        }
    ));

    let mut tenancy_cfg = Config::default();
    tenancy_cfg.tenancy.enabled = true;
    let tenant_node = test_node(tenancy_cfg);
    let bad_namespace = tenant_node
        .handle_client_http(ClientRequest::Get {
            key: "k".into(),
            namespace: Some("bad::namespace".into()),
        })
        .await;
    assert!(matches!(
        bad_namespace,
        ClientResponse::Error {
            code: ErrorCode::InternalError,
            ..
        }
    ));

    let watch_guard = tenant_node
        .handle_client_tcp(ClientRequest::Watch {
            key: "k".into(),
            namespace: None,
        })
        .await;
    assert!(matches!(
        watch_guard,
        ClientResponse::Error {
            code: ErrorCode::InternalError,
            ..
        }
    ));
    let unwatch_guard = tenant_node
        .handle_client_tcp(ClientRequest::Unwatch {
            key: "k".into(),
            namespace: None,
        })
        .await;
    assert!(matches!(
        unwatch_guard,
        ClientResponse::Error {
            code: ErrorCode::InternalError,
            ..
        }
    ));

    let mut value_limit_cfg = Config::default();
    value_limit_cfg.cache.value_size_limit_bytes = 2;
    let value_limit_node = test_node(value_limit_cfg);
    let too_large = value_limit_node
        .handle_client(ClientRequest::Set {
            key: "big".into(),
            value: Bytes::from_static(b"big"),
            ttl_secs: None,
            namespace: None,
        })
        .await;
    assert!(matches!(
        too_large,
        ClientResponse::Error {
            code: ErrorCode::ValueTooLarge,
            ..
        }
    ));

    let mut key_limit_cfg = Config::default();
    key_limit_cfg.cache.max_keys = 1;
    let key_limit_node = test_node(key_limit_cfg);
    let first = key_limit_node
        .handle_client(ClientRequest::Set {
            key: "k1".into(),
            value: Bytes::from_static(b"v1"),
            ttl_secs: None,
            namespace: None,
        })
        .await;
    assert!(matches!(first, ClientResponse::Ok { .. }));
    let second = key_limit_node
        .handle_client(ClientRequest::Set {
            key: "k2".into(),
            value: Bytes::from_static(b"v2"),
            ttl_secs: None,
            namespace: None,
        })
        .await;
    assert!(matches!(
        second,
        ClientResponse::Error {
            code: ErrorCode::KeyLimitReached,
            ..
        }
    ));

    let pattern_node = test_node(Config::default());
    for key in ["pat:a", "pat:b", "ttl:a", "ttl:b"] {
        let resp = pattern_node
            .handle_client(ClientRequest::Set {
                key: key.into(),
                value: Bytes::from_static(b"value"),
                ttl_secs: None,
                namespace: None,
            })
            .await;
        assert!(matches!(resp, ClientResponse::Ok { .. }));
    }
    let ttl = pattern_node
        .handle_client(ClientRequest::SetTtlByPattern {
            pattern: "ttl:*".into(),
            ttl_secs: Some(30),
            namespace: None,
        })
        .await;
    assert!(matches!(
        ttl,
        ClientResponse::PatternTtlUpdated { updated: 2 }
    ));
    let deleted = pattern_node
        .handle_client(ClientRequest::DeleteByPattern {
            pattern: "pat:*".into(),
            namespace: None,
        })
        .await;
    assert!(matches!(
        deleted,
        ClientResponse::PatternDeleted { deleted: 2 }
    ));
}

#[tokio::test]
async fn set_nx_creates_once_and_keeps_existing_value() {
    let node = test_node(Config::default());

    let first = node
        .handle_client(ClientRequest::SetNx {
            key: "lease".into(),
            value: Bytes::from_static(b"holder-a"),
            ttl_secs: Some(30),
            namespace: None,
        })
        .await;
    let first_version = match first {
        ClientResponse::SetNx {
            created: true,
            version,
        } => version,
        other => panic!("unexpected first setnx response: {other:?}"),
    };

    let second = node
        .handle_client(ClientRequest::SetNx {
            key: "lease".into(),
            value: Bytes::from_static(b"holder-b"),
            ttl_secs: Some(30),
            namespace: None,
        })
        .await;
    match second {
        ClientResponse::SetNx {
            created: false,
            version,
        } => assert_eq!(version, first_version),
        other => panic!("unexpected second setnx response: {other:?}"),
    }

    let stored = node
        .handle_client(ClientRequest::Get {
            key: "lease".into(),
            namespace: None,
        })
        .await;
    match stored {
        ClientResponse::Value { value, version, .. } => {
            assert_eq!(value, Bytes::from_static(b"holder-a"));
            assert_eq!(version, first_version);
        }
        other => panic!("unexpected get response after setnx: {other:?}"),
    }
}

#[tokio::test]
async fn incr_updates_counter_and_rejects_bad_values() {
    let node = test_node(Config::default());

    let created = node
        .handle_client(ClientRequest::Incr {
            key: "counter".into(),
            delta: 2,
            ttl_secs_on_create: Some(30),
            namespace: None,
        })
        .await;
    match created {
        ClientResponse::Counter { value, version } => {
            assert_eq!(value, 2);
            assert_eq!(version, 1);
        }
        other => panic!("unexpected incr create response: {other:?}"),
    }

    let updated = node
        .handle_client(ClientRequest::Incr {
            key: "counter".into(),
            delta: -1,
            ttl_secs_on_create: Some(60),
            namespace: None,
        })
        .await;
    match updated {
        ClientResponse::Counter { value, version } => {
            assert_eq!(value, 1);
            assert_eq!(version, 2);
        }
        other => panic!("unexpected incr update response: {other:?}"),
    }

    let invalid_value = node
        .handle_client(ClientRequest::Set {
            key: "not-int".into(),
            value: Bytes::from_static(b"abc"),
            ttl_secs: None,
            namespace: None,
        })
        .await;
    assert!(matches!(invalid_value, ClientResponse::Ok { .. }));

    let mismatch = node
        .handle_client(ClientRequest::Incr {
            key: "not-int".into(),
            delta: 1,
            ttl_secs_on_create: None,
            namespace: None,
        })
        .await;
    assert!(matches!(
        mismatch,
        ClientResponse::Error {
            code: ErrorCode::TypeMismatch,
            ..
        }
    ));

    let max_value = node
        .handle_client(ClientRequest::Set {
            key: "max-int".into(),
            value: Bytes::from(i64::MAX.to_string().into_bytes()),
            ttl_secs: None,
            namespace: None,
        })
        .await;
    assert!(matches!(max_value, ClientResponse::Ok { .. }));

    let overflow = node
        .handle_client(ClientRequest::Incr {
            key: "max-int".into(),
            delta: 1,
            ttl_secs_on_create: None,
            namespace: None,
        })
        .await;
    assert!(matches!(
        overflow,
        ClientResponse::Error {
            code: ErrorCode::Overflow,
            ..
        }
    ));
}
