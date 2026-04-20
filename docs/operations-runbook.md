# Ditto Operations Runbook

This runbook is for day-2 operations of a running Ditto cluster.

## 1) Fast Health Triage

Run:

```bash
dittoctl cluster get status
dittoctl node status all
```

Check first:

- primary exists and active node count is as expected,
- `committed_index` moves forward under write traffic,
- no sustained spikes in:
  - `rate_limited_requests_total`
  - `circuit_breaker_reject_total`
  - `client_request_latency_gt_500ms_total`
  - `client_latency_p95_estimate_ms` / `client_latency_p99_estimate_ms`
  - source skew in `client_requests_tcp_total` vs `client_requests_http_total`
  - `client_error_total` (especially `client_error_throttle_total` / `client_error_availability_total`)
  - `client_errors_http_total` / `client_errors_tcp_total` rapid growth on one ingress path
  - `anti_entropy_repair_trigger_total`
  - `anti_entropy_repair_throttled_total`
  - `namespace_quota_reject_total` (when tenancy is enabled).
- snapshot restore freshness (`snapshot_last_load_age_secs`) is within expected operational window after restart events.
- snapshot restore failures do not trend up (`snapshot_restore_failure_total`, `snapshot_restore_policy_block_total`).
- hot-key fallback pressure is controlled (`hot_key_wait_timeout_total`, `hot_key_stale_served_total`, `hot_key_stale_cache_entries`).

## 2) Common Incident Playbooks

### A) Elevated write latency / timeouts

Actions:

1. Verify quorum path:
   - inspect `status`, `is_primary`, and node reachability.
2. Confirm gossip tuning:
   - `gossip_interval_ms`, `gossip_dead_ms`.
3. Check anti-entropy pressure:
   - `anti_entropy_*` counters and recent logs.
4. If needed, temporarily increase write timeout:
   - `dittoctl node set write-timeout-ms <target> <ms>`.

### B) Namespace quota rejection spike

Symptoms:

- client errors with `NamespaceQuotaExceeded`,
- growing `namespace_quota_reject_total`.

Actions:

1. Identify affected tenant namespace from client request path/header.
2. Confirm current quota config:
   - `tenancy_enabled`,
   - `tenancy_max_keys_per_namespace`.
3. Mitigate:
   - raise quota with
     - `dittoctl node set tenancy-max-keys-per-namespace <target> <n>`,
   - or clear stale keys in that namespace pattern.

### C) Mixed-version upgrade warning

Symptoms:

- `mixed_version_peers_detected_total` increases.

Actions:

1. Validate target rollout version for each node.
2. Keep write path conservative during rollout.
3. Complete rollout quickly to remove mixed state.

## 3) Tenant Quota Tuning Guide

Start conservative:

- enable tenancy only where required,
- set `tenancy_max_keys_per_namespace` from observed key cardinality + 20-30% headroom.

Observe:

- key growth trend by tenant,
- `namespace_quota_reject_total`,
- eviction and memory pressure.

Adjust:

1. Increase namespace quota if legitimate growth is blocked.
2. Keep global `max_keys` and memory limits aligned with total namespace quotas.
3. Revisit per-tenant retention/TTL patterns.

## 4) Safe Runtime Change Checklist

Before change:

- capture `dittoctl node status all`,
- capture current property values (`dittoctl node describe all`).

Apply change:

- one setting at a time,
- start with one node where feasible.

After change:

- re-check status counters after 5-10 minutes,
- verify client success rate and latency.

## 5) Post-Incident Template

- Incident start/end (UTC)
- Affected nodes / tenants
- Trigger and detection path
- Mitigation applied
- Metrics before/after
- Follow-up action items

## 6) Alert Presets (Latency/Error Burn-Rate)

Suggested starter thresholds for local/dev-like environments:

- Warning:
  - `client_latency_p95_estimate_ms > 100` for 10m, or
  - `client_error_total` growth > 1% of `client_requests_total` over 10m.
- Critical:
  - `client_latency_p99_estimate_ms > 500` for 5m, or
  - `client_error_total` growth > 5% of `client_requests_total` over 5m.
- Source-specific anomaly:
  - `client_errors_http_total` (or `client_errors_tcp_total`) contributes > 80% of new errors over 10m while request share for that source is < 60%.
- Snapshot freshness:
  - Warning: `snapshot_last_load_age_secs > 86_400` (24h) on nodes expected to restart from snapshots daily.
  - Critical: `snapshot_last_load_age_secs > 604_800` (7d) on snapshot-based restart environments.
- Snapshot restore failures:
  - Warning: `snapshot_restore_failure_total` increases by >= 1 in 30m.
  - Critical: `snapshot_restore_policy_block_total` increases by >= 1 in 30m (misconfiguration/regression signal).

Tuning notes:

1. Raise latency thresholds for low-power local machines.
2. Keep error burn-rate thresholds strict; they detect regressions earlier than absolute counters.
3. Recalibrate after major traffic-pattern changes (new client rollout, quota policy changes, restore events).
