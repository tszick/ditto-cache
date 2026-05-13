# Ditto Operations Runbook

This runbook is for day-2 operations of a running Ditto cluster.

Validation entrypoints:

- CI dry-run entrypoint: `.github/workflows/preprod-runbook-validation.yml` -> `./scripts/preprod-runbook-validate.ps1 -DryRun`
- Manual real-run entrypoint: same workflow with `real_run=true` on a self-hosted Windows runner

## 0) Deployment Readiness Checklist

Use this checklist before promoting a release candidate to any shared
environment. A deployment is ready only when the code gates, runtime security
posture, backup/restore evidence, and rollback path are all known.

### Preflight gates

Run from `ditto-cache`:

```bash
cargo fmt --check
cargo check -p dittod
cargo clippy -p dittod -- -D warnings
cargo test --workspace
python scripts/validate-repo-hygiene.py
python scripts/validate-backup-restore-policy.py
python scripts/validate-dast-evidence.py
python scripts/validate-coverage-thresholds.py
```

Release gate expectations:

- `.github/workflows/release-gate.yml` must be green for the candidate commit.
- `.github/workflows/preprod-runbook-validation.yml` must pass in dry-run mode on PRs.
- A real-run preprod validation must pass before production promotion when the
  release changes clustering, persistence, restore, security posture, runtime
  properties, or client-facing protocol behavior.
- `DITTO_INSECURE=true` must not be present in non-dev environments.

### Runtime security posture

Before sending traffic to a deployed node, verify:

```bash
dittoctl node doctor all
dittoctl node describe all
dittoctl node status all
```

Required production posture:

- `strict-security-enforced=true`
- `insecure-runtime-enabled=false`
- `tcp-production-safe=true`
- mTLS enabled on cluster/admin traffic
- HTTP/management auth enabled
- backup encryption configured when backup, restore, export, or import gates are enabled

### Rollout sequence

1. Capture a baseline:
   - `dittoctl cluster get status`
   - `dittoctl node status all`
   - `dittoctl node describe all`
2. Confirm current backup/restore gates:
   - `persistence_platform_allowed`
   - `persistence_runtime_enabled`
   - `persistence_backup_enabled`
   - `persistence_import_enabled`
3. If persistence backup is enabled, trigger and record a fresh backup before rollout:
   - `dittoctl node backup <target>`
4. Roll one node at a time.
5. After each node restart, wait for:
   - node status `Active`,
   - stable primary election,
   - advancing `committed_index`,
   - no unexpected restore failure counters.
6. Run `dittoctl node doctor all` after the canary node and after full rollout.

### Runtime property changes

Runtime property changes are supported through `dittoctl node set ...`, but
they should be treated as operational changes:

- apply one property at a time,
- prefer one node first when the setting affects traffic handling,
- record the before/after value from `dittoctl node describe`,
- watch latency, error, rate-limit, circuit-breaker, hot-key, read-repair, and
  anti-entropy counters for at least 5-10 minutes.

Properties that affect bind addresses and ports are persisted but require a
restart. Do not mix these with live tuning changes in the same rollout step.

### Rollback path

Rollback is acceptable only when all of the following are true:

- the previous binary/config artifact is available,
- protocol compatibility is known for the rollback window,
- backup/restore policy gates are unchanged or explicitly revalidated,
- the cluster can retain quorum during the rollback sequence.

Rollback sequence:

1. Stop traffic to the canary or affected node where possible.
2. Set the node inactive if it is still participating:
   - `dittoctl node set active <target> false`
3. Restore the previous binary/config.
4. Start the node and wait for recovery.
5. Run:
   - `dittoctl node status <target>`
   - `dittoctl node doctor <target>`
6. Continue node-by-node only after the previous node is healthy.

### Go/no-go evidence

Attach or record the following for production promotion:

- release gate run URL,
- preprod real-run validation result,
- `dittoctl node doctor all` output,
- `dittoctl node status all` output,
- backup/restore policy validation result,
- security scan evidence validation result,
- any approved exceptions with owner and expiry.

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
  - `namespace_latency_top` (watch namespace-level p95/p99 outliers)
  - `hot_key_top_usage` (hot key concentration trend)
  - source skew in `client_requests_tcp_total` vs `client_requests_http_total`
  - `client_error_total` (especially `client_error_throttle_total` / `client_error_availability_total`)
  - `client_errors_http_total` / `client_errors_tcp_total` rapid growth on one ingress path
  - `anti_entropy_repair_trigger_total`
  - `anti_entropy_repair_throttled_total`
  - `anti_entropy_budget_exhausted_total`
  - `read_repair_budget_exhausted_total`
  - `namespace_quota_reject_total` (when tenancy is enabled).
- snapshot restore freshness (`snapshot_last_load_age_secs`) is within expected operational window after restart events.
- snapshot restore failures do not trend up (`snapshot_restore_failure_total`, `snapshot_restore_policy_block_total`) and success ratio stays healthy (`snapshot_restore_success_ratio_pct`).
- hot-key fallback pressure is controlled (`hot_key_wait_timeout_total`, `hot_key_stale_served_total`, `hot_key_stale_cache_entries`, `hot_key_adaptive_limit_decrease_total`).

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

## 7) Runbook Validation Modes

- Dry-run mode:
  - validates script wiring and expected scenario flow without Docker side effects,
  - intended for routine CI on push/PR.
- Real-run mode:
  - targets the `ditto-docker` environment from a self-hosted Windows runner,
  - validates node-loss / recovery, restore telemetry, namespace quota telemetry, and namespace/hot-key probe paths,
  - validates release go/no-go security posture from `/health/summary` (`strict_security_enforced`, `insecure_runtime_enabled`, `tcp_production_safe`, `tcp_supported_topology`),
  - runs `dittoctl node doctor all` against the same preprod cluster using authenticated HTTPS access to `ditto-mgmt` (Basic in local Docker, Bearer/OIDC in SSO environments),
  - should be used before major runtime changes or operational drills.

## 8) Management Auth Operations

`ditto-mgmt` accepts either Basic or Bearer admin auth, depending on `[admin]`
configuration:

- Local Docker/preprod lab mode can keep `[admin].password_hash` and use
  `dittoctl` `username` / `password`.
- SSO mode should configure Bearer introspection and omit `[admin].password_hash`
  so Basic is not accepted.
- Use a Ditto-specific required scope or audience for SSO mode, for example
  `ditto.mgmt` or `ditto-mgmt`.

This affects only management clients (`browser`, `dittoctl`) talking to
`ditto-mgmt :7781`. Direct application clients talking to `dittod :7777` or
`:7778` still use the node TCP token or node HTTP Basic auth.
