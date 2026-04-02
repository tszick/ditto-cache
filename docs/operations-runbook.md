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
  - `anti_entropy_repair_trigger_total`
  - `namespace_quota_reject_total` (when tenancy is enabled).

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
