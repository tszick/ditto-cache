# Ditto Backlog Guide

This document tracks the next major development themes for the distributed cache runtime.

## Goals

- Keep production behavior stable by default (feature flags + safe defaults).
- Prioritize operational value first (resilience, visibility, controlled rollout).
- Ship each item with tests, docs, and runtime observability.

## Backlog Themes (8 points)

1. Multi-tenant isolation (`namespace` + per-tenant quotas)
- What: isolate keys, limits, and memory budgets per tenant.
- Why: prevents noisy-neighbor issues and supports safer shared clusters.
- Status: planned.

2. Rate limit + circuit breaker
- What: request throttling and fail-fast protection on repeated failures.
- Why: improves stability during spikes and partial outages.
- Status: in progress (Sprint 1 foundation delivered).

3. Hot-key protection
- What: single-flight/request coalescing and soft-TTL patterns for hot keys.
- Why: reduces thundering herd and CPU spikes.
- Status: planned.

4. Read-repair + anti-entropy
- What: periodic drift detection and automatic key/version reconciliation.
- Why: keeps replicas aligned after transient failures.
- Status: delivered (Sprint 3).

5. Rolling-upgrade compatibility mode
- What: protocol/version negotiation for mixed-version clusters.
- Why: lower-risk, near-zero-downtime upgrades.
- Status: delivered (Sprint 3 hooks).

6. Snapshot + fast restart
- What: startup warm state from snapshot, then catch-up deltas.
- Why: significantly faster recovery and restart times.
- Status: planned.

7. Deep observability
- What: latency histograms, categorized errors, replication-lag metrics.
- Why: faster incident diagnosis and better capacity planning.
- Status: planned (Sprint 1 base metrics included).

8. Chaos/Jepsen-style resilience tests
- What: automated partition, node-failure, and timing-fault scenarios.
- Why: validates safety and recovery behavior before production incidents.
- Status: planned.

## Roadmap (4 sprints)

### Sprint 1 (stability + visibility baseline)

- Implement rate-limit and circuit-breaker configuration + runtime counters.
- Expose limiter/breaker states in node stats and CLI/mgmt status output.
- Add initial observability hooks and docs.

Deliverables:
- Config/env support (`DITTO_RATE_LIMIT_*`, `DITTO_CIRCUIT_BREAKER_*`).
- Runtime throttling/fail-fast guard in client request path.
- Stats fields for limiter/breaker state and counters.

### Sprint 2 (load behavior hardening)

- Implement hot-key protection (coalescing + soft TTL design).
- Add snapshot write/load flow for fast restart.
- Expand smoke/integration tests for load spikes and restart paths.

Sprint 2 execution plan:

- Objective:
  - Keep latency and write success stable during client idle/reconnect churn and load spikes.
  - Reduce false cluster degradation caused by aggressive timeout defaults.
  - Deliver restart-time improvement with snapshot bootstrap.

- In-scope workstreams:
  - S2.1 Connection timeout hardening:
    - Make TCP frame-read timeout configurable via env/config.
    - Raise safe default from short idle-kick behavior to long-lived client-friendly value.
    - Ensure status/config introspection shows effective timeout values.
  - S2.2 Gossip stability tuning:
    - Expose and document dead-node threshold tuning (`gossip_dead_ms`) for production presets.
    - Add guardrails for too-low values and clear warnings in logs/docs.
  - S2.3 Hot-key protection MVP:
    - Add single-flight request coalescing for concurrent reads of same key.
    - Add bounded waiter protection (prevent unbounded fan-in memory growth).
    - Add counters for coalesced hits and fallback executions.
  - S2.4 Snapshot fast-restart MVP:
    - Add manual snapshot save/load flow (admin-triggered).
    - On startup: optional snapshot restore, then normal replication catch-up.
    - Add status fields for snapshot age/load duration.
  - S2.5 Test and verification pack:
    - k6 profile for idle-then-burst traffic.
    - Restart scenario test: baseline cold start vs snapshot restore.
    - Regression checks for quorum write path and timeout/error mapping.

- Out of scope:
  - Full anti-entropy reconciliation logic (Sprint 3).
  - Tenant isolation/quotas (Sprint 4).
  - Full chaos suite (Sprint 4).

- Acceptance criteria:
  - No periodic reconnect churn under idle clients with default config.
  - No false OFFLINE flapping in nominal 3-node local cluster defaults.
  - Coalescing reduces duplicate backend work for same-key burst reads.
  - Snapshot restore measurably reduces restart warm-up time vs cold baseline.
  - New metrics visible in node stats and documented in admin/CLI guides.

- Implementation order (recommended):
  1. S2.1 timeout hardening
  2. S2.2 gossip tuning/guardrails
  3. S2.5 targeted regression tests for above
  4. S2.3 hot-key coalescing MVP
  5. S2.4 snapshot fast-restart MVP
  6. S2.5 final benchmark + docs pass

- Risks and mitigations:
  - Risk: longer frame timeout may keep dead sockets longer.
    - Mitigation: retain heartbeat/ping behavior and socket error accounting.
  - Risk: overly high gossip dead threshold delays true failure detection.
    - Mitigation: publish environment-specific recommended ranges.
  - Risk: coalescing lock contention under very hot keys.
    - Mitigation: shard coalescing map and cap waiter counts.
  - Risk: snapshot format drift over versions.
    - Mitigation: version header + compatibility checks from day one.

Sprint 2 progress update (2026-04-02):

- Completed:
  - S2.1 Connection timeout hardening (configurable `frame_read_timeout_ms`, safer default, status visibility).
  - S2.2 Gossip stability tuning (default `gossip_dead_ms=15000`, low-value guardrails and warnings).
  - S2.3 Hot-key protection MVP (single-flight coalescing, waiter cap, runtime counters, status exposure).
  - S2.4 Snapshot fast-restart MVP (startup restore option + admin restore command + status metadata).
  - S2.5 targeted regression tests for restore policy gate and restore stats update.
  - S2.5 benchmark playbook and load profile for idle-then-burst + cold-vs-restore restart comparison.
- In progress:
  - S2.5 final benchmark execution and result capture (latency/error baseline vs restore run).

Sprint 3 kickoff update (2026-04-02):

- Implemented:
  - Read-repair on miss MVP (default OFF):
    - Non-primary local GET miss can query primary and, on hit, trigger async `run_resync`.
    - Runtime controls: `read-repair-on-miss-enabled`, `read-repair-min-interval-ms`.
    - Env overrides: `DITTO_READ_REPAIR_ON_MISS_ENABLED`, `DITTO_READ_REPAIR_MIN_INTERVAL_MS`.
    - Observability counters in status: trigger/success/throttled totals.
- Next:
  - Anti-entropy periodic reconciliation strategy (lag-threshold + key-version sample mismatch trigger done; next step is full shard/keyspace reconciliation).
  - Rolling-upgrade protocol compatibility negotiation.

Sprint 3 completion update (2026-04-02):

- Completed:
  - Read-repair on miss runtime controls + counters fully integrated in stats/mgmt/CLI.
  - Anti-entropy background reconciliation extended with:
    - lag-threshold repair trigger,
    - key-version sample mismatch trigger,
    - bounded full keyspace reconcile (`anti_entropy_full_reconcile_every`, `anti_entropy_full_reconcile_max_keys`) with counters.
  - Rolling-upgrade compatibility hooks:
    - node `protocol-version` property exposure,
    - periodic mixed-version probe against peers with runtime controls and counters.
  - Validation coverage:
    - unit tests for mixed-version response classification and full-reconcile scheduling helper,
    - workspace test suite passes with new stats fields and controls.

### Sprint 3 (consistency + upgrades)

- Add read-repair and anti-entropy background reconciliation.
- Implement rolling upgrade compatibility hooks (protocol negotiation).
- Validate mixed-version cluster behavior in CI scenarios.

### Sprint 4 (enterprise hardening)

- Implement tenant isolation and quotas.
- Add chaos test suite for partitions, delays, and crash/restart loops.
- Publish operational runbooks and failure playbooks.

## Definition of Done (for each backlog item)

- Feature is disabled by default and guardrailed by config/env.
- Runtime and API/CLI observability added (stats, logs, counters).
- Unit + integration coverage for success and failure paths.
- Documentation updated (`README`, admin/CLI guides, this backlog).
