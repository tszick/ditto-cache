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
- Status: planned.

5. Rolling-upgrade compatibility mode
- What: protocol/version negotiation for mixed-version clusters.
- Why: lower-risk, near-zero-downtime upgrades.
- Status: planned.

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
