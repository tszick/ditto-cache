# Ditto Backlog Guide

This document tracks the delivered development themes for the distributed cache runtime and records the hardening closure history.

## Goals

- Keep production behavior stable by default (feature flags + safe defaults).
- Prioritize operational value first (resilience, visibility, controlled rollout).
- Ship each item with tests, docs, and runtime observability.

## Open Development Backlog

Only one development program remains open in this backlog:

1. Production-ready closure program
- Goal: turn the current dev-hardened state into a production-ready candidate with enforceable gates, explicit security policy, repeatable pre-prod validation, and final release-readiness sign-off.
- Status: in progress.
- Current active sprint: Sprint 5.

### Sprint 1: Release gate and source-of-truth alignment

- Make release-readiness gates explicit and enforceable across `ditto-cache` and `ditto-client`.
- Prevent protocol drift between `ditto-cache` protocol contract and `ditto-client` committed snapshot.
- Keep release checklist and gate documentation aligned with the actual workflows.
- Status: completed (repo-side).
- Completed:
  - `ditto-client` protocol parity workflow now checks snapshot drift against `ditto-cache`,
  - `ditto-cache` release gate now includes protocol drift, perf regression, and release-readiness summary jobs.
  - release-readiness checklist documented in `docs/release-readiness-checklist.md`.
- Operational note:
  - GitHub branch protection / required-check configuration must require the documented gate names in both repositories.

### Sprint 2: TCP security policy and startup guardrails

- Close the production policy for client TCP port `7777`.
- Ensure startup/security behavior matches the supported production topology.
- Remove ambiguity around insecure/dev-only bypass paths.
- Status: completed (repo-side).
- Progress:
  - `dittod` strict startup now rejects non-loopback TCP client exposure without `client_auth_token`,
  - `node describe` now surfaces `client-auth-enabled`, `tcp-client-bind-loopback-only`, and `tcp-production-safe`,
  - `/health/summary` now surfaces `tcp_client_auth_enabled`, `tcp_client_bind_loopback_only`, `tcp_production_safe`, and `tcp_supported_topology`,
  - `dittoctl node doctor` now reports unsupported TCP exposure as `CRITICAL`,
  - insecure/dev bypass is now explicit in operator views (`insecure-runtime-enabled`, `strict-security-enforced`), and `dittoctl node doctor` reports insecure runtime bypass as `CRITICAL`,
  - node/default docs now describe TCP auth as mandatory for non-loopback production exposure.

### Sprint 3: Pre-prod verification and operational readiness

- Stabilize real-run runbook validation.
- Formalize release go/no-go operational checks.
- Ensure `dittoctl doctor`, telemetry, and runbooks are sufficient for release decisions.
- Progress:
  - real-run runbook validation now asserts strict-security and supported TCP topology from `/health/summary`,
  - real-run validator now emits a compact `go_no_go` summary block for release-candidate decisions,
  - `dittoctl` now supports authenticated/self-signed HTTPS access to `ditto-mgmt` for dev/preprod usage,
  - real-run validator now runs `dittoctl node doctor all` and carries its verdict in the same validation output,
  - operations/admin/release docs now describe the same release go/no-go path and require `dittoctl node doctor all` to stay free of `CRITICAL` findings.

### Sprint 4: Performance and coverage gates

- Promote report-first quality signals into credible blocking gates where justified.
- Clarify the meaning of perf gates and raise critical-path coverage enforcement.
- Status: completed (repo-side).
- Progress:
  - perf gate classification is now explicit in script output and docs: smoke-regression only, debug single-node HTTP path, insecure dev bypass, not production sign-off,
  - release/admin/readme docs now describe the same scope limit for the current perf baseline gate,
  - coverage policy is now documented as mixed maturity rather than fully report-first:
    - `ditto-cache` Rust total line coverage no-regression gate is active on PRs,
    - `ditto-client` Node.js, Go, Python, and Java no-regression gates are active on PRs,
    - the current repo-side policy is now explicit: required base-branch no-regression on the active coverage lanes, without claiming global absolute minimum thresholds.

### Sprint 5: Final production-readiness pass

- Re-run full release-readiness review after Sprint 1-4 changes.
- Classify residual gaps as blockers vs near-term improvements.
- Close documentation and publish final known limitations.

## Backlog Themes (8 points)

1. Multi-tenant isolation (`namespace` + per-tenant quotas)
- What: isolate keys, limits, and memory budgets per tenant.
- Why: prevents noisy-neighbor issues and supports safer shared clusters.
- Status: delivered (Sprint 4).

2. Rate limit + circuit breaker
- What: request throttling and fail-fast protection on repeated failures.
- Why: improves stability during spikes and partial outages.
- Status: delivered (Sprint 1 baseline + hardening complete).

3. Hot-key protection
- What: single-flight/request coalescing and soft-TTL patterns for hot keys.
- Why: reduces thundering herd and CPU spikes.
- Status: delivered (Sprint 2 MVP + hardening complete).

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
- Status: delivered (MVP + observability hardening complete).

7. Deep observability
- What: latency histograms, categorized errors, replication-lag metrics.
- Why: faster incident diagnosis and better capacity planning.
- Status: delivered (Sprint 4 + post-sprint observability expansion).

8. Chaos/Jepsen-style resilience tests
- What: automated partition, node-failure, and timing-fault scenarios.
- Why: validates safety and recovery behavior before production incidents.
- Status: delivered (Sprint 4).

## Historical Roadmap (Delivered 4 sprints)

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
  - S2.5 final benchmark execution and result capture (latency/error baseline vs restore run).

Sprint 2 benchmark result capture (2026-04-07):

- Environment:
  - local Docker 3-node cluster + `ditto-mgmt` (`ditto-docker/docker-compose.yml`),
  - profile: `ditto-docker/load/k6/scripts/ditto-idle-burst.js`,
  - run parameters: `IDLE_DURATION=30s`, `BURST_DURATION=45s`, `BURST_RPS=10`.
- Baseline (cold restart path):
  - `node-1` cold restart to `/ping` healthy: `2981 ms`,
  - k6 result: `http_req_failed=0.00%`,
  - k6 latency: `avg=648.6ms`, `p90=1.64s`, `p95=2.22s`, `max=3.36s`.
- Restore run:
  - snapshot backup + restore completed on `node-1`,
  - restore result: `entries=200`, `snapshot_last_load_entries=200`,
  - restore duration fields reported `0 ms` on this small local dataset,
  - k6 result: `http_req_failed=0.00%`,
  - k6 latency: `avg=266.33ms`, `p90=290.65ms`, `p95=301.63ms`, `max=455.22ms`.
- Outcome:
  - Sprint 2 benchmark execution and result capture completed.
  - Idle-then-burst profile remained error-free in the restore run and showed materially lower tail latency than the captured baseline run.

Sprint 3 kickoff update (2026-04-02):

- Implemented:
  - Read-repair on miss MVP (default OFF):
    - Non-primary local GET miss can query primary and, on hit, trigger async `run_resync`.
    - Runtime controls: `read-repair-on-miss-enabled`, `read-repair-min-interval-ms`.
    - Env overrides: `DITTO_READ_REPAIR_ON_MISS_ENABLED`, `DITTO_READ_REPAIR_MIN_INTERVAL_MS`.
    - Observability counters in status: trigger/success/throttled totals.
- Follow-up:
  - anti-entropy full shard/keyspace reconciliation shipped in the Sprint 3 completion update below,
  - rolling-upgrade compatibility hooks also shipped in the Sprint 3 completion update below.

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

Sprint 4 progress update (2026-04-02):

- Delivered in the first Sprint 4 slice:
  - Tenant isolation phase 1 delivered:
    - optional namespace-aware keyspace (`tenancy.enabled`, `tenancy.default_namespace`),
    - optional per-namespace key quota (`tenancy.max_keys_per_namespace`),
    - runtime observability counters (`namespace_quota_reject_total`) and status fields.
  - mgmt/CLI namespace UX delivered:
    - `dittoctl cache ... --namespace <tenant>` support,
    - mgmt cache proxy forwards namespace via `X-Ditto-Namespace`,
    - admin key operations scope correctly by namespace.
  - chaos + runbook docs delivered:
    - `docs/chaos-playbook.md`,
    - `docs/operations-runbook.md`,
    - helper script: `scripts/chaos-smoke.ps1`.
  - CI dry-run path delivered:
    - `.github/workflows/chaos-dry-run.yml` runs `chaos-smoke.ps1 -DryRun` on push/PR/manual trigger,
    - no nightly schedule (environment-dependent real runs stay manual).

Sprint 4 completion update (2026-04-07):

- Completed:
  - Tenant isolation and quota controls finalized:
    - namespace-aware keyspace isolation,
    - per-namespace key quota enforcement,
    - node/mgmt/CLI observability fields and counters.
  - Chaos suite expanded to cover all target classes:
    - crash/restart loop scenario,
    - network partition/reconnect scenario,
    - timing-fault pause/unpause scenario.
  - Operational documentation finalized:
    - `docs/operations-runbook.md` incident playbooks,
    - `docs/chaos-playbook.md` automated + manual scenarios,
  - README test and CI guidance aligned with Sprint 4 scope.

Rate limit + circuit breaker closure update (2026-04-09):

- Completed:
  - config/env toggles and guardrails (`DITTO_RATE_LIMIT_*`, `DITTO_CIRCUIT_BREAKER_*`),
  - runtime request gating in client path (`RateLimited`, `CircuitOpen`),
  - status/mgmt/CLI observability fields and counters (`rate_limited_requests_total`, `circuit_breaker_state`, open/reject totals),
  - HTTP status mapping alignment (`429` for rate-limit, `503` for open-circuit),
  - dedicated unit coverage for token-bucket rejection counting and circuit state transitions (open -> half-open -> closed).

Deep observability closure update (2026-04-09):

- Completed:
  - node-level request latency histogram buckets (`<=1ms`, `<=5ms`, `<=20ms`, `<=100ms`, `<=500ms`, `>500ms`),
  - endpoint-level request + error split (`tcp` / `http` / `internal`) in node stats, health summary, mgmt node status and `dittoctl node status`,
  - percentile export integration from histogram buckets (`client_latency_p50_estimate_ms`, `p90`, `p95`, `p99`),
  - categorized client error counters (`auth`, `throttle`, `availability`, `validation`, `internal`, `other`),
  - exposure in node stats, health summary JSON, mgmt node status view, and `dittoctl node status` output,
  - dashboard/alert presets for latency/error burn-rate added to `docs/operations-runbook.md`,
  - targeted unit coverage for latency bucket, source split, percentile estimation and error-category accounting.

Snapshot + fast restart closure update (2026-04-09):

- Completed:
  - snapshot restore age metric (`snapshot_last_load_age_secs`) added to node stats,
  - surfaced in health summary, mgmt node status, and `dittoctl node status`,
  - snapshot restore counters in node stats (`snapshot_restore_attempt_total`, `snapshot_restore_success_total`, `snapshot_restore_failure_total`, `snapshot_restore_not_found_total`, `snapshot_restore_policy_block_total`),
  - snapshot restore counters surfaced in health summary, mgmt node status and `dittoctl node status`,
  - snapshot restore regression coverage extended with policy-blocked, invalid snapshot and successful restore counter assertions,
  - snapshot freshness threshold guidance added to `docs/operations-runbook.md`.

Hot-key observability update (2026-04-09):

- Completed in this slice:
  - added `hot_key_inflight_keys` runtime metric to show current in-flight single-flight key count,
  - surfaced in node stats, health summary, mgmt node status, and `dittoctl node status`.

Hot-key hardening closure update (2026-04-10):

- Completed:
  - added follower wait timeout guard (`hot_key.follower_wait_timeout_ms`) to avoid indefinite waiter blocking,
  - added soft-stale serving (`hot_key.stale_ttl_ms`, `hot_key.stale_max_entries`) for overload fallback paths,
  - added runtime counters (`hot_key_wait_timeout_total`, `hot_key_stale_served_total`, `hot_key_stale_cache_entries`),
  - exposed new hot-key fields via node stats, health summary, mgmt node status and `dittoctl node status`,
  - extended tests for follower-timeout stale serving and waiter-cap stale serving behavior.

Hardening closure update (2026-04-10):

- Completed:
  - `ditto-cache` dependency upgrade pass (workspace verification + regression test run),
  - `ditto-client` dependency upgrade pass (Node/Java/Python/Go verification and updates where needed),
  - client functional parity alignment slice (error mapping + namespace handling consistency hardening),
  - documentation sync (`ditto-client` guide + client README updates),
  - namespace quota observability 2.0 follow-up pass (`namespace_quota_top_usage`, reject rate/trend, mgmt/API/CLI alignment, telemetry coverage),
  - `ditto-docker` parity regression pack for auth/quota/rate/circuit/unknown-error mappings,
  - `dittoctl doctor` diagnostics hardening pass (`OK/WARN/CRITICAL` summary + non-zero exit on critical findings),
  - `ditto-client` release automation prep (`release-dry-run.yml`, changelog/version planning helper).

Coverage hardening status (2026-04-10):

- Completed in this slice:
  - coverage report workflows added for both repositories,
  - Java JaCoCo report generation enabled in `ditto-client`,
  - targeted critical-path coverage expanded across strict validation, quota/error mapping, `dittoctl doctor`, and watch/reconnect paths,
  - PR no-regression checks added:
    - `ditto-client`: Node line coverage and Go statement coverage vs base branch,
    - `ditto-cache`: Rust total line coverage vs base branch.
- Remaining gap:
  - repository-wide and critical-path threshold enforcement is not yet proven as a stable blocking gate,
  - coverage status should still be treated as partial/report-first hardening rather than fully closed.

P2 automation and release-gate closure update (2026-04-19 to 2026-04-20):

- Completed:
  - `ditto-client` protocol snapshot / SDK parity gate added and `NamespaceQuotaExceeded` error-code alignment completed (`.github/workflows/protocol-parity.yml`, `contracts/protocol-contract.snapshot.json`),
  - `ditto-cache` protocol contract drift gate added (`.github/workflows/protocol-contract.yml`),
  - pre-prod runbook validator added with dry-run CI entrypoint and manual real-run workflow inputs (`.github/workflows/preprod-runbook-validation.yml`, `scripts/preprod-runbook-validate.ps1`),
  - perf baseline regression gate added (`.github/workflows/perf-gate.yml`, `docs/perf-baseline.json`),
  - release gate hardened to include runbook validation and repeated flaky-suite passes (`.github/workflows/release-gate.yml`),
  - runbook validator hardened with multi-node telemetry assertions plus namespace/hot-key probe checks,
  - recovery telemetry and runbook validation tightened in the node/runtime path.

## Definition of Done (for each backlog item)

- Feature is disabled by default and guardrailed by config/env.
- Runtime and API/CLI observability added (stats, logs, counters).
- Unit + integration coverage for success and failure paths.
- Documentation updated (`README`, admin/CLI guides, this backlog).
