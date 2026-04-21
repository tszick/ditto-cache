# Ditto Release Readiness Checklist

This checklist is the repo-side source of truth for what must be green before a build is treated as a production-ready candidate.

## Required CI Checks

### `ditto-cache`

- `Release Readiness Summary` from `.github/workflows/release-gate.yml`
  - covers:
    - `Runbook Validation (Dry Run Gate)`
    - `Protocol Contract Drift Gate`
    - `Perf Baseline Regression Gate`
    - `Rust Build and Test Gate`
- `Runbook Validation (Real Run Gate)` on `main`
  - self-hosted Windows runner only
  - validates live `ditto-docker` runbook scenarios against the expected dev/preprod cluster

Perf gate interpretation:
- `Perf Baseline Regression Gate` is currently a smoke-regression guard, not a production-like performance sign-off.
- It runs against a debug single-node HTTP path with `DITTO_INSECURE=true`.
- Use it to catch obvious latency regressions, not to approve production capacity or secure-path readiness by itself.

Coverage gate interpretation:
- `ditto-cache` coverage is partially enforced today via `Rust Coverage No Regression (PR)`.
- This is a stable required no-regression gate on Rust total line coverage versus the base branch, not a full repository threshold policy.
- `ditto-client` coverage now has PR no-regression checks on Node.js, Go, Python, and Java lanes.
- Current repo-side coverage policy is base-branch no-regression:
  - `ditto-cache`: Rust total line coverage must not regress on PRs.
  - `ditto-client`: Node.js, Go, Python, and Java coverage must not regress on PRs.
- Cross-repo repository-wide absolute minimum thresholds are still not the same thing as these lane-by-lane required no-regression gates.

### `ditto-client`

- `Snapshot + SDK Parity` from `.github/workflows/protocol-parity.yml`
  - covers:
    - protocol snapshot drift against `ditto-cache`
    - snapshot structure validation
    - contract spec validation
    - SDK error-code parity against the committed protocol snapshot

## Manual / Operator Checks

- Confirm the target branch protection in GitHub requires the CI checks from `.github/required-checks.json`.
- Confirm no manual runbook validation exceptions are being used for the target release.
- Confirm any release note / changelog generation flow has been reviewed for the target version.
- Confirm the latest real-run validator output marks `go_no_go.release_candidate=pass`.
- Confirm the same real-run validator output marks `strict_security_enforced=pass` and `tcp_topology_supported=pass`.
- Confirm the same real-run validator output marks `doctor_clean=pass`.
- Confirm `dittoctl node doctor all` returns no `CRITICAL` findings on the target preprod cluster.
- Complete `docs/production-candidate-signoff.md` for the target candidate.

## Supported Production TCP Topology

- Supported:
  - TCP port `7777` exposed on a non-loopback bind only when `client_auth_token` is configured.
  - TCP port `7777` bound to loopback-only for local/dev access without token auth.
- Not supported for production:
  - TCP port `7777` exposed on `0.0.0.0`, private IP, or public IP without `client_auth_token`.
- Operator visibility:
  - `/health/summary` exposes `tcp_client_auth_enabled`, `tcp_client_bind_loopback_only`, `tcp_production_safe`, and `tcp_supported_topology`.
  - `/health/summary` also exposes `insecure_runtime_enabled` and `strict_security_enforced`.
  - `dittoctl node describe` exposes `client-auth-enabled`, `tcp-client-bind-loopback-only`, `tcp-production-safe`, `insecure-runtime-enabled`, and `strict-security-enforced`.
  - `dittoctl node doctor` treats unsupported TCP exposure or insecure runtime bypass as `CRITICAL`.

## Current Scope Limit

- This checklist closes Sprint 1: gate alignment and source-of-truth drift protection.
- It does not by itself make the project production-ready.
- Sprint 1 and Sprint 2 repo-side closure are complete.
- Sprint 3 repo-side progress is in place, but the real-run validation still needs execution on the self-hosted preprod path.
- Sprint 4 repo-side closure is in place: perf gate scope is clarified, and the current coverage policy is documented as required base-branch no-regression across the active lanes.
- Sprint 5 review is now documented in `docs/production-readiness-review.md`.
- Required-check policy is now documented in `.github/required-checks.json`.
- Final candidate sign-off template now exists in `docs/production-candidate-signoff.md`.
- Known limitations are now documented in `docs/known-limitations.md`.
- The remaining open work is now:
  - operational Sprint 3 execution on the self-hosted preprod path,
  - confirmation of GitHub required-check enforcement,
  - final production-candidate sign-off after those blockers are closed.
