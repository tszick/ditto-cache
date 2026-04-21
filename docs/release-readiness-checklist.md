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

### `ditto-client`

- `Snapshot + SDK Parity` from `.github/workflows/protocol-parity.yml`
  - covers:
    - protocol snapshot drift against `ditto-cache`
    - snapshot structure validation
    - contract spec validation
    - SDK error-code parity against the committed protocol snapshot

## Manual / Operator Checks

- Confirm the target branch protection in GitHub requires the CI checks above.
- Confirm no manual runbook validation exceptions are being used for the target release.
- Confirm any release note / changelog generation flow has been reviewed for the target version.

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
- Sprint 3 is the next open area: pre-prod verification and operational readiness.
