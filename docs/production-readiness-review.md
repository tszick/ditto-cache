# Ditto Production Readiness Review

This document is the Sprint 5 repo-side verdict for the current `ditto-cache` + `ditto-client` hardening state.

## Current Verdict

Current verdict: not yet a production-ready candidate.

Reason:
- Sprint 1-4 repo-side hardening is materially in place,
- but a small set of operational and release-governance blockers still remain open,
- so the current state is best described as dev-hardened with partial preprod closure, not fully production-ready.

## Release Blockers

1. Real-run preprod validation has not yet been executed on the self-hosted Windows/preprod path.
- Repo-side support exists:
  - `.github/workflows/preprod-runbook-validation.yml`
  - `.github/workflows/release-gate.yml`
  - `scripts/preprod-runbook-validate.ps1`
- Blocking reason:
  - the workflow/script path is implemented, but there is no completed operational sign-off recorded from the live self-hosted run.

2. Required GitHub branch protection / required checks are still awaiting confirmation against the repo-side policy manifest.
- Repo-side policy now exists in:
  - `.github/required-checks.json`
- Repo-side sign-off template now exists in:
  - `docs/production-candidate-signoff.md`
- Blocking reason:
  - production-readiness still depends on GitHub actually enforcing the required checks described in the manifest.

3. Final production-candidate sign-off has not yet been recorded.
- Repo-side sign-off template now exists in:
  - `docs/production-candidate-signoff.md`
- Blocking reason:
  - the decision record still needs the real-run result, branch-protection confirmation, and explicit performance acceptance for the target candidate.

4. Performance evidence is still smoke-regression grade, not production-like sign-off.
- Current perf gate is intentionally documented as:
  - debug build
  - single-node HTTP path
  - `DITTO_INSECURE=true`
- Blocking reason:
  - this is useful regression protection, but not sufficient evidence for production secure-path capacity or latency sign-off by itself.

## Near-Term Hardening

1. Cross-repo absolute coverage thresholds remain intentionally out of scope.
- Current policy is required base-branch no-regression.
- This is acceptable for the current conservative hardening model, but still less strict than a global minimum-threshold policy.

2. Release governance still depends on operational discipline.
- The docs now define the release path clearly,
- but final trust still depends on the team consistently using:
  - the real-run validator,
  - `dittoctl node doctor all`,
  - the documented release checklist.

## Later Improvements

1. Add a production-like secure perf lane.
- Goal:
  - at least one secure-path baseline that better represents release traffic and startup posture.

2. Consider a stronger cross-SDK coverage threshold policy.
- Goal:
  - move beyond no-regression-only once the current lanes are stable enough to support stricter minima without causing noisy failures.

3. Known limitations are now documented, but still need release-level acknowledgement.
- Artifact:
  - `docs/known-limitations.md`

## Exit Criteria For Production-Ready Candidate

The project can be called a production-ready candidate only when all of these are true:

- the self-hosted real-run validator completes successfully on the target preprod path,
- the same validation window reports `go_no_go.release_candidate=pass`,
- `dittoctl node doctor all` stays free of `CRITICAL` findings on the target preprod cluster,
- `.github/required-checks.json` is confirmed as enforced on the target branches,
- `docs/production-candidate-signoff.md` is completed for the target candidate,
- release approval does not rely on the smoke-only perf gate as the sole performance sign-off.
