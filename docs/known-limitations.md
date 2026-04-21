# Ditto Known Limitations

This document records non-blocking limitations that may remain even after the release blockers are closed.

## Current Non-Blocking Limitations

1. Performance gating is smoke-regression grade.
- The current perf lane is useful for detecting obvious regressions.
- It is not a production-like secure-path capacity benchmark by itself.

2. Coverage policy is conservative no-regression, not absolute threshold enforcement.
- `ditto-cache` uses Rust PR no-regression on total line coverage.
- `ditto-client` uses Node.js / Go / Python / Java PR no-regression.
- This is intentional until stricter thresholds are proven stable enough to avoid noisy failures.

3. Self-signed TLS remains normal in local/dev and lab-style preprod paths.
- Local and preprod validation may legitimately use self-signed certificates and explicit trust exceptions.
- Production should still use the expected trust chain and strict verification.

## Not A Known-Limitations Escape Hatch

The following are not covered by this document and still count as blockers:

- missing self-hosted real-run validation,
- missing branch-protection / required-check confirmation against `.github/required-checks.json`,
- missing final production-candidate sign-off record.
