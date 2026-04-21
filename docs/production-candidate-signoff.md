# Production Candidate Sign-Off

Use this document as the final approval record before calling a build a production-ready candidate.

## Release Identity

- Date (UTC):
- Candidate branch / commit:
- Review owner:

## Required Checks Policy Confirmation

Reference policy:
- `.github/required-checks.json`

Confirmation:
- [ ] `ditto-cache` branch protection matches the required checks policy.
- [ ] `ditto-client` branch protection matches the required checks policy.
- Evidence / notes:

## CI Gate Confirmation

- [ ] `Release Readiness Summary` is green on the target candidate.
- [ ] `Runbook Validation (Real Run Gate)` is green on the target candidate.
- [ ] `Snapshot + SDK Parity` is green on the target candidate.
- Evidence / links:

## Real-Run Operational Confirmation

- [ ] Real-run validator executed on the self-hosted preprod path.
- [ ] Output reports `go_no_go.release_candidate=pass`.
- [ ] Output reports `doctor_clean=pass`.
- [ ] `dittoctl node doctor all` is free of `CRITICAL` findings in the same validation window.
- Validation run reference / notes:

## Performance Sign-Off

- [ ] Release approval does not rely on the smoke-only perf gate as the sole performance signal.
- [ ] A human reviewer explicitly accepted the current performance evidence for this candidate.
- Evidence / notes:

## Known Limitations Acknowledgement

Reference:
- `docs/known-limitations.md`

- [ ] Known limitations were reviewed and accepted for this candidate.
- Notes:

## Final Decision

- [ ] Approved as production-ready candidate.
- [ ] Not approved.
- Decision notes:
