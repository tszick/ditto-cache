# Security scanning

Ditto cache uses repository-level scanners only. Container image scanning is
intentionally out of scope here because Ditto is consumed inside the Tinyme
dev/runtime environment, where the runtime surface is already scanned.

## GitHub Actions

`.github/workflows/security-scan.yml` runs:

- Gitleaks secret scanning.
- Trivy filesystem scanning for HIGH and CRITICAL vulnerabilities and
  misconfigurations.
- `cargo audit` for Rust dependency advisories.
- Nuclei DAST only when manually started with an explicitly authorized
  `dast_target_url`.

The Nuclei job disables interactsh and excludes intrusive/fuzz templates.

## Local reproduction

```bash
docker run --rm -v "$(pwd):/repo" zricethezav/gitleaks:latest \
  detect --source=/repo --no-banner --redact

docker run --rm -v "$(pwd):/repo" aquasec/trivy:latest \
  fs /repo --scanners vuln,misconfig --severity HIGH,CRITICAL \
  --ignore-unfixed --skip-dirs /repo/target

cargo audit
```

DAST must only be run against an authorized local or dev Ditto endpoint:

```bash
docker run --rm --network host projectdiscovery/nuclei:latest \
  -target http://127.0.0.1:7781/ \
  -severity medium,high,critical \
  -tags cve,exposure,misconfig,xss,sqli,ssrf,redirect \
  -no-interactsh -exclude-tags fuzz,intrusive \
  -rate-limit 50 -timeout 10
```
