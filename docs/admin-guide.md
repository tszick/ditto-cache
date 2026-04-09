# Ditto Admin Guide

This guide covers cluster administration with `ditto-mgmt` and `dittoctl`.

## What is required for what

- Application traffic (`dittod` on 7777/7778): does not require `ditto-mgmt`.
- Management operations (dashboard + admin API + `dittoctl`): require `ditto-mgmt`.

## Components

- `dittod`: cache node runtime
- `ditto-mgmt`: management API/UI service
- `dittoctl`: CLI client for `ditto-mgmt`

`dittoctl` only talks to `ditto-mgmt`.

## Start `ditto-mgmt`

```bash
ditto-mgmt
# or
ditto-mgmt /path/to/mgmt.toml
```

Default config location:

- Linux/macOS: `~/.config/ditto/mgmt.toml`
- Windows: `%APPDATA%\ditto\mgmt.toml`

## Important startup constraints (current behavior)

In this codebase, `ditto-mgmt` runs in strict mode and refuses startup when:

- `[tls].enabled` is `false` (management-to-node admin RPC must use mTLS),
- `[admin].password_hash` is missing,
- `[server].tls_cert` or `[server].tls_key` is missing.

So management API/UI is effectively HTTPS + Basic Auth only.

## Minimal `mgmt.toml`

```toml
[server]
bind = "0.0.0.0"
port = 7781
tls_cert = "/certs/mgmt.pem"
tls_key  = "/certs/mgmt.key"

[connection]
seeds = ["node-1:7779", "node-2:7779", "node-3:7779"]
cluster_port = 7779
timeout_ms = 3000

[tls]
enabled = true
ca_cert = "/certs/ca.pem"
cert = "/certs/mgmt.pem"
key  = "/certs/mgmt.key"

[admin]
username = "admin"
password_hash = "$2b$12$..."

[http_client_auth]
username = "ditto"
password = "plain-text-password"
```

Notes:
- `[http_client_auth]` is only needed when node HTTP auth is enabled and you want mgmt proxy endpoints to work.
- Password hash is bcrypt.

## Generate password hash

```bash
dittoctl hash-password
```

Paste hash into:
- `mgmt.toml` -> `[admin].password_hash` (mgmt UI/API auth)
- `node.toml` -> `[http_auth].password_hash` (node HTTP API auth)

## Environment variable overrides (`ditto-mgmt`)

- `DITTO_MGMT_BIND` -> `server.bind`
- `DITTO_MGMT_TLS_CERT` -> `server.tls_cert`
- `DITTO_MGMT_TLS_KEY` -> `server.tls_key`
- `DITTO_MGMT_ADMIN_USER` -> `admin.username`
- `DITTO_MGMT_ADMIN_PASSWORD_HASH` -> `admin.password_hash`
- `DITTO_MGMT_HTTP_AUTH_USER` -> `http_client_auth.username`
- `DITTO_MGMT_HTTP_AUTH_PASSWORD` -> `http_client_auth.password`

## Configure `dittoctl`

Default config file:

- Linux/macOS: `~/.config/ditto/kvctl.toml`
- Windows: `%APPDATA%\ditto\kvctl.toml`

Example:

```toml
[mgmt]
url = "https://localhost:7781"
timeout_ms = 3000

[output]
format = "binary"
```

## Common admin workflows

### Cluster status

```bash
dittoctl cluster get status
dittoctl cluster get primary
dittoctl cluster list nodes
dittoctl cluster list active-set
```

### Node inspection and configuration

```bash
dittoctl node status all
dittoctl node doctor all
dittoctl node describe all
dittoctl node get active local
dittoctl node set active local false
dittoctl node set persistence-runtime-enabled local true
```

Persistence is only effective when both conditions are true:

`enabled = PERSISTENCE_PLATFORM_ALLOWED && persistence_runtime_enabled`

Platform gates are disabled by default, so backup/export/import stay blocked until explicitly enabled.
`node status` includes the persistence fields (`persistence_platform_allowed`, `persistence_runtime_enabled`, `persistence_enabled`, plus per-feature flags).
`node status` also includes rate-limit and circuit-breaker runtime fields (`rate_limit_enabled`, `rate_limited_requests_total`, `circuit_breaker_state`, and related counters).
`node describe` includes replication tuning properties (`write-timeout-ms`, `gossip-interval-ms`, `gossip-dead-ms`) and `frame-read-timeout-ms`.
`node status` now also includes hot-key coalescing fields (`hot_key_enabled`, `hot_key_coalesced_hits_total`, `hot_key_fallback_exec_total`, `hot_key_inflight_keys`).
`node status` includes read-repair counters (`read_repair_enabled`, `read_repair_trigger_total`, `read_repair_success_total`, `read_repair_throttled_total`).
`node status` includes anti-entropy counters (`anti_entropy_runs_total`, `anti_entropy_repair_trigger_total`, `anti_entropy_last_detected_lag`, `anti_entropy_key_checks_total`, `anti_entropy_key_mismatch_total`).
`node status` includes full anti-entropy reconcile counters (`anti_entropy_full_reconcile_runs_total`, `anti_entropy_full_reconcile_key_checks_total`, `anti_entropy_full_reconcile_mismatch_total`).
`node status` includes mixed-version probe counters (`mixed_version_probe_runs_total`, `mixed_version_peers_detected_total`, `mixed_version_probe_errors_total`, `mixed_version_last_detected_peer_count`).
`node status` includes tenancy fields (`tenancy_enabled`, `tenancy_default_namespace`, `tenancy_max_keys_per_namespace`, `namespace_quota_reject_total`).
`node status` includes snapshot restore metadata (`snapshot_last_load_path`, `snapshot_last_load_duration_ms`, `snapshot_last_load_entries`, `snapshot_last_load_age_secs`) and restore counters (`snapshot_restore_attempt_total`, `snapshot_restore_success_total`, `snapshot_restore_failure_total`, `snapshot_restore_not_found_total`, `snapshot_restore_policy_block_total`).
`node doctor` provides a quick `OK/WARN/CRITICAL` diagnostics summary and exits non-zero when critical issues are detected.

Recommended gossip baseline:
- Start with `gossip_interval_ms=200` and `gossip_dead_ms=15000`.
- Avoid values below `3000` unless you explicitly accept higher false-OFFLINE risk.

Example runtime tuning commands:

```bash
dittoctl node set rate-limit-enabled local true
dittoctl node set rate-limit-requests-per-sec local 1500
dittoctl node set rate-limit-burst local 3000
dittoctl node set hot-key-enabled local true
dittoctl node set hot-key-max-waiters local 128
dittoctl node set read-repair-on-miss-enabled local true
dittoctl node set read-repair-min-interval-ms local 5000
dittoctl node set anti-entropy-enabled local true
dittoctl node set anti-entropy-interval-ms local 60000
dittoctl node set anti-entropy-lag-threshold local 32
dittoctl node set anti-entropy-key-sample-size local 64
dittoctl node set anti-entropy-full-reconcile-every local 10
dittoctl node set anti-entropy-full-reconcile-max-keys local 2000
dittoctl node set mixed-version-probe-enabled local true
dittoctl node set mixed-version-probe-interval-ms local 30000
dittoctl node set tenancy-enabled local true
dittoctl node set tenancy-default-namespace local default
dittoctl node set tenancy-max-keys-per-namespace local 10000
dittoctl node set circuit-breaker-enabled local true
dittoctl node set circuit-breaker-failure-threshold local 25
dittoctl node set circuit-breaker-open-ms local 5000
dittoctl node set circuit-breaker-half-open-max-requests local 5
```

Suggested baseline values:

| Environment | rate-limit-enabled | requests-per-sec | burst | circuit-breaker-enabled | failure-threshold | open-ms |
|---|---:|---:|---:|---:|---:|---:|
| Dev | false | 1000 | 2000 | false | 20 | 5000 |
| Staging | true | 500 | 1000 | true | 10 | 3000 |
| Production (start point) | true | 1000 | 2000 | true | 20 | 5000 |

### Cache operations through mgmt

```bash
dittoctl cache list keys local --pattern "user:*" --namespace tenant-a
dittoctl cache list stats all
dittoctl cache set local demo "value" --ttl 60 --namespace tenant-a
dittoctl cache get key local demo --namespace tenant-a
dittoctl cache delete local demo --namespace tenant-a
```

### Bulk TTL update from CLI

```bash
dittoctl cache set-ttl local "session:*" --ttl 300 --namespace tenant-a
# remove ttl
dittoctl cache set-ttl local "session:*" --namespace tenant-a
```

### Backup and flush

```bash
dittoctl node backup local
dittoctl node restore-snapshot local
dittoctl cache flush local
```

`cache flush all` requires interactive confirmation.
`node backup` is rejected when persistence backup is disabled by policy.
`node restore-snapshot` is rejected when persistence import is disabled by policy.

## Security checklist

- Keep cluster/admin channel (`:7779`) mTLS enabled.
- Keep mgmt UI/API on HTTPS.
- Set strong bcrypt password hashes for both mgmt and node HTTP auth.
- Use distinct credentials for mgmt admin and node HTTP auth.
- If using self-signed certs in dev, trust CA locally instead of disabling verification in production.

## Troubleshooting

- `dittoctl` cannot connect:
  - verify `kvctl.toml` URL,
  - verify mgmt container/process is listening on 7781,
  - verify cert trust and credentials.
- Mgmt cannot reach nodes:
  - verify `connection.seeds`,
  - verify `[tls]` cert/key/CA paths,
  - verify node cluster port 7779 is reachable.
- Mgmt proxy operations return auth errors:
  - verify `[http_client_auth]` matches node `[http_auth]` credentials.
