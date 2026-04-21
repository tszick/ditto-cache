# dittoctl Reference

`dittoctl` is the Ditto management CLI.

## Connection model

- `dittoctl` talks to `ditto-mgmt` over HTTP/HTTPS.
- It does not connect to `dittod` directly.

## Configuration

Config file:

- Linux/macOS: `~/.config/ditto/kvctl.toml`
- Windows: `%APPDATA%\ditto\kvctl.toml`

Example:

```toml
[mgmt]
url = "https://localhost:7781"
timeout_ms = 3000
username = "admin"
password = "replace-me"
insecure_skip_verify = true

[output]
format = "binary"
```

Notes:
- `username` / `password` are optional and used for `ditto-mgmt` HTTP Basic Auth.
- `insecure_skip_verify = true` is intended for self-signed local/dev TLS only.

Local config changes can also be made from CLI:

```bash
dittoctl node set url local https://localhost:7781
dittoctl node set timeout local 5000
dittoctl node set format local binary
```

## Global syntax

```bash
dittoctl [--config <path>] <resource> <command> [args]
```

Resources:

- `node`
- `cache`
- `cluster`
- `hash-password`

## `hash-password`

```bash
dittoctl hash-password
```

Prompts for a password and prints bcrypt hash.

## Node commands

### Describe

```bash
dittoctl node describe <target>
```

`node describe` includes replication/socket tuning fields such as `write-timeout-ms`, `gossip-interval-ms`, `gossip-dead-ms`, and `frame-read-timeout-ms`.

### Get property

```bash
dittoctl node get <property> <target>
```

### Set property

```bash
dittoctl node set <property> <target> <value>
```

Special local settings:
- `target=local` + `property=url|timeout|format` updates `kvctl.toml`.

Persistence runtime toggle:

```bash
dittoctl node set persistence-runtime-enabled <target> <true|false>
```

Effective policy is:

`enabled = PERSISTENCE_PLATFORM_ALLOWED && persistence_runtime_enabled`

Resilience runtime tuning:

```bash
dittoctl node set write-quorum-mode <target> <all-active|majority>
dittoctl node set rate-limit-enabled <target> <true|false>
dittoctl node set rate-limit-requests-per-sec <target> <number>
dittoctl node set rate-limit-burst <target> <number>
dittoctl node set hot-key-enabled <target> <true|false>
dittoctl node set hot-key-max-waiters <target> <number>
dittoctl node set hot-key-adaptive-waiters-enabled <target> <true|false>
dittoctl node set hot-key-adaptive-min-waiters <target> <number>
dittoctl node set hot-key-adaptive-success-threshold <target> <number>
dittoctl node set hot-key-adaptive-state-max-keys <target> <number>
dittoctl node set read-repair-on-miss-enabled <target> <true|false>
dittoctl node set read-repair-min-interval-ms <target> <number>
dittoctl node set read-repair-max-per-minute <target> <number>
dittoctl node set anti-entropy-enabled <target> <true|false>
dittoctl node set anti-entropy-interval-ms <target> <number>
dittoctl node set anti-entropy-min-repair-interval-ms <target> <number>
dittoctl node set anti-entropy-lag-threshold <target> <number>
dittoctl node set anti-entropy-key-sample-size <target> <number>
dittoctl node set anti-entropy-full-reconcile-every <target> <number>
dittoctl node set anti-entropy-full-reconcile-max-keys <target> <number>
dittoctl node set anti-entropy-budget-max-checks-per-run <target> <number>
dittoctl node set anti-entropy-budget-max-duration-ms <target> <number>
dittoctl node set mixed-version-probe-enabled <target> <true|false>
dittoctl node set mixed-version-probe-interval-ms <target> <number>
dittoctl node set tenancy-enabled <target> <true|false>
dittoctl node set tenancy-default-namespace <target> <string>
dittoctl node set tenancy-max-keys-per-namespace <target> <number>
dittoctl node set circuit-breaker-enabled <target> <true|false>
dittoctl node set circuit-breaker-failure-threshold <target> <number>
dittoctl node set circuit-breaker-open-ms <target> <number>
dittoctl node set circuit-breaker-half-open-max-requests <target> <number>
```

### List sub-resources

```bash
dittoctl node list ports <target>
```

### Backup

```bash
dittoctl node backup <target>
```

Backup works only when persistence backup is enabled by policy (platform allow + runtime enabled).

### Restore Latest Snapshot

```bash
dittoctl node restore-snapshot <target>
```

Restore works only when persistence import is enabled by policy (platform allow + runtime enabled).

### Status

```bash
dittoctl node status <target>
```

`target` can be `local`, `all`, or a specific node address/name expected by mgmt.
Status output includes persistence policy flags (`persistence_platform_allowed`, `persistence_runtime_enabled`, `persistence_enabled`, and per-feature states).
Status output also includes resilience-runtime fields (`rate_limit_enabled`, `rate_limited_requests_total`, `hot_key_enabled`, `hot_key_adaptive_waiters_enabled`, `hot_key_coalesced_hits_total`, `hot_key_fallback_exec_total`, `hot_key_wait_timeout_total`, `hot_key_stale_served_total`, `hot_key_inflight_keys`, `hot_key_stale_cache_entries`, `hot_key_adaptive_state_keys`, `hot_key_adaptive_limit_increase_total`, `hot_key_adaptive_limit_decrease_total`, `read_repair_enabled`, `read_repair_trigger_total`, `read_repair_success_total`, `read_repair_throttled_total`, `read_repair_budget_exhausted_total`, `anti_entropy_runs_total`, `anti_entropy_repair_trigger_total`, `anti_entropy_repair_throttled_total`, `anti_entropy_last_detected_lag`, `anti_entropy_key_checks_total`, `anti_entropy_key_mismatch_total`, `anti_entropy_budget_exhausted_total`, `circuit_breaker_enabled`, `circuit_breaker_state`, `circuit_breaker_open_total`, `circuit_breaker_reject_total`), request-observability fields (`client_requests_total`, source split `client_requests_{tcp,http,internal}_total`, latency buckets, estimated latency percentiles `client_latency_p50/p90/p95/p99_estimate_ms`, namespace-level latency summary `namespace_latency_top`, hot-key pressure summary `hot_key_top_usage`, source-split errors `client_errors_{tcp,http,internal}_total`, categorized `client_error_*` counters), and snapshot restore metadata (`snapshot_last_load_path`, `snapshot_last_load_duration_ms`, `snapshot_last_load_entries`, `snapshot_last_load_age_secs`, `snapshot_restore_attempt_total`, `snapshot_restore_success_total`, `snapshot_restore_failure_total`, `snapshot_restore_not_found_total`, `snapshot_restore_policy_block_total`, `snapshot_restore_success_ratio_pct`).

## Cache commands

### List keys or stats

```bash
dittoctl cache list keys <target> [--pattern "user:*"] [--namespace <tenant>]
dittoctl cache list stats <target>
```

### Get key/ttl view

```bash
dittoctl cache get key <target> <key> [--namespace <tenant>]
dittoctl cache get ttl <target> <key> [--namespace <tenant>]
```

### Set key

```bash
dittoctl cache set <target> <key> <value> [--ttl <seconds>] [--namespace <tenant>]
```

### Delete key

```bash
dittoctl cache delete <target> <key> [--namespace <tenant>]
```

### Flush

```bash
dittoctl cache flush <target>
```

`cache flush all` asks for confirmation.

### Set compressed flag

```bash
dittoctl cache set-compressed <target> <key> <true|false> [--namespace <tenant>]
```

### Bulk TTL update by pattern

```bash
dittoctl cache set-ttl <target> <pattern> [--ttl <seconds>] [--namespace <tenant>]
```

Behavior:
- pattern uses glob wildcard (`*`)
- if `--ttl` omitted (or `0`), TTL is removed from matched keys
- `--namespace` scopes cache operations to one tenant namespace

## Cluster commands

### List

```bash
dittoctl cluster list nodes
dittoctl cluster list active-set
```

### Get

```bash
dittoctl cluster get status
dittoctl cluster get primary
dittoctl cluster get committed-index
```

## Typical examples

```bash
dittoctl cluster get status
dittoctl node describe all
dittoctl cache list keys local --pattern "session:*" --namespace tenant-a
dittoctl cache set local app:cfg "v1" --ttl 300 --namespace tenant-a
dittoctl cache set-ttl local "session:*" --ttl 120 --namespace tenant-a
dittoctl cache set-ttl local "session:*" --namespace tenant-a
```

## Error handling notes

- Transport/auth/TLS issues are surfaced from mgmt responses.
- For mgmt HTTPS with self-signed certs in dev, trust the CA on host.
- If mgmt is down, `dittoctl` cannot operate even if cache nodes are healthy.
- HTTP API: rate-limited requests return `429`, open-circuit requests return `503`.
