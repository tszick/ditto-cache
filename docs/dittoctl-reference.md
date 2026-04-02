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

[output]
format = "binary"
```

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
dittoctl node set rate-limit-enabled <target> <true|false>
dittoctl node set rate-limit-requests-per-sec <target> <number>
dittoctl node set rate-limit-burst <target> <number>
dittoctl node set hot-key-enabled <target> <true|false>
dittoctl node set hot-key-max-waiters <target> <number>
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
Status output also includes resilience-runtime fields (`rate_limit_enabled`, `rate_limited_requests_total`, `hot_key_enabled`, `hot_key_coalesced_hits_total`, `hot_key_fallback_exec_total`, `circuit_breaker_enabled`, `circuit_breaker_state`, `circuit_breaker_open_total`, `circuit_breaker_reject_total`) and snapshot restore metadata (`snapshot_last_load_path`, `snapshot_last_load_duration_ms`, `snapshot_last_load_entries`).

## Cache commands

### List keys or stats

```bash
dittoctl cache list keys <target> [--pattern "user:*"]
dittoctl cache list stats <target>
```

### Get key/ttl view

```bash
dittoctl cache get key <target> <key>
dittoctl cache get ttl <target> <key>
```

### Set key

```bash
dittoctl cache set <target> <key> <value> [--ttl <seconds>]
```

### Delete key

```bash
dittoctl cache delete <target> <key>
```

### Flush

```bash
dittoctl cache flush <target>
```

`cache flush all` asks for confirmation.

### Set compressed flag

```bash
dittoctl cache set-compressed <target> <key> <true|false>
```

### Bulk TTL update by pattern

```bash
dittoctl cache set-ttl <target> <pattern> [--ttl <seconds>]
```

Behavior:
- pattern uses glob wildcard (`*`)
- if `--ttl` omitted (or `0`), TTL is removed from matched keys

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
dittoctl cache list keys local --pattern "session:*"
dittoctl cache set local app:cfg "v1" --ttl 300
dittoctl cache set-ttl local "session:*" --ttl 120
dittoctl cache set-ttl local "session:*"
```

## Error handling notes

- Transport/auth/TLS issues are surfaced from mgmt responses.
- For mgmt HTTPS with self-signed certs in dev, trust the CA on host.
- If mgmt is down, `dittoctl` cannot operate even if cache nodes are healthy.
- HTTP API: rate-limited requests return `429`, open-circuit requests return `503`.
