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

### Cache operations through mgmt

```bash
dittoctl cache list keys local --pattern "user:*"
dittoctl cache list stats all
dittoctl cache set local demo "value" --ttl 60
dittoctl cache get key local demo
dittoctl cache delete local demo
```

### Bulk TTL update from CLI

```bash
dittoctl cache set-ttl local "session:*" --ttl 300
# remove ttl
dittoctl cache set-ttl local "session:*"
```

### Backup and flush

```bash
dittoctl node backup local
dittoctl cache flush local
```

`cache flush all` requires interactive confirmation.
`node backup` is rejected when persistence backup is disabled by policy.

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
