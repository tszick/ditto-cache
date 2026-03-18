# dittoctl – Command Reference

`dittoctl` is the admin CLI for Ditto clusters. All commands are routed through
**`ditto-mgmt`** — a management service that must be running and reachable.

---

## Architecture

```
dittoctl  ──HTTP(S)──►  ditto-mgmt:7781  ──TCP/mTLS──►  node-1:7779
Browser   ──HTTP(S)──►  ditto-mgmt:7781  ──TCP/mTLS──►  node-2:7779
```

`ditto-mgmt` must be running for `dittoctl` to work. Start it alongside the cluster:

```bash
ditto-mgmt [path/to/mgmt.toml]
```

The web UI is available at `https://localhost:7781` (or `http://` when HTTPS is
not configured) once `ditto-mgmt` is running.

---

## ditto-mgmt Configuration

`ditto-mgmt` reads `~/.config/ditto/mgmt.toml` (Linux/macOS) or `%APPDATA%\ditto\mgmt.toml` (Windows).

```toml
[server]
bind = "0.0.0.0"    # "site-local" | "0.0.0.0" | "localhost" | "<ip>"
port = 7781
# Optional: HTTPS on port 7781 (server-only TLS, no client cert required).
# tls_cert = "/etc/ditto/certs/mgmt.pem"
# tls_key  = "/etc/ditto/certs/mgmt.key"

[connection]
seeds        = ["node-1:7779"]   # at least one cluster node
cluster_port = 7779
timeout_ms   = 3000

# mTLS – required when the cluster has TLS enabled on port 7779
[tls]
enabled = false
ca_cert = ""   # path to CA certificate (PEM)
cert    = ""   # path to client certificate (PEM)
key     = ""   # path to client private key (PEM)

# Optional: HTTP Basic Auth for the management UI and API (port 7781).
# Generate the hash with: dittoctl hash-password
# [admin]
# username      = "admin"
# password_hash = "$2b$12$..."

# Optional: credentials for proxying cache requests to dittod HTTP port (7778).
# Required only when DITTO_HTTP_AUTH_* is enabled on the nodes.
# [http_client_auth]
# username = "ditto"
# password = "plaintext-password"
```

---

## dittoctl Configuration

`dittoctl` reads `~/.config/ditto/kvctl.toml` (Linux/macOS) or `%APPDATA%\ditto\kvctl.toml` (Windows).

```toml
[mgmt]
url        = "https://localhost:7781"   # use https:// when ditto-mgmt has TLS configured
timeout_ms = 3000

[output]
format = "binary"
```

**Change the mgmt URL from the CLI:**

```bash
dittoctl node set local url "https://my-server:7781"
# Plain HTTP (when TLS is not configured):
dittoctl node set local url "http://my-server:7781"
```

---

## Target syntax

Every command that operates on a node requires a **target** argument:

| Target | Meaning |
|--------|---------|
| `local` | The node running on localhost (uses `cluster_port` from config) |
| `node-2` | A specific node by name or address (`host:port` also accepted) |
| `all` | All cluster nodes — discovered automatically from seeds + gossip. For Docker-from-host setups, list all published ports in seeds: `["127.0.0.1:7779", "127.0.0.1:7789", "127.0.0.1:7799"]` |

---

## Command structure

```
dittoctl <resource> <verb> [<what>] <target> [value] [flags]
```

---

## hash-password

### `dittoctl hash-password`

Interactively prompts for a plain-text password and prints its **bcrypt hash**.
Use the output as the value for `password_hash` in `[http_auth]` (node.toml) or
`[admin]` (mgmt.toml).

```bash
dittoctl hash-password
Enter password: ****
$2b$12$abc123...
```

The hash is printed to stdout so it can be piped or copied directly into a
config file or environment variable.

---

## node

### `dittoctl node describe <target>`

Show all properties of a node.

```bash
dittoctl node describe local
dittoctl node describe node-2
dittoctl node describe all
```

Example output:
```
  Node: 127.0.0.1:7779

  property               value
  ──────────────────────────────────────────────────
  id                     node-1
  committed-index        142
  uptime                 7234s
  status                 Active
  primary                true
  client-port            7777
  http-port              7778
  cluster-port           7779
  gossip-port            7780
  max-memory             512mb
  default-ttl            0s
  value-size-limit       104857600
  max-keys               100000
  compression-enabled    true
  compression-threshold  4096
```

---

### `dittoctl node status <target>`

Show health status for one or all nodes: id, memory usage, heartbeat latency, uptime, and backup storage.

```bash
dittoctl node status local
dittoctl node status node-2
dittoctl node status all
```

Example output:
```
  Node: 127.0.0.1:7779

  id                     node-1
  committed-index        142
  memory                 214217728 / 536870912 bytes
  heartbeat              3ms
  uptime                 2h 0m 34s
  backup-storage         47185920 bytes
```

---

### `dittoctl node get <property> <target>`

Get a single property value.

```bash
dittoctl node get active        local
dittoctl node get status        node-2
dittoctl node get primary               # which node is currently primary
dittoctl node get committed-index node-2
```

---

### `dittoctl node set <property> <target> <value>`

Set a property on one or all nodes.

```bash
dittoctl node set active    node-2  false     # maintenance mode
dittoctl node set active    node-2  true      # re-activate
dittoctl node set active    all     false     # cluster-wide maintenance
dittoctl node set max-memory local  1024mb
dittoctl node set default-ttl local 3600
dittoctl node set value-size-limit local 1048576   # max value: 1 MiB
dittoctl node set max-keys         local 50000      # max 50 000 keys
dittoctl node set compression-enabled  local true
dittoctl node set compression-threshold local 8192   # compress values > 8 KB
dittoctl node set primary  node-2  true              # force-elect node-2 as primary
```

**Port changes** (`client-port`, `http-port`, `cluster-port`, `gossip-port`):

```bash
dittoctl node set http-port node-1 7900
```

Port changes are saved to `node.toml` immediately, but take effect only after restart.
The node **must already be `Inactive`** before setting a port property; the server
returns an error if the node is `Active`.

**Writable properties:**

| Property | Type | Notes |
|----------|------|-------|
| `active` / `status` | `true`/`false` | Runtime toggle, no restart |
| `primary` | `true`/`false` | Force-elect; broadcast to peers |
| `client-port` | `u16` | Restart required |
| `http-port` | `u16` | Restart required |
| `cluster-port` | `u16` | Restart required |
| `gossip-port` | `u16` | Restart required |
| `max-memory` | `<N>mb` | Runtime |
| `default-ttl` | `<N>` (secs) | Runtime |
| `value-size-limit` | bytes, 0=unlimited | Runtime |
| `max-keys` | count, 0=unlimited | Runtime |
| `compression-enabled` | `true`/`false` | Runtime |
| `compression-threshold` | bytes (≥ 4096, only increasable) | Runtime |

**Read-only properties:** `id`, `committed-index`, `uptime`

> **Note:** Setting `active = false` takes effect immediately at runtime (no restart needed).
> The node rejects all client requests (GET / SET / DELETE) and returns `NODE_INACTIVE`,
> but continues to run, participate in gossip, and stay in sync — making it immediately
> ready to be re-activated. Intended for rolling updates and maintenance windows.

---

### `dittoctl node list <what> <target>`

```bash
dittoctl node list ports local          # show all port assignments
dittoctl node list ports all
```

---

### `dittoctl node backup <target>`

Trigger an immediate on-demand backup on a node. The node must already be `Inactive`. After the backup completes, the node
automatically syncs from the primary and re-joins as `Active`. The backup file is written to
the directory configured in `[backup].path` in `node.toml`.

```bash
dittoctl node backup local
dittoctl node backup node-2
dittoctl node backup all     # backs up every discovered node
```

Example output:
```
  127.0.0.1:7779 ← backup written: ./backups/node-1_backup_2024.01.15_02-00-00_UTC.bin
```

> The `[backup]` section must be present in `node.toml` (see **Scheduled backup** below).
> For scheduled automatic backups, configure `[backup].schedule`.

---

## Scheduled backup (node.toml)

Ditto has a built-in cron-based backup scheduler. Configure it in `node.toml`:

```toml
[backup]
enabled      = true
schedule     = "0 2 * * *"    # daily at 02:00 UTC (standard 5-field cron)
path         = "./backups"     # directory for backup files
format       = "binary"        # binary | json
retain_days  = 30              # delete files older than N days (0 = never delete)
# Optional: AES-256-GCM encryption for backup files
# encryption_key = "<64 hex chars>"   # generate: openssl rand -hex 32
```

**Backup flow** (identical for scheduled and manual `node backup`):
1. Node is set to `Inactive`
2. Store snapshot is serialized and written to a timestamped file
3. If `encryption_key` is set, the file is encrypted with AES-256-GCM
4. Node syncs from the primary and re-joins as `Active`
5. Files older than `retain_days` are deleted from the backup directory

**File naming convention:**
```
{node_id}_backup_{YYYY.MM.DD_HH-MM-SS}_UTC.{bin|json}          # unencrypted
{node_id}_backup_{YYYY.MM.DD_HH-MM-SS}_UTC.{bin|json}.enc      # encrypted
```
Examples:
- `node-1_backup_2026.02.25_02-00-00_UTC.bin`
- `node-1_backup_2026.02.25_02-00-00_UTC.bin.enc`

**Default values** (when the `[backup]` section is absent from `node.toml`):

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Must be set to `true` to activate scheduling |
| `schedule` | `"0 2 * * *"` | Cron expression (5-field: min hour dom month dow) |
| `path` | `"./backups"` | Backup output directory (relative to the node's working dir) |
| `format` | `"binary"` | `binary` (compact, faster) or `json` (human-readable) |
| `retain_days` | `30` | Auto-delete files older than this; `0` disables rotation |
| `encryption_key` | absent | When set: AES-256-GCM encryption, `.enc` file suffix |

---

## cache

### `dittoctl cache list <what> <target>`

```bash
dittoctl cache list keys  local                      # all keys
dittoctl cache list keys  node-2 --pattern "user:*"  # filtered
dittoctl cache list stats local                      # memory, hit rate, evictions
dittoctl cache list stats all
```

Example stats output:
```
  Node: 127.0.0.1:7779

  keys                   18 432
  memory                 214 MB / 512 MB
  evictions              301
  hits                   1 204 887
  misses                 3 201
  hit-rate               99%
```

---

### `dittoctl cache get <what> <target> <key>`

```bash
dittoctl cache get key local  user:42
dittoctl cache get ttl node-2 session:abc123
```

---

### `dittoctl cache set <target> <key> <value> [--ttl <secs>]`

Writes a key directly via the client TCP port (default: 7777). The write is coordinated by the primary and replicated to all active nodes.

```bash
dittoctl cache set local  user:42 "Alice"
dittoctl cache set node-2 session:abc123 "tok_xyz"
dittoctl cache set local  tmp:key "expiring" --ttl 300    # expires in 5 minutes
```

---

### `dittoctl cache delete <target> <key>`

```bash
dittoctl cache delete local  user:42
dittoctl cache delete node-2 session:abc123
```

---

### `dittoctl cache flush <target>`

Remove all keys from a node's cache.

```bash
dittoctl cache flush local
dittoctl cache flush node-2
dittoctl cache flush all      # requires interactive confirmation
```

> ⚠ Flushing `all` requires typing `yes` at the confirmation prompt:
> ```
>   ⚠ This will flush cache on ALL nodes (node-1, node-2, node-3).
>   Type "yes" to confirm: _
> ```

---

### `dittoctl cache set-compressed <target> <key> <true|false>`

Manually override the compression flag for an individual key.

- `true` – compress the current value if it is not already compressed
- `false` – decompress the current value if it is compressed

```bash
dittoctl cache set-compressed local  bigval true
dittoctl cache set-compressed node-2 session:abc123 false
```

The automatic compression setting (`compression-enabled`, `compression-threshold`) is applied when a key is written. This command lets you re-compress or decompress an existing key without rewriting it.

Use `dittoctl cache get key <target> <key>` to check the current `compressed` flag:
```
  key          bigval
  value        ...
  version      7
  freq         3
  compressed   true
  ttl          (no ttl)
```

---

### `dittoctl cache set-ttl <target> <pattern> [--ttl <secs>]`

Set the TTL for all keys matching a glob pattern (`*` wildcard). The pattern is
matched against every live key on the target node(s); only non-expired keys are
affected.

- `--ttl <secs>` — new TTL in seconds from now
- Omitting `--ttl` (or passing `0`) **removes** the TTL, so the keys live forever

```bash
dittoctl cache set-ttl local  "user:1234:*"  --ttl 3600   # 1 hour
dittoctl cache set-ttl node-2 "session:*"    --ttl 300    # 5 minutes
dittoctl cache set-ttl all    "cache:tmp:*"  --ttl 60     # all nodes, 60 s
dittoctl cache set-ttl local  "legacy:*"                  # remove TTL
```

Example output:
```
  127.0.0.1:7779 ← 42 key(s) updated
```

> Wildcard listing uses the same glob syntax as `cache list keys --pattern`.

---

## cluster

### `dittoctl cluster list <what>`

```bash
dittoctl cluster list nodes        # all nodes with status
dittoctl cluster list active-set   # only currently active nodes
```

Example output:
```
  node-id                                status       primary    committed
  ────────────────────────────────────────────────────────────────────────
  018e1a2b-3c4d-7e8f-9a0b-1c2d3e4f5a6b  Active       true       142
  018e1a2b-3c4d-7e8f-9a0b-1c2d3e4f5a6c  Active       false      142
  018e1a2b-3c4d-7e8f-9a0b-1c2d3e4f5a6d  Syncing      false      138

  Total: 3 node(s)
```

---

### `dittoctl cluster get <what>`

```bash
dittoctl cluster get status            # active / syncing / offline counts
dittoctl cluster get primary           # ID of the current primary
dittoctl cluster get committed-index   # highest committed log index
```

Example output:
```
  total:   3
  active:  2
  syncing: 1
  offline: 0
```

---

## Common workflows

### Rolling update (zero downtime)

```bash
# 1. Take node-2 out of service
dittoctl node set active node-2 false

# 2. Deploy / restart node-2 with new binary
#    (node stays synced while inactive)

# 3. Re-activate
dittoctl node set active node-2 true

# 4. Verify
dittoctl node get status node-2
```

### Backup

```bash
# One command per node — deactivates, backs up, then syncs and re-activates automatically
dittoctl node backup local
dittoctl node backup all     # backs up every cluster node
```

For unattended scheduled backups, configure `[backup]` in `node.toml` — see **Scheduled backup** above.

### Check cluster health

```bash
dittoctl cluster get status
dittoctl cluster list nodes
dittoctl cache list stats all
```
