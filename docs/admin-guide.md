# Ditto – Admin Guide

This guide covers day-to-day administration of a Ditto cluster using both the
**`dittoctl` command-line tool** and the **`ditto-mgmt` web dashboard**.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Starting ditto-mgmt](#starting-ditto-mgmt)
- [Web Dashboard](#web-dashboard)
- [dittoctl — CLI Reference](#dittoctl--cli-reference)
  - [Configuration](#configuration)
  - [node commands](#node-commands)
  - [cache commands](#cache-commands)
  - [cluster commands](#cluster-commands)
- [Node configuration — node.toml](#node-configuration--nodetoml)
  - [Replication \[replication\]](#replication-replication)
  - [HTTP Basic Auth \[http\_auth\]](#http-basic-auth-http_auth)
  - [File logging \[log\]](#file-logging-log)
  - [Scheduled backup \[backup\]](#scheduled-backup-log)
  - [Environment variable overrides — dittod](#environment-variable-overrides--dittod)
- [ditto-mgmt configuration](#ditto-mgmt-configuration)
  - [mgmt.toml reference](#mgmttoml-reference)
  - [Environment variable overrides — ditto-mgmt](#environment-variable-overrides--ditto-mgmt)
- [Common Workflows](#common-workflows)
  - [Rolling restart](#rolling-restart)
  - [Enable HTTP Basic Auth on 7778 and 7781](#enable-http-basic-auth-on-7778-and-7781)
  - [Enable file logging](#enable-file-logging)
  - [Scheduled backup](#scheduled-backup)
  - [Enable / disable a node](#enable--disable-a-node)
  - [Force-elect a primary](#force-elect-a-primary)

---

## Prerequisites

| Component | Required | Notes |
|-----------|----------|-------|
| `dittod` nodes | ✅ | At least one running node |
| `ditto-mgmt` | ✅ | Must be reachable from your workstation |
| `dittoctl` binary | ✅ for CLI | In `PATH` or use the full path |
| Browser | ✅ for Web UI | Any modern browser |

Both tools communicate exclusively with `ditto-mgmt` (default port **7781**).
Neither tool connects directly to `dittod` nodes.

---

## Starting ditto-mgmt

```bash
# Start with default config (~/.config/ditto/mgmt.toml)
ditto-mgmt

# Start with an explicit config file
ditto-mgmt /etc/ditto/mgmt.toml
```

`ditto-mgmt` prints the listening address on startup:

```
ditto-mgmt listening on http://0.0.0.0:7781    # plain HTTP (default)
ditto-mgmt listening on https://0.0.0.0:7781   # when tls_cert + tls_key are set
```

Full configuration reference is in the
[ditto-mgmt configuration](#ditto-mgmt-configuration) section below.

If the config file does not exist, built-in defaults are used (single local
node, TLS and auth disabled).

---

## Web Dashboard

Open **https://\<mgmt-host\>:7781** in a browser (or `http://` if HTTPS is not configured).

> When using the Docker test cluster with a self-signed certificate, accept the
> browser warning once ("Advanced → Proceed to localhost").  The generated
> `mgmt.pem` includes `localhost` and `127.0.0.1` as SANs.

### Layout

```
┌──────────────────────────────────────────────────────────────┐
│  🗄 Ditto Cluster Management        [Auto-refresh ●ON]  [↺]  │
│  Last updated: 14:32:01                                       │
├──────────────────────────────────────────────────────────────┤
│ ●  node-1   PRIMARY   142   ████░ 40%   3ms   2h 0m   –       │
│                              [Deactivate]  [Backup]           │
│                                                               │
│ ●  node-2   –         142   ████░ 38%   4ms   1h 58m  –       │
│                              [Deactivate]  [Backup]           │
│                                                               │
│ ⚠  node-3   –         –     –           –     –       –       │
│             (unreachable)                                     │
├──────────────────────────────────────────────────────────────┤
│ ▼ Details: node-1                                             │
│   node_id:    d7a482a9-...                                    │
│   backup:     47 MB                                           │
│   keys:       12 345                                          │
│   evictions:  0     hit-rate: 98%                             │
└──────────────────────────────────────────────────────────────┘
```

### Status indicators

| Symbol | Meaning |
|--------|---------|
| 🟢 green dot | Node is `Active` and reachable |
| 🟡 yellow dot | Node is `Inactive` (maintenance) but reachable |
| 🔴 red ⚠ | Node is unreachable (gossip timeout / network issue) |

### Controls

| Button | Action |
|--------|--------|
| **Activate** | Sets node status to `Active` |
| **Deactivate** | Sets node status to `Inactive` (graceful maintenance mode) |
| **Backup** | Triggers an immediate backup; node must be `Inactive` |
| **↺** | Manual refresh |
| **Auto-refresh toggle** | Polls `/api/nodes` every 3 seconds when enabled |

Click any row to expand the **Details** panel showing all node stats.

---

## Node configuration — node.toml

Each `dittod` node is configured via a TOML file (default: `node.toml` in the
working directory, or the path given as the first CLI argument).  All sections
other than `[node]`, `[cluster]`, `[cache]`, and `[replication]` are optional
and have safe defaults, so you only need to specify what you want to change.

---

### Replication `[replication]`

Controls gossip heartbeat timing and write-quorum behaviour.

```toml
[replication]
gossip_interval_ms = 200    # how often each node broadcasts a UDP heartbeat
gossip_dead_ms     = 3000   # ms without heartbeat before a peer is marked Offline
write_timeout_ms   = 500    # write-quorum ACK timeout in ms
```

#### Field reference

| Field | Default | Description |
|-------|---------|-------------|
| `gossip_interval_ms` | `200` | UDP heartbeat interval.  Lower = faster failure detection and faster convergence after topology changes, at the cost of slightly more network traffic. |
| `gossip_dead_ms` | `3000` | A peer is marked `Offline` when no heartbeat has arrived within this window.  The default of `3000 ms` (15× the heartbeat interval) tolerates brief connectivity disruptions that can occur when a container on the same Docker bridge is stopped. |
| `write_timeout_ms` | `500` | How long the primary waits for acknowledgements from active peers before failing a write. |

> **Primary election** — the active node with the lexicographically smallest
> UUID is elected primary automatically.  When `gossip_dead_ms` is too
> aggressive relative to the gossip interval, a momentary network hiccup
> (e.g. Docker bridge ARP flush on container teardown) can cause nodes to
> briefly mark each other `Offline`, each electing themselves primary.
> Keeping `gossip_dead_ms ≥ 10 × gossip_interval_ms` prevents this.

> **`DITTO_GOSSIP_DEAD_MS`** — overrides `gossip_dead_ms` at runtime without
> editing the config file (useful for Docker Compose overrides):
> ```bash
> DITTO_GOSSIP_DEAD_MS=3000 dittod node.toml
> ```

---

### HTTP Basic Auth `[http_auth]`

Protects the dittod **HTTP REST port** (7778) with HTTP Basic Auth.
When `password_hash` is absent the endpoint is open (default).

The `/ping` health-check endpoint is always exempt — no credentials needed
for container/load-balancer health checks.

```toml
[http_auth]
username      = "ditto"         # optional; defaults to "ditto" when omitted
password_hash = "$2b$12$..."    # bcrypt hash — generate with: dittoctl hash-password
```

> **Clients must send `Authorization: Basic <base64(user:pass)>` on every
> request.**  Example with curl:
> ```bash
> curl -u ditto:mysecret http://localhost:7778/key/foo
> ```

> **ditto-mgmt proxy** — when 7778 auth is enabled, set matching credentials
> in ditto-mgmt's `[http_client_auth]` section (or env vars) so that cache
> get/set/delete proxied via ditto-mgmt continue to work.

---

### TCP Client Authentication (Port 7777)

Protects the dittod **TCP binary client port** (7777) with a shared secret token.

```toml
[node]
# Optional shared secret for TCP client authentication (port 7777)
client_auth_token = "my-super-secret-token"
```

> **Security Warning regarding Port 7777 and 7780**
> Production deployment requires network isolation (VPC, internal Docker networks) or strict firewall rules (iptables/UFW) restricting access to the client port (7777) and gossip port (7780) only to authorized application servers, even if `client_auth_token` is used.

---

### File logging `[log]`

By default `dittod` writes log output only to **stderr**.  The `[log]` section
enables an additional **rolling file log** in a directory of your choice.

```toml
[log]
enabled     = false          # set to true to write logs to files
path        = "./logs"       # directory to write log files into (created if absent)
rotation    = "daily"        # hourly | daily | never
retain_days = 30             # delete files older than N days (0 = keep forever)
level       = "info"         # trace | debug | info | warn | error
```

#### Field reference

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | When `true`, log records are written to rolling files **in addition to stderr**. |
| `path` | `"./logs"` | Directory for log files.  Created automatically on startup if it does not exist. |
| `rotation` | `"daily"` | How often a new log file is started.  `"hourly"` / `"daily"` / `"never"`. |
| `retain_days` | `30` | Log files older than this many days are deleted automatically.  `0` disables cleanup. |
| `level` | `"info"` | Minimum severity written to the log file: `trace` · `debug` · `info` · `warn` · `error`. |

> **`RUST_LOG` override** — if the environment variable `RUST_LOG` is set it
> takes precedence over `level` for both console and file output.  This is
> useful for raising verbosity temporarily without editing the config:
> ```bash
> RUST_LOG=dittod=debug dittod node.toml
> ```

#### Log file naming

`tracing-appender` appends a timestamp suffix to the base name `dittod.log`:

| Rotation | Example filename |
|----------|-----------------|
| `daily` | `dittod.log.2026-02-23` |
| `hourly` | `dittod.log.2026-02-23-14` |
| `never` | `dittod.log` (single file, grows indefinitely) |

The automatic cleanup task runs **hourly** and removes only files whose name
starts with `dittod.log`, so other files in the same directory are untouched.

---

### Scheduled backup `[backup]`

```toml
[backup]
enabled        = false
schedule       = "0 2 * * *"   # cron: min hour dom month dow  →  daily at 02:00 UTC
path           = "./backups"    # output directory (created if absent)
format         = "binary"       # binary | json
retain_days    = 30             # 0 = never delete
# encryption_key = "<64 hex chars>"  # optional AES-256-GCM key — generate: openssl rand -hex 32
```

When `encryption_key` is set, backup files are encrypted with AES-256-GCM and
written with an `.enc` suffix (e.g. `node-1_backup_2026.02.25_02-00-00_UTC.bin.enc`).
The key is independent of the TLS certificates — use a separate key stored securely.

See the [Scheduled backup](#scheduled-backup) workflow for operational details.

---

### Environment variable overrides — dittod

All key `node.toml` settings can be overridden via environment variables.
Env vars take precedence over the config file and are the recommended way
to configure `dittod` in Docker / Kubernetes deployments.

| Variable | Overrides | Example value |
|----------|-----------|---------------|
| `DITTO_NODE_ID` | `node.id` | `node-1` |
| `DITTO_ACTIVE` | `node.active` | `true` |
| `DITTO_BIND_ADDR` | `node.bind_addr` | `0.0.0.0` |
| `DITTO_CLUSTER_BIND_ADDR` | `node.cluster_bind_addr` | `site-local` |
| `DITTO_SEEDS` | `cluster.seeds` (comma-separated) | `node-2:7779,node-3:7779` |
| `DITTO_MAX_MEMORY_MB` | `cache.max_memory_mb` | `512` |
| `DITTO_GOSSIP_DEAD_MS` | `replication.gossip_dead_ms` | `3000` |
| `DITTO_TLS_ENABLED` | `tls.enabled` | `true` |
| `DITTO_TLS_CA_CERT` | `tls.ca_cert` | `/certs/ca.pem` |
| `DITTO_TLS_CERT` | `tls.cert` | `/certs/node1.pem` |
| `DITTO_TLS_KEY` | `tls.key` | `/certs/node1.key` |
| `DITTO_HTTP_AUTH_USER` | `http_auth.username` | `ditto` |
| `DITTO_HTTP_AUTH_PASSWORD_HASH` | `http_auth.password_hash` | `$2b$12$...` |
| `DITTO_BACKUP_ENCRYPTION_KEY` | `backup.encryption_key` | 64 hex chars (AES-256-GCM) |
| `RUST_LOG` | Log level (console **and** file) | `dittod=debug` |

---

## ditto-mgmt configuration

### mgmt.toml reference

```toml
[server]
bind = "0.0.0.0"   # "site-local" | "0.0.0.0" | "localhost" | "<ip>"
port = 7781        # listening port
# Optional HTTPS — serve 7781 over TLS when both are set:
# tls_cert = "/certs/mgmt.pem"
# tls_key  = "/certs/mgmt.key"

[connection]
seeds        = ["node-1:7779", "node-2:7779", "node-3:7779"]
cluster_port = 7779   # admin/cluster TCP port of the nodes
timeout_ms   = 3000   # RPC timeout per node

# mTLS for outbound connections to dittod cluster/admin port (7779).
[tls]
enabled = false
ca_cert  = "/etc/ditto/certs/ca.pem"
cert     = "/etc/ditto/certs/mgmt.pem"
key      = "/etc/ditto/certs/mgmt.key"

# Optional: HTTP Basic Auth for the management UI/API (port 7781).
# When password_hash is absent, authentication is disabled.
# [admin]
# username      = "admin"          # defaults to "admin" when omitted
# password_hash = "$2b$12$..."     # generate with: dittoctl hash-password

# Optional: credentials for proxying cache data requests to dittod's HTTP
# port (7778).  Required only when [http_auth] is enabled on the nodes.
# [http_client_auth]
# username = "ditto"
# password = "plaintext-password"  # plaintext — only stored on mgmt server
```

When neither `tls_cert` nor `tls_key` is set, ditto-mgmt serves plain HTTP.
When both are set, it automatically switches to HTTPS.

```
ditto-mgmt listening on https://0.0.0.0:7781
```

### Environment variable overrides — ditto-mgmt

| Variable | Overrides | Example value |
|----------|-----------|---------------|
| `DITTO_MGMT_TLS_CERT` | `server.tls_cert` | `/certs/mgmt.pem` |
| `DITTO_MGMT_TLS_KEY` | `server.tls_key` | `/certs/mgmt.key` |
| `DITTO_MGMT_ADMIN_USER` | `admin.username` | `admin` |
| `DITTO_MGMT_ADMIN_PASSWORD_HASH` | `admin.password_hash` | `$2b$12$...` |
| `DITTO_MGMT_BIND` | `server.bind` | `site-local` |
| `DITTO_MGMT_HTTP_AUTH_USER` | `http_client_auth.username` | `ditto` |
| `DITTO_MGMT_HTTP_AUTH_PASSWORD` | `http_client_auth.password` | `mysecret` |

---

## dittoctl — CLI Reference

### `hash-password`

Generate a bcrypt password hash for use in `[http_auth]` (dittod) or
`[admin]` (ditto-mgmt) config sections.

```bash
dittoctl hash-password
# Enter password: ****
# $2b$12$abc123...
```

Paste the output into the appropriate `password_hash` field and restart
the service (or set via env var, which takes effect immediately for
newly-started processes).

---

### Configuration

Config file: `~/.config/ditto/kvctl.toml`

```toml
[mgmt]
url        = "https://localhost:7781"   # use https:// when ditto-mgmt has TLS configured
timeout_ms = 3000

[output]
format = "binary"
```

Override at runtime:

```bash
# Change the management URL (saved to config)
dittoctl node set url local http://mgmt.internal:7781

# Change the request timeout (saved to config)
dittoctl node set timeout local 5000

```

---

### node commands

#### `node describe <target>`

Show all properties of a node.

```bash
dittoctl node describe local
dittoctl node describe 127.0.0.1:7779
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
  value-size-limit       104857600b
  max-keys               100000
  compression-enabled    true
  compression-threshold  4096b
```

#### `node get <property> <target>`

Read a single property value.

```bash
dittoctl node get status local
dittoctl node get committed-index 127.0.0.1:7779
```

#### `node set <property> <target> <value>`

Write a node property.

```bash
# Toggle active state
dittoctl node set active local false
dittoctl node set active 127.0.0.1:7779 true

# Change memory limit (live, no restart needed)
dittoctl node set max-memory local 1024

# Change port (node must be Inactive first)
dittoctl node set cluster-port local 7889

# Force this node as primary
dittoctl node set primary local true

# Clear primary pin (revert to auto-election)
dittoctl node set primary local false
```

Writable properties:

| Property | Accepts | Restart? |
|----------|---------|----------|
| `active` / `status` | `true`/`false` or `Active`/`Inactive` | No |
| `primary` | `true`/`false` | No |
| `max-memory` | integer MB (e.g. `512`) | No |
| `default-ttl` | integer seconds | No |
| `value-size-limit` | integer bytes (`0` = unlimited) | No |
| `max-keys` | integer (`0` = unlimited) | No |
| `compression-enabled` | `true`/`false` | No |
| `compression-threshold` | integer bytes (≥ 4096) | No |
| `client-port` | port number | **Yes** |
| `http-port` | port number | **Yes** |
| `cluster-port` | port number | **Yes** |
| `gossip-port` | port number | **Yes** |

#### `node list ports <target>`

Show only port-related properties.

```bash
dittoctl node list ports local
```

#### `node status <target>`

Quick health view (id, memory, heartbeat, uptime, backup storage).

```bash
dittoctl node status local
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

#### `node backup <target>`

Trigger an immediate backup (writes to the node's configured backup directory).

```bash
dittoctl node backup local
dittoctl node backup all
```

> **Note:** The node must be `Inactive`. After the backup completes, the node
> syncs from the primary and automatically re-joins the cluster as `Active`.

---

### cache commands

#### `cache list keys <target>`

List all cache keys on a node.

```bash
dittoctl cache list keys local
dittoctl cache list keys all
dittoctl cache list keys local --pattern "session:*"
```

#### `cache list stats <target>`

Show cache statistics for one or all nodes.

```bash
dittoctl cache list stats local
dittoctl cache list stats all
```

Example output:

```
  Node: 127.0.0.1:7779
  keys                   12345
  memory                 214217728/536870912 bytes
  evictions              0
  hits                   998712
  misses                 1288
  hit-rate               99%
```

#### `cache get key <target> <key>`

Read a cache value.

```bash
dittoctl cache get key local mykey
```

#### `cache get ttl <target> <key>`

Read TTL (currently shows the stored value; full TTL inspection requires
the admin describe path).

```bash
dittoctl cache get ttl local mykey
```

#### `cache set <target> <key> <value>`

Write a cache value.

```bash
dittoctl cache set local greeting "hello world"
dittoctl cache set local session:abc "user-data" --ttl 3600
```

#### `cache delete <target> <key>`

Delete a cache key.

```bash
dittoctl cache delete local session:abc
```

#### `cache flush <target>`

Evict **all** keys from a node's cache.

```bash
dittoctl cache flush local

# Flushing all nodes prompts for confirmation:
dittoctl cache flush all
#   ⚠ This will flush cache on ALL nodes. Type "yes" to confirm:
```

#### `cache set-compressed <target> <key> true|false`

Toggle LZ4 compression for a specific key.

```bash
dittoctl cache set-compressed local bigvalue true
dittoctl cache set-compressed local smallvalue false
```

---

### cluster commands

#### `cluster list nodes`

Show all cluster nodes with status, primary flag, and committed index.

```bash
dittoctl cluster list nodes
```

Example output:

```
  node-id                                status       primary    committed
  ────────────────────────────────────────────────────────────────────────
  d7a482a9-1234-...                      Active       yes        142
  b3c9f110-5678-...                      Active       no         142
  a1e0d221-9abc-...                      Inactive     no         141

  Total: 3 node(s)
```

#### `cluster list active-set`

Show only `Active` nodes.

```bash
dittoctl cluster list active-set
```

#### `cluster get status`

Aggregate cluster health counters.

```bash
dittoctl cluster get status
```

Example output:

```
  total:    3
  active:   2
  syncing:  0
  inactive: 1
  offline:  0
```

#### `cluster get primary`

Show the UUID of the current primary node.

```bash
dittoctl cluster get primary
```

#### `cluster get committed-index`

Show the committed log index from the first reachable node.

```bash
dittoctl cluster get committed-index
```

---

## Common Workflows

### Enable HTTP Basic Auth on 7778 and 7781

#### Step 1 — Generate password hashes

```bash
# Hash for dittod HTTP REST (7778)
dittoctl hash-password
# Enter password: ****
# $2b$12$<node-hash>

# Hash for ditto-mgmt UI/API (7781)
dittoctl hash-password
# Enter password: ****
# $2b$12$<mgmt-hash>
```

#### Step 2 — Configure dittod nodes

Add to every `node.toml` (or via env var):

```toml
[http_auth]
username      = "ditto"
password_hash = "$2b$12$<node-hash>"
```

Or via environment variable:
```bash
DITTO_HTTP_AUTH_USER=ditto
DITTO_HTTP_AUTH_PASSWORD_HASH=$2b$12$<node-hash>
```

Restart each node.  Verify that `/ping` still works without credentials:

```bash
curl http://localhost:7778/ping              # → 200 {"pong":true}
curl http://localhost:7778/key/foo          # → 401 Unauthorized
curl -u ditto:mysecret http://localhost:7778/key/foo   # → normal response
```

#### Step 3 — Configure ditto-mgmt to proxy with credentials

In `mgmt.toml`:

```toml
[admin]
username      = "admin"
password_hash = "$2b$12$<mgmt-hash>"

[http_client_auth]
username = "ditto"
password = "mysecret"   # plaintext — used only for outbound proxying to 7778
```

Restart ditto-mgmt.  Browser will show a native login dialog when `[admin]`
is set.  dittoctl does not need changes for the admin API — it uses the
mgmt URL which may have its own auth separate from the cache API.

#### Optional — Enable HTTPS on 7781

```toml
# mgmt.toml
[server]
tls_cert = "/certs/mgmt.pem"
tls_key  = "/certs/mgmt.key"
```

Or via environment variables (used in `docker-compose.yml`):
```bash
DITTO_MGMT_TLS_CERT=/certs/mgmt.pem
DITTO_MGMT_TLS_KEY=/certs/mgmt.key
```

Update the `dittoctl` config to use `https://`:

```bash
dittoctl node set url local https://localhost:7781
```

> The `mgmt.pem` generated by `gen-certs.sh` includes `localhost` and
> `127.0.0.1` as SANs — browsers accept it without warnings on the dev machine.

---

### Rolling restart

Applies a config change (e.g. port change) to each node without cluster
downtime.  Complete the sequence one node at a time.

```bash
# 1. Deactivate the node
dittoctl node set active 127.0.0.1:7779 false

# 2. Apply the change (e.g. new port)
dittoctl node set cluster-port 127.0.0.1:7779 7889

# 3. Restart the dittod process on that host
#    (systemctl restart dittod, or kill + relaunch)

# 4. Reactivate once the node has rejoined gossip
dittoctl node set active 127.0.0.1:7879 true

# 5. Confirm it's back
dittoctl cluster list nodes
```

Repeat for each node.

---

### Enable file logging

#### Standalone / bare-metal

1. Add (or uncomment) the `[log]` section in `node.toml`:

   ```toml
   [log]
   enabled     = true
   path        = "/var/log/ditto"
   rotation    = "daily"
   retain_days = 14
   level       = "info"
   ```

2. Restart the node.  The log directory is created automatically.

3. Verify that log files are appearing:

   ```bash
   ls /var/log/ditto/
   # dittod.log.2026-02-23

   tail -f /var/log/ditto/dittod.log.2026-02-23
   ```

#### Docker / Docker Compose

Mount a host directory so logs survive container restarts:

```yaml
# docker-compose.yml
services:
  node-1:
    image: dittod
    volumes:
      - ./logs/node-1:/data/logs   # host path : container path
```

```toml
# docker/node1.toml
[log]
enabled     = true
path        = "/data/logs"
rotation    = "daily"
retain_days = 7
level       = "info"
```

Inspect logs from the host:

```bash
# Live tail
tail -f ./logs/node-1/dittod.log.2026-02-23

# Or directly inside the container
docker exec ditto-node-1 tail -f /data/logs/dittod.log.2026-02-23
```

#### Raise log verbosity without restarting

```bash
# Linux / macOS
RUST_LOG=dittod=debug dittod node.toml

# Docker run override
docker run -e RUST_LOG=dittod=debug dittod
```

---

### Scheduled backup

Automatic cron-based backup — add to `node.toml`:

```toml
[backup]
enabled     = true
schedule    = "0 2 * * *"   # daily at 02:00 UTC
path        = "./backups"
format      = "binary"
retain_days = 30
# Optional: encrypt backup files (AES-256-GCM)
# encryption_key = "<openssl rand -hex 32 output>"
```

Or enable encryption via environment variable:

```bash
# Generate a key (keep this safe — you need it to restore!)
openssl rand -hex 32
# e.g.: a3f1c9e2...

# Set in docker-compose.yml or as env var:
DITTO_BACKUP_ENCRYPTION_KEY=a3f1c9e2...
```

Encrypted backup files get an `.enc` suffix:
```
node-1_backup_2026.02.25_02-00-00_UTC.bin.enc
```

Manual on-demand backup script:

```bash
#!/bin/bash
# Example cron script: 0 2 * * * /usr/local/bin/ditto-backup.sh

NODE="127.0.0.1:7779"

# Deactivate
dittoctl node set active $NODE false

# Run backup (node syncs from primary and re-joins Active automatically)
dittoctl node backup $NODE
```

---

### Enable / disable a node

```bash
# Gracefully take node offline (stops accepting client requests, stays synced)
dittoctl node set active 127.0.0.1:7779 false

# Bring it back online
dittoctl node set active 127.0.0.1:7779 true
```

When a node is reactivated it automatically enters a brief **Syncing** state:
it fetches any log entries it missed while inactive from the peer with the
highest committed index, then rejoins the active write quorum as **Active**.
The re-sync is a background task — the node serves read requests immediately
and only participates in write quorum once it has caught up.

```
Inactive  →  Syncing  →  Active
              (re-sync from peer, takes < 1 s for typical backlogs)
```

The dashboard shows the transient `Syncing` status during this window.

---

### Force-elect a primary

By default the active node with the smallest UUID is primary.  Override:

```bash
# Force node-2 to be primary (run on node-2)
dittoctl node set primary 127.0.0.1:7789 true

# Clear the pin — revert to automatic election
dittoctl node set primary 127.0.0.1:7789 false
```
