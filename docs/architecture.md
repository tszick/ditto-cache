# Ditto – Architecture & Deployment Guide

---

## Table of Contents

- [Overview](#overview)
- [Component Diagram](#component-diagram)
- [Port Reference](#port-reference)
- [Protocols](#protocols)
  - [Client Protocol (TCP/HTTP)](#client-protocol-tcphttp)
  - [Cluster Protocol (TCP/mTLS)](#cluster-protocol-tcpmtls)
  - [Gossip Protocol (UDP)](#gossip-protocol-udp)
  - [Admin Protocol](#admin-protocol)
  - [Management HTTP API (ditto-mgmt)](#management-http-api-ditto-mgmt)
- [Consistency Model](#consistency-model)
  - [Active Set](#active-set)
  - [Write Path (two-phase)](#write-path-two-phase)
  - [Read Path](#read-path)
  - [Primary Election](#primary-election)
  - [Node Recovery](#node-recovery)
- [Cache Design](#cache-design)
  - [LFU Eviction](#lfu-eviction)
  - [TTL](#ttl)
  - [LZ4 Compression](#lz4-compression)
- [Deployment](#deployment)
  - [Single-node (development)](#single-node-development)
  - [Three-node production cluster](#three-node-production-cluster)
  - [Multi-site / cross-DC](#multi-site--cross-dc)
  - [Sizing guidelines](#sizing-guidelines)
- [High Availability](#high-availability)
- [Security](#security)
  - [mTLS](#mtls)
  - [HTTP Basic Auth](#http-basic-auth)
  - [Certificate setup](#certificate-setup)
- [Backup and Recovery](#backup-and-recovery)

---

## Overview

Ditto is a distributed, strongly-consistent in-memory key-value cache.

Key design decisions:

| Principle | Implementation |
|-----------|---------------|
| **No stale reads** | Every active node holds a complete copy of all committed data; reads are always local. |
| **Strong write consistency** | Writes are committed only after every currently-active node confirms (Active Set two-phase). |
| **Auto-healing** | Nodes that fall behind are automatically moved to `Syncing`, catch up, and re-join the active set. |
| **Minimal dependencies** | Pure Rust, no external consensus library; custom gossip and two-phase commit. |

---

## Component Diagram

```
                        ┌──────────────────────────────────────────────────┐
                        │           Client Applications                    │
                        │  (TCP binary port 7777  |  HTTP REST port 7778)  │
                        └────────────────┬─────────────────────────────────┘
                                         │ reads + writes
                        ┌────────────────▼─────────────────────────────────┐
                        │                dittod Cluster                    │
                        │                                                   │
                        │  ┌──────────┐   ┌──────────┐   ┌──────────┐     │
                        │  │  node-1  │◄─►│  node-2  │◄─►│  node-3  │     │
                        │  │ (Primary)│   │          │   │          │     │
                        │  └────┬─────┘   └────┬─────┘   └────┬─────┘     │
                        │       │              │               │            │
                        │       └──────────────┴───────────────┘           │
                        │         Cluster TCP :7779 (mTLS optional)        │
                        │         Gossip UDP  :7780                        │
                        └───────────────────────────┬──────────────────────┘
                                                    │ admin TCP/mTLS :7779
                        ┌───────────────────────────▼──────────────────────┐
                        │              ditto-mgmt :7781                    │
                        │   (HTTP REST API + embedded Bootstrap 5 Web UI)  │
                        └───────────────────────────┬──────────────────────┘
                                                    │ HTTP :7781
                              ┌─────────────────────┴────────────────────┐
                              │                                           │
                    ┌─────────▼──────────┐              ┌────────────────▼─────────┐
                    │  dittoctl  (CLI)   │              │  Browser  (Web Dashboard)│
                    └────────────────────┘              └──────────────────────────┘
```

`ditto-mgmt` is the single point of contact for all administrative operations.
Both `dittoctl` and the web dashboard communicate exclusively over HTTP with
`ditto-mgmt`, which in turn talks to `dittod` nodes via the admin TCP protocol.

---

## Port Reference

| Port | Protocol | Layer | Purpose |
|------|----------|-------|---------|
| **7777** | TCP | Client | Binary key-value protocol (GET/SET/DELETE); optional shared token auth |
| **7778** | HTTP or HTTPS | Client | REST key-value API (`/key/<k>`); optional HTTPS + HTTP Basic Auth |
| **7779** | TCP (mTLS optional) | Cluster + Admin | Inter-node replication, forward, recovery; Admin RPC |
| **7780** | UDP | Gossip | Heartbeat broadcasts, active-set updates |
| **7781** | HTTP or HTTPS | Management | `ditto-mgmt` REST API + Web UI; optional HTTPS + HTTP Basic Auth |

> Port layout for a multi-node deployment: nodes are distinguished by shifting
> the base port by 10 (e.g. node-1 uses 7777–7780, node-2 uses 7787–7790, etc.).

---

## Protocols

### Client Protocol (TCP/HTTP)

Clients may use either:

- **TCP binary** (port 7777): `bincode`-serialised `ClientRequest` /
  `ClientResponse` frames with a 4-byte big-endian length prefix. Optionally requires an `Auth { token }` frame first if `client_auth_token` is set.
- **HTTP REST** (port 7778):
  - `GET /key/<k>` → value as plain text
  - `PUT /key/<k>[?ttl=N]` → set value (body = raw bytes)
  - `DELETE /key/<k>` → delete key

Requests may be sent to **any active node** — non-primary nodes forward writes
to the primary transparently.

### Cluster Protocol (TCP/mTLS)

All inter-node communication uses the same `ClusterMessage` enum serialised
with `bincode` + length-prefix framing.  The admin protocol is embedded as
`ClusterMessage::Admin(AdminRequest)`.

Key messages:

| Message | Direction | Purpose |
|---------|-----------|---------|
| `Prepare` | Primary → all active | Phase 1 of two-phase write |
| `PrepareAck` | Follower → primary | Phase 1 acknowledgement |
| `Commit` | Primary → all active | Phase 2 — make visible |
| `CommitAck` | Follower → primary | Phase 2 acknowledgement |
| `Forward` | Any → primary | Non-primary forwards a client write |
| `ForwardResponse` | Primary → non-primary | Result of a forwarded write |
| `RequestLog` | Recovering → any active | Catch-up request |
| `LogEntries` | Active → recovering | Catch-up response |
| `Synced` | Recovering → all | "I am fully caught up" |
| `ForcePrimary` | New primary → all | Override primary election |
| `Admin` | mgmt/CLI → node | Admin RPC request |
| `AdminResponse` | Node → mgmt/CLI | Admin RPC response |

### Gossip Protocol (UDP)

Sent by every node every `gossip_interval_ms` (default 200 ms) to all known
peers.

| Message | Purpose |
|---------|---------|
| `Heartbeat` | Announces: node ID, cluster address, status, last-applied index |
| `ActiveSetUpdate` | Broadcast when active-set membership changes |

A node is marked `Offline` after `gossip_dead_ms` (default 1 000 ms) of silence.

### Admin Protocol

Tunnelled over port 7779 as `ClusterMessage::Admin(AdminRequest)`.

Key operations:

| Request | Purpose |
|---------|---------|
| `GetStats` | Full `NodeStats` snapshot |
| `Describe` | All node properties as key-value pairs |
| `GetProperty` / `SetProperty` | Read / write a single property |
| `ListKeys` | Enumerate cache keys (optional glob pattern) |
| `GetKeyInfo` | Value + TTL + freq + compression flag for one key |
| `SetKeyProperty` | Toggle per-key compression |
| `FlushCache` | Evict all keys |
| `BackupNow` | Trigger an immediate backup |
| `ClusterStatus` | Gossip-visible node list |

### Management HTTP API (ditto-mgmt)

All endpoints return JSON.  `target` can be `local`, `all`, or `host:cluster_port`.

#### Node endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/nodes` | All nodes with full health data |
| GET | `/api/nodes/:target/status` | Status of one node |
| GET | `/api/nodes/:target/describe` | All node properties |
| GET | `/api/nodes/:target/property/:name` | Single property |
| POST | `/api/nodes/:target/property/:name` | Set property |
| POST | `/api/nodes/:target/set-active` | Toggle active/inactive |
| POST | `/api/nodes/:target/backup` | Trigger backup |

#### Cluster endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/cluster` | Aggregate status + node list |
| GET | `/api/cluster/primary` | Current primary node ID |

#### Cache endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/cache/:target/stats` | Cache statistics |
| POST | `/api/cache/:target/flush` | Flush all keys |
| GET | `/api/cache/:target/keys` | List keys (`?pattern=glob`) |
| GET | `/api/cache/:target/keys/:key` | Get value (proxied to node HTTP) |
| PUT | `/api/cache/:target/keys/:key` | Set value (proxied to node HTTP) |
| DELETE | `/api/cache/:target/keys/:key` | Delete key (proxied to node HTTP) |
| POST | `/api/cache/:target/keys/:key/compressed` | Set compression flag |

---

## Consistency Model

### Active Set

The **Active Set** is the set of nodes that participate in write quorum.  A node
is in the active set when its gossip status is `Active`.

Ditto guarantees:
- Every committed write is present on **all** active-set members before `OK`
  is returned to the client.
- A read from any active-set member always returns the latest committed value.

If a node becomes unreachable (gossip timeout), it is excluded from the active
set and writes continue with the remaining nodes.  When the node returns, it
enters `Syncing` mode, catches up on missed entries, and re-joins the active set.

### Write Path (two-phase)

```
Client      Non-primary         Primary          Other active nodes
  │               │                │                     │
  │──SET(k,v)────►│                │                     │
  │               │──FORWARD──────►│                     │
  │               │                │──PREPARE(i,k,v)────►│
  │               │◄──PREPARE──────┤                     │
  │               │──PrepareAck───►│◄────────PrepareAck──│
  │               │                │  (all ACKed)        │
  │               │                │  apply locally      │
  │               │                │──COMMIT(i)─────────►│
  │               │◄──COMMIT───────┤                     │
  │               │──CommitAck────►│◄─────────CommitAck──│
  │◄──OK───────────────────────────│                     │
```

Phase 1 (Prepare): The primary assigns a monotonic log index, writes the entry
to the in-memory write log, and sends `Prepare` to all active peers.  All peers
must ACK within `write_timeout_ms`.

Phase 2 (Commit): The primary applies the entry locally, broadcasts `Commit`,
and increments `committed_index`.  Followers apply and ACK.

If phase 1 times out, the write is rejected with `WriteTimeout` — no state
change occurs.

### Read Path

Reads are always **local** — any active node serves GET directly from its
in-memory store without any inter-node round-trip.

```
Client      Any Active Node
  │               │
  │──GET(k)──────►│
  │               │  local lookup (O(1))
  │◄──VALUE───────│
```

### Primary Election

The primary is the active node with the **smallest UUID** (lexicographic).
This is deterministic and needs no coordination message — every node derives the
same result from the gossip-visible active set.

The primary pin (`SetProperty "primary" "true"`) overrides auto-election by
broadcasting a `ForcePrimary` message to all peers.  All nodes honour the pin
until it is cleared (`false`) or the pinned node leaves the active set.

### Node Recovery

1. Node starts → waits for gossip to discover peers (600 ms delay).
2. Sends `RequestLog { from_index }` to the primary (or furthest peer as fallback).
3. Receives `LogEntries`, applies each entry in order.
4. Status → `Active`, broadcasts `Synced`.

When a node is reactivated (`Inactive → Active`) or completes a backup, the same
resync flow runs: it syncs from the primary before rejoining the active set.
A background version-check task (default every 30 s) compares the node's
`last_applied` index against the primary's; if lagging, a resync is triggered
automatically.

---

## Cache Design

### LFU Eviction

Ditto uses a **Least-Frequently-Used (LFU)** policy.  Each cache entry carries
a `freq_count` incremented on every read.  When `max_memory_mb` is exceeded,
the entry with the lowest frequency is evicted first.

Eviction is O(log n) per operation (implemented with a BTreeMap of frequency
buckets).

### TTL

- A global `default_ttl_secs = 0` means no TTL (keys live forever).
- TTL can be set per-key at write time: `PUT /key/foo?ttl=3600`.
- A background task sweeps expired keys every second.

### LZ4 Compression

When `compression.enabled = true`, values larger than `compression.threshold_bytes`
(minimum 4 096, default 4 096) are automatically compressed with LZ4 before
storage and decompressed on read.

The threshold can only be **increased** at runtime (prevents re-compressing
already-stored values that were compressed under a lower threshold).

Per-key override: `SetKeyProperty key="mykey" name="compressed" value="false"`
decompresses a specific key and marks it as uncompressable.

---

## Deployment

### Single-node (development)

```bash
# Build
cargo build --release --manifest-path src/Cargo.toml

# Start node
./src/target/release/dittod src/dittod/node.toml

# Start management service
./src/target/release/ditto-mgmt

# Open Web UI
open http://localhost:7781
```

### Three-node production cluster

**Recommended:** Run three nodes on separate hosts for fault tolerance.  The
cluster can lose one node without interrupting reads or writes.

Example layout:

```
Host A  192.168.1.10 — node-1  ports 7777–7780
Host B  192.168.1.11 — node-2  ports 7777–7780
Host C  192.168.1.12 — node-3  ports 7777–7780
Mgmt    192.168.1.20 — ditto-mgmt port 7781
```

`node.toml` on Host A:

```toml
[node]
id                = "node-1"
bind_addr         = "0.0.0.0"    # client ports (7777, 7778)
cluster_bind_addr = "site-local" # cluster ports (7779, 7780) — auto-selects private interface
client_port       = 7777
http_port         = 7778
cluster_port      = 7779
gossip_port       = 7780
active            = true

[cluster]
seeds     = ["192.168.1.11:7779", "192.168.1.12:7779"]
max_nodes = 100

[cache]
max_memory_mb = 4096

[tls]
enabled = true
ca_cert = "/etc/ditto/certs/ca.pem"
cert    = "/etc/ditto/certs/node.pem"
key     = "/etc/ditto/certs/node.key"
```

`mgmt.toml` on the management host:

```toml
[server]
bind = "0.0.0.0"    # "site-local" | "0.0.0.0" | "localhost" | "<ip>"
port = 7781
# Optional: HTTPS on port 7781 (server-only TLS, no client cert required).
# tls_cert = "/etc/ditto/certs/mgmt.pem"
# tls_key  = "/etc/ditto/certs/mgmt.key"

[connection]
seeds        = ["192.168.1.10:7779", "192.168.1.11:7779", "192.168.1.12:7779"]
cluster_port = 7779
timeout_ms   = 3000

[tls]
enabled = true
ca_cert  = "/etc/ditto/certs/ca.pem"
cert     = "/etc/ditto/certs/mgmt.pem"
key      = "/etc/ditto/certs/mgmt.key"

# Optional: Basic Auth for the management UI / API.
# [admin]
# username      = "admin"
# password_hash = "$2b$12$..."   # dittoctl hash-password

# Optional: credentials to proxy requests to dittod port 7778
# when DITTO_HTTP_AUTH_* is enabled on the nodes.
# [http_client_auth]
# username = "ditto"
# password = "plaintext-password"
```

### Multi-site / cross-DC

Ditto's two-phase write protocol is synchronous — every active node must ACK
before the write is confirmed.  For cross-DC deployments consider:

- Keeping all nodes in the same region (low latency ≤ 10 ms is recommended).
- Or: increase `write_timeout_ms` to accommodate higher latency.
- Or: run separate per-DC clusters with application-level replication.

### Sizing guidelines

| Parameter | Recommendation |
|-----------|---------------|
| Nodes | 3 (tolerates 1 failure) or 5 (tolerates 2 failures) |
| `max_memory_mb` | 60–70% of available RAM per node |
| `write_timeout_ms` | 2–5× the P99 inter-node RTT |
| `gossip_dead_ms` | At least 5× `gossip_interval_ms` |
| Backup frequency | At least daily; use `node backup` or cron |

---

## High Availability

Ditto is **AP** during network partition scenarios:

- When a node becomes unreachable it is removed from the active set.
- Writes continue with the remaining nodes (`N-1` quorum).
- Reads from a non-active node are rejected (node returns `NodeInactive`).
- When the partition heals, the offline node re-syncs and re-joins automatically.

**What happens at the write path when a node is lost:**

```
Before:  active-set = {node-1, node-2, node-3}  quorum = 3/3
After:   active-set = {node-1, node-2}           quorum = 2/2
```

Clients experience zero downtime.  The third node catches up when it returns.

---

## Security

### Authentication

Ditto supports authentication on its client and management ports:

| Port | Service | Auth Type | Config section | Env var overrides |
|------|---------|-----------|----------------|-------------------|
| **7777** | `dittod` TCP | Shared Token | `[node]` in `node.toml` | `DITTO_CLIENT_AUTH_TOKEN` |
| **7778** | `dittod` REST API | Basic Auth | `[http_auth]` in `node.toml` | `DITTO_HTTP_AUTH_USER`, `DITTO_HTTP_AUTH_PASSWORD_HASH` |
| **7781** | `ditto-mgmt` UI/API | Basic Auth | `[admin]` in `mgmt.toml` | `DITTO_MGMT_ADMIN_USER`, `DITTO_MGMT_ADMIN_PASSWORD_HASH` |

For the **TCP binary protocol**, clients must send an `Auth` request with the correct token as their first message if authentication is enabled.

For **HTTP Basic Auth**, passwords are never stored in plain text — only their **bcrypt hash**. Use `dittoctl hash-password` to generate a hash from a plain-text password. If the password hash is absent from the config, authentication is disabled and all requests are allowed through.

`/ping` on port 7778 is always exempt from authentication so health-check tooling works without credentials.

### Denial of Service (DoS) Protection

To prevent OOM attacks, the server enforces a maximum payload size for all incoming TCP and cluster messages. This is configurable via `max_message_size_bytes` (default 128 MB) overriding `DITTO_MAX_MESSAGE_SIZE_BYTES`. Payloads exceeding this limit will be dropped.

### mTLS

When `[tls] enabled = true`:

- All connections on port 7779 (cluster + admin) require a client certificate
  signed by the configured CA.
- `ditto-mgmt` uses the same CA + a dedicated certificate to connect to nodes.
- Gossip (UDP 7780) and client port 7777 (TCP binary) are **not** TLS-protected.
- Client port 7778 (HTTP REST) is plain HTTP by default; it optionally supports
  HTTPS (server-only TLS using the node certificate) and HTTP Basic Auth.

The TLS server name used for every handshake is **`ditto-cluster`**.  All
node and mgmt certificates must include `ditto-cluster` as a DNS SAN.

### Certificate setup

```bash
# Generate CA + per-node + mgmt certificates (script in docker/)
bash docker/gen-certs.sh          # Linux/macOS
# Windows: & "C:\Program Files\Git\usr\bin\bash.exe" docker/gen-certs.sh
```

The script creates `docker/certs/` with:

```
ca.pem          CA certificate
node1.pem / .key   — SAN: ditto-cluster, node-1
node2.pem / .key   — SAN: ditto-cluster, node-2
node3.pem / .key   — SAN: ditto-cluster, node-3
mgmt.pem  / .key   — SAN: ditto-cluster, ditto-mgmt, localhost, 127.0.0.1
client.pem / .key  — for dittoctl if used with direct TLS
```

The `mgmt.pem` includes `localhost` and `127.0.0.1` as SANs so that browsers
accept it without warnings when connecting to `https://localhost:7781`.

All certificates are signed by the generated self-signed CA.

### Backup encryption

Backup files (binary / JSON) contain unencrypted plaintext by default.
To protect data at rest, set `backup.encryption_key` in `node.toml` or via
the `DITTO_BACKUP_ENCRYPTION_KEY` environment variable:

```bash
# Generate a 32-byte key (keep this safe — required for restore)
openssl rand -hex 32
```

When set, each backup file is encrypted with **AES-256-GCM** using a fresh
random nonce per file and written with an `.enc` suffix.  The key is
completely separate from the TLS certificates.

File format (encrypted):
```
[4 bytes magic "DENC"] [1 byte version] [12 bytes nonce] [ciphertext + 16 byte tag]
```

---

## Backup and Recovery

### Automatic periodic backup

Configure `[backup]` in `node.toml`:

```toml
[backup]
enabled     = true
schedule    = "0 2 * * *"    # cron, daily at 02:00 UTC
path        = "/var/ditto/backups"
format      = "binary"       # binary | json
retain_days = 30
# Optional AES-256-GCM encryption — generate key: openssl rand -hex 32
# encryption_key = "<64 hex chars>"
```

### Manual backup (via admin)

```bash
# Deactivate, backup, reactivate
dittoctl node set active local false
dittoctl node backup local
dittoctl node set active local true
```

### Disaster recovery from backup

1. Stop the node.
2. Replace or clear the data directory with the backup files.
3. Start the node in inactive mode (`active = false`).
4. Reactivate:
   ```bash
   dittoctl node set active local true
   ```
5. The node enters `Syncing`, fetches any writes made since the backup from the primary,
   and automatically rejoins the active set.
