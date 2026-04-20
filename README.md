# Ditto

**A distributed, strongly-consistent in-memory key-value cache — written in Rust.**

Ditto uses an *Active Set* replication model: every active node always holds a
complete, up-to-date copy of all committed data.  Reads are always local.
No stale reads, ever.

---

## Features

- **Strong write consistency** — a write is confirmed only after every currently-active node ACKs it (Active Set two-phase commit)
- **Zero stale reads** — reads are always served locally from any active node
- **Auto-healing** — nodes that fall behind enter `Syncing`, catch up on missed log entries, and re-join the active set automatically
- **Gossip-based discovery** — nodes discover each other and track health via a lightweight UDP heartbeat protocol
- **Automatic primary election** — the active node with the smallest UUID becomes the write coordinator; override with `dittoctl node set primary`
- **Dual client API** — binary TCP (port 7777) and HTTP REST (port 7778)
- **Web management dashboard** — Bootstrap 5 UI served by `ditto-mgmt` for real-time cluster monitoring and node control
- **Admin CLI** — `dittoctl` for scripting and automation
- **mTLS authentication** — optional mutual TLS on the cluster/admin port (7779)
- **HTTPS** — optional server-side TLS on the dittod HTTP REST port (7778) and the management port (7781)
- **HTTP Basic Auth** — optional username/password protection on the HTTP REST port (7778) and management port (7781); passwords stored as bcrypt hashes
- **Backup encryption** — optional AES-256-GCM encryption for backup files; encrypted files use a dedicated key separate from the TLS certificates
- **Persistence policy gates** - backup/export/import are disabled by default and require both platform env allow + runtime admin enable
- **Read-repair on miss (optional)** - follower miss can query primary and trigger async repair
- **Anti-entropy reconciliation (optional)** - lag-threshold, key-sample mismatch, and bounded full keyspace reconcile triggers
- **Mixed-version upgrade probe** - surfaces protocol-version drift counters during rolling upgrades
- **Tenant isolation + quota (optional)** - namespace-scoped keyspace and per-namespace key limits
- **LFU eviction** — least-frequently-used entries evicted when the memory limit is reached
- **TTL support** — per-key or global default TTL with a background sweep
- **Transparent LZ4 compression** — large values compressed automatically; decompressed on read; per-key override via `dittoctl`
- **Runtime-configurable** — memory limits, key limits, compression settings, TTL and ports all writable at runtime without full restart

---

## Architecture

```
 Client apps  ────────────────────────  TCP :7777 / HTTP :7778
                                                  │
              ┌───────────────────────────────────▼──────────────────────────┐
              │                      dittod Cluster                          │
              │   ┌──────────┐   ┌──────────┐   ┌──────────┐               │
              │   │  node-1  │◄─►│  node-2  │◄─►│  node-3  │               │
              │   │(Primary) │   │          │   │          │               │
              │   └──────────┘   └──────────┘   └──────────┘               │
              │     Cluster TCP :7779 (mTLS optional) + Gossip UDP :7780    │
              └──────────────────────────────┬────────────────────────────────┘
                                             │ admin TCP/mTLS :7779
                                 ┌───────────▼──────────────┐
                                 │       ditto-mgmt          │
                                 │  REST API + Web UI :7781  │
                                 └───────────┬──────────────┘
                                             │ HTTP :7781
                              ┌──────────────┴──────────────┐
                              │                             │
                    ┌─────────▼──────┐          ┌──────────▼──────┐
                    │  dittoctl CLI  │          │  Browser (UI)   │
                    └────────────────┘          └─────────────────┘
```

`ditto-mgmt` is the single management plane: both the CLI and the browser connect
to it over HTTP.  Nodes are accessed via the binary TCP admin protocol.

### Write flow (two-phase)

```
Client      Non-primary       Primary           Other active nodes
  │               │               │                      │
  │──SET(k,v)────►│               │                      │
  │               │──FORWARD─────►│                      │
  │               │◄──PREPARE─────┤──PREPARE(i,k,v)─────►│
  │               │──PrepareAck──►│◄────────PrepareAck───│
  │               │               │  apply locally       │
  │               │◄──COMMIT──────┤──COMMIT(i)──────────►│
  │               │──CommitAck───►│◄────────CommitAck────│
  │◄──OK──────────────────────────│                      │
```

### Read flow

Reads are always local — any active node serves GET directly from its
in-memory store without any inter-node round-trip.

---

## Project Structure

```
ditto/src/
├── Cargo.toml              Cargo workspace
├── ditto-protocol/         Shared wire types (Client, Cluster, Gossip, Admin)
│   └── src/lib.rs          Protocol enums + encode/decode helpers
├── dittod/                 Node daemon
│   ├── src/
│   │   ├── main.rs         Startup, server bootstrap
│   │   ├── config.rs       TOML config loader + env-var overrides
│   │   ├── node.rs         NodeHandle — write coordination, admin handler
│   │   ├── store/          KV store, LFU eviction, TTL sweep, LZ4 compression
│   │   ├── replication/    Write log, Active Set, primary election
│   │   ├── network/        TCP :7777, HTTP :7778, Cluster+Admin :7779
│   │   └── gossip/         UDP :7780 heartbeat engine
│   └── node.toml           Example node config
├── ditto-mgmt/             Management service
│   ├── src/
│   │   ├── main.rs         HTTP server entry point (Axum)
│   │   ├── config.rs       mgmt.toml loader
│   │   ├── tls.rs          mTLS connector for outbound node RPCs
│   │   ├── node_client.rs  Admin TCP/mTLS client + address resolver
│   │   ├── api/            REST handlers: nodes, cluster, cache
│   │   └── web/            Embedded Bootstrap 5 dashboard (include_str!)
├── dittoctl/               Admin CLI
│   ├── src/
│   │   ├── main.rs         Clap CLI root
│   │   ├── client.rs       HTTP wrapper (reqwest)
│   │   ├── config.rs       ~/.config/ditto/kvctl.toml
│   │   └── commands/       node / cache / cluster subcommands
│   └── kvctl.toml          Example CLI config
└── docker/
    ├── Dockerfile          Multi-stage build (rust:slim → debian:slim)
    ├── docker-compose.yml  3-node test cluster with mTLS
    └── gen-certs.sh        Test certificate generator (openssl)
```

---

## Getting Started

### Prerequisites

- [Rust](https://rustup.rs) stable ≥ 1.75
- Windows: Microsoft C++ Build Tools (rustup will prompt during install)
- Docker + Docker Compose v2 (optional, for the 3-node test cluster)

### Build

```bash
cd ditto/src
cargo build --release
```

Produced binaries:

| Binary | Path | Purpose |
|--------|------|---------|
| `dittod` | `target/release/dittod` | Node daemon |
| `ditto-mgmt` | `target/release/ditto-mgmt` | Management service |
| `dittoctl` | `target/release/dittoctl` | Admin CLI |

### Single-node quick start

```bash
# Dev-only shortcut:
# default node.toml/mgmt.toml are rejected by strict security checks unless
# DITTO_INSECURE=true is set.
DITTO_INSECURE=true ./target/release/dittod dittod/node.toml

# Start the management service
DITTO_INSECURE=true ./target/release/ditto-mgmt

# Open the web dashboard (plain HTTP when no TLS cert is configured)
open http://localhost:7781

# Test with curl
curl -X PUT http://localhost:7778/key/hello -d "world"
curl http://localhost:7778/key/hello

# Batch write (seed/load test helper)
curl -X POST http://localhost:7778/keys/batch \
  -H "Content-Type: application/json" \
  -d '{"items":[{"key":"k1","value":"v1"},{"key":"k2","value":"v2","ttl_secs":60}]}'

# Operator health summary
curl http://localhost:7778/health/summary
```

Example response:

```json
{
  "availability": "ready",
  "node_id": "d4f4f23c-1260-4f8f-94fe-7ea1b9b4b8b1",
  "status": "Active",
  "is_primary": true,
  "committed_index": 42,
  "key_count": 128,
  "memory_used_bytes": 1048576,
  "memory_max_bytes": 536870912,
  "uptime_secs": 3600,
  "rate_limit_enabled": true,
  "rate_limited_requests_total": 0,
  "circuit_breaker_enabled": true,
  "circuit_breaker_state": "closed",
  "anti_entropy_last_detected_lag": 0,
  "mixed_version_last_detected_peer_count": 0,
  "namespace_quota_reject_total": 0,
  "namespace_quota_reject_rate_per_min": 0,
  "namespace_quota_reject_trend": "steady",
  "namespace_quota_top_usage": [],
  "persistence_enabled": true,
  "snapshot_last_load_age_secs": 42,
  "tenancy_enabled": false
}
```

### 3-node Docker cluster

```bash
# Generate TLS certificates (first time only)
bash ../ditto-docker/gen-certs.sh   # Linux/macOS
# Windows: & "C:\Program Files\Git\usr\bin\bash.exe" ../ditto-docker/gen-certs.sh

# Start all four containers (3 nodes + mgmt)
docker compose -f ../ditto-docker/docker-compose.yml up --build

# Web dashboard (mgmt uses HTTPS when tls_cert/tls_key are set)
open https://localhost:7781   # self-signed cert — accept in browser

# Verify cluster
./target/release/dittoctl cluster list nodes
```

Docker deployment files are in the sibling project: `../ditto-docker/`.

---

## Configuration

### Node daemon — `node.toml`

Production baseline note:
- strict security startup checks expect mTLS + HTTP auth to be configured.
- If you keep the relaxed/dev values, start with `DITTO_INSECURE=true` only in local environments.

```toml
[node]
id                = "node-1"       # unique node name
bind_addr         = "0.0.0.0"      # client ports (7777, 7778): "0.0.0.0" | "localhost" | "<ip>"
cluster_bind_addr = "site-local"   # cluster ports (7779, 7780): "site-local" | "0.0.0.0" | "localhost" | "<ip>"
client_port       = 7777           # TCP binary client protocol
http_port         = 7778           # HTTP REST API
cluster_port      = 7779           # inter-node + admin (mTLS optional)
gossip_port       = 7780           # UDP gossip
active            = true           # false = maintenance mode

[cluster]
seeds     = ["node-2:7779", "node-3:7779"]
max_nodes = 100

[cache]
max_memory_mb          = 512
default_ttl_secs       = 0
value_size_limit_bytes = 104857600
max_keys               = 100000

[compression]
enabled         = true
threshold_bytes = 4096

[replication]
write_timeout_ms   = 500
write_quorum_mode  = "all-active" # all-active | majority
gossip_interval_ms = 200
gossip_dead_ms     = 15000
version_check_interval_ms = 30000
read_repair_on_miss_enabled = false
read_repair_min_interval_ms = 5000
anti_entropy_enabled = false
anti_entropy_interval_ms = 60000
anti_entropy_min_repair_interval_ms = 30000
anti_entropy_lag_threshold = 32
anti_entropy_key_sample_size = 64
anti_entropy_full_reconcile_every = 10
anti_entropy_full_reconcile_max_keys = 2000
mixed_version_probe_enabled = true
mixed_version_probe_interval_ms = 30000

[tls]
enabled = true
ca_cert = "/etc/ditto/certs/ca.pem"
cert    = "/etc/ditto/certs/node.pem"
key     = "/etc/ditto/certs/node.key"

[http_auth]
username      = "ditto"
password_hash = "$2b$12$..."  # generate with: dittoctl hash-password
```

**Environment variable overrides** (useful for Docker / Kubernetes):

| Variable | Config field |
|----------|-------------|
| `DITTO_NODE_ID` | `node.id` |
| `DITTO_ACTIVE` | `node.active` |
| `DITTO_CLIENT_AUTH_TOKEN` | `node.client_auth_token` |
| `DITTO_SEEDS` | `cluster.seeds` (comma-separated) |
| `DITTO_MAX_MEMORY_MB` | `cache.max_memory_mb` |
| `DITTO_GOSSIP_DEAD_MS` | `replication.gossip_dead_ms` |
| `DITTO_WRITE_QUORUM_MODE` | `replication.write_quorum_mode` (`all-active` \| `majority`) |
| `DITTO_READ_REPAIR_ON_MISS_ENABLED` | `replication.read_repair_on_miss_enabled` |
| `DITTO_READ_REPAIR_MIN_INTERVAL_MS` | `replication.read_repair_min_interval_ms` |
| `DITTO_READ_REPAIR_MAX_PER_MINUTE` | `replication.read_repair_max_per_minute` |
| `DITTO_ANTI_ENTROPY_ENABLED` | `replication.anti_entropy_enabled` |
| `DITTO_ANTI_ENTROPY_INTERVAL_MS` | `replication.anti_entropy_interval_ms` |
| `DITTO_ANTI_ENTROPY_MIN_REPAIR_INTERVAL_MS` | `replication.anti_entropy_min_repair_interval_ms` |
| `DITTO_ANTI_ENTROPY_LAG_THRESHOLD` | `replication.anti_entropy_lag_threshold` |
| `DITTO_ANTI_ENTROPY_KEY_SAMPLE_SIZE` | `replication.anti_entropy_key_sample_size` |
| `DITTO_ANTI_ENTROPY_FULL_RECONCILE_EVERY` | `replication.anti_entropy_full_reconcile_every` |
| `DITTO_ANTI_ENTROPY_FULL_RECONCILE_MAX_KEYS` | `replication.anti_entropy_full_reconcile_max_keys` |
| `DITTO_ANTI_ENTROPY_BUDGET_MAX_CHECKS_PER_RUN` | `replication.anti_entropy_budget_max_checks_per_run` |
| `DITTO_ANTI_ENTROPY_BUDGET_MAX_DURATION_MS` | `replication.anti_entropy_budget_max_duration_ms` |
| `DITTO_MIXED_VERSION_PROBE_ENABLED` | `replication.mixed_version_probe_enabled` |
| `DITTO_MIXED_VERSION_PROBE_INTERVAL_MS` | `replication.mixed_version_probe_interval_ms` |
| `DITTO_TLS_ENABLED` | `tls.enabled` |
| `DITTO_TLS_CA_CERT` | `tls.ca_cert` |
| `DITTO_TLS_CERT` | `tls.cert` |
| `DITTO_TLS_KEY` | `tls.key` |
| `DITTO_BIND_ADDR` | `node.bind_addr` |
| `DITTO_CLUSTER_BIND_ADDR` | `node.cluster_bind_addr` |
| `DITTO_FRAME_READ_TIMEOUT_MS` | `node.frame_read_timeout_ms` |
| `DITTO_HTTP_AUTH_USER` | `http_auth.username` |
| `DITTO_HTTP_AUTH_PASSWORD_HASH` | `http_auth.password_hash` |
| `DITTO_BACKUP_ENCRYPTION_KEY` | `backup.encryption_key` (hex-encoded 32-byte AES-256-GCM key) |
| `DITTO_SNAPSHOT_RESTORE_ON_START` | `backup.restore_on_start` |
| `DITTO_PERSISTENCE_PLATFORM_ALLOWED` | global platform gate for persistence features |
| `DITTO_PERSISTENCE_BACKUP_ALLOWED` | platform gate for backup |
| `DITTO_PERSISTENCE_EXPORT_ALLOWED` | platform gate for export |
| `DITTO_PERSISTENCE_IMPORT_ALLOWED` | platform gate for import |
| `DITTO_TENANCY_ENABLED` | `tenancy.enabled` |
| `DITTO_TENANCY_DEFAULT_NAMESPACE` | `tenancy.default_namespace` |
| `DITTO_TENANCY_MAX_KEYS_PER_NAMESPACE` | `tenancy.max_keys_per_namespace` |
| `DITTO_RATE_LIMIT_ENABLED` | node request rate limiter enable flag |
| `DITTO_RATE_LIMIT_REQUESTS_PER_SEC` | token bucket refill rate |
| `DITTO_RATE_LIMIT_BURST` | token bucket burst capacity |
| `DITTO_HOT_KEY_ENABLED` | hot-key GET coalescing enable flag |
| `DITTO_HOT_KEY_MAX_WAITERS` | max concurrent waiters per in-flight key |
| `DITTO_HOT_KEY_ADAPTIVE_WAITERS_ENABLED` | enable adaptive per-key waiter limit tuning |
| `DITTO_HOT_KEY_ADAPTIVE_MIN_WAITERS` | minimum adaptive waiter limit per key |
| `DITTO_HOT_KEY_ADAPTIVE_SUCCESS_THRESHOLD` | successful coalesced follower responses before adaptive limit increases |
| `DITTO_HOT_KEY_ADAPTIVE_STATE_MAX_KEYS` | max retained per-key adaptive waiter state entries |
| `DITTO_CIRCUIT_BREAKER_ENABLED` | circuit breaker enable flag |
| `DITTO_CIRCUIT_BREAKER_FAILURE_THRESHOLD` | consecutive failure threshold to open |
| `DITTO_CIRCUIT_BREAKER_OPEN_MS` | open-state timeout before half-open probes |
| `DITTO_CIRCUIT_BREAKER_HALF_OPEN_MAX_REQUESTS` | successful half-open probes needed to close |

`gossip_dead_ms` guardrails:
- values lower than `3 * gossip_interval_ms` are clamped upward at startup.
- values below `3000` may cause false OFFLINE flapping under transient pauses.

Persistence effective state:

`persistence_enabled = PERSISTENCE_PLATFORM_ALLOWED && persistence_runtime_enabled`

Default behavior is secure-by-default: all persistence platform gates are `false`, so backup/export/import are disabled until explicitly enabled.

Tenancy behavior:

- When `tenancy.enabled=true`, client keys are isolated by namespace internally (`namespace::key`).
- HTTP clients can set namespace with header: `X-Ditto-Namespace: <tenant>`.
- If namespace is omitted, `tenancy.default_namespace` is used.
- Per-tenant quota is controlled by `tenancy.max_keys_per_namespace` (`0 = unlimited`).

### Management service — `~/.config/ditto/mgmt.toml`

```toml
[server]
bind = "0.0.0.0"    # "site-local" | "0.0.0.0" | "localhost" | "<ip>"
port = 7781
# HTTPS on port 7781 (server-only TLS, no client cert required).
tls_cert = "/etc/ditto/certs/mgmt.pem"
tls_key  = "/etc/ditto/certs/mgmt.key"

[connection]
seeds        = ["node-1:7779", "node-2:7779", "node-3:7779"]
cluster_port = 7779
timeout_ms   = 3000

[tls]
enabled = true

[admin]
username      = "admin"
password_hash = "$2b$12$..."  # generate with: dittoctl hash-password

# Optional: credentials for proxying cache requests to dittod HTTP port (7778).
# Required only when DITTO_HTTP_AUTH_* is set on the nodes.
# [http_client_auth]
# username = "ditto"
# password = "plaintext-password"
```

### Admin CLI — `~/.config/ditto/kvctl.toml`

```toml
[mgmt]
url        = "https://localhost:7781"   # use https:// when ditto-mgmt has TLS enabled
timeout_ms = 3000

[output]
format = "binary"
```

---

## Admin CLI Quick Reference

```bash
# Cluster overview
dittoctl cluster list nodes
dittoctl cluster get status
dittoctl cluster get primary

# Node control
dittoctl node status all
dittoctl node doctor all
dittoctl node describe local
dittoctl node set active 127.0.0.1:7779 false    # maintenance mode
dittoctl node set max-memory local 1024           # live, no restart
dittoctl node set persistence-runtime-enabled local true  # requires platform allow

# Cache operations
dittoctl cache list stats all
dittoctl cache list keys local --pattern "user:*" --namespace tenant-a
dittoctl cache set local mykey "hello" --ttl 3600 --namespace tenant-a
dittoctl cache get key local mykey --namespace tenant-a
dittoctl cache flush local

# Generate a bcrypt password hash (for [http_auth] or [admin] config)
dittoctl hash-password
```

Full reference: [docs/admin-guide.md](docs/admin-guide.md)

---

## Testing

```bash
# Unit and integration tests
cargo test --workspace

# CI-equivalent chaos script sanity check (no Docker side effects)
powershell -ExecutionPolicy Bypass -File .\scripts\chaos-smoke.ps1 -DryRun -Iterations 1

# Real chaos smoke (restart + delay + partition) on a running Docker cluster
powershell -ExecutionPolicy Bypass -File .\scripts\chaos-smoke.ps1 -Iterations 1

# Quick smoke test (requires Docker cluster)
docker compose -f ../ditto-docker/docker-compose.yml up -d --build
sleep 15
# Nodes serve HTTPS when TLS is enabled; use --cacert or -k for self-signed certs.
# HTTP Basic Auth is enabled in the default compose — add -u ditto:password.
# /ping is always auth-free (health check exempt).
curl -sfk -u ditto:qwe123asd -X PUT https://localhost:7778/key/test -d "hello"
curl -sfk -u ditto:qwe123asd https://localhost:7788/key/test    # same value from node-2
curl -sfk -u ditto:qwe123asd https://localhost:7798/key/test    # same value from node-3

# Windows note:
# If host curl fails with schannel (SEC_E_NO_CREDENTIALS), run smoke curl inside containers:
docker exec ditto-node-1 sh -lc "curl -sfk -u ditto:qwe123asd -X PUT https://localhost:7778/key/test -d 'hello'"
docker exec ditto-node-2 sh -lc "curl -sfk -u ditto:qwe123asd https://localhost:7778/key/test"
docker exec ditto-node-3 sh -lc "curl -sfk -u ditto:qwe123asd https://localhost:7778/key/test"

# Fault tolerance test
docker stop ditto-node-3
curl -sfk -u ditto:qwe123asd -X PUT https://localhost:7778/key/ft -d "still works"
curl -sfk -u ditto:qwe123asd https://localhost:7788/key/ft
docker start ditto-node-3             # auto-syncs when restarted
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [docs/admin-guide.md](docs/admin-guide.md) | Full `dittoctl` reference + web dashboard guide |
| [docs/architecture.md](docs/architecture.md) | Architecture, protocols, write/read flows, deployment sizing |
| [docs/dittoctl-reference.md](docs/dittoctl-reference.md) | Supplementary CLI reference |
| [docs/backlog-guide.md](docs/backlog-guide.md) | Product backlog + multi-sprint roadmap |
| [docs/chaos-playbook.md](docs/chaos-playbook.md) | Chaos/fault validation scenarios (partition + delay + restart) |
| [docs/operations-runbook.md](docs/operations-runbook.md) | Incident response and tenant quota operations |

---

## CI Workflows

Current GitHub Actions workflows:

- `CodeQL` (`.github/workflows/codeql.yml`)
  - Purpose: static security + quality analysis for Rust code.
  - Triggers: push/PR on `main` + weekly scheduled scan.
- `Chaos Dry Run` (`.github/workflows/chaos-dry-run.yml`)
  - Purpose: validate chaos script wiring without Docker side effects.
  - Triggers: push/PR on `main` + manual run (`workflow_dispatch`).
  - Command: `./scripts/chaos-smoke.ps1 -DryRun -Iterations 1`
- `Release Gate` (`.github/workflows/release-gate.yml`)
  - Purpose: enforce baseline Rust quality gate before merge/release.
  - Triggers: push/PR on `main`.
  - Commands: `cargo check`, `cargo test --workspace`, `cargo test -p ditto-protocol`, `cargo audit`
- `Protocol Contract` (`.github/workflows/protocol-contract.yml`)
  - Purpose: enforce schema-first protocol contract sync (`ditto-protocol/schema/protocol-contract.json`).
  - Triggers: push/PR on `main`.
  - Commands: `./scripts/generate-protocol-contract.ps1` + drift check.
- `Pre-Prod Runbook Validation` (`.github/workflows/preprod-runbook-validation.yml`)
  - Purpose: automate runbook scenario validation entrypoint (node-loss / restore telemetry / quota telemetry).
  - Triggers: push/PR on `main` + manual run.
  - Command: `./scripts/preprod-runbook-validate.ps1 -DryRun`
- `Performance Gate` (`.github/workflows/perf-gate.yml`)
  - Purpose: block regressions on p50/p95/p99 latency against committed baseline.
  - Triggers: push/PR on `main` + manual run.
  - Command: `./scripts/perf-gate.ps1 -Samples 80 -Warmup 10`

Manual run (GitHub UI):

1. Open **Actions** tab.
2. Select **Chaos Dry Run** workflow.
3. Click **Run workflow**.
4. Choose `main` branch and run.

---

## Security

mTLS on the cluster/admin port (7779) is supported.  When enabled, every
connection (node-to-node and mgmt-to-node) must present a client certificate
signed by the configured CA.  The TLS server name used for all handshakes is
`ditto-cluster` — all certificates must include this as a DNS SAN.

Client port 7777 (TCP binary) and gossip (UDP/7780) are not TLS-protected.
Current default runtime policy is strict security:
- `dittod` refuses startup without `[tls].enabled=true` and `[http_auth].password_hash`.
- `ditto-mgmt` refuses startup without mTLS, admin password hash, and HTTPS cert/key.
- `DITTO_INSECURE=true` bypasses these checks for local/dev only.

Port 7778 (HTTP REST) and port 7781 (ditto-mgmt) can run HTTPS (server-only TLS)
and HTTP Basic Auth. In production, keep strict mode enabled.

The `mgmt.pem` certificate generated by `gen-certs.sh` includes `localhost`
and `127.0.0.1` as SANs so browsers accept it without warnings on the dev machine.

**Backup encryption** is separate from TLS: set `DITTO_BACKUP_ENCRYPTION_KEY`
(hex-encoded 32-byte key) to encrypt backup files with AES-256-GCM.  The key
is independent of the TLS certificates.  Generate a key with:

```bash
openssl rand -hex 32
```

Backup integrity:
- every backup payload now has a SHA-256 checksum sidecar (`*.sha256`),
- snapshot restore verifies checksum before decrypt/deserialize,
- restore fails when the sidecar is missing or mismatched.

```bash
# Generate test certificates (includes localhost SAN for the mgmt cert)
bash ../ditto-docker/gen-certs.sh
```

See [docs/architecture.md#security](docs/architecture.md#security) for details.

---

## Dependency Maintenance

```bash
cargo install cargo-audit && cargo audit        # CVE scan
cargo install cargo-outdated && cargo outdated  # outdated deps
cargo install cargo-deny && cargo deny check    # policy enforcement
rustup update                                   # Rust toolchain
```

---

## License

See [LICENSE](LICENSE).
