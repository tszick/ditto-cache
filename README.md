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
# Start the node daemon (default config — plain HTTP)
./target/release/dittod dittod/node.toml

# Start the management service
./target/release/ditto-mgmt

# Open the web dashboard (plain HTTP when no TLS cert is configured)
open http://localhost:7781

# Test with curl
curl -X PUT http://localhost:7778/key/hello -d "world"
curl http://localhost:7778/key/hello
```

### 3-node Docker cluster

```bash
# Generate TLS certificates (first time only)
bash docker/gen-certs.sh   # Linux/macOS
# Windows: & "C:\Program Files\Git\usr\bin\bash.exe" docker/gen-certs.sh

# Start all four containers (3 nodes + mgmt)
docker compose -f docker/docker-compose.yml up --build

# Web dashboard (mgmt uses HTTPS when tls_cert/tls_key are set)
open https://localhost:7781   # self-signed cert — accept in browser

# Verify cluster
./target/release/dittoctl cluster list nodes
```

See [docs/docker-setup.md](docs/docker-setup.md) for the full guide.

---

## Configuration

### Node daemon — `node.toml`

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
gossip_interval_ms = 200
gossip_dead_ms     = 1000

[tls]
enabled = false
ca_cert = "/etc/ditto/certs/ca.pem"
cert    = "/etc/ditto/certs/node.pem"
key     = "/etc/ditto/certs/node.key"

# Optional: HTTP Basic Auth on the REST port (7778).
# Generate the hash with: dittoctl hash-password
# [http_auth]
# username      = "ditto"
# password_hash = "$2b$12$..."
```

**Environment variable overrides** (useful for Docker / Kubernetes):

| Variable | Config field |
|----------|-------------|
| `DITTO_NODE_ID` | `node.id` |
| `DITTO_ACTIVE` | `node.active` |
| `DITTO_SEEDS` | `cluster.seeds` (comma-separated) |
| `DITTO_MAX_MEMORY_MB` | `cache.max_memory_mb` |
| `DITTO_TLS_ENABLED` | `tls.enabled` |
| `DITTO_TLS_CA_CERT` | `tls.ca_cert` |
| `DITTO_TLS_CERT` | `tls.cert` |
| `DITTO_TLS_KEY` | `tls.key` |
| `DITTO_BIND_ADDR` | `node.bind_addr` |
| `DITTO_CLUSTER_BIND_ADDR` | `node.cluster_bind_addr` |
| `DITTO_HTTP_AUTH_USER` | `http_auth.username` |
| `DITTO_HTTP_AUTH_PASSWORD_HASH` | `http_auth.password_hash` |
| `DITTO_BACKUP_ENCRYPTION_KEY` | `backup.encryption_key` (hex-encoded 32-byte AES-256-GCM key) |
| `DITTO_PERSISTENCE_PLATFORM_ALLOWED` | global platform gate for persistence features |
| `DITTO_PERSISTENCE_BACKUP_ALLOWED` | platform gate for backup |
| `DITTO_PERSISTENCE_EXPORT_ALLOWED` | platform gate for export |
| `DITTO_PERSISTENCE_IMPORT_ALLOWED` | platform gate for import |

Persistence effective state:

`persistence_enabled = PERSISTENCE_PLATFORM_ALLOWED && persistence_runtime_enabled`

Default behavior is secure-by-default: all persistence platform gates are `false`, so backup/export/import are disabled until explicitly enabled.

### Management service — `~/.config/ditto/mgmt.toml`

```toml
[server]
bind = "0.0.0.0"    # "site-local" | "0.0.0.0" | "localhost" | "<ip>"
port = 7781
# Optional: HTTPS on port 7781 (server-only TLS, no client cert required).
# tls_cert = "/etc/ditto/certs/mgmt.pem"
# tls_key  = "/etc/ditto/certs/mgmt.key"

[connection]
seeds        = ["node-1:7779", "node-2:7779", "node-3:7779"]
cluster_port = 7779
timeout_ms   = 3000

[tls]
enabled = false          # set true + certs when nodes use mTLS

# Optional: HTTP Basic Auth for the management UI and API (port 7781).
# Generate the hash with: dittoctl hash-password
# [admin]
# username      = "admin"
# password_hash = "$2b$12$..."

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
dittoctl node describe local
dittoctl node set active 127.0.0.1:7779 false    # maintenance mode
dittoctl node set max-memory local 1024           # live, no restart
dittoctl node set persistence-runtime-enabled local true  # requires platform allow

# Cache operations
dittoctl cache list stats all
dittoctl cache list keys local --pattern "user:*"
dittoctl cache set local mykey "hello" --ttl 3600
dittoctl cache get key local mykey
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

# Quick smoke test (requires Docker cluster)
docker compose -f docker/docker-compose.yml up -d --build
sleep 15
# Nodes serve HTTPS when TLS is enabled; use --cacert or -k for self-signed certs.
# HTTP Basic Auth is enabled in the default docker-compose.yml — add -u ditto:password.
# /ping is always auth-free (health check exempt).
curl -sfk -u ditto:qwe123asd -X PUT https://localhost:7778/key/test -d "hello"
curl -sfk -u ditto:qwe123asd https://localhost:7788/key/test    # same value from node-2
curl -sfk -u ditto:qwe123asd https://localhost:7798/key/test    # same value from node-3

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
| [docs/docker-setup.md](docs/docker-setup.md) | Docker test environment setup and operations |
| [docs/dittoctl-reference.md](docs/dittoctl-reference.md) | Supplementary CLI reference |

---

## Security

mTLS on the cluster/admin port (7779) is supported.  When enabled, every
connection (node-to-node and mgmt-to-node) must present a client certificate
signed by the configured CA.  The TLS server name used for all handshakes is
`ditto-cluster` — all certificates must include this as a DNS SAN.

Client port 7777 (TCP binary) and gossip (UDP/7780) are not TLS-protected.
Port 7778 (HTTP REST) and port 7781 (ditto-mgmt) are plain HTTP by default;
both optionally support HTTPS (server-only TLS) and HTTP Basic Auth.

The `mgmt.pem` certificate generated by `gen-certs.sh` includes `localhost`
and `127.0.0.1` as SANs so browsers accept it without warnings on the dev machine.

**Backup encryption** is separate from TLS: set `DITTO_BACKUP_ENCRYPTION_KEY`
(hex-encoded 32-byte key) to encrypt backup files with AES-256-GCM.  The key
is independent of the TLS certificates.  Generate a key with:

```bash
openssl rand -hex 32
```

```bash
# Generate test certificates (includes localhost SAN for the mgmt cert)
bash docker/gen-certs.sh
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
