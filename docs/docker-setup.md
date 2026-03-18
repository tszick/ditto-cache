# Ditto – Docker Test Environment

This guide walks through running a **3-node Ditto cluster + ditto-mgmt** locally
using Docker Compose — the quickest way to verify the full stack before deploying
to production.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Directory Layout](#directory-layout)
- [Quick Start](#quick-start)
- [Port Mapping](#port-mapping)
- [TLS Certificates](#tls-certificates)
- [Container Configuration](#container-configuration)
- [Running ditto-mgmt Alongside](#running-ditto-mgmt-alongside)
- [Interacting with the Cluster](#interacting-with-the-cluster)
  - [Web Dashboard](#web-dashboard)
  - [dittoctl](#dittoctl)
  - [curl](#curl)
- [Common Operations](#common-operations)
- [Troubleshooting](#troubleshooting)
- [Building a Custom Image](#building-a-custom-image)

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Docker | 24+ | Engine |
| Docker Compose | v2 (`docker compose`) | Included in Docker Desktop |
| Rust toolchain | 1.75+ | Only needed if you want to build `dittoctl`/`ditto-mgmt` locally |
| `curl` | any | For quick smoke tests |
| `openssl` | any | For generating test certificates |

---

## Directory Layout

```
ditto/src/
├── docker/
│   ├── Dockerfile            Multi-stage Rust build → Debian slim runtime
│   ├── docker-compose.yml    3-node cluster definition
│   ├── gen-certs.sh          Test certificate generator (openssl)
│   └── certs/                Generated certificates (not committed to git)
│       ├── ca.pem
│       ├── node1.pem / .key
│       ├── node2.pem / .key
│       ├── node3.pem / .key
│       └── mgmt.pem  / .key
└── dittod/
    └── node.toml             Default node config baked into the image
```

---

## Quick Start

```bash
# 1. Clone / enter the repo
cd ditto/src

# 2. Generate test TLS certificates (first time only)
bash docker/gen-certs.sh          # Linux/macOS
# Windows: & "C:\Program Files\Git\usr\bin\bash.exe" docker/gen-certs.sh

# 3. Build images and start all four containers (3 nodes + mgmt)
docker compose -f docker/docker-compose.yml up --build

# 4. Open the web dashboard — ditto-mgmt serves HTTPS when tls_cert/tls_key are set
open https://localhost:7781   # macOS
# or
xdg-open https://localhost:7781  # Linux
# or navigate manually: https://localhost:7781
```

Wait about 10–15 seconds for all containers to report healthy, then you should
see three green nodes in the dashboard.  The `mgmt` container only starts after
all three nodes pass their healthcheck.

> **Self-signed certificate:** The generated `mgmt.pem` includes `localhost` and
> `127.0.0.1` as SANs so modern browsers accept it after a one-time "Advanced →
> Proceed to localhost" confirmation.  To avoid the warning permanently, import
> `docker/certs/ca.pem` into your OS trust store.

---

## Port Mapping

Each node runs on the same internal ports (7777–7780) but is exposed on the
host with a 10-port offset.  `ditto-mgmt` is now also a container:

| Container | TCP client | HTTP | Cluster/Admin | Gossip (UDP) | Web UI |
|-----------|-----------|------|---------------|--------------|--------|
| `ditto-node-1` | **7777** | **7778** | **7779** | **7780** | — |
| `ditto-node-2` | **7787** | **7788** | **7789** | **7790** | — |
| `ditto-node-3` | **7797** | **7798** | **7799** | **7800** | — |
| `ditto-mgmt`   | — | — | — | — | **7781** |

All four containers share a Docker bridge network (`ditto-net`).  Within the
network, containers refer to each other by service name (`node-1`, `node-2`,
`node-3`, `mgmt`).  The `mgmt` container uses Docker service names as seeds:
`node-1:7779`, `node-2:7779`, `node-3:7779`.

---

## TLS Certificates

`docker/gen-certs.sh` generates a self-signed CA, three node certificates,
a management service certificate, and a client certificate.

```bash
bash docker/gen-certs.sh
# Creates: docker/certs/
#   ca.pem
#   node1.pem / node1.key
#   node2.pem / node2.key
#   node3.pem / node3.key
#   mgmt.pem  / mgmt.key     ← SANs: ditto-cluster, ditto-mgmt, localhost, 127.0.0.1
#   client.pem / client.key  ← for dittoctl direct mTLS
```

The `mgmt.pem` certificate includes `localhost` and `127.0.0.1` as Subject Alternative
Names so that browsers accept the cert when connecting to `https://localhost:7781`.

All node and mgmt certificates also include `ditto-cluster` as a DNS SAN, which is
required by the mTLS handshake on port 7779.

The `docker-compose.yml` mounts `./certs` into each container at `/certs`.
TLS is enabled by environment variable:

```yaml
DITTO_TLS_ENABLED=true
DITTO_TLS_CA_CERT=/certs/ca.pem
DITTO_TLS_CERT=/certs/node1.pem
DITTO_TLS_KEY=/certs/node1.key
```

### Disabling TLS for quick testing

Edit `docker/docker-compose.yml` and set `DITTO_TLS_ENABLED=false` in all three
node services and remove `DITTO_MGMT_TLS_CERT` / `DITTO_MGMT_TLS_KEY` from the
`mgmt` service.  No `certs` volume mount is needed in that case.
The healthcheck URLs must also be changed back to `http://`.

---

## Container Configuration

The `Dockerfile` uses a **multi-stage build** with three stages:

```dockerfile
# Stage 1 — build (shared by both runtime images)
FROM rust:1.85-slim AS builder
WORKDIR /build
COPY . .
RUN cargo build --release --bin dittod --bin ditto-mgmt

# Stage 2 — dittod runtime
FROM debian:bookworm-slim AS dittod
RUN apt-get install -y ca-certificates curl
COPY --from=builder /build/target/release/dittod /usr/local/bin/dittod
COPY dittod/node.toml /etc/ditto/node.toml
ENTRYPOINT ["dittod", "/etc/ditto/node.toml"]

# Stage 3 — ditto-mgmt runtime
FROM debian:bookworm-slim AS ditto-mgmt
RUN apt-get install -y ca-certificates curl
COPY --from=builder /build/target/release/ditto-mgmt /usr/local/bin/ditto-mgmt
ENTRYPOINT ["ditto-mgmt", "/etc/ditto/mgmt.toml"]
```

Docker Compose selects the correct target per service via `build.target`:
- Node services → `target: dittod`
- Management service → `target: ditto-mgmt`

Both share the same build cache, so only one Rust compilation is needed.

The baked-in `node.toml` provides base defaults.  All cluster-specific settings
(`node.id`, seeds, TLS) are overridden at runtime via environment variables:

| Variable | Config field | Example |
|----------|-------------|---------|
| `DITTO_NODE_ID` | `node.id` | `node-1` |
| `DITTO_ACTIVE` | `node.active` | `true` |
| `DITTO_SEEDS` | `cluster.seeds` | `node-2:7779,node-3:7779` |
| `DITTO_MAX_MEMORY_MB` | `cache.max_memory_mb` | `512` |
| `DITTO_TLS_ENABLED` | `tls.enabled` | `true` |
| `DITTO_TLS_CA_CERT` | `tls.ca_cert` | `/certs/ca.pem` |
| `DITTO_TLS_CERT` | `tls.cert` | `/certs/node1.pem` |
| `DITTO_TLS_KEY` | `tls.key` | `/certs/node1.key` |
| `DITTO_CLIENT_AUTH_TOKEN` | `node.client_auth_token` | `my-secret-token` |
| `DITTO_HTTP_AUTH_USER` | `http_auth.username` | `ditto` |
| `DITTO_HTTP_AUTH_PASSWORD_HASH` | `http_auth.password_hash` | `$2b$12$...` (bcrypt) |
| `DITTO_MAX_MESSAGE_SIZE_BYTES` | `node.max_message_size_bytes` | `134217728` |
| `DITTO_GOSSIP_DEAD_MS` | `replication.gossip_dead_ms` | `3000` |
| `DITTO_BACKUP_ENCRYPTION_KEY` | `backup.encryption_key` | 64 hex chars (AES-256-GCM) |
| `RUST_LOG` | tracing level | `dittod=info` |

---

## ditto-mgmt Container

`ditto-mgmt` is included as the fourth service in the Compose stack (`mgmt`).
It waits for all three node healthchecks to pass before starting, then connects
to them using Docker service names as seeds (`node-1:7779` etc.).

Its configuration is mounted read-only from `docker/mgmt.toml`:

```toml
[server]
bind = "0.0.0.0"    # "site-local" | "0.0.0.0" | "localhost" | "<ip>"
port = 7781
# HTTPS is enabled in the default docker-compose.yml via env vars:
#   DITTO_MGMT_TLS_CERT=/certs/mgmt.pem
#   DITTO_MGMT_TLS_KEY=/certs/mgmt.key
# Disable by removing those env vars and the certs volume mount.
# tls_cert = "/certs/mgmt.pem"
# tls_key  = "/certs/mgmt.key"

[connection]
seeds        = ["node-1:7779", "node-2:7779", "node-3:7779"]
cluster_port = 7779
timeout_ms   = 3000

[tls]
enabled = true
ca_cert  = "/certs/ca.pem"
cert     = "/certs/mgmt.pem"
key      = "/certs/mgmt.key"

# Optional: Basic Auth for the management UI / API.
# Generate the hash with: dittoctl hash-password
# [admin]
# username      = "admin"
# password_hash = "$2b$12$..."

# Optional: credentials for proxying cache requests to dittod port 7778.
# Required when DITTO_HTTP_AUTH_* is set on the nodes.
# [http_client_auth]
# username = "ditto"
# password = "plaintext-password"
```

The `mgmt.pem` / `mgmt.key` pair is generated by `gen-certs.sh` alongside the
node certificates.

If you want to run `ditto-mgmt` on the host instead of in Docker (e.g. for
faster iteration), point it at the exposed host ports:

```bash
ditto-mgmt /tmp/mgmt-host.toml
# mgmt-host.toml: seeds = ["127.0.0.1:7779","127.0.0.1:7789","127.0.0.1:7799"]
```

---

## Interacting with the Cluster

### Web Dashboard

Navigate to **https://localhost:7781** while `ditto-mgmt` is running.

You should see three nodes with green status dots, the primary marked, and
memory bars for each.

> First visit: the browser will warn about the self-signed certificate.
> Click "Advanced → Proceed to localhost" to continue.

### dittoctl

Build `dittoctl` from source or copy the release binary to your `PATH`:

```bash
cargo build --release --bin dittoctl --manifest-path src/Cargo.toml
cp src/target/release/dittoctl /usr/local/bin/dittoctl
```

Configure it to reach `ditto-mgmt`:

```bash
# Linux / macOS
mkdir -p ~/.config/ditto
cat > ~/.config/ditto/kvctl.toml <<'EOF'
[mgmt]
url        = "https://localhost:7781"   # HTTPS when ditto-mgmt has TLS configured
timeout_ms = 3000

[output]
format = "binary"
EOF
```

Then use any `dittoctl` command:

```bash
dittoctl cluster list nodes
dittoctl node status all
dittoctl cache list stats all
```

### curl

Test data operations directly against node HTTP ports:

> **Note:** Port 7778 serves **HTTPS** and requires HTTP Basic Auth in the default
> `docker-compose.yml`.  Add `-k` (or `--cacert docker/certs/ca.pem`) for the
> self-signed cert and `-u ditto:qwe123asd` for auth.
> `/ping` is always auth-free — useful for quick health checks.

```bash
# Health check (no auth needed)
curl -sfk https://localhost:7778/ping

# Set a key on node-1
curl -sfk -u ditto:qwe123asd -X PUT https://localhost:7778/key/greeting -d "hello from docker"

# Read from node-2 (should return "hello from docker")
curl -sfk -u ditto:qwe123asd https://localhost:7788/key/greeting

# Read from node-3
curl -sfk -u ditto:qwe123asd https://localhost:7798/key/greeting
```

Use the management API directly (ditto-mgmt also uses HTTPS + Basic Auth):

```bash
# All nodes status
curl -sfk -u admin:qwe123asd https://localhost:7781/api/nodes | python -m json.tool

# Cluster overview
curl -sfk -u admin:qwe123asd https://localhost:7781/api/cluster | python -m json.tool

# Cache stats from all nodes
curl -sfk -u admin:qwe123asd "https://localhost:7781/api/cache/all/stats" | python -m json.tool
```

---

## Common Operations

### Restart the cluster (keep data)

```bash
docker compose -f docker/docker-compose.yml restart
```

### Stop the cluster

```bash
docker compose -f docker/docker-compose.yml down
```

### Wipe all containers and rebuild

```bash
docker compose -f docker/docker-compose.yml down --volumes
docker compose -f docker/docker-compose.yml up --build
```

### Tail logs from a single node

```bash
docker logs -f ditto-node-1
```

### Simulate a node failure

```bash
# Stop node-3
docker stop ditto-node-3

# Confirm cluster continues with 2 nodes
dittoctl cluster get status
curl -sfk -u ditto:qwe123asd -X PUT https://localhost:7778/key/test -d "still works"
curl -sfk -u ditto:qwe123asd https://localhost:7788/key/test

# Restart node-3; it will sync and re-join automatically
docker start ditto-node-3
```

### Flush cache on all nodes

```bash
# Via CLI (prompts for confirmation)
dittoctl cache flush all

# Via management API
curl -sfk -u admin:qwe123asd -X POST https://localhost:7781/api/cache/all/flush
```

---

## Troubleshooting

### Nodes don't form a cluster

**Symptom:** `dittoctl cluster list nodes` shows only one node, or
`active: 1` in cluster status.

**Checks:**
1. TLS certificates — ensure `gen-certs.sh` ran successfully and
   `docker/certs/` is not empty.
2. `docker logs ditto-node-1` — look for TLS handshake errors.
3. All three containers on the same Docker network? Run `docker network inspect ditto-net`.

### `ditto-mgmt` can't reach nodes

**Symptom:** All nodes show `reachable: false` in the dashboard.

**Checks:**
1. Are nodes healthy? `docker ps` → confirm status `healthy`.
2. Is TLS configured in both `mgmt.toml` and the containers?
   mTLS must be enabled on both sides or disabled on both sides.
3. Firewall rules blocking ports 7779/7789/7799?

### HTTP 401 on port 7778 or 7781

**Symptom:** `curl` or the browser returns `401 Unauthorized`.

**Checks:**
1. Port 7778 (node HTTP): `DITTO_HTTP_AUTH_USER` and `DITTO_HTTP_AUTH_PASSWORD_HASH`
   are set in `docker-compose.yml`.  Add `-u ditto:password` to your `curl` commands.
   `/ping` is always exempt — `curl http://localhost:7778/ping` always works.
2. Port 7781 (mgmt UI/API): `DITTO_MGMT_ADMIN_USER` and
   `DITTO_MGMT_ADMIN_PASSWORD_HASH` are set.  Use `-u admin:password` or log in
   via the browser's Basic Auth dialog.
3. If the management service proxies key requests to nodes and you see 401 in mgmt
   logs, ensure `DITTO_MGMT_HTTP_AUTH_USER` and `DITTO_MGMT_HTTP_AUTH_PASSWORD`
   match the credentials set on the nodes.

### `cargo build` fails in Docker on Apple Silicon

The official `rust:1.85-slim` image works for `linux/amd64` and `linux/arm64`.
If you are cross-compiling, pass the platform explicitly:

```bash
docker compose build --platform linux/amd64
```

### Healthcheck keeps failing

The healthcheck polls `https://localhost:7778/ping` (with `-k` for self-signed certs).
If the node starts but the HTTP server is slow to bind, increase the `start_period`
in the Compose file:

```yaml
healthcheck:
  start_period: 10s
```

---

## Building a Custom Image

To build a standalone image without Compose:

```bash
# From the src/ directory
docker build -f docker/Dockerfile -t ditto:latest .

# Run a single-node cluster (TLS disabled)
docker run -d \
  --name ditto-node-1 \
  -p 7777:7777 -p 7778:7778 -p 7779:7779 -p 7780:7780/udp \
  -e DITTO_NODE_ID=node-1 \
  -e DITTO_TLS_ENABLED=false \
  ditto:latest
```

To include `ditto-mgmt` in the image, modify the `Dockerfile` to also build
and copy the `ditto-mgmt` binary:

```dockerfile
RUN cargo build --release --bin dittod --bin ditto-mgmt
COPY --from=builder /build/target/release/ditto-mgmt /usr/local/bin/ditto-mgmt
```
