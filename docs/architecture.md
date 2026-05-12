# Ditto Architecture Guide

This document describes the current runtime architecture of the Ditto stack in this repository.

## Scope

- `dittod`: distributed cache node process
- `ditto-mgmt`: optional management plane (UI + REST API)
- `dittoctl`: CLI that talks to `ditto-mgmt`
- Client SDKs and direct client protocols

`dittod` can run and serve application traffic without `ditto-mgmt`.
`ditto-mgmt` is required only for management UI/API and for `dittoctl` commands.

## High-level topology

```text
Application clients
  |- TCP binary protocol  -> dittod :7777
  |- HTTP/HTTPS REST API  -> dittod :7778

Management clients
  |- Browser UI           -> ditto-mgmt :7781
  |- dittoctl             -> ditto-mgmt :7781

Internal cluster traffic
  |- Replication/Admin RPC (TCP, mTLS required by default) -> :7779
  |- Gossip heartbeats (UDP)                     -> :7780
```

## Data plane vs management plane

### Data plane (`dittod`)

- Serves `GET/SET/DELETE/PING` for app clients.
- Serves pattern operations:
  - `DeleteByPattern { pattern }`
  - `SetTtlByPattern { pattern, ttl_secs }`
- Maintains full in-memory dataset on all active nodes.
- Coordinates write replication through an active-set commit flow.

### Management plane (`ditto-mgmt` + `dittoctl`)

- `ditto-mgmt` exposes cluster/node/cache admin APIs and the web dashboard.
- `dittoctl` is an HTTP client for `ditto-mgmt`; it does not talk to nodes directly.
- Management clients authenticate to `ditto-mgmt` with either HTTP Basic auth
  or `Authorization: Bearer <oauth2-access-token>` when Bearer/OIDC
  introspection is configured.

## Protocols and ports

| Port | Owner | Purpose |
|---|---|---|
| 7777 | `dittod` | TCP binary client protocol |
| 7778 | `dittod` | HTTP/HTTPS client API |
| 7779 | `dittod` | Cluster replication + admin RPC |
| 7780/udp | `dittod` | Gossip heartbeat |
| 7781 | `ditto-mgmt` | Management UI + REST API |

The TCP, cluster, gossip, and admin payloads share a single protobuf contract
defined in `ditto-protocol/proto/ditto.proto` (encoded via `prost`, wrapped in a
versioned `Envelope`, length-prefixed with a 4-byte big-endian header on TCP).
The generated `ditto-protocol/schema/protocol-contract.json` is the snapshot
that the drift gate enforces (see [admin-guide.md](admin-guide.md#validation-and-gates)).

## Client protocol capabilities

### Core operations

- `Get { key }`
- `Set { key, value, ttl_secs }`
- `Delete { key }`
- `Ping`
- Optional TCP auth handshake via `Auth { token }`

### Watch operations (TCP)

- `Watch { key }` -> `Watching`
- `Unwatch { key }` -> `Unwatched`
- Async server push: `WatchEvent { key, value, version }`

Behavior:
- Watch state is connection-scoped.
- On key delete, push event contains `value = None`.
- Read timeout is enforced only when auth is expected but not yet completed.
  This prevents idle watch connections from being dropped during normal operation.

### Pattern operations (TCP + HTTP)

- `DeleteByPattern { pattern }` -> `PatternDeleted { deleted }`
- `SetTtlByPattern { pattern, ttl_secs }` -> `PatternTtlUpdated { updated }`

Pattern semantics:
- Glob-style matching with `*` wildcard.
- `ttl_secs = None` removes TTL for matched keys.
- API-level convention: `ttl=0` is treated as TTL removal.

## HTTP API (dittod)

Current key endpoints:

- `GET /ping`
- `GET /stats`
- `GET /health/summary`
- `GET /key/{key}`
- `PUT /key/{key}?ttl=<seconds>`
- `DELETE /key/{key}`
- `POST /keys/batch` with JSON `{ "items": [{ "key": "...", "value": "...", "ttl_secs": <number|null> }] }`
- `POST /keys/delete-by-pattern` with JSON `{ "pattern": "..." }`
- `POST /keys/ttl-by-pattern` with JSON `{ "pattern": "...", "ttl_secs": <number|null> }`

Auth/TLS:
- Optional HTTP Basic auth (`[http_auth]`).
- Optional HTTPS when node TLS cert/key are configured for HTTP listener.
- This node REST auth is separate from `ditto-mgmt` admin auth. Node REST does
  not currently validate SSO/OIDC Bearer tokens directly.

## Replication model summary

- Active nodes form the write quorum set.
- Writes are coordinated by the current primary.
- Every active node applies committed entries; reads are served locally.
- Nodes falling behind can move to syncing mode and catch up.
- Optional read-repair-on-miss mode can query primary on local GET miss and trigger async resync.
- Optional anti-entropy loop can periodically trigger resync on lag threshold, sampled key-version mismatch, and bounded full keyspace reconcile mismatch.
- Optional mixed-version probe checks peer `protocol-version` and surfaces compatibility risk via node stats counters.
- Optional tenancy mode isolates keys by namespace and can enforce per-namespace key quotas.
- `/health/summary` also surfaces quota pressure telemetry (`namespace_quota_top_usage`, reject trend/rate) for operator diagnostics.

## Security model summary

### `dittod`

- Strict by default in this codebase:
  - startup is rejected if cluster/admin mTLS is disabled,
  - startup is rejected if HTTP Basic auth hash is not configured.
  - startup is rejected if TCP port `7777` is bound on a non-loopback address without `client_auth_token`.
- `DITTO_INSECURE=true` bypasses strict checks for local/dev only, is blocked in release builds, and should be treated as a non-production runtime.
- TCP client token auth is mandatory for non-loopback exposure of `:7777`.

### `ditto-mgmt`

`ditto-mgmt` is strict by default in this codebase:

- refuses startup if management-to-node mTLS is disabled,
- refuses startup if neither Basic nor Bearer admin auth is configured,
- refuses startup if management HTTPS cert/key are not configured.

Mgmt admin auth modes:

- Basic mode: `[admin].password_hash` plus optional `[admin].username`.
- Bearer mode: either `[admin].bearer_token_sha256` for an opaque pre-shared
  token hash, or `[admin].bearer_introspection_url` for OAuth2/OIDC token
  introspection.
- Bearer introspection accepts active OAuth2 access tokens and can require a
  Ditto-specific scope and/or audience.
- Basic and Bearer can be configured together for migration, but SSO-only
  environments should omit `[admin].password_hash` so Basic is not accepted.
- Basic and Bearer callers are assigned roles with `[admin].basic_role` and
  `[admin].bearer_role`: `read-only`, `operator`, or `admin`.
- `read-only` cannot mutate state or reveal cache values. `operator` can perform
  operational cache/node actions, but restore, node property writes, and
  `reveal=true` cache value reads require `admin`.

Mgmt-to-node auth boundaries:

- Admin RPC to nodes uses mTLS on `:7779`.
- Mgmt cache proxy calls to node REST use `[http_client_auth]` service
  credentials when node `[http_auth]` is enabled.
- The end-user Bearer token is not forwarded to nodes because node REST/TCP do
  not currently implement JWT/OIDC validation or RBAC.

Both management plane and data plane enforce strict security defaults in this repository.

## Docker notes

In `ditto-docker`:

- 3 `dittod` nodes run with TLS enabled.
- `ditto-mgmt` runs as a separate optional service for UI/API operations.
- Client test environments (Node.js, Go, Rust, Java, Python) use dedicated compose files under `clients/`.

## Practical implications

- If `ditto-mgmt` is down, app traffic to `dittod` (7777/7778) can still work.
- If `ditto-mgmt` is down, `dittoctl` and web dashboard operations are unavailable.
- Switching `ditto-mgmt` from Basic to Bearer does not change direct app-client
  auth for `dittod` TCP/HTTP ports.
- For long-lived watch workloads, keep auth configured correctly and avoid idle-kill timers on the client side.
