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
  |- Replication/Admin RPC (TCP, optional mTLS) -> :7779
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

## Protocols and ports

| Port | Owner | Purpose |
|---|---|---|
| 7777 | `dittod` | TCP binary client protocol |
| 7778 | `dittod` | HTTP/HTTPS client API |
| 7779 | `dittod` | Cluster replication + admin RPC |
| 7780/udp | `dittod` | Gossip heartbeat |
| 7781 | `ditto-mgmt` | Management UI + REST API |

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
- `GET /key/:key`
- `PUT /key/:key?ttl=<seconds>`
- `DELETE /key/:key`
- `POST /keys/batch` with JSON `{ "items": [{ "key": "...", "value": "...", "ttl_secs": <number|null> }] }`
- `POST /keys/delete-by-pattern` with JSON `{ "pattern": "..." }`
- `POST /keys/ttl-by-pattern` with JSON `{ "pattern": "...", "ttl_secs": <number|null> }`

Auth/TLS:
- Optional HTTP Basic auth (`[http_auth]`).
- Optional HTTPS when node TLS cert/key are configured for HTTP listener.

## Replication model summary

- Active nodes form the write quorum set.
- Writes are coordinated by the current primary.
- Every active node applies committed entries; reads are served locally.
- Nodes falling behind can move to syncing mode and catch up.

## Security model summary

### `dittod`

- Optional mTLS on cluster/admin channel (`:7779`).
- Optional Basic auth on client HTTP API (`:7778`).
- Optional client token auth for TCP (`:7777`).

### `ditto-mgmt`

`ditto-mgmt` is strict by default in this codebase:

- refuses startup if management-to-node mTLS is disabled,
- refuses startup if admin password hash is not configured,
- refuses startup if management HTTPS cert/key are not configured.

This strictness applies to the management plane only; data plane availability still depends on `dittod`.

## Docker notes

In `ditto-docker`:

- 3 `dittod` nodes run with TLS enabled.
- `ditto-mgmt` runs as a separate optional service for UI/API operations.
- Client test environments (Node/Java/Python) use dedicated compose files under `clients/`.

## Practical implications

- If `ditto-mgmt` is down, app traffic to `dittod` (7777/7778) can still work.
- If `ditto-mgmt` is down, `dittoctl` and web dashboard operations are unavailable.
- For long-lived watch workloads, keep auth configured correctly and avoid idle-kill timers on the client side.
