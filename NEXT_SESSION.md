# Ditto coverage work - next session notes

Date: 2026-05-01

## Saved state

- `ditto-client` commit: `99b34fa upgrade: client test coverage`
- `ditto-cache` HEAD before this continuation: `ae88017 upgrade: test coverage`
- Current continuation has uncommitted test-only changes in `ditto-cache`.

## What was completed

`ditto-client` reached the target range for each component:

- Go client: `80.5%` statement coverage
- Node.js client: `80.04%` line coverage
- Python client: `80%` source line coverage
- Rust client: `82.93%` line coverage
- Java client: `84.37%` line coverage

Main test additions:

- HTTP/TCP full API smoke tests for Go, Node.js, Python, and Java clients
- Rust validation and error branch tests
- Protocol round-trip and bad decode tests in `ditto-cache/ditto-protocol`

## Current cache coverage

`ditto-cache` improved, but is still far from the 80% goal:

- Workspace line coverage: `77.31%`
- `ditto-protocol` line coverage: `84.26%`

Latest continuation added:

- Mock admin-RPC tests for `ditto-mgmt/src/api/cache.rs`
- Mock admin-RPC aggregate/primary tests for `ditto-mgmt/src/api/cluster.rs`
- Mock admin-RPC node handler tests for `ditto-mgmt/src/api/nodes.rs`
- Framing and target-validation tests for `ditto-mgmt/src/node_client.rs`
- Local HTTP mock tests for `dittoctl/src/client.rs`
- Command-level HTTP request tests for `dittoctl/src/commands/cache.rs`
- Command-level HTTP request tests for `dittoctl/src/commands/cluster.rs`
- Command-level HTTP request tests for `dittoctl/src/commands/node.rs`
- Network integration/helper tests for `dittod/src/network/http_server.rs`
- TCP frame/helper tests for `dittod/src/network/tcp_server.rs`
- Cluster RPC helper tests for `dittod/src/network/cluster_server.rs`
- TLS parsing/error-path tests for `dittod/src/network/tls.rs`
- Cluster/admin runtime tests for `dittod/src/node.rs`
- Gossip heartbeat/update/sender/receiver/reaper tests for `dittod/src/gossip/engine.rs`
- TTL background sweep test for `dittod/src/store/ttl.rs`
- Backup checksum, malformed snapshot, and encryption/decryption error-path tests for `dittod/src/backup.rs`
- Store lifecycle, compression, limits, snapshot/restore, eviction, and glob tests for `dittod/src/store/kv_store.rs`
- Backup gate, plaintext protobuf snapshot, encrypted JSON snapshot/restore, and scheduler disabled/invalid schedule tests for `dittod/src/backup.rs`
- Client runtime guard tests for inactive node, invalid namespace, Watch/Unwatch misuse, value/key limits, DeleteByPattern, and SetTtlByPattern in `dittod/src/node.rs`
- Main helper tests for env bool parsing, replication guardrail extremes, TCP auth disabled-port handling, and log rotation in `dittod/src/main.rs`
- Cluster server inbound frame tests for Forward/Admin requests and oversized frames in `dittod/src/network/cluster_server.rs`
- `ditto-mgmt` config default/partial TOML tests and TLS disabled/missing/invalid PEM edge tests
- `dittoctl` CLI parse/default config tests
- `dittoctl` path-based config load/save tests and cache delete/list-stats/set-ttl command request tests
- `ditto-mgmt` router/state helper tests, strict-security/env override helper tests, and embedded web index response test
- `dittoctl` reachable node status observability rendering test and critical doctor diagnosis test for unreachable/insecure/quota-pressure nodes
- TCP client handler tests for auth rejection, auth success + ping dispatch, and watch/unwatch push events in `dittod/src/network/tcp_server.rs`
- Broad admin `SetProperty` runtime/config update test for `dittod/src/node.rs`, including inactive-only bind/port changes, cache/store limits, replication, tenancy, rate limit, hot-key, circuit breaker, and invalid compression-threshold handling
- Extracted `dittod/src/main.rs` environment override parsing into testable helpers, with tests for primary and legacy startup knobs plus invalid numeric/empty token handling

The largest remaining gaps are in:

- `ditto-mgmt` cache API error/result formatting edges and auth edges
- `dittoctl/src/main.rs` password/input paths
- remaining `dittod` runtime edges in `node.rs`, HTTP/TLS paths, and daemon startup paths that still require heavier integration
- successful mTLS acceptor/connector handshakes if/when cert fixtures are added

## Verified commands

Client-side checks that passed:

```powershell
go test ./...
npm.cmd run build
node --test --test-isolation=none --experimental-test-coverage tests/*.test.mjs
python -m unittest discover -s tests -v
cargo test
cargo llvm-cov --all-targets --summary-only
.\gradlew.bat --no-daemon test jacocoTestReport --console=plain
```

Cache-side checks that passed:

```powershell
cargo test -p ditto-protocol
cargo llvm-cov --workspace --summary-only
cargo test -p ditto-mgmt
cargo test -p dittoctl
cargo test -p dittod
cargo test --workspace
cargo fmt -p dittod
cargo fmt -p ditto-mgmt -p dittoctl
cargo llvm-cov -p dittod --summary-only
```

## Next recommended work

Continue with `ditto-cache` coverage. The best next step is likely `ditto-mgmt` cache/auth edge cases or `dittoctl/src/main.rs` password/input paths; `dittod` can still improve, but the remaining daemon startup paths are heavier integration work. Successful mTLS handshake tests with cert fixtures are also still useful.
