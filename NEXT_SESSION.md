# Ditto coverage work - next session notes

Date: 2026-05-01

## Saved state

- `ditto-client` commit: `99b34fa upgrade: client test coverage`
- `ditto-cache` commit before this note: `c690cc8 Update lib.rs`
- Both repos were clean after the coverage work was saved.

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

- Workspace line coverage: `64.64%`
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

The largest remaining gaps are in:

- `ditto-mgmt` router/auth/config/TLS edges
- Remaining `dittoctl` local config/main command branches
- larger runtime paths in `dittod/src/node.rs`, `dittod/src/main.rs`, remaining backup edges
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
```

## Next recommended work

Continue with `ditto-cache` coverage. The best next step is to keep deepening runtime tests for `dittod/src/node.rs`, then cover remaining backup edges. Smaller cleanup remains for `dittod/src/main.rs`, successful mTLS handshakes with cert fixtures, `dittoctl` local config/main branches, and `ditto-mgmt` config/TLS/router edges.
