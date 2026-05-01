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

- Workspace line coverage: `35.95%`
- `ditto-protocol` line coverage: `52.23%`

The largest remaining gaps are in:

- `ditto-mgmt/src/api/*`
- `dittoctl`
- `dittod/src/network/*`
- larger runtime paths in `dittod/src/node.rs`

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
```

## Next recommended work

Continue with `ditto-cache` coverage. The best next step is to build focused test harnesses for the management API, CLI command formatting/client behavior, and network handlers. Getting the workspace to 80% will likely require integration-style tests, not only small unit tests.
