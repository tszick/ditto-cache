# Ditto Chaos Playbook

This playbook validates resilience behavior for Sprint 4 hardening goals.

## Prerequisites

- Docker Desktop is running.
- Ditto cluster is up from `../ditto-docker/docker-compose.yml`.
- Node HTTP auth credentials are default (`ditto / qwe123asd`) or updated in commands.

## 1) Automated Chaos Smoke (Recommended)

Run from `ditto-cache`:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\chaos-smoke.ps1 -Iterations 3
```

Optional tenant-scoped run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\chaos-smoke.ps1 -Iterations 3 -Namespace tenant-a
```

Dry-run (prints actions only):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\chaos-smoke.ps1 -DryRun
```

What it validates:

- restart loop: stop/start `ditto-node-2` while writes continue,
- timing fault: pause/unpause `ditto-node-3` while writes continue,
- replication visibility across nodes after restart,
- optional network partition + reconnect catch-up for `ditto-node-2`.

## 2) Manual Partition Scenario

```bash
# partition node-2 from cluster network
docker network disconnect -f ditto-docker_ditto-net ditto-node-2

# write while partitioned
docker exec ditto-node-1 sh -lc "curl -sfk -u ditto:qwe123asd -X PUT https://localhost:7778/key/chaos:manual -d 'ok'"

# reconnect and wait for catch-up
docker network connect ditto-docker_ditto-net ditto-node-2
sleep 6

# verify node-2 catches up
docker exec ditto-node-2 sh -lc "curl -sfk -u ditto:qwe123asd https://localhost:7778/key/chaos:manual"
```

Expected: node-2 returns the written value after reconnect.

## 3) Restart Loop Scenario

```bash
docker stop ditto-node-3
docker exec ditto-node-1 sh -lc "curl -sfk -u ditto:qwe123asd -X PUT https://localhost:7778/key/chaos:restart -d 'ok'"
docker start ditto-node-3
sleep 5
docker exec ditto-node-3 sh -lc "curl -sfk -u ditto:qwe123asd https://localhost:7778/key/chaos:restart"
```

Expected: value is available on node-3 after restart.

## 4) Timing-Fault (Pause/Unpause) Scenario

```bash
docker pause ditto-node-3
sleep 6
docker exec ditto-node-1 sh -lc "curl -sfk -u ditto:qwe123asd -X PUT https://localhost:7778/key/chaos:delay -d 'ok'"
docker unpause ditto-node-3
sleep 5
docker exec ditto-node-3 sh -lc "curl -sfk -u ditto:qwe123asd https://localhost:7778/key/chaos:delay"
```

Expected: node-3 returns the written value after unpause/catch-up.

## Failure Signals to Capture

- repeated `NoQuorum` or `WriteTimeout` responses under single-node outage,
- prolonged `OFFLINE` status after reconnect,
- missing replication after restart/partition recovery,
- rising `namespace_quota_reject_total` unexpectedly in non-quota tests.

## Suggested Evidence Bundle

- command transcript with timestamps,
- `docker ps` output before/after scenario,
- `dittoctl node status all` snapshot,
- last 100 lines of `ditto-node-1/2/3` logs.
