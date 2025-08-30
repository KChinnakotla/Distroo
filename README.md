# kvstore (Raft-backed KV — scaffolding)

This is a **minimal, runnable scaffold** for a distributed key–value store with **Raft leader election** (heartbeats & voting) and HTTP KV APIs.
It's designed as a learning project you can extend to full Raft log replication.

## What's included
- **HTTP APIs** for `GET/PUT/DELETE` at `/kv/*` (writes only accepted by the leader).
- **Raft (minimal)**: follower→candidate→leader election, RequestVote, AppendEntries heartbeats, randomized timeouts.
- **CLI** `kvctl` for get/put/del (auto-discovers the current leader).
- **Dockerfile** & **docker-compose.yml** to spin up a 3-node cluster.
- **Proto spec** (`proto/kv.proto`) for a future gRPC upgrade (not used by the HTTP server yet).
- **WAL stub** (`internal/storage/wal.go`) ready to be wired in for durability.

> NOTE: This is **Milestone 1–2** scaffolding. It elects a leader and gates writes to the leader. It does **not** yet replicate a Raft log or apply entries
across nodes—so data is not consistent cluster-wide yet. You'll implement log replication, commit index, and snapshots as you proceed.

## Quick start (Docker)
```bash
# From project root
docker compose -f deploy/docker-compose.yml up --build
# Wait ~2–5 seconds for election; check logs to see who is leader.
```

In another shell:
```bash
# PUT (auto-discovers leader)
docker compose exec kv1 kvctl --seeds http://kv1:8080,http://kv2:8080,http://kv3:8080 put mykey "hello"
# GET
docker compose exec kv2 kvctl --seeds http://kv1:8080,http://kv2:8080,http://kv3:8080 get mykey
# DELETE
docker compose exec kv3 kvctl --seeds http://kv1:8080,http://kv2:8080,http://kv3:8080 del mykey
```

Check health and role:
```
curl -s http://localhost:8081/raft/leader | jq .
curl -s http://localhost:8081/raft/state  | jq .
```

## Run locally (no Docker)
Terminal 1:
```bash
go run ./cmd/kvnode --id=kv1 --addr=:8081 --advertise=http://localhost:8081 --peers=http://localhost:8082,http://localhost:8083 --data=.data/kv1
```
Terminal 2:
```bash
go run ./cmd/kvnode --id=kv2 --addr=:8082 --advertise=http://localhost:8082 --peers=http://localhost:8081,http://localhost:8083 --data=.data/kv2
```
Terminal 3:
```bash
go run ./cmd/kvnode --id=kv3 --addr=:8083 --advertise=http://localhost:8083 --peers=http://localhost:8081,http://localhost:8082 --data=.data/kv3
```

CLI (another terminal):
```bash
go run ./cmd/kvctl --seeds http://localhost:8081,http://localhost:8082,http://localhost:8083 put k v
```

## Next steps to implement
- Raft log with AppendEntries for actual replication (matchIndex/nextIndex).
- Commit index and state machine apply loop (wire KV Store to committed log).
- WAL & snapshots.
- Read-index (linearizable reads) or leader leases.
- Membership changes (joint consensus).
- Observability (Prometheus metrics, traces).

Directory layout:
```
kvstore/
  cmd/
    kvnode/        # runs a node (KV + Raft HTTP)
    kvctl/         # CLI client
  internal/
    raft/          # election, heartbeats, HTTP RPC handlers
    server/        # KV HTTP handlers (leader-only writes)
    storage/       # WAL stub
  proto/
    kv.proto       # future gRPC (not yet wired)
  deploy/
    docker-compose.yml
  Dockerfile
```

Happy hacking!
