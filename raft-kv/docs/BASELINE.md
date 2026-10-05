# Baseline: raft-kv @ `6b3205c` (before any Phase 3+ change)

Recorded 2026-10-05. Raw outputs were captured to files during the run; the numbers below are copied from them.

## Environment

| | |
|---|---|
| CPU | Intel Xeon @ 2.80 GHz, **2 vCPU** (cloud VM, shared host) |
| RAM | 7 GiB |
| Disk / FS | virtio block device, **ext4** |
| OS | Linux 6.18.44 (Firecracker microVM) |
| Toolchain | go1.24.7 linux/amd64 |
| Network | all nodes on localhost (127.0.0.1), no injected latency |

## Commands and results

| # | Command | Result | Notes |
|---|---|---|---|
| 1 | `gofmt -l .` | **PASS** | no files listed |
| 2 | `go vet ./...` | **PASS** | exit 0 |
| 3 | `go test -count=1 ./...` | **PASS** | 32 tests (raft 19, kv 8, lincheck 5); wall time 75 s (kv 73.4 s, raft 59.8 s) |
| 4 | `go test -race -count=1 ./...` | **PASS** | no races reported; kv 69.4 s, raft 65.3 s |
| 5 | `go test -count=1 -cover ./...` | **PASS** | raft **76.2 %**, kv **59.0 %**, lincheck **88.3 %**, cmd/* **0 %** |
| 6 | `staticcheck ./...` | **NOT RUN** | install failed: `proxy.golang.org` → 403 "Host not in allowlist"; `GOPROXY=direct` → `honnef.co` CONNECT rejected by the sandbox egress policy |
| 7 | `golangci-lint`, `govulncheck` | **NOT RUN** | same egress restriction. The module has **no third-party dependencies** (stdlib only) |
| 8 | `docker build` | **NOT RUN** | Docker daemon available, but base images can't be pulled (Docker Hub, mirror.gcr.io and public.ecr.aws all 403 in this sandbox) |
| 9 | `make build && ./scripts/chaos.sh` (`DURATION=30`) | **PASS** | 6 kill/restart cycles; `ops=99588 errors=0 throughput=3320 ops/s`, p50 4.40 ms, p99 12.36 ms, max 128 ms. **Caveat:** `cluster.sh kill` sends SIGTERM (graceful), and the script doesn't check linearizability |

## Benchmarks (`kvctl bench`, 3-node cluster from `scripts/cluster.sh`, localhost)

32 closed-loop clients, 1,000 keys, 64-byte values, 10 s each, FileStorage with real fsync, snapshot threshold 4 MiB:

| Read ratio | Throughput | p50 | p95 | p99 | max | Errors |
|---|---:|---:|---:|---:|---:|---:|
| 0.90 | 7,105 ops/s | 4.01 ms | 8.75 ms | 12.06 ms | 25.3 ms | 0 |
| 0.50 | 5,049 ops/s | 5.87 ms | 11.76 ms | 15.28 ms | 47.5 ms | 0 |

After the run all replicas had converged (leader commit 32,319, followers 32,304 and still catching up at read time) and snapshots had trimmed the logs (first index 21,783 / 32,029).

Not measured by existing tooling: cluster sizes 1 and 5, concurrency sweeps, CPU/RSS, replication lag, election recovery time, snapshot duration, injected network latency or loss, and multi-machine runs.

## Audit probes (throwaway copy; not committed)

These ran in `/tmp/raft-audit`, a disposable copy. See `docs/COMBINED_ARCHITECTURE_AUDIT.md` §16.

| Probe | Result |
|---|---|
| Follower receives entries 1..10 with commit 10, then a delayed RPC (prev 3, no entries, commit 12) | **commitIndex 10 → 3** (R-M1) |
| 5 fsynced WAL records (50 entries), flip one byte at 1/3 of the file | **`Load` returns no error, 10/50 entries, WAL truncated 3,260 → 652 bytes** (R-H1) |
