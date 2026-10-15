# Requirements matrix (audit 2026-10-14)

Statuses: VERIFIED, IMPLEMENTED BUT UNVERIFIED, PARTIAL, FAILED, BLOCKED, NOT IMPLEMENTED.
Evidence paths are under `docs/evidence/2026-10-14/` unless stated.

| # | Requirement | Baseline | Final | Evidence / notes |
|---|---|---|---|---|
| 1 | Raft safety (election, log matching, commit rule, pre-vote) | VERIFIED | VERIFIED | `raft/raft_test.go` with invariant monitor; `test-race-final.log` |
| 2 | Linearizable reads/writes (ReadIndex, sessions) | VERIFIED | VERIFIED | `lincheck` tests; every chaos run checked with lincheck |
| 3 | Process chaos (SIGKILL/SIGSTOP) | VERIFIED | VERIFIED | `baseline-chaos-seed1.*`, `chaos-baseline-faults-3node-seed7.*` |
| 4 | Network-partition chaos between real processes | NOT IMPLEMENTED | VERIFIED | `cmd/kvchaos/netem.go`; `chaos-net-5node-seed{1,2}.*`, `chaos-all-5node-seed{3,4}.*` |
| 5 | Chaos negative control (checker catches a broken server under partitions) | NOT IMPLEMENTED | FAILED | `chaos-negative-control-unsafe-reads.*`: the deliberately broken server was NOT caught in 40 s. In-process `TestCheckerCatchesStaleReads` still catches it. The process-level workload is too weak at producing stale reads. |
| 6 | WAL durability, torn tail, mid-WAL corruption | VERIFIED | VERIFIED | `raft/storage_test.go` |
| 7 | Disk-fault injection (fsync EIO, short write, torn/cut/reordered crash) | NOT IMPLEMENTED | VERIFIED (simulated) | `raft/diskfault_test.go`; `diskfault-before-fix.log`, `diskfault-after-fix.log`. Not tested on real failing hardware. |
| 8 | Snapshots (chunked, resumable, verified) | VERIFIED | VERIFIED | `raft/snapshot_test.go` |
| 9 | Membership reconfiguration | NOT IMPLEMENTED | VERIFIED | `raft/membership_test.go` (9 tests), `cmd/kvserver/admin_test.go`, `chaos-membership-3node-seed5.*`, `chaos-all-5node-seed{3,4}.*`; mutation runs `mutation-membership-round{1,2}.log` |
| 10 | Membership during partitions / leader crash | NOT IMPLEMENTED | VERIFIED | `TestMembershipChurnUnderPartitions`, `TestMembershipLeaderCrashDuringChange`, kvchaos membership+partition runs |
| 11 | Joint consensus / leadership transfer | NOT IMPLEMENTED | NOT IMPLEMENTED | single-server changes chosen (ADR 0006) |
| 12 | 5-node cluster | PARTIAL (in-process tests only) | VERIFIED | 5-process chaos and failover runs |
| 13 | Failover within 2x election timeout | NOT MEASURED | VERIFIED (local) | `failover-5node-run{1,2,3}.*`: 45 trials, max 757 ms to first committed write; 44/45 within 600 ms (2x min timeout), 45/45 within 1,200 ms (2x max) |
| 14 | Benchmarks | VERIFIED (older VM) | PARTIAL | `bench.log`: 2,065–2,466 ops/s (50 % reads) and 2,639–3,284 ops/s (90 % reads), measured at load average 22–31 on 4 vCPU; not comparable with older numbers |
| 15 | gRPC transport | NOT IMPLEMENTED | NOT IMPLEMENTED | transport is gob over HTTP/1.1 |
| 16 | LSM (Rust) state store via C ABI | BLOCKED | BLOCKED | `baseline-lsm.log`: `lsm-capi` crate not published |
| 17 | Static analysis | PARTIAL | VERIFIED (CI) | staticcheck PASS after fixes; govulncheck BLOCKED locally (in CI) |
| 18 | Docker image | NOT RUN | VERIFIED | `docker-build-smoke.log` |
| 19 | CI that actually runs | NOT IMPLEMENTED | VERIFIED | `.github/workflows/raft-kv-ci.yml`; run 38740044982 green (fmt, vet, staticcheck, govulncheck on latest Go, race tests, both chaos runs, docker smoke) |
| 20 | Backup / restore tooling | NOT IMPLEMENTED | NOT IMPLEMENTED | — |
