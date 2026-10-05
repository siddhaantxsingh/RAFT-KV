# Final audit: raft-kv (+ the Raft/LSM integration)

Scope: from the audit commit (`562a3f0`) to the current HEAD. Original
findings are in [COMBINED_ARCHITECTURE_AUDIT.md](COMBINED_ARCHITECTURE_AUDIT.md).

## Findings status

| ID | Severity | Finding | Status | Evidence |
|---|---|---|---|---|
| R-H1 | High | Mid-WAL corruption silently truncated acknowledged entries | **Fixed** | `TestFileStorageMidWALCorruptionFailsLoad` and 3 related tests |
| R-H2 | High | Single-RPC snapshots with a 600 ms deadline | **Fixed** | ADR 0002; chunk tests; every snapshot test now uses 64-byte chunks |
| R-H3 | High | No TLS/auth; unbounded RPC bodies | **Fixed** (opt-in) | ADR 0004; `secure` tests; `TestSecureClusterEndToEnd` |
| R-M1 | Medium | commitIndex could decrease | **Fixed** | `TestCommitIndexNeverDecreases` (failed before: 10 → 3) |
| R-M2 | Medium | Snapshot I/O under the Raft lock | **Fixed** | `TestSnapshotWriteDoesNotHoldRaftLock` |
| R-M3 | Medium | Sync vs snapshot close race | **Fixed** | `TestFileStorageSyncConcurrentWithSnapshot` (fails without the fix) |
| R-M4 | Medium | Unbounded session table; IDs invented for anonymous requests | **Fixed** | ADR 0003; `kv/session_test.go` |
| R-M5 | Medium | Chaos used SIGTERM and never checked linearizability | **Fixed** | `cmd/kvchaos`; 4 result files in `docs/results/` |
| R-L1 | Low | CAS "" means absent; in-memory state only | CAS semantics **unchanged** (documented); persistence **added** via `-store lsm` | RAFT_LSM_INTEGRATION.md |
| NEW-1 | High | Values over 1 MiB silently truncated and stored | **Fixed** | `TestSecureClusterEndToEnd` (413, value not stored) |
| NEW-2 | Medium | Snapshot data could be paired with a newer/older index once snapshot I/O left the lock | **Prevented by design** | leader sends the stored snapshot's own index/term (ADR 0002) |

## Verification (final code)

| Check | Result |
|---|---|
| `gofmt -l .` (empty), `go vet ./...` | **PASS** |
| `go test -race -count=1 ./...` (55 tests incl. subtests' parents) | **PASS** |
| `go test -tags lsm -race ./kv/lsmstore ./cmd/...` | **PASS** |
| Invariant monitor active in all Raft harness tests (two full -race runs) | **PASS** |
| Real-process chaos, memory store: 3 runs (30–41 s, 6–9 SIGKILLs each) | **PASS**: linearizable, converged |
| Real-process chaos, LSM store: 1 run (41 s, 10 SIGKILLs) | **PASS**: linearizable, converged |
| Benchmarks, 3 runs each, same-VM baseline comparison | **RUN**, see PERFORMANCE.md |
| `govulncheck`, `staticcheck`, `golangci-lint` | **NOT RUN** locally (module proxy blocked); govulncheck is in CI |
| `docker build` / container smoke test | **NOT RUN** locally (registry blocked); defined in CI |
| Network-partition chaos between processes, disk-fault injection | **NOT RUN** (not implemented) |
| Membership changes | **NOT IMPLEMENTED** (ADR 0005) |

## Scores (0–10)

| Area | Score | Why not higher |
|---|---:|---|
| Consensus correctness | 8 | Paper-level safety with invariant monitoring, Figure-8 stress and lincheck; no TLA+ model or formal proof. |
| Durability | 8 | Checksummed, crash-safe compaction, corruption refused; no power-loss simulation. |
| Fault tolerance testing | 7 | Real-process SIGKILL/SIGSTOP plus linearizability; no inter-process partitions or disk faults. |
| Security | 7 | mTLS, tokens and limits, tested end to end; off by default, no ACLs, no rotation. |
| Operability | 6 | readyz, metrics with histograms, structured logs; no membership changes, backup or admin tooling. |
| Performance | 6 | Group commit and batched ReadIndex; measured honestly; single-machine numbers only. |
| Integration design | 8 | Clean interface, double-WAL protocol argued and tested, independent builds. Full-dump snapshots limit scale. |
| Documentation | 8 | Guarantees mapped to tests, ADRs, and explicit not-done lists. |

**Not production-ready.** The blockers are membership changes, backup and
restore tooling, and partition/disk-fault testing across real processes.
