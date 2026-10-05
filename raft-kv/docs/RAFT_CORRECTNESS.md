# Raft correctness: what is guaranteed and how it is checked

## Safety properties and their checks

| Property (Raft paper §5) | How it is enforced | How it is checked |
|---|---|---|
| **Election Safety**: at most one leader per term | majority votes, term persisted before any reply (`saveHardState`), pre-vote | invariant monitor samples every peer every 2 ms in every harness test; `TestInitialElection`, `TestReElection`, `TestManyElections` |
| **Log Matching** | prevLogIndex/prevLogTerm check; truncation only on an actual conflict | `TestBackup`, `TestFigure8Unreliable`; a conflict at or below commitIndex **panics** as a safety violation instead of rewriting history |
| **Leader Completeness** | election restriction (`logUpToDate`); commit only entries from the current term (§5.4.2) plus a leader no-op | Figure-8 stress test |
| **State Machine Safety** | apply in index order from committed entries only | the harness apply recorder fails the test if two peers ever apply different commands at one index; KV histories checked with lincheck |
| commitIndex monotonic | `max` on follower commit updates (fix R-M1) | `TestCommitIndexNeverDecreases` (failed before: 10 → 3); invariant monitor |
| Linearizable reads | ReadIndex: commit in current term + quorum heartbeat round | `TestCheckerCatchesStaleReads` proves the checker catches stale reads; `TestKVNoMinorityProgress` |
| Exactly-once client writes | (ClientID, Seq) session table, bounded with deterministic eviction; expired sessions are rejected, never re-applied | `kv/session_test.go` |

## Durability

| Claim | Mechanism | Evidence |
|---|---|---|
| Followers acknowledge only durable entries | `Append(entries, sync=true)` before replying | code review; crash/restart tests |
| Leader counts itself only after fsync | `durableIndex` advanced by the syncer | group-commit design (DESIGN.md) |
| WAL damage is never silently truncated | torn tail accepted only at the end of the last segment; damage followed by intact data → `ErrCorruptWAL`, files untouched (fix R-H1) | `TestFileStorageMidWALCorruptionFailsLoad` (before: 10 of 50 entries kept, file shrunk 3,260 → 652 B), `TestFileStorageCorruptLengthFailsLoad`, `TestFileStorageZeroTailIsTorn` |
| hardstate/snapshot corruption is detected | `[magic][crc32c]` framing; legacy files still load | `TestFileStorageStateFilesChecksummed` |
| Snapshot + compaction are crash-safe at every point | snapshot file written first; the stored log stays a superset; new segment with the retained suffix fsynced before old segments are deleted | `TestFileStorageCompactionCrashLeavesOldSegments`, `TestFileStorageLoadsLegacyWAL` |
| No fsync on a closed handle | `syncMu` serialises Sync with segment rotation (fix R-M3) | `TestFileStorageSyncConcurrentWithSnapshot` (fails with "file already closed" without the fix) |

## Liveness-related fixes

- **Chunked, resumable, verified InstallSnapshot** (R-H2). Previously a
  snapshot that couldn't be sent within one 600 ms RPC could never reach a
  lagging follower. Tests: `TestInstallSnapshotChunksResumeAndVerify`,
  `TestInstallSnapshotRejectsCorruptAssembly`, and every snapshot test now
  runs with 64-byte chunks.
- **Snapshot I/O outside the Raft lock** (R-M2). Heartbeats and votes no
  longer stall behind a slow disk: `TestSnapshotWriteDoesNotHoldRaftLock`.

## Fault testing

- In-process (`raft/`, `kv/`): partitions, packet loss, reordering, delays,
  crash/restart, snapshots, all under `-race`.
- Real processes (`cmd/kvchaos`): SIGKILL (random and leader-targeted) plus
  restart, and SIGSTOP/SIGCONT pauses, against three `kvserver` processes;
  the full client history is checked for linearizability and the replicas
  must converge. Results are in [results/](results/): 4 runs, 56k–77k ops
  each, all linearizable and converged.

## Not implemented / not verified

- **Cluster membership changes** (joint consensus or single-server changes)
  are **not implemented**; membership is static. A half-done version would
  be a safety risk, so it was left out deliberately (rule 11).
- No model checking (TLA+) or formal proof.
- The chaos harness does not partition the network between processes or
  inject disk faults. Partitions are covered only by the in-process tests.
- Losing acknowledged entries from the *middle* of the last WAL segment by
  truncation exactly at a record boundary is indistinguishable from a
  crash-torn tail and is not detected (the same limitation every
  length-prefixed log has without a separate durable high-water mark).
