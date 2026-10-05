# Raft + LSM integration

raft-kv can keep its replicated state machine in
[lsm-engine](https://github.com/siddhaantxsingh/lsm-engine) instead of
memory. The two projects stay independent: raft-kv builds and runs without
Rust, and the LSM store is compiled in only with `-tags lsm`.

```bash
make build-lsm            # builds ../lsm-engine's liblsm_capi.a, then kvserver with -tags lsm
STORE=lsm ./scripts/cluster.sh start
make test-lsm chaos-lsm
```

## Layering

```
kvserver ──> kv.Server ──> raft.Raft ──> raft.Storage (FileStorage: WAL segments + snapshot)
                │
                └──> kv.StateStore ──> MemStore            (default)
                                  └──> lsmstore.Store ──cgo──> liblsm_capi ──> lsm-engine
```

- Raft knows nothing about the LSM. It delivers committed entries and asks
  for snapshots as opaque bytes.
- The LSM decides nothing about commit. It only ever receives entries Raft
  has already committed.
- `kv.StateStore` is the whole contract: `Apply(index, writes, sessions)`,
  `Get`, `AppliedIndex`, `LoadSessions`, `Dump`, `Restore`.

## The double-WAL question

Both layers have a write-ahead log. Fsyncing both on every write would pay
two fsyncs per operation for one durability requirement. The protocol makes
**the Raft log the WAL of record** and lets the LSM write without fsync:

1. An entry is durable (fsynced on a majority) before it is committed, and
   committed before it is applied.
2. Applying entry *i* writes **one LSM batch** containing the data changes,
   the client-session changes and `applied = i`. A batch is atomic in the
   LSM, so after any crash the store holds exactly the effects of entries
   `1..J` for some J (its recovered applied index). It is never partial.
3. On start, kv.Server compares J with Raft's snapshot index S:
   - `J >= S`: keep the store; Raft re-delivers entries after S, and the
     server skips those `<= J`.
   - `J < S`: the store is older than the snapshot and the log before S is
     gone, so restore the store from the snapshot (full state), then
     re-apply entries after S.
4. Snapshots are a full logical dump (data + sessions + index), in the same
   format the in-memory store uses. Replicas with different stores can
   therefore exchange snapshots via InstallSnapshot.

Why this is safe even though the LSM may lose its most recent applied
entries in a crash:

- Raft never discards log entries above its snapshot index S, and the
  snapshot covers everything at or below S. So every entry the store might
  have lost is still recoverable from either the log or the snapshot.
- Re-applying is idempotent because each entry is applied at most once per
  store state: the applied index sits in the same atomic batch as the
  effects, and the session table (exactly-once for client retries) is
  restored with the same consistency.

`Restore` builds the new state in a side directory, writes the applied
index **last**, flushes, and then swaps directories. A crash at any point
leaves either the old store or one whose applied index is 0, which simply
triggers another restore from the snapshot.

## Evidence

| Claim | Test |
|---|---|
| Store contract (atomic apply, sessions, applied index, dump/restore, persistence across reopen) | `kv/lsmstore: TestStoreContract` |
| A store left behind the Raft log is brought forward by log replay | `TestStaleStoreIsBroughtForwardFromRaftLog/log_replay`: the store is replaced by an older copy; 250 later appends must reappear |
| …and by snapshot restore when the log was compacted past it | `TestStaleStoreIsBroughtForwardFromRaftLog/snapshot_restore` |
| Linearizable under real process crashes with the LSM store | `kvchaos -store lsm`: [results/chaos-lsm-seed-7.json](results/chaos-lsm-seed-7.json), 65,170 ops, 10 SIGKILLs (6 leader-targeted), 3 pauses, linearizable, replicas converged |
| Cost | ~7 % (90/10 reads) and ~13 % (50/50) lower throughput than the in-memory store; see [PERFORMANCE.md](PERFORMANCE.md) |

## Limitations

- Snapshots are full logical dumps: O(state) to build and to ship. An
  LSM-native checkpoint (hard-linked SSTs) would make them cheap, but needs
  a checkpoint API in lsm-engine and a file-based InstallSnapshot path.
  Neither is implemented.
- A cgo call per Get/Apply. Batching several applied entries into one LSM
  write would cut this further; it is not done.
- Power loss is not simulated. SIGKILL keeps the OS page cache, so the
  chaos runs exercise the "store behind the log" path only through the
  explicit stale-copy test.
