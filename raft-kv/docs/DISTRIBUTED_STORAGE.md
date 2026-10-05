# The combined system and where it would go next

```
clients ─HTTPS/mTLS─▶ kvserver ×3
                         ├─ kv.Server: sessions, linearizable Get/Put/Append/Delete/CAS, ReadIndex reads
                         ├─ raft.Raft: pre-vote, group commit, chunked snapshots
                         │    └─ FileStorage: segmented, checksummed WAL + framed snapshot/hardstate
                         └─ StateStore: memory  |  lsm-engine via C ABI (-tags lsm)
```

What one write costs: one fsync on each of a majority of replicas (the
leader's runs in parallel with replication), one LSM batch without fsync,
and one round trip leader → follower.

## Honest capability statement

| | Status |
|---|---|
| Linearizable single-key operations, exactly-once retries | implemented, checked by lincheck in-process and against real processes |
| Survives minority crashes, leader crashes, process pauses | yes (chaos results) |
| Persistent state larger than RAM | with `-store lsm`, data lives in the LSM. Snapshots are still full logical dumps, held in memory while being built |
| Secure transport | mTLS + tokens available; off by default |
| Membership changes | **not implemented** (static 3- or 5-node clusters) |
| Multi-key transactions, range queries over the API | not implemented (the LSM supports scans; the API doesn't expose them) |
| Sharding / multi-Raft | not implemented |

## Scaling roadmap (in order of value)

1. **Membership changes.** Single-server changes (Raft thesis §4.1) with a
   catch-up phase for new nodes. This is a prerequisite for replacing
   failed machines without downtime.
2. **LSM-native snapshots.** Add a checkpoint API to lsm-engine (hard-link
   live SSTs + a MANIFEST copy) and stream files through InstallSnapshot.
   Snapshot cost then goes from O(data) to O(#files).
3. **Batch applies.** Apply all entries committed in one round as a single
   LSM write (one cgo call), and pipeline AppendEntries.
4. **Range API.** Expose ordered scans with ReadIndex consistency, which the
   LSM already supports.
5. **Sharding.** Split the key space into ranges, each with its own Raft
   group (multi-Raft), and add a placement/metadata service. This is the
   step from "replicated store" to "distributed database". It is large, so
   it comes last.
6. **Follower/lease reads** to scale reads, with clock-bound caveats
   documented.

## Operating it

- `/readyz` is ready when the replica knows a current leader, has heard
  from it recently, and its apply lag is under `-ready-max-lag`.
- `/metrics` exposes Raft term/role/indices, election/snapshot counters,
  session count, and per-route request counts plus latency histograms.
- On `ErrCorruptWAL` a replica refuses to start. Recover by restoring its
  directory from backup, or by wiping it and letting the leader re-send a
  snapshot (safe while a majority is healthy).
