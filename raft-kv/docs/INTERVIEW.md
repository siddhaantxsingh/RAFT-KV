# Interview notes (raft-kv)

## Two-minute walkthrough
A client write reaches the leader's `Submit`. `raft.Start` appends to the
in-memory log and the WAL without fsync, the syncer fsyncs in the
background (group commit), and replicators send AppendEntries in parallel.
Followers fsync before acking. Once a majority holds the entry durably and
it is from the current term, it commits, and the applier hands it to the
KV state machine, which dedups by (client, seq) and replies. Reads use
ReadIndex: record commitIndex, confirm leadership with one heartbeat round
shared by all queued reads, wait until applied ≥ that index, then read
locally.

## Questions to expect

**How do you know it's linearizable?**
Every test that runs clients records a history (call/return times and
observed outputs) and checks it with a Wing–Gong–Lowe search, partitioned
by key. I tested the tester: an unsafe read path makes the checker fail.
The same check runs on real processes under SIGKILL/SIGSTOP in `kvchaos`.

**Why does the follower's commitIndex use max?**
A delayed AppendEntries covering a shorter prefix with a newer LeaderCommit
used to set `commitIndex = min(LeaderCommit, lastNew)` and could move it
backwards (10 → 3 in my test). Commit must be monotonic, so it only ever
advances.

**Two WALs: Raft's and the LSM's. Isn't that double fsync?**
No. The Raft log is the WAL of record. The LSM writes each applied entry as
one atomic batch including `applied = i`, without fsync. After a crash the
store holds a consistent prefix; on start I take the newer of the store and
Raft's snapshot and re-apply the rest from the log. I tested this by
replacing the store with a stale copy, both with and without log
compaction.

**What happens when a snapshot is bigger than one RPC can carry?**
It used to fail forever (600 ms deadline). Now it is chunked with a CRC
over the whole snapshot and a resumable offset. The follower verifies size
and checksum before installing, and the leader always sends the stored
snapshot's own index, never `firstIndex()`.

**Why aren't membership changes implemented?**
They change who counts toward a majority. Getting that subtly wrong breaks
safety, and I'd rather document a static cluster than ship a half-correct
joint consensus. Single-server changes are first on the roadmap.

**What does `ErrSessionExpired` protect against?**
The session table is bounded. After eviction, a retry of an old write
would look new and be applied twice. Rejecting an unknown session's
Seq > 1 makes that a visible error. The client starts a new session and
learns the outcome is ambiguous.

**What isn't tested?**
Network partitions between real processes, disk faults, power loss, and
anything multi-machine.
