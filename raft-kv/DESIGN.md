# Design notes

This document explains *why* the system is built the way it is. Read it alongside the code; each section names the file and function to look at.

1. [The problem](#1-the-problem)
2. [Raft in one page](#2-raft-in-one-page)
3. [Leader election and pre-vote](#3-leader-election-and-pre-vote)
4. [Log replication and fast backup](#4-log-replication-and-fast-backup)
5. [The commit rule and the leader no-op](#5-the-commit-rule-and-the-leader-no-op)
6. [Durability: the WAL](#6-durability-the-wal)
7. [Group commit](#7-group-commit)
8. [Snapshots and log compaction](#8-snapshots-and-log-compaction)
9. [The KV state machine and exactly-once semantics](#9-the-kv-state-machine-and-exactly-once-semantics)
10. [Linearizable reads: ReadIndex](#10-linearizable-reads-readindex)
11. [Concurrency model inside a replica](#11-concurrency-model-inside-a-replica)
12. [Testing strategy and the linearizability checker](#12-testing-strategy-and-the-linearizability-checker)
13. [Performance journey](#performance-journey)
14. [Limitations and what I'd do next](#14-limitations-and-what-id-do-next)
15. [Interview questions to be ready for](#15-interview-questions-to-be-ready-for)

---

## 1. The problem

We want a key/value store that keeps working when machines crash or the network splits, and that behaves **as if it were a single machine**: once a write is acknowledged, every later read sees it (*linearizability*). Replicating data is easy; making every replica agree on the **same order** of operations despite failures is the hard part. That agreement problem is *consensus*.

**Why 3 or 5 replicas?** Raft needs a majority (quorum) to make progress. With `2f+1` replicas the system tolerates `f` failures: 3 tolerate 1, 5 tolerate 2. Any two majorities overlap in at least one node, and that overlap is what carries committed data across leader changes.

**CAP:** during a partition this system picks **consistency** (CP). The minority side refuses requests instead of serving possibly stale data. `TestKVNoMinorityProgress` checks this.

## 2. Raft in one page

Every node is a **follower**, **candidate**, or **leader**. Time is divided into **terms** (monotonically increasing integers), and each term has at most one leader.

- The leader accepts client commands, appends them to its **log**, and replicates them with `AppendEntries`.
- An entry is **committed** once it is stored on a majority. Committed entries are applied to the state machine in log order on every node.
- If followers stop hearing heartbeats, one times out, becomes a candidate, increments the term, and asks for votes (`RequestVote`).
- A node votes for at most one candidate per term, and only for a candidate whose log is **at least as up-to-date** as its own (compare last term, then last index). This **election restriction** guarantees a new leader already has every committed entry.

Key safety property (Log Matching): if two logs have an entry with the same index and term, the logs are identical up to that index. `AppendEntries` enforces it by including `(PrevLogIndex, PrevLogTerm)`, and the follower rejects the RPC if it doesn't have that entry.

Code: `raft/raft.go`. The `Raft` struct fields mirror Figure 2 of the paper.

## 3. Leader election and pre-vote

`ticker()` checks every 10 ms whether `electionDeadline` has passed. Election timeouts are **randomized** in [300, 600) ms so two followers rarely time out together (split votes). The heartbeat interval (100 ms) is well below the minimum timeout, so a healthy leader is never deposed by accident.

**Pre-vote (thesis §9.6).** Problem: a node cut off by a partition keeps timing out and incrementing its term. When it rejoins, its high term forces the healthy leader to step down, a pointless disruption. Fix: before incrementing its term, a candidate runs a **pre-vote** round asking "*would* you vote for me in term+1?" Peers say yes only if (a) they haven't heard from a leader within the minimum election timeout, and (b) the candidate's log is up to date. Pre-votes change no state. A partitioned node never gets a pre-vote majority, so it never inflates its term.

> **Bug I hit:** my first version decided "heard from leader recently" with `now < electionDeadline`. Each node's deadline is randomized independently, so node A could time out while B's deadline was still in the future. B refused A's pre-vote; then B timed out, but A had just reset its own timer and refused B. The result was a **livelock** with no leader, ever. Fix: track `lastHeartbeat` separately and compare against the fixed `ElectionTimeoutMin`. Caught by `TestReElection`.

## 4. Log replication and fast backup

Each follower has a long-lived `replicator(peer)` goroutine that sleeps on a condition variable. `Start()` and the heartbeat loop signal it. When it wakes, it sends **everything** from `nextIndex[peer]` onward (up to 512 entries). Entries that arrive while an RPC is in flight are sent together in the next one, so batching comes for free.

On rejection the leader must find where the logs diverge. Decrementing `nextIndex` by one per round trip is O(n) RTTs. **Fast backup** has the follower reply with:

- `XTerm`: the term of its conflicting entry, and `XIndex`: the first index it holds in that term
- or `XTerm = -1` and `XLen` if its log is simply too short

The leader then jumps: if it has entries in `XTerm`, it sets `nextIndex` to just after its last entry in that term; otherwise to `XIndex`. That skips a whole term per round trip. `TestBackup` builds 50-entry conflicting suffixes to exercise this.

**Truncation subtlety:** a follower truncates only on an actual term conflict, never just because an RPC has fewer entries. Otherwise a delayed, reordered old RPC could delete entries a newer RPC already appended, and those may already be counted toward a commit.

## 5. The commit rule and the leader no-op

A leader may advance `commitIndex` to `N` only if a majority has `matchIndex ≥ N` **and `log[N].term == currentTerm`** (`advanceCommit`). Counting replicas of an *old-term* entry isn't safe: Figure 8 of the paper shows such an entry can be overwritten by a future leader even after reaching a majority. It becomes committed only indirectly, when a current-term entry after it commits.

Consequence: a newly elected leader can't commit anything until it commits an entry from its own term. So `becomeLeader()` immediately appends a **no-op** entry (`Command == nil`). This also matters for ReadIndex (§10).

## 6. Durability: the WAL

Raft's safety requires `currentTerm`, `votedFor`, and the log to be on stable storage **before** answering an RPC. If a node forgot a vote after a crash, it could vote twice in one term and two leaders could be elected.

`raft/storage.go`, `FileStorage`:

| File | Contents | How it's written |
|---|---|---|
| `hardstate` | term, votedFor (tiny) | temp file → fsync → rename → fsync dir (atomic replace) |
| `wal` | append-only records `[len u32][crc32c u32][gob entries]` | append; fsync per the durability contract |
| `snapshot` | `{index, term, data}` | atomic replace |

Each WAL record is a batch of entries that *replaces the log from its first index onward*. A follower's truncation is just "append a record starting at the conflict index", and replay applies the same rule. On startup, `Load()` replays records until it hits a short read or a CRC mismatch, then **truncates the file there**. A partially written record belongs to a write that never returned, so it was never acknowledged and dropping it is safe. Tests: `TestFileStorageTornTail`, `TestFileStorageCorruptRecord`.

Why rename for atomic replace? POSIX `rename` atomically swaps the directory entry, so a crash leaves either the old file or the new one, never half of each. The directory fsync makes the rename itself durable.

## 7. Group commit

fsync costs about 1 to 10 ms on real disks, far more than anything else on the write path. Two tricks:

1. **The leader doesn't fsync inside `Start()`.** `leaderAppend` writes to the WAL (OS page cache) and wakes the `syncer` goroutine. The syncer records `target = lastIndex`, calls `fsync`, and only then advances the leader's own `matchIndex`. Every `Start()` between two fsyncs shares one flush (**group commit**).
2. **The leader's fsync runs in parallel with replication** (thesis §10.2.1). The leader's own copy counts toward the majority only once durable, so safety holds: an entry is committed only when a majority has it **on disk**.

Followers fsync before acknowledging `AppendEntries`, but a single RPC usually carries a batch, so the cost is amortized there too.

## 8. Snapshots and log compaction

Without compaction the log grows forever and restarts replay everything. When the WAL exceeds `-snapshot-bytes` (default 4 MiB), the KV server serializes its state (data map + dedup table + applied index) and calls `rf.Snapshot(index, data)`. Raft then:

- keeps `log[index]` as a **sentinel** (`log[0]` always holds the index and term of the last compacted entry, which is what `PrevLogTerm` checks need)
- writes the snapshot, then rewrites the WAL with only the retained suffix

A crash between those two writes is harmless. The old WAL entries are at or below the snapshot index and `Load()` skips them.

A follower that falls behind the leader's first log index can't be caught up with entries the leader no longer has. It gets `InstallSnapshot` instead: it replaces its state and keeps any log suffix that matches. The snapshot reaches the service through `applyCh` (`SnapshotValid`), and the applier delivers it in order with entries. Test: `TestSnapshotInstall`.

**Bug avoided:** `Snapshot()` originally checked `index <= lastApplied`, but Raft's `lastApplied` updates only after the applier sends a whole batch, so the service's snapshot requests were silently ignored. It now checks `commitIndex`; anything the service has applied is committed.

## 9. The KV state machine and exactly-once semantics

`kv/server.go`. The client calls `Submit(op)`. The server calls `rf.Start(op)`, registers a **waiter** keyed by log index, and blocks until the applier reaches that index.

- If the entry applied at that index has a **different term** than the one we got from `Start`, a new leader overwrote our slot. Return `ErrWrongLeader` and the client retries elsewhere.
- A ticker also checks that we're still leader in the same term, so a deposed leader doesn't wait forever.

**Exactly-once.** Networks lose replies. A client that times out retries, and a naïve server would apply `Append("x")` twice. Each `Clerk` has a random 63-bit `ClientID` and an increasing `Seq`. The state machine stores `lastApplied[clientID] = {seq, result}` and skips any op with `seq ≤` the stored one, returning the cached result. The dedup table lives **inside the replicated state** (and the snapshot), so every replica, including a future leader, makes the same decision.

**Bug I hit (tail latency):** `Submit` called `Start` and *then* took `s.mu` to register its waiter. On a fast path the entry could commit and apply in between, so the result went nowhere and the client waited the full 1 s retry timeout. That showed up as `max=1.03s` in benchmarks. Fix: register the waiter while holding `s.mu` across `Start`. Lock order is always `s.mu → rf.mu`, and Raft never takes `s.mu`, so there's no deadlock. Max latency dropped to ~85 ms.

## 10. Linearizable reads: ReadIndex

The simplest correct read puts `Get` in the log. That costs a log append, replication and an fsync, just to read.

Why can't the leader just read its local map? Because it might **not be the leader any more**. A partition may have elected a new leader that has accepted newer writes. `TestCheckerCatchesStaleReads` shows exactly this stale read.

**ReadIndex** (thesis §6.4), `raft.ReadIndex` + `kv.fastRead`:

1. The leader must have committed an entry in its current term (the §5 no-op). Otherwise its `commitIndex` might be behind what earlier leaders committed. Until then we fall back to log reads (`ErrNoCommitInTerm`).
2. `readIndex := commitIndex`.
3. Send a heartbeat round. If a majority still accepts us in this term, no newer leader existed at the moment we recorded `readIndex`.
4. Wait until the state machine has applied `≥ readIndex`, then read locally.

**Batching.** My first version ran one heartbeat round per read. Under 32 clients it was *slower* than log reads (2.7k vs 4.2k ops/s) because log writes batch naturally and per-read rounds didn't. Now `ReadIndex` enqueues, a `readLoop` goroutine takes the whole queue, runs **one** round, and answers everyone. This is safe because every queued read arrived before the round started. Result: 9.25k ops/s on a 99%-read workload.

**Why not leases?** Lease reads skip the heartbeat round by assuming bounded clock drift. ReadIndex needs no clock assumptions; I chose correctness without timing assumptions.

## 11. Concurrency model inside a replica

| Goroutine | Role |
|---|---|
| `ticker` | starts elections when the deadline passes |
| `heartbeatLoop` (per leadership term) | wakes replicators every 100 ms |
| `replicator` × (n−1) | one in-flight AppendEntries/InstallSnapshot per follower |
| `syncer` | group-commit fsync for the leader |
| `applier` | delivers committed entries/snapshots on `applyCh` |
| `readLoop` | batches ReadIndex confirmation rounds |
| KV `applyLoop` | applies to the map, wakes waiters, triggers snapshots |

Rules that keep this correct:

- One mutex (`rf.mu`) protects all Raft state. Simple to reason about, and contention isn't the bottleneck (fsync and network are).
- **Never hold `rf.mu` across an RPC or a channel send.** Drop the lock, do the I/O, re-acquire, then **re-check** that `currentTerm`/`role` haven't changed before using the reply. Stale replies are the main source of Raft bugs.
- The applier never holds `rf.mu` while sending on `applyCh`, so a slow state machine can't stall RPC handling (and can't deadlock with `rf.Snapshot`).
- Everything runs under `go test -race`.

## 12. Testing strategy and the linearizability checker

Distributed bugs show up under specific interleavings, so tests have to **create** bad interleavings:

- `raft/network.go`: an in-memory transport that can disconnect nodes, drop requests or replies (10% each), and delay messages. A killed node's handlers become unreachable.
- `MemoryStorage.Copy()` simulates crash + restart: the restarted node sees only durable state.
- The Raft harness checks the core safety property on every apply: **no two nodes apply different commands at the same index**, and applies happen in order.

**Linearizability checker (`lincheck/`).** Passing "get returns what I last wrote" checks per client isn't enough. Linearizability is a property of the whole concurrent history. Every KV test records each operation's call time, return time, input and output, and then asks: *is there a total order of operations consistent with real time (if A returned before B was called, A comes first) under which every output matches a sequential KV store?*

Algorithm (Wing & Gong, with Lowe's memoization):

- Put call/return events in a time-sorted doubly linked list.
- Walk the list. At a *call*, try to linearize that op now: apply it to the model state; if the output matches and `(set of linearized ops, state)` hasn't been seen before, remove the op from the list, push to a stack, and restart from the head.
- At a *return* whose op isn't linearized yet: that op had to take effect before this point and couldn't. **Backtrack** by popping the stack and trying the next candidate.
- Empty list means linearizable. Empty stack on backtrack means not linearizable.
- The memo cache (bitset + state) prunes repeated search states. The history is **partitioned by key** first (operations on different keys commute), which keeps the exponential search small.
- Timed-out operations are "unknown": they may take effect at any time after their call, or never.

The checker is itself tested (`lincheck_test.go`), including stale reads, flip-flopping reads, and two winners of the same CAS lock. `TestCheckerCatchesStaleReads` proves it catches a real bug end-to-end.

## Performance journey

Same setup throughout: 3 replicas + load generator on one 2-vCPU VM, 32 clients, 64 B values.

| Step | Change | 50% reads | 99% reads | p50 | max |
|---|---|---:|---:|---:|---:|
| 0 | persist = gob-encode **whole log** + fsync, every write | 399 | — | 75 ms | 184 ms |
| 1 | WAL with incremental appends + group commit | 3,992 | — | 7.2 ms | **1.03 s** |
| 2 | fix waiter-registration race (§9) | 4,095 | 4,226 | 7.3 ms | 85 ms |
| 3 | ReadIndex, one round per read | 3,264 | 2,718 | 11 ms | — |
| 4 | **batched** ReadIndex | 4,600 | 9,250 | 3.0 ms (99% reads) | 63 ms |

Lessons: (1) measure before optimizing, because the bottleneck was obvious only after profiling the persist path; (2) an "optimization" can make things slower if it gives up batching; (3) max latency finds bugs that averages hide.

## 14. Limitations and what I'd do next

- **Static membership.** Adding or removing replicas needs joint consensus or single-server changes (thesis ch. 4).
- **In-memory state machine.** The dataset must fit in RAM, and snapshots serialize the whole map. A real system would use an LSM tree (see my `lsm-engine` project) and incremental snapshots.
- **Single Raft group.** Throughput is bounded by one leader. Scaling out needs sharding into many groups (multi-Raft, as in CockroachDB/TiKV).
- **Snapshots are sent in one RPC**, which is fine for MBs and not for GBs; production systems chunk and stream them.
- **No auth/TLS** on the peer or client APIs.
- **Benchmarks run on one machine**, so network latency is ~0 and CPU is shared. Real cross-machine numbers would have lower throughput per client but less CPU contention.

## 15. Interview questions to be ready for

1. *Why does Raft need a majority and not, say, 2 of 5?* Two disjoint sets of 2 could each commit conflicting entries; majorities always intersect.
2. *What happens if the leader crashes after an entry is replicated to a majority but before it responds to the client?* The entry is committed. The election restriction ensures the new leader has it, and the client's retry is deduplicated via (ClientID, Seq).
3. *Why can't a leader count replicas of an old-term entry to commit it?* Figure 8: it could still be overwritten. Commit only current-term entries; older ones commit indirectly.
4. *Why the no-op on election?* To commit an entry in the current term promptly, which unblocks committing older entries and enables ReadIndex.
5. *How do you make reads linearizable without the log?* ReadIndex (§10). Explain leases and why you didn't use them.
6. *What must be fsynced, and when?* term/vote before replying to RequestVote; entries before acknowledging AppendEntries; leader entries before they count toward commit.
7. *How do you detect a torn write?* Length + CRC per record; truncate at the first bad record on recovery.
8. *What's pre-vote and why do you need it?* §3. Mention the livelock bug.
9. *How did you test it?* Fault-injecting network, crash simulation, linearizability checking, chaos script, race detector. Mention testing the checker itself.
10. *How would you scale writes?* Sharding into multiple Raft groups with a placement service.
11. *What's the difference between linearizability and serializability?* Linearizability: single-object, real-time order. Serializability: multi-object transactions, any serial order.
12. *Where is the bottleneck now?* fsync latency and the single leader. Next steps would be pipelining AppendEntries (multiple in flight per follower) and sharding.
