# Combined Architecture Audit: `lsm-engine` (Rust) and `raft-kv` (Go)

**Phase 0 deliverable.** No production code was changed to write this audit. Every source file, test, script and CI definition in both repositories was read. Suspected defects were checked with throwaway probe tests in disposable copies of the repositories (`/tmp/lsm-audit`, `/tmp/raft-audit`). They are **not** committed, and their results are quoted below. The same file lives in both repositories.

| | `lsm-engine` | `raft-kv` |
|---|---|---|
| Commit audited | `edfb4a8` | `6b3205c` |
| Language / toolchain | Rust 2024, rustc 1.97.0 | Go 1.24.7 |
| Size | ~3.5k lines incl. tests (`db.rs` 919, `version.rs` 429, `sstable.rs` 378) | ~3.6k lines incl. tests (`raft.go` 902, `kv/server.go` 389, `storage.go` 389) |
| Dependencies | `crc32fast` (+ dev: `proptest`, `tempfile`) | Go standard library only |

Severity scale: **Critical** = acknowledged data can be lost or a safety property violated under normal operation. **High** = the same under media faults, hostile input, or large state; or a crash/abort from corrupt input. **Medium** = a correctness or robustness gap with bounded impact. **Low** = quality or operability.

Evidence labels: **confirmed** (reproduced by a probe or test), **analysis** (from reading the code, not yet reproduced), **measured** (benchmark output in `docs/BASELINE.md`).

---

## 1. Current architecture of the LSM engine

```
Db (db.rs)
 ├─ State (Mutex): mem, imm, wal_number, VersionSet, visible_seq, snapshots, writer queue, bg_error
 ├─ wal: Mutex<LogWriter>               (log.rs: [crc32 u32][len u32][payload])
 ├─ cache: Arc<BlockCache>              (cache.rs: 16-shard LRU, bytes-bounded)
 ├─ 1 background thread "lsm-bg"        (flush imm > automatic compaction > manual compaction)
 └─ Stats (atomics)

MemTable (memtable.rs)   RwLock<BTreeMap<InternalKey, Vec<u8>>>
SSTable  (sstable.rs)    [data blk|crc]* [bloom|crc] [index|crc] footer(40B: 4×u64 + magic)
Block    (block.rs)      prefix-compressed entries, restart array every 16 keys
Version  (version.rs)    7 levels of Arc<FileMeta>; VersionEdit; MANIFEST; CURRENT
Compaction (compaction.rs)  LevelDB-style leveled, score-based picking, trivial moves
Iterators (iterator.rs)  MergingIterator (linear min over children), DbIterator (MVCC filter)
Keys (key.rs)            user_key ++ LE u64 trailer (seq<<8 | kind), ordered (ukey asc, trailer desc)
```

Public API (`lib.rs`): `Db::{open, put, delete, write, write_opt, get, get_opt, snapshot, iter, scan, flush, compact_all, stats, level_summary, level_bytes, write_amplification, cache_hit_rate}`, `WriteBatch`, `Snapshot`, `ReadOptions`, `Options`, `TableOptions`, `DbIterator`, `Error`. CLI: `lsmctl` (put/get/delete/scan/stats/bench/crash-writer).

## 2. Current architecture of Raft KV

```
cmd/kvserver ── http.ServeMux on ONE port:
                 /raft/{vote,append,snapshot}   gob over HTTP POST (raft/http_transport.go)
                 /kv/{key}                      REST client API (kv/http.go)
                 /status /metrics /healthz
kv.Server (kv/server.go)
 ├─ map[string]string state, lastApplied map[clientID]{seq,result} (dedup), waiters by log index
 ├─ Submit(): Get → ReadIndex fast path; writes → raft.Start + wait for apply
 └─ snapshots: gob(map + dedup + appliedIdx) when Storage.StateSize() ≥ -snapshot-bytes (4 MiB)
raft.Raft (raft/raft.go)
 ├─ pre-vote + election, per-peer replicator goroutines, fast conflict backup (XTerm/XIndex/XLen)
 ├─ leader group commit: append unsynced, syncer goroutine fsyncs, matchIndex[me] = durableIndex
 ├─ ReadIndex with batched leadership-confirmation rounds (readLoop/confirmRound)
 ├─ snapshots: Snapshot(index), InstallSnapshot (single RPC with whole snapshot)
 └─ applier goroutine (never holds rf.mu while sending on applyCh)
raft.Storage: FileStorage (hardstate + snapshot via atomic rename; wal = [len][crc32c][gob []LogEntry])
              MemoryStorage (tests; Copy() models restart)
lincheck: Wing–Gong–Lowe linearizability checker, P-compositional by key
cmd/kvctl: CLI + closed-loop load generator (p50/p95/p99)
```

Membership is **static**: peers are given by `-peers` and indexed by position.

## 3. Storage path

**LSM.** The memtable is frozen when it reaches `write_buffer_size` (4 MiB) or on `flush()`. The background thread writes it as an L0 SST (`write_level0` → `TableBuilder::finish` → `sync_all`), then appends a `VersionEdit{log_number = current WAL, added L0 file}` to the MANIFEST with fsync. It installs the new `Version` and deletes WAL files numbered below `log_number`. Compaction merges inputs into new SSTs (each fsynced), logs an edit that deletes the inputs and adds the outputs, and the inputs are unlinked when the last `Arc<FileMeta>` referencing them drops (`FileMeta::drop` + `obsolete` flag).

**Raft KV.** Raft durably stores `HardState` (term, votedFor), the log (`wal`) and the latest snapshot. Application state lives **only in memory** (`map[string]string`). It becomes durable only through Raft (log replay on restart) and snapshots (a gob of the whole map). There is no separate storage engine today, which is exactly the gap the LSM integration fills.

## 4. Write path

**LSM** (`Inner::write`, group commit):
1. A writer enqueues an `Arc<Mutex<Writer>>` under the state lock and waits until it is at the front.
2. The leader runs `make_room_for_write`: 1 ms soft slowdown once at ≥ 8 L0 files, a hard wait at ≥ 12 or while `imm` is still flushing, and a memtable switch (new WAL file) when full.
3. It merges queued batches up to 1 MiB, never absorbing a sync writer into a non-sync group. It assigns `[last_seq+1, …]` and advances `versions.last_seq`.
4. It **drops the state lock**, appends one WAL record, `flush(sync)` (`sync_data` when sync), and inserts into the captured memtable `Arc`.
5. It re-takes the lock, publishes `visible_seq` (batch-atomic visibility) and marks followers done.

**Raft KV:** `Submit` holds `s.mu` across `rf.Start` (fixes a lost-waiter race) → leader `leaderAppend` (unsynced WAL append, wakes syncer and replicators) → followers `HandleAppendEntries` append **with fsync** before acking → leader `advanceCommit` on a majority of `matchIndex` (counting itself only up to `durableIndex`) for current-term entries → applier → `kv.handleApply` dedups by `(ClientID, Seq)`, mutates the map and wakes the waiter if the term matches.

## 5. Read path

**LSM `get`:** clone `(mem, imm, current Version, seq)` under the lock, then search lock-free: memtable → immutable → L0 newest-first (range check, bloom, index seek, block via cache) → one binary-searched file per level L1–L6. The first hit wins; a tombstone means not-found.
**LSM `iter`/`scan`:** `MergingIterator` over a **full copy** of each memtable (`MemTable::iter` clones every entry), each L0 table, and one lazy `LevelIterator` per level, wrapped by `DbIterator`. `DbIterator` applies snapshot visibility, newest-version selection, tombstone hiding and an exclusive upper bound. Forward only.

**Raft KV `Get`:** `ReadIndex` (default):
1. Must have committed an entry in the current term (the no-op).
2. `readIndex = commitIndex`.
3. Confirm leadership with a majority heartbeat round. Rounds are batched across concurrent readers.
4. Wait until `appliedIdx ≥ readIndex`, then read the map.

If the term has not committed yet, it falls back to a log read. `-log-reads` forces log reads.

## 6. Recovery path

**LSM `Db::open`:**
1. Read `CURRENT`, then replay the MANIFEST (`read_log`; a torn tail ends the replay).
2. For every `*.log` with number ≥ `log_number`, in numeric order: read the whole file, replay batches into a fresh memtable, write an L0 SST, and update `last_seq`.
3. Create a new WAL, write a **new MANIFEST** snapshot, swap `CURRENT` atomically (tmp + fsync + rename + dir fsync).
4. `delete_obsolete_files`: unreferenced SSTs, old logs and old MANIFESTs.

**Raft:** `FileStorage.Load`:
1. Decode `hardstate` and `snapshot` (decode errors are fatal).
2. Scan `wal` records, stopping at the first bad record, then **truncate the file there**.
3. Rebuild the log, applying in-record truncation semantics and skipping entries ≤ snapshot index.

`raft.New` sets `commitIndex = lastApplied = snapshotIndex`. `kv.NewServer` restores the snapshot, and committed entries are re-applied as the new leader's commit index propagates.

## 7. Snapshot path

**LSM** has *MVCC snapshots* (a registered sequence number). There is no on-disk checkpoint/export, which a Raft snapshot would need.

**Raft KV:** `handleApply` → `snapshotLocked()` (gob of the full map + dedup table + appliedIdx, under `s.mu`) → `rf.Snapshot(index)`. That call trims the in-memory log and, **under `rf.mu`**, calls `storage.SaveSnapshot`: atomic write of the snapshot file, then an atomic rewrite of `wal` with the retained suffix. Followers far behind get `InstallSnapshot`: **one RPC carrying the entire snapshot**, with a timeout of `4 × RPCTimeout` (600 ms by default). It is saved under `rf.mu` and handed to the service via `pendingSnapshot`.

## 8. Concurrency model

**LSM:**
- **State lock and leader:** one state `Mutex` protects metadata. The group-commit leader is the only WAL writer, and WAL I/O happens outside the state lock.
- **Memtable:** behind an `RwLock`.
- **Readers:** hold `Arc` clones (memtable, `Version`), so reads never block on compaction.
- **Background work:** one background thread does flushes and compactions serially. Compaction I/O runs outside the lock; file numbers are allocated under it.
- **Snapshots:** a `BTreeMap<seq, refcount>`.

**Raft:**
- One `rf.mu` per node.
- Goroutines: ticker, an election per timeout, one replicator per peer (woken via `sync.Cond`), a heartbeat loop per leadership term, applier, syncer and readLoop.
- RPC handlers run under `rf.mu`. Storage writes (including fsyncs on followers and snapshot saves) happen **while holding `rf.mu`**.
- The KV layer locks `s.mu → rf.mu`, never the reverse.

## 9. Persistence model

| Artifact | Format | Integrity | Atomicity | fsync |
|---|---|---|---|---|
| LSM WAL `NNNNNN.log` | `[crc32][len u32][batch]` | CRC32 per record | record | `sync_data` per group if `sync=true`; full flush on memtable switch |
| LSM SST `NNNNNN.sst` | blocks + bloom + index + footer | CRC32 per block; **footer has no CRC** | file is referenced only after `sync_all` | `sync_all` on finish; **no directory fsync** |
| LSM MANIFEST | same log format, `VersionEdit` records | CRC32 per record | record | `sync_data` per edit |
| LSM CURRENT | text | none | tmp + rename | file + **directory fsync** |
| Raft `hardstate` | gob | **none** (atomic rename only) | tmp + rename | file + directory |
| Raft `snapshot` | gob `{Index, Term, Data}` | **none** | tmp + rename | file + directory |
| Raft `wal` | `[len][crc32c][gob []LogEntry]` | CRC32C per record | record | follower: per append; leader: syncer group commit |

## 10. Failure model (as designed)

Both systems assume crash-stop with intact storage (fail-recover). They also assume page-cache contents survive a process crash (SIGKILL); power loss and media corruption are not modelled. Raft assumes the network can drop, delay, duplicate and reorder messages and can partition; the in-memory test network injects drops, delays (0–27 ms), partitions and long disconnections. Byzantine faults, clock skew beyond election timeouts, and disk corruption beyond a torn tail are out of scope.

## 11. Existing invariants

**LSM:**
- Internal keys sort as (user key asc, seq desc).
- `visible_seq` advances only after the memtable insert, so batches are atomic to readers.
- No user key spans two files in a level ≥ 1.
- L1+ files in a level don't overlap.
- A table is fsynced before any MANIFEST edit references it.
- Inputs are deleted only after the edit that drops them is durable, and only once no `Version` references them.
- Compaction keeps the newest version visible to the oldest snapshot.
- Tombstones are dropped only at the base level and only below the oldest snapshot.
- Compaction checks iterator `status()` before installing outputs.

**Raft:**
- `currentTerm`/`votedFor` are persisted before replying.
- Followers fsync entries before acking.
- The leader counts itself only up to `durableIndex`.
- Only current-term entries are committed by counting.
- Conflicting suffixes are truncated only on an actual term conflict (stale-RPC safe).
- The applier never holds `rf.mu` while sending.
- ReadIndex requires a current-term commit and a quorum round.
- KV: at-most-once application per `(ClientID, Seq)`.

## 12. Existing guarantees (claimed in READMEs) and their evidence

| Claim | Evidence today | Assessment |
|---|---|---|
| LSM: every acknowledged `sync=true` write survives a crash | `tests/crash.rs`: 5 SIGKILL rounds, ~15k acked writes verified (see BASELINE) | Holds for **process** crash. Power loss untested (§16 L-H3). Media corruption: no (§16 L-H2) |
| LSM: batches atomic | `write_batch_is_atomic_and_ordered` (single-thread); visibility protocol by reading | Plausible; no concurrent-reader atomicity test |
| LSM: snapshots isolated across compaction | `snapshots_survive_compaction` | Yes, single snapshot |
| LSM: model-equivalent to BTreeMap | proptest, 48 cases default / 500 in CI; put/delete/get/scan/flush/compact/reopen | Yes, for those ops. No snapshots, batches, concurrency or crashes in the model |
| LSM: corruption detected | `detects_corrupted_table_on_read` (data-block bit flip) | Data blocks only. A corrupt footer **aborts the process** (§16 L-H1) |
| Raft KV: linearizable | `lincheck` on histories from 5-node clusters under unreliable network, partitions and crash/restart; checker proven to reject stale reads (`TestCheckerCatchesStaleReads`) | Strong evidence for the tested schedules |
| Raft: crash-safe persistence | MemoryStorage `Copy()` restarts; FileStorage torn-tail tests | MemoryStorage treats unsynced appends as durable, so the group-commit fsync ordering is not exercised by crash tests |
| Chaos-tested | `scripts/chaos.sh` (kills/restarts with zero client errors) | Uses `kill` = **SIGTERM (graceful)**, not SIGKILL. No linearizability check. Client retries hide failures until a 5 s timeout |

## 13. Existing tests

**LSM (21 tests):**
- 9 unit tests: coding, key ordering, log torn tail, block, bloom, cache, sstable build/read/corruption, version-edit round trip.
- 10 integration tests in `tests/db.rs`, including the proptest model.
- 1 SIGKILL crash test.
- 1 doc test (compile only).
- 1 empty test target (the `lsmctl` binary).

**Raft KV (32 tests):**
- Raft: 14 tests (elections, agreement, failures, concurrent starts, rejoin, backup, persist/crash, Figure 8 unreliable, 3 snapshot tests) plus 5 FileStorage tests.
- KV: 8 tests (basic, concurrent, unreliable, partitions, crash-restart, snapshot+partitions+crash+unreliable, no-minority-progress, checker-catches-stale-reads).
- lincheck: 5 tests.

Coverage: raft 76.2 %, kv 59.0 %, lincheck 88.3 %, cmd 0 %.

## 14. Existing benchmarks

- **LSM:** `lsmctl bench` covers fillseq, fillrandom (T threads), compact_all + write-amp, readrandom, readmissing and readseq. `--sync-only` measures group commit with T threads. It reports mean latency only (no percentiles), uses 16-byte keys and 100-byte values, and has no Zipfian or scan-range workloads.
- **Raft KV:** `kvctl bench` is a closed loop with N clients, a read ratio and a key space, against a running cluster. It reports throughput and p50/p95/p99/max. It runs on localhost only, with no injected latency.

## 15. Known limitations (documented by the projects themselves)

- **LSM** (DESIGN §9): `BTreeMap` memtable; a single background thread; no compression; one file handle per table; no range deletes, column families or prefix bloom filters; no subcompactions.
- **Raft:** static membership, whole-snapshot RPC, in-memory state machine. HTTP transport: gob and plain HTTP, no TLS or authentication.

## 16. Correctness risks (ranked)

### LSM

| ID | Severity | Evidence | Finding |
|---|---|---|---|
| **L-C1** | **Critical** | **confirmed** | **`Db::flush()` racing with a write group loses acknowledged writes permanently.** `flush()` can call `switch_memtable` from any thread while a group-commit leader has released the state lock between assigning sequence numbers and inserting into its captured memtable. The leader may write its WAL record into the *old* WAL, while the background flush snapshots the old memtable (now `imm`) *before* the leader's insert lands. The flush edit then advances `log_number` past the old WAL, which is deleted. Probe: 4 writers + a thread looping `flush()`, with a 2 ms sleep inserted between the leader's WAL write and memtable insert (a delay only; no logic change, equivalent to OS preemption). Every one of 20 rounds lost 656–2,653 of 12,000 acknowledged writes, both in memory and **after reopen**. Without the widened window, 20 rounds lost nothing, so the race is real but rare. `compact_all()` calls `flush()`, so any operator- or Raft-driven flush is exposed. Must be fixed before any integration. |
| **L-H1** | High | **confirmed** | **A corrupt SSTable footer or block handle aborts the process.** `read_block` allocates `size + 4` bytes from on-disk values without checking them against the file length. Setting the footer's filter size to 2^40 → `memory allocation of 1099511627780 bytes failed` → SIGABRT (not catchable with `catch_unwind`). The footer has no checksum. |
| **L-H2** | High | **confirmed** | **Mid-WAL corruption silently discards acknowledged writes, and can produce a non-prefix state.** `read_log` stops at the first bad record and returns the prefix with `clean = false`; `Db::open` ignores `clean`. Probe: 100 `sync=true` writes, one flipped byte mid-log → open succeeds and **50/100** acknowledged keys are gone, with no error. Because later WAL files are still replayed after a truncated earlier one, the recovered state can contain later writes without earlier ones, which is not even a point-in-time state. (RocksDB's default "point-in-time" mode also stops at the first corruption, but it stops replay of *all* later logs too and lets the operator choose stricter modes.) |
| **L-H3** | High | analysis | **No directory fsync after creating WAL and SST files.** Only the `CURRENT` swap fsyncs the directory. After power loss, a new WAL holding `sync=true`-acknowledged writes, or an SST already referenced by a durable MANIFEST edit, can lose its directory entry on filesystems that don't order metadata. SIGKILL tests cannot detect this because the page cache survives. |
| **L-H4** | High | **confirmed** | **Iterators silently skip L0 tables that fail to open.** `Db::iter` does `if let Ok(t) = f.table(..)` for L0. Probe: remove an L0 SST after open → `iter()` yields **0/100** keys and `status()` is `Ok(())`, while `get()` correctly returns an I/O error. Scans can return incomplete data as if complete. |
| L-M1 | Medium | analysis | Malformed data that passes the CRC (writer bug, or crafted file) can panic: `key::user_key` on keys shorter than 8 bytes, `coding::get_u32/u64` on short slices, `VersionEdit` with short keys reaching `smallest_user()`. `key::kind()` maps unknown kind bytes to `Put` silently. Block parse errors end iteration without setting an error status. |
| L-M2 | Medium | analysis | WAL record length is `u32`: a single batch over 4 GiB silently wraps the length → corrupt log. Block restart offsets are `u32`. There are no key/value/batch size limits. |
| L-M3 | Medium | analysis | Recovery reads each WAL file fully into memory and replays it into one unbounded memtable, regardless of `write_buffer_size`. |
| L-M4 | Medium | analysis | All write-path errors are flattened to `Error::Corruption(String)`, losing I/O error kinds. `bg_error` is sticky until reopen (acceptable, but undocumented). |
| L-M5 | Medium | analysis | `Options` are not validated (e.g. `l0_stop_trigger < l0_slowdown_trigger`, zero sizes, `level_multiplier` overflow in `max_bytes_for_level`). |
| L-L1 | Low | analysis | `delete_obsolete_logs` does directory I/O while holding the state lock. |

### Raft KV

| ID | Severity | Evidence | Finding |
|---|---|---|---|
| **R-H1** | High | **confirmed** | **Mid-WAL corruption truncates every later fsynced entry, on disk.** `Load` stops at the first bad record and `Truncate(good)`s the file. Probe: 50 fsynced entries in 5 records, one byte flipped at 1/3 → `err = nil`, **10/50** entries recovered, file truncated 3,260 → 652 bytes. A follower that acknowledged those entries (counted toward a commit quorum) silently forgets them. Combined with one more failure, a committed entry can be lost cluster-wide. The `hardstate` and `snapshot` files have no checksum. |
| **R-H2** | High | analysis | **Snapshot transfer does not scale.** `InstallSnapshot` sends the whole snapshot in one RPC with a 600 ms default deadline and no chunking, resume or checksum. A follower behind a large state can never finish catching up. The entire snapshot sits in memory on both sides. |
| **R-H3** | High | analysis | **No transport security or authentication.** Peer RPCs (`/raft/*`) and the client API share one plain-HTTP port. Anyone who can reach it can send `AppendEntries`/`InstallSnapshot` and overwrite replica state. gob decoding of request bodies has no size limit, so memory exhaustion is possible. |
| R-M1 | Medium | **confirmed** | **`commitIndex` can move backwards.** `HandleAppendEntries` sets `commitIndex = min(LeaderCommit, lastNew)` whenever `LeaderCommit > commitIndex`. A delayed RPC with a short prefix lowers it. Probe: 10 → 3. `lastApplied` stays monotonic, but the commit-index invariant and snapshot-installation decisions (`LastIncludedIndex <= commitIndex`) rely on it. |
| R-M2 | Medium | analysis | `Snapshot()` and `HandleInstallSnapshot` write and fsync the snapshot and rewrite the WAL **while holding `rf.mu`**. A large snapshot stalls heartbeats, votes and replication, which can trigger spurious elections. |
| R-M3 | Medium | analysis | `FileStorage.Sync()` captures the WAL handle under the lock but fsyncs after releasing it. A concurrent `SaveSnapshot` can close that handle → `Sync` error → `must()` panics the node. |
| R-M4 | Medium | analysis | The dedup table (`lastApplied`) grows forever. Header-less HTTP requests get a fresh random client ID each, so every such write adds a permanent entry (memory and snapshot growth; a DoS vector). |
| R-M5 | Medium | analysis | The chaos script's "kill" is SIGTERM (graceful shutdown path). Pass criterion: zero client errors. No linearizability check, no SIGKILL, no network faults. |
| R-L1 | Low | analysis | CAS uses `""` to mean "absent", so a key holding `""` cannot be distinguished from a missing one. The KV state is fully in memory. Snapshots gob-encode the entire map under `s.mu` on the apply path. |

## 17. Performance bottlenecks (analysis; to be profiled before any change)

**LSM:**
- **Scan setup cost:** `MemTable::iter()` copies the whole memtable (up to 4 MiB) for every iterator, so short scans pay O(memtable).
- **Flush starvation:** one background thread means flushes wait behind long compactions, causing write stalls at L0 limits.
- **Base-level check:** `is_base_level_for_key` scans every file in every deeper level per tombstone/key: O(files) per key.
- **Merge cost:** `MergingIterator` finds the minimum linearly, O(children) per step. `DbIterator` allocates a key and value copy per entry.
- **Cache hashing:** the block cache hashes keys with SipHash and does `BTreeMap` LRU bookkeeping under the shard mutex on every hit.
- **File handles:** one `File` per live table, never closed while the table is live.
- **No compression.**
- **Measured:** fillrandom on 2 threads is ~146–158k ops/s (BASELINE), well below fillseq (~302–325k). The README's 262k fillrandom figure was **not reproduced** in this environment.

**Raft KV:**
- **Locked I/O:** follower fsync and snapshot I/O happen under `rf.mu`.
- **Encoding cost:** gob encoding per RPC and per WAL record; HTTP/1.1 request per RPC.
- **Read latency:** every write waits for leader fsync + follower fsync + one apply; ReadIndex reads cost one heartbeat round each batch.
- **Measured** (3 nodes, localhost, 32 clients): 90/10 → 7,105 ops/s, p99 12.1 ms; 50/50 → 5,049 ops/s, p99 15.3 ms.

## 18. Security weaknesses

- **LSM** (embedded, so the threat model is a hostile or corrupt data directory): unbounded allocation from on-disk lengths (L-H1); panics on malformed CRC-valid data (L-M1); no size limits (L-M2). Paths are built from internal numbers only (no traversal from user input). `cargo audit`: 0 advisories across 41 crates.
- **Raft KV:**
  - No TLS or mTLS, no node or client authentication, no authorization (R-H3).
  - Request bodies limited to 1 MiB on the client API but **unbounded on `/raft/*`**.
  - `http.Server` sets only `ReadHeaderTimeout` (no read, write or idle timeouts) and has no connection limits.
  - The dedup-table growth DoS (R-M4).
  - `kvctl`'s default server list points at localhost.
  - No secrets exist in either repo (verified by reading every file).
  - Go has no third-party deps; `govulncheck`/`staticcheck` could not be installed (module proxy blocked; see BASELINE).

## 19. Operational weaknesses

- **LSM:**
  - No metrics export or structured logs.
  - `Stats` holds counters only, with no latency histograms.
  - No config validation.
  - No graceful close API beyond `Drop`.
  - No `repair`/`verify` tooling.
- **Raft KV:**
  - `/healthz` always returns ok (no readiness or leadership signal).
  - Metrics are minimal: no latency histograms, election count, replication lag or snapshot timings.
  - Unstructured `log` output.
  - The Dockerfile runs as non-root but has no `HEALTHCHECK`.
  - CI has no coverage, no staticcheck and no Compose validation.
  - No backup/restore procedure.
  - Static membership: replacing a node means a cluster-wide restart.

## 20. Integration strategy (Raft KV on top of the LSM engine)

Constraints: the two projects must stay independently usable (Rule 18), and the integration must go through a clean storage abstraction (Rule 19). The language boundary (Go ↔ Rust) is the first decision.

| Option | Pros | Cons |
|---|---|---|
| **A. C ABI + cgo** (`lsm-capi` cdylib/staticlib; Go `kv/lsmstore` behind a `lsm` build tag) | In-process, no extra hop; the LSM stays a Rust crate; the Go default build stays pure-Go | cgo build complexity; an unsafe FFI surface that must be fuzzed; Go GC vs Rust ownership at the boundary |
| B. LSM as a sidecar process (Unix-socket RPC) | Process isolation; no cgo | Extra hop per op; two processes to supervise; more failure modes |
| C. Port the LSM to Go | One language | Duplicates and diverges from the tested Rust engine |

**Recommendation: A**, with a pure-Go `StateMachineStore` interface in `raft-kv`, so that `MapStore` (current behaviour) and `LSMStore` (cgo) are interchangeable:

```go
type StateMachineStore interface {
    // Apply atomically applies the effects of the entry at raftIndex, together with
    // the client dedup record and the applied index, in ONE storage batch.
    Apply(raftIndex uint64, ops []Mutation, dedup DedupRecord) error
    Get(key []byte) ([]byte, bool, error)
    Scan(start, end []byte, limit int) ([]KV, error)
    AppliedIndex() (uint64, error)          // durable applied index (what survives a crash)
    Checkpoint(dir string) (CheckpointMeta, error) // consistent on-disk image at AppliedIndex
    Restore(dir string, meta CheckpointMeta) error
    Close() error
}
```

**Double-WAL design (the key decision):**
- **Raft's log is the write-ahead log of record.** An operation is durable once committed by a majority's fsynced Raft logs.
- The LSM applies entries with **`sync=false`**: its own WAL fsync would be redundant. The applied index and the dedup record are written in the **same `WriteBatch`** as the operation's effects, so "effect applied" and "applied index" can never disagree.
- On restart the state machine reads `AppliedIndex()` from the LSM and Raft re-delivers entries `> AppliedIndex`. Application must be **deterministic and idempotent per index**. The batch is atomic and keyed by index, so a replay of an index ≤ AppliedIndex is skipped.
- **Raft log truncation boundary:** Raft may discard log entries only up to the LSM's *durably flushed* applied index, not the in-memory one. Otherwise an unsynced LSM memtable lost in a crash would need entries Raft already deleted. This requires a new LSM API exposing the durable sequence (or a synchronous `flush()` that returns the persisted applied index), and the **L-C1 flush race must be fixed first**.
- **Snapshots** become LSM checkpoints: flush, then hard-link the live SSTs and MANIFEST into a checkpoint directory at a known applied index. Shipping one means a **chunked, checksummed, resumable** InstallSnapshot (fixes R-H2) and an atomic directory swap on the follower.

Details, the full crash matrix and the snapshot protocol are deferred to `docs/RAFT_LSM_INTEGRATION.md` (Phase 4).

## Recommended order of work (input to the next phases)

1. **Fix confirmed correctness defects first**, each with a regression test that fails before the fix: L-C1, L-H4, L-H1, L-H2 (configurable recovery mode, error by default on non-tail corruption), R-H1, R-M1, R-M3. Then L-H3 (directory fsyncs) with a crash-point test harness.
2. Strengthen the test infrastructure: expanded model test (snapshots, batches, concurrency, crash points), fuzz targets for all decoders, a fault-injecting filesystem shim, SIGKILL-based Raft chaos with linearizability checking.
3. Observability and configuration validation (needed to measure anything honestly).
4. Measured performance work (memtable, background pools, compression, table cache, cache policy), each change benchmarked before and after.
5. Raft hardening: chunked snapshots, snapshot I/O off the lock, TLS/mTLS + auth, request limits, dedup-table bounds.
6. The integration (C ABI, `StateMachineStore`, double-WAL protocol, checkpoint snapshots) and combined crash/linearizability tests.
7. Dynamic membership (joint consensus) **only after** the above is green. It is the riskiest change to the core.
