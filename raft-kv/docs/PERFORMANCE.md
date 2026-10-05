# Performance

Raw outputs: [results/bench/](results/bench/). Setup throughout: 3
replicas plus the load generator on one 2-vCPU cloud VM (localhost), 32
closed-loop clients (`kvctl bench -clients 32 -duration 10s`), 1,000 keys,
64-byte values, FileStorage with a real fsync per commit, snapshot
threshold 4 MiB.

The VM was re-provisioned between the original baseline
([BASELINE.md](BASELINE.md): 7,105 ops/s at 90 % reads) and these runs, and
it got slower. Only same-VM numbers are compared below.

## Current code vs baseline commit (same VM, interleaved)

`results/bench/interleaved.txt`:

| Read ratio | Baseline `6b3205c` (3 runs) | Current, memory store (3 runs) |
|---|---:|---:|
| 0.90 | 5,570 – 5,694 ops/s | 5,026 – 5,562 ops/s |
| 0.50 | 3,949 – 4,103 ops/s | 3,763 – 4,034 ops/s |

The ranges overlap at 50/50. At 90/10 the current code averages about 5 %
lower. This is consistent with the added per-request work (metrics
middleware, session handling, body limits) but it was not profiled, so it
is reported as "0–5 % lower, not attributed".

## Memory store vs LSM store

`results/bench/{memory,lsm}-{1,2,3}-*.txt`, three runs each (cluster
restarted per run):

| Read ratio | Memory | LSM (`-store lsm`) | Δ (means) |
|---|---:|---:|---:|
| 0.90 | 5,438 – 5,608 ops/s, p99 14.0–15.3 ms | 4,974 – 5,328 ops/s, p99 14.5–17.2 ms | ≈ −7 % |
| 0.50 | 3,848 – 4,040 ops/s, p99 18.6–19.6 ms | 3,352 – 3,434 ops/s, p99 22.5–24.3 ms | ≈ −13 % |

The cost grows with the write fraction. Every applied write is a cgo call
plus an LSM write batch, and every Get crosses cgo. The fsync budget is
unchanged, because the LSM writes without fsync (see
[RAFT_LSM_INTEGRATION.md](RAFT_LSM_INTEGRATION.md)). Zero client errors in
all runs.

## Chaos throughput (not a benchmark)

Under continuous SIGKILLs and pauses, `kvchaos` clients completed
56,876–76,586 operations in 30–41 s (results/chaos-*.json). That includes
failover stalls, so it is a robustness number, not a throughput claim.

## Historical figures

The README's "performance journey" table (WAL + group commit 10.3×,
batched ReadIndex 2.2×) was measured during the original development on
the earlier VM state. Those were relative improvements within one session
and were not re-measured here.

## Not measured

Multi-machine clusters, network latency, larger values or key spaces,
memory and CPU per replica, snapshot transfer time, and recovery time after
a crash.
