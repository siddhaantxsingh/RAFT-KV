# Résumé bullets (raft-kv + lsm-engine)

> **2026-10-14 audit:** the transport is HTTP, not gRPC, and there is no
> C++ code. The LSM bullet cannot be reproduced from public sources yet
> (see docs/AUDIT_2026-10-14.md). Bullets with current evidence:
>
> - Implemented **dynamic membership** (single-server changes with learner catch-up, Raft thesis §4) and tested it under partitions, leader crashes and real-process churn.
> - Built a **fault-injecting proxy network between real server processes** (partitions, one-way links, packet loss) and checked every run's history for linearizability: 7/7 runs, up to 28k operations each.
> - Measured **5-node leader failover: max 757 ms over 45 trials** to the first committed write (2x election timeout = 600–1,200 ms), on one machine.
> - Found and fixed **two WAL durability bugs** with disk-fault injection (short writes, fsync failure, torn and reordered crashes).

All numbers are reproducible from the repositories (docs/PERFORMANCE.md,
docs/results/). Choose 3–4.

- Built a **linearizable, fault-tolerant key/value store** on a from-scratch Raft implementation in Go: pre-vote, group commit, batched ReadIndex reads, chunked/resumable snapshots, and exactly-once client sessions. **~7.2k lines incl. tests**, standard library only.
- Wrote a **real-process chaos harness** that SIGKILLs and pauses replicas (half the kills aimed at the leader) and checks every client operation with my own Wing–Gong–Lowe linearizability checker. **4 runs, up to 76k ops under 6–10 crashes each, all linearizable.**
- Audited and hardened the system: fixed **10 correctness/security bugs with failing-first regression tests**. These included silent WAL truncation of acknowledged entries, a commit index that could move backwards, and API writes over 1 MiB being silently truncated and stored.
- Added **mTLS and bearer-token authentication**, request limits, readiness checks and Prometheus histograms, tested end to end over real TLS listeners with a generated CA.
- **Integrated my Rust LSM engine as the replicated state machine** through a C ABI and cgo. The Raft log serves as the only fsynced WAL, and data, sessions and applied index are written atomically per entry. Measured cost: −7 % (read-heavy) to −13 % (50/50) throughput versus in-memory.
