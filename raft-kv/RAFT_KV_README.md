::: {align="center"}
# Raft KV

### A fault-tolerant, linearizable distributed key-value store built from scratch in Go.

```{=html}
<p>
```
`<img src="https://img.shields.io/badge/Go-Systems%20Programming-00ADD8?style=for-the-badge&logo=go" alt="Go">`{=html}
`<img src="https://img.shields.io/badge/Consensus-Raft-orange?style=for-the-badge" alt="Raft">`{=html}
`<img src="https://img.shields.io/badge/Consistency-Linearizable-success?style=for-the-badge" alt="Linearizable">`{=html}
`<img src="https://img.shields.io/badge/Storage-LSM%20optional-purple?style=for-the-badge" alt="LSM">`{=html}
`<img src="https://img.shields.io/badge/Security-mTLS%20%2B%20Bearer%20Auth-blue?style=for-the-badge" alt="Security">`{=html}
```{=html}
</p>
```
```{=html}
<p>
```
`<strong>`{=html}Raft consensus · durable WAL · snapshots · linearizable
reads · exactly-once retries · chaos testing`</strong>`{=html}
```{=html}
</p>
```
:::

------------------------------------------------------------------------

## ⚡ What is this?

Raft KV is a distributed key-value store implemented from scratch in Go
using only the standard library for its core system.

It combines:

-   Raft consensus
-   durable write-ahead logging
-   snapshots and snapshot installation
-   linearizable reads through `ReadIndex`
-   client sessions for exactly-once retry semantics
-   an optional LSM-backed state store
-   a custom linearizability checker
-   TLS/mTLS and bearer-token authentication
-   process-level chaos testing

The goal is not to wrap an existing database.

> **The consensus, persistence, failure handling, and correctness
> machinery are the project.**

------------------------------------------------------------------------

## 🧱 Architecture

``` mermaid
flowchart LR
    C["kvctl / HTTP client"] --> L["Raft Leader"]

    subgraph Cluster["Raft Cluster"]
        L --> F1["Follower"]
        L --> F2["Follower"]
    end

    L --> S1["State Store"]
    F1 --> S2["State Store"]
    F2 --> S3["State Store"]

    S1 --> D1["WAL + Snapshot / LSM"]
    S2 --> D2["WAL + Snapshot / LSM"]
    S3 --> D3["WAL + Snapshot / LSM"]
```

### Write path

``` text
Client
  ↓
Leader
  ↓
Raft log
  ↓
WAL
  ↓
replicate to followers
  ↓
majority durable
  ↓
commit
  ↓
apply to state machine
  ↓
client response
```

### Read path

Reads do not blindly trust the leader's local state.

``` text
Read request
    ↓
ReadIndex confirmation
    ↓
wait until local state reaches read index
    ↓
read local state machine
```

This prevents a stale or isolated leader from serving an invalid
linearizable read.

------------------------------------------------------------------------

## 🧠 Raft implementation

The consensus layer includes:

-   leader election
-   re-election after failures
-   pre-vote
-   log replication
-   fast conflict backup
-   current-term commit handling
-   quorum enforcement
-   stale leader recovery
-   snapshot installation
-   persistent Raft state
-   unreliable-network test transport
-   partition/drop/delay injection

The project intentionally tests the protocol under conditions that break
naive implementations.

------------------------------------------------------------------------

## 💾 Durability

The persistence path uses a CRC-protected WAL and snapshots.

``` text
Raft entry
   ↓
WAL append
   ↓
replication
   ↓
fsync / durable majority
   ↓
commit
   ↓
state-machine apply
```

Recovery handles:

-   truncated WAL tails
-   torn writes
-   CRC failures
-   crash/restart
-   snapshots
-   snapshot corruption
-   compaction interactions

The optional LSM-backed state store connects this project to the
companion [`lsm-engine`](https://github.com/siddhaantxsingh/lsm-engine).

------------------------------------------------------------------------

## 🔗 Raft + LSM

The project can run with an LSM-backed state machine:

``` text
                  Distributed Layer
Client
  ↓
Raft leader
  ↓
Replicated log
  ↓
Committed command
  ↓
LSM-backed state machine
  ↓
WAL / MemTable / SSTables
```

This creates a full distributed-storage stack rather than a consensus
algorithm sitting on top of an in-memory map.

------------------------------------------------------------------------

## 📦 KV semantics

Supported operations include:

  Operation         Description
  ----------------- ------------------------------
  `GET`             Linearizable read
  `PUT`             Insert/replace
  `APPEND`          Append to existing value
  `DELETE`          Delete a key
  `CAS`             Compare-and-swap
  Client sessions   Exactly-once retry semantics

For retry-safe writes, clients can provide:

``` text
X-Client-ID
X-Seq
```

Requests without session metadata remain at-least-once.

Oversized keys/values are rejected with explicit errors rather than
silently truncated.

------------------------------------------------------------------------

## 🌐 HTTP API

  Method     Endpoint                Purpose
  ---------- ----------------------- ------------------
  `GET`      `/kv/{key}`             Read
  `PUT`      `/kv/{key}`             Write
  `POST`     `/kv/{key}?op=append`   Append
  `POST`     `/kv/{key}?op=cas`      Compare-and-swap
  `DELETE`   `/kv/{key}`             Delete
  `GET`      `/status`               Replica status
  `GET`      `/metrics`              Metrics
  `GET`      `/healthz`              Liveness
  `GET`      `/readyz`               Readiness

Followers return the leader identity so clients can redirect.

------------------------------------------------------------------------

## 🔐 Security

The hardened configuration includes:

-   TLS
-   mutual TLS for replica communication
-   bearer-token authentication
-   certificate validation
-   request body limits
-   key/value limits
-   timeout controls
-   corrupted-storage rejection

The repository includes an end-to-end three-replica TLS test and a
generated test PKI.

See [`docs/SECURITY.md`](docs/SECURITY.md).

------------------------------------------------------------------------

## 🧪 Correctness is a first-class feature

A distributed database that "usually works" isn't enough.

The project includes a custom **linearizability checker** and uses it
against real concurrent histories.

Test scenarios include:

-   leader elections
-   re-elections
-   partitions
-   packet loss
-   delayed messages
-   stale leaders
-   crash/restart
-   snapshots
-   snapshot + crash
-   full-cluster crashes
-   concurrent clients
-   minority-partition read attempts

### Testing the tester

The repository intentionally enables an unsafe stale-read path and
verifies that the checker rejects the resulting history.

That gives the correctness infrastructure a negative test of its own.

------------------------------------------------------------------------

## 💥 Real-process chaos

The `kvchaos` tool exercises actual replica processes rather than only
an in-memory mock network.

Recorded runs:

  ----------------------------------------------------------------------------
  Run                Duration      Operations        SIGKILLs Result
  ----------- --------------- --------------- --------------- ----------------
  Recorded         \~30--41 s      \~56k--77k           6--10 Linearizable +
  chaos runs                                                  converged

  ----------------------------------------------------------------------------

One of the recorded runs used the LSM-backed state store.

The project also documents the performance journey, including a
tail-latency issue found and fixed during optimization.

------------------------------------------------------------------------

## 📈 Performance

The project benchmarks several persistence/read paths.

### Recorded 3-replica benchmark

  Configuration                         Throughput         p50
  -------------------------------- --------------- -----------
  Whole-log persistence baseline       \~399 ops/s   \~75.5 ms
  WAL + group commit                 \~4,095 ops/s    \~7.3 ms
  Log-based reads, 99% reads         \~4,226 ops/s    \~6.9 ms
  Batched `ReadIndex`, 99% reads     \~9,250 ops/s    \~3.0 ms

The exact benchmark environment and methodology are documented in
[`docs/PERFORMANCE.md`](docs/PERFORMANCE.md).

These numbers are **single-machine measurements**, not a claim of
production capacity.

------------------------------------------------------------------------

## 🚀 Quick start

### Build and test

``` bash
make test
make build
```

### Start a local three-replica cluster

``` bash
make cluster
```

### Use the CLI

``` bash
./bin/kvctl put hello world
./bin/kvctl get hello
./bin/kvctl cas hello world raft
```

### Chaos test

``` bash
make chaos
```

### Docker

``` bash
docker compose up -d
```

### Optional LSM integration

``` bash
make build-lsm
```

This requires the companion `lsm-engine` repository and Rust toolchain.

------------------------------------------------------------------------

## 📁 Repository structure

``` text
raft/
├── consensus
├── election
├── replication
├── snapshots
├── ReadIndex
└── durable storage

kv/
├── replicated state machine
├── client sessions
├── REST API
├── Clerk
└── LSM-backed store

lincheck/
└── linearizability checker

secure/
└── TLS / mTLS / bearer auth / limits

cmd/
├── kvserver
├── kvctl
└── kvchaos

docs/
├── correctness
├── performance
├── security
├── architecture
├── ADRs
└── final audits
```

------------------------------------------------------------------------

## 📚 Documentation

  ------------------------------------------------------------------------------------------------------------------
  Topic                               Link
  ----------------------------------- ------------------------------------------------------------------------------
  Distributed-storage architecture    [`docs/DISTRIBUTED_STORAGE.md`](docs/DISTRIBUTED_STORAGE.md)

  Raft correctness                    [`docs/RAFT_CORRECTNESS.md`](docs/RAFT_CORRECTNESS.md)

  Raft + LSM integration              [`docs/RAFT_LSM_INTEGRATION.md`](docs/RAFT_LSM_INTEGRATION.md)

  Performance                         [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md)

  Security                            [`docs/SECURITY.md`](docs/SECURITY.md)

  Final audit                         [`docs/FINAL_AUDIT.md`](docs/FINAL_AUDIT.md)

  Combined architecture audit         [`docs/COMBINED_ARCHITECTURE_AUDIT.md`](docs/COMBINED_ARCHITECTURE_AUDIT.md)

  Interview guide                     [`docs/INTERVIEW.md`](docs/INTERVIEW.md)

  ADRs                                [`docs/adr/`](docs/adr/)
  ------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------

## 🚧 Honest limitations

The project deliberately does not claim:

-   dynamic cluster membership
-   production-scale multi-host benchmark capacity
-   power-loss fault injection
-   disk-fault simulation
-   backup/restore tooling
-   sharding / multiple Raft groups

The system is a **serious educational and portfolio distributed-storage
implementation**, not a replacement for etcd, CockroachDB, TiKV, or
RocksDB.

------------------------------------------------------------------------

::: {align="center"}
### Consensus is easy to describe.

### Making it survive failures while preserving linearizability is the project.
:::
