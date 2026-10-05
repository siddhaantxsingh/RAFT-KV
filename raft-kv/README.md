<div align="center">
<h1>Raft KV</h1>

<h3>A fault-tolerant, linearizable distributed key-value store built from scratch in Go.</h3>

<p>
<img src="https://img.shields.io/badge/Go-00ADD8?style=for-the-badge&logo=go&logoColor=white">
<img src="https://img.shields.io/badge/Raft-Consensus-FF8C00?style=for-the-badge">
<img src="https://img.shields.io/badge/Linearizability-Verified%20by%20History-2E8B57?style=for-the-badge">
<img src="https://img.shields.io/badge/WAL-%2B%20Snapshots-6A5ACD?style=for-the-badge">
<img src="https://img.shields.io/badge/mTLS%20%2B%20Auth-Hardened-1E90FF?style=for-the-badge">
</p>
**Consensus · durable state · linearizable reads · exactly-once retries
· snapshots · chaos testing**
</div>

------------------------------------------------------------------------

## ✦ What is Raft KV?

Raft KV is a distributed key-value store whose core consensus,
persistence, correctness testing, and failure handling are implemented
from scratch in Go.

It is designed around a simple contract:

> **A successful write is replicated through Raft, committed by quorum,
> applied to the state machine, and survives the failure scenarios the
> system claims to tolerate.**

The project includes both an in-memory state store and an optional
**Rust LSM-backed store** through a C ABI.

------------------------------------------------------------------------

## 🧭 System architecture

``` mermaid
flowchart LR
    C["Client / kvctl / HTTP"] --> L["Raft Leader"]

    subgraph R["Raft Cluster"]
        L --> F1["Follower"]
        L --> F2["Follower"]
    end

    L --> SM1["State Machine"]
    F1 --> SM2["State Machine"]
    F2 --> SM3["State Machine"]

    SM1 --> S1["WAL / Snapshot / LSM"]
    SM2 --> S2["WAL / Snapshot / LSM"]
    SM3 --> S3["WAL / Snapshot / LSM"]
```

### Write path

``` mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant F1 as Follower 1
    participant F2 as Follower 2
    participant D as Durable State

    C->>L: PUT / CAS / APPEND
    L->>F1: AppendEntries
    L->>F2: AppendEntries
    F1-->>L: replicated
    F2-->>L: replicated
    L->>D: apply committed command
    L-->>C: success
```

### Linearizable read path

``` mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant Q as Quorum
    participant S as State Machine

    C->>L: GET
    L->>Q: ReadIndex confirmation
    Q-->>L: quorum confirmed
    L->>S: wait until applied >= read index
    S-->>L: value
    L-->>C: linearizable result
```

The read path avoids simply trusting a leader's local state after a
possible partition.

------------------------------------------------------------------------

## 🧠 Raft implementation

The consensus layer includes:

-   leader election
-   re-election
-   pre-vote
-   log replication
-   fast conflict backup
-   current-term commit handling
-   quorum enforcement
-   stale-leader handling
-   chunked snapshot installation
-   persistent Raft state
-   unreliable transport tests
-   invariant monitoring

The implementation is exercised with both deterministic tests and real
process failures.

------------------------------------------------------------------------

## 💾 Durability model

``` mermaid
flowchart TB
    A["Client command"] --> B["Leader Raft log"]
    B --> C["Replicate to followers"]
    C --> D{"Majority replicated"}
    D -->|No| E["Retry / wait"]
    D -->|Yes| F["Commit index advances"]
    F --> G["Apply to state machine"]
    G --> H["Client response"]
```

The storage layer protects against:

-   torn WAL tails
-   CRC failures
-   mid-WAL corruption
-   crash/restart
-   snapshot corruption
-   snapshot/close races
-   unsafe compaction ordering

A mid-WAL corruption bug that could silently truncate acknowledged
entries was found and fixed with regression tests.

------------------------------------------------------------------------

## 🔗 Raft + LSM

The optional persistent state path is:

``` mermaid
flowchart LR
    A["Client"] --> B["Raft"]
    B --> C["Committed command"]
    C --> D["LSM-backed state machine"]
    D --> E["WAL"]
    D --> F["MemTable"]
    F --> G["SSTables"]
    G --> H["Leveled compaction"]
```

The integration is intentionally explicit about its trade-off: the Raft
log is the consensus record, while the LSM is the durable state-machine
backend.

See [`docs/RAFT_LSM_INTEGRATION.md`](docs/RAFT_LSM_INTEGRATION.md).

------------------------------------------------------------------------

## 📦 KV semantics

  Operation        Semantics
  ---------------- -----------------------------
  `GET`            Linearizable read
  `PUT`            Replace / insert
  `APPEND`         Append value
  `DELETE`         Remove key
  `CAS`            Compare-and-swap
  Client session   Exactly-once retry behavior

Retry-aware clients can attach bounded session identifiers and sequence
numbers.

Anonymous requests do not silently receive fabricated session
identities.

------------------------------------------------------------------------

## 🛡️ Security

The hardened secure transport includes:

-   TLS
-   mutual TLS for replica traffic
-   bearer-token authentication
-   certificate validation
-   body limits
-   key/value limits
-   timeout controls
-   corruption rejection

The secure three-replica path is tested end to end.

Security is **opt-in** in the current architecture; ACLs and credential
rotation are not yet implemented.

------------------------------------------------------------------------

## 🧪 Correctness is part of the system

The project includes a custom **linearizability checker** and an
invariant monitor.

Test coverage includes:

-   elections
-   re-elections
-   quorum loss
-   replication
-   follower rejoin
-   log conflict recovery
-   Figure-8 style stress
-   snapshots
-   crash/restart
-   full-cluster crashes
-   concurrent clients
-   minority-partition reads
-   packet loss / delayed delivery

### Negative testing

The repository intentionally exercises unsafe read behavior and verifies
that the history checker rejects invalid executions.

The goal is not:

> "the tests passed."

The goal is:

> **the test infrastructure can detect a class of correctness violation
> when the implementation is made unsafe.**

------------------------------------------------------------------------

## 💥 Real-process chaos

``` mermaid
stateDiagram-v2
    [*] --> Healthy
    Healthy --> Partitioned: inject fault
    Partitioned --> Healthy: heal
    Healthy --> LeaderKilled: SIGKILL leader
    LeaderKilled --> Recovered: restart
    Recovered --> Healthy
    Healthy --> Snapshotting: snapshot threshold
    Snapshotting --> Healthy
```

Recorded real-process chaos:

-   memory-backed state: **3 runs**
-   30--41 seconds per run
-   6--9 SIGKILLs per run
-   linearizable histories
-   replicas converged

LSM-backed run:

-   **1 run**
-   \~41 seconds
-   **10 SIGKILLs**
-   linearizable
-   converged

Chaos throughput completed approximately **56k--77k operations per
run**. This is reported as a robustness result, not a throughput
benchmark.

------------------------------------------------------------------------

## 📈 Performance

All current benchmark numbers below come from same-VM interleaved runs
on a **3-replica, 2-vCPU machine** with 32 closed-loop clients and real
fsync per commit.

### Memory state store

  Read ratio             Throughput             p99
  ------------ -------------------- ---------------
  90% reads      5,026--5,562 ops/s   14.0--15.3 ms
  50% reads      3,763--4,034 ops/s   18.6--19.6 ms

### LSM-backed state store

  Read ratio             Throughput             p99
  ------------ -------------------- ---------------
  90% reads      4,974--5,328 ops/s   14.5--17.2 ms
  50% reads      3,352--3,434 ops/s   22.5--24.3 ms

The LSM path costs throughput because each state-machine operation
crosses the Go/C boundary and executes an LSM write batch.

Historical optimization experiments also demonstrated major relative
improvements from WAL/group commit and batched `ReadIndex`; those older
figures are preserved as development-history results rather than
presented as current-machine capacity.

------------------------------------------------------------------------

## 🔍 Observability

The server exposes:

``` text
/healthz
/readyz
/status
/metrics
```

Metrics include request and Raft-related measurements with histograms.

Structured logs and request/session metadata are used to make failures
diagnosable.

------------------------------------------------------------------------

## 🚀 Quick start

### Build

``` bash
make build
```

### Test

``` bash
go test -race ./...
```

### LSM integration tests

``` bash
go test -tags lsm -race ./kv/lsmstore ./cmd/...
```

### Local cluster

``` bash
make cluster
```

### CLI

``` bash
./bin/kvctl put hello world
./bin/kvctl get hello
./bin/kvctl cas hello world raft
```

### Chaos

``` bash
make chaos
```

### Docker

``` bash
docker compose up -d
```

------------------------------------------------------------------------

## 📁 Repository map

``` text
raft/
├── raft.go
├── storage.go
├── network.go
├── http_transport.go
└── tests

kv/
├── server.go
├── client.go
├── store.go
├── session tests
└── lsmstore/

lincheck/
└── linearizability checker

secure/
├── TLS / mTLS
├── auth
├── limits
└── secure tests

cmd/
├── kvserver
├── kvctl
└── kvchaos

docs/
├── correctness
├── performance
├── security
├── integration
├── results
└── ADRs
```

------------------------------------------------------------------------

## 📚 Documentation

  ----------------------------------------------------------------------------------------------------
  Topic                               Link
  ----------------------------------- ----------------------------------------------------------------
  Distributed storage                 [`docs/DISTRIBUTED_STORAGE.md`](docs/DISTRIBUTED_STORAGE.md)

  Raft correctness                    [`docs/RAFT_CORRECTNESS.md`](docs/RAFT_CORRECTNESS.md)

  Raft + LSM                          [`docs/RAFT_LSM_INTEGRATION.md`](docs/RAFT_LSM_INTEGRATION.md)

  Performance                         [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md)

  Security                            [`docs/SECURITY.md`](docs/SECURITY.md)

  Interview guide                     [`docs/INTERVIEW.md`](docs/INTERVIEW.md)

  Final audit                         [`docs/FINAL_AUDIT.md`](docs/FINAL_AUDIT.md)

  ADRs                                [`docs/adr/`](docs/adr/)
  ----------------------------------------------------------------------------------------------------

------------------------------------------------------------------------

## 🚧 Honest limitations

Not currently claimed:

-   dynamic cluster membership
-   backup/restore tooling
-   power-loss simulation
-   disk-fault injection
-   inter-process network partition chaos
-   sharding / multiple Raft groups
-   multi-machine throughput capacity

The project is a **serious distributed-systems implementation**, not a
replacement for etcd/CockroachDB/TiKV.

------------------------------------------------------------------------

<div align="center">
### The interesting part of Raft is not electing a leader.

### It is preserving the system's contract when the world stops cooperating.
</div>
