# ADR 0001: The Raft log is the WAL of record; the state store does not fsync

**Status:** accepted

## Context
With the LSM as the state machine store there are two WALs. Fsyncing both
doubles the fsync cost per write for no added durability.

## Decision
Apply entry i as one atomic LSM batch (data + sessions + applied index =
i) written without fsync. On start, use the newer of the store's applied
index and Raft's snapshot, and re-apply later entries from the Raft log.

## Consequences
One fsync per commit, as before. The store may lag after a crash, which
recovery handles explicitly. Verified by the stale-store tests and chaos
with `-store lsm`. Details: docs/RAFT_LSM_INTEGRATION.md.
