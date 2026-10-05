# ADR 0002: Chunked snapshots; two-phase snapshot save + log compaction

**Status:** accepted (fixes R-H2, R-M2, R-M3)

## Decision
- InstallSnapshot sends CRC-checked chunks with a resumable offset. The
  leader always sends the stored snapshot's own index/term.
- `Storage.SaveSnapshot` writes only the snapshot file, without the Raft
  lock. `CompactLog(index, retained)` then rotates to a new WAL segment
  holding the retained suffix, fsyncs it, and deletes older segments, under
  the lock.
- The WAL is segmented. A legacy single-file WAL still loads.

## Consequences
The stored log is a superset of what the snapshot doesn't cover at every
instant, so crashes anywhere are safe. Large snapshots can reach slow
followers, and a slow disk no longer blocks heartbeats. Storage interface
changed (internal API).
