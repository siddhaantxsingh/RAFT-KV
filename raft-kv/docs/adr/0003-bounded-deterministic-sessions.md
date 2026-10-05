# ADR 0003: Bounded, deterministic session table

**Status:** accepted (fixes R-M4)

## Decision
Anonymous requests keep no dedup state. The table is capped (default 100k);
past the cap the least recently written 10 % are evicted in (raft index,
client id) order, which is deterministic across replicas. A write from an
unknown session with Seq > 1 is rejected (`ErrSessionExpired`, HTTP 409)
rather than applied, since it might be a retry of an already-applied write.

## Consequences
Memory and snapshot size are bounded. A retry of a session's *first* write
after eviction is undetectable, which needs 100k newer sessions in between.
This is documented.
