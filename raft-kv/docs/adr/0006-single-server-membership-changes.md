# ADR 0006: Single-server membership changes

**Status:** accepted (2026-10-14). Supersedes ADR 0005.

## Context
Membership was static. Replacing a failed node needed a full restart with
a new peer list, which is unsafe if done wrong and blocks operations.

## Decision
Implement single-server changes (Raft thesis §4.1–4.3), not joint consensus:

- A configuration is the full member list in a log entry (`EntryConfig`).
  Each server uses the latest configuration in its log, committed or not.
  If that entry is truncated, it falls back to the previous one.
- A change adds or removes at most one voter. The leader only proposes a
  change after it has committed an entry in its current term, and only
  when no earlier change is uncommitted (the 2015 raft-dev correction).
- New servers join as learners (`kvserver -join`); `AddServer` waits for
  catch-up in rounds (each round must finish within the minimum election
  timeout) before promoting.
- A leader that removes itself does not count its own vote and steps down
  once the change commits. Learners and removed servers never campaign.
- Vote requests that arrive within the minimum election timeout of
  hearing from a leader are ignored (§4.2.3), on top of pre-vote.
- The configuration at the snapshot index is saved with the snapshot and
  sent in `InstallSnapshot`. Old snapshot files without it still load.
- The bootstrap configuration (voters `0..n-1` from `-peers`) is implicit
  and used only until a configuration entry exists.

## Consequences
- Tests: `raft/membership_test.go` (add, remove follower and leader,
  one change at a time, revert on truncation, restart + snapshot,
  learner joins from snapshot, leader crash during change, randomised
  churn under partitions), `cmd/kvserver/admin_test.go`, and `kvchaos
  -faults membership` against real processes.
- Not done: joint consensus (changing several voters at once), leadership
  transfer (`TimeoutNow`), and automatic removal of failed servers.
- Re-adding a server under the same ID with wiped data is not supported;
  use a new ID.
