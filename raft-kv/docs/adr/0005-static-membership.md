# ADR 0005: Keep membership static for now

**Status:** accepted

## Context
The hardening plan listed joint-consensus membership changes. They touch
election, commit and snapshot logic at once, and a subtle bug there breaks
safety.

## Decision
Do not ship a partial implementation. Membership stays static and the gap
is documented. The single-server-change approach (Raft thesis §4.1) is
first on the roadmap.

## Consequences
Replacing a failed node requires a planned restart with a new peer list.
