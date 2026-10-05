# ADR 0004: mTLS + bearer tokens, off by default

**Status:** accepted (fixes R-H3)

## Decision
Standard-library TLS. Mutual TLS when a CA is configured, plus separate
peer and client bearer tokens (constant-time compare). Request size limits
and HTTP timeouts are always on. Security stays opt-in so the local
quick-start and the test harness remain simple, and the README states that
the defaults are only for trusted networks.

## Consequences
No new dependencies. Certificate management is the operator's job, and
there is no rotation without a restart.
