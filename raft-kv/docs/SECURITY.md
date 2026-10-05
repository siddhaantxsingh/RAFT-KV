# Security

## Threat model

A replica serves three surfaces on one port: peer RPCs (`/raft/*`), the
client API (`/kv/*`) and operational endpoints (`/status`, `/metrics`,
`/healthz`, `/readyz`).

| Threat | Default (no flags) | With hardening flags |
|---|---|---|
| Eavesdropping on peers/clients | **exposed** (plain HTTP) | TLS 1.2+ (`-tls-cert/-tls-key`) |
| Forged Raft RPCs (vote/append/snapshot) | **exposed**: anyone who can reach the port can disrupt or corrupt the cluster | mTLS (`-tls-ca`: peers must present a cert signed by the cluster CA) and/or a peer bearer token (`-peer-token-file`) |
| Unauthorised client reads/writes | exposed | mTLS and/or a client bearer token (`-client-token-file`), separate from the peer token |
| Oversized requests / memory exhaustion | bounded: RPC bodies capped (64 MiB, 413); keys ≤ 4 KiB (414), values ≤ 1 MiB (413, **never truncated**); header ≤ 64 KiB | same |
| Slowloris / stuck connections | read-header 5 s, read 30 s, write 30 s, idle 2 min | same |
| Session-table exhaustion | anonymous requests keep no state; table bounded at 100k sessions with deterministic eviction | same |
| Corrupted on-disk state | checksummed WAL, hardstate, snapshot; mid-log damage refuses to start | same |
| Operational endpoints leaking data | they expose no keys or values; not authenticated, so probes work (still behind mTLS when enabled) | same |

**Run the defaults only on a trusted, isolated network.** For anything else
use mTLS plus tokens:

```bash
kvserver -id 0 -peers https://n0:7000,https://n1:7000,https://n2:7000 \
  -tls-cert node.crt -tls-key node.key -tls-ca ca.crt \
  -peer-token-file peer.token -client-token-file client.token
kvctl -s https://n0:7000,... -tls-ca ca.crt -tls-cert client.crt -tls-key client.key -token-file client.token get k
```

Tokens must be ≥ 16 characters and are compared in constant time.

## Bug fixed during hardening

PUT/append/CAS bodies over 1 MiB used to be **silently truncated** by
`io.LimitReader`, and the truncated value was stored and acknowledged. They
are now rejected with 413 (`TestSecureClusterEndToEnd` checks that the
oversized value is not stored).

## Evidence

- `secure/secure_test.go`: mTLS accepts a CA-signed client; rejects a
  client with no certificate, one from a foreign CA, and a server outside
  the CA. Also covers token checks, short tokens and body limits.
- `cmd/kvserver/main_test.go`: three replicas over real mTLS listeners with
  both tokens. Checks the 401s on each surface, TLS rejection of a rogue
  certificate, 413 without truncation, and probe endpoints.

## Not done

- No authorisation beyond "has the token/cert" (no per-key ACLs, no
  per-client identities).
- No certificate rotation without a restart; no encryption at rest.
- `govulncheck`/`staticcheck` could not be installed in the authoring
  sandbox (module proxy blocked). CI runs govulncheck.
