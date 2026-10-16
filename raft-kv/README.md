# Raft KV (Go module)

This folder holds the Go module for Raft KV: a Raft-replicated, linearizable key-value store.

The full project README (architecture, quick start, results, verification, limitations) is at the repository root: **[../README.md](../README.md)**.

Quick start from this folder:

```bash
make build   # bin/kvserver, bin/kvctl, bin/kvchaos
make test    # go test -race -count=1 ./...
make cluster # local 3-node cluster on ports 7000-7002
```

Key documents: [DESIGN.md](DESIGN.md), [docs/AUDIT_2026-10-14.md](docs/AUDIT_2026-10-14.md), [docs/REQUIREMENTS_MATRIX.md](docs/REQUIREMENTS_MATRIX.md), [docs/PERFORMANCE.md](docs/PERFORMANCE.md), [docs/adr/](docs/adr/).
