# Baseline (2026-10-14, before changes, commit eebaf3a)

Machine: shared Linux cloud container, 4 vCPU, 15 GiB RAM, go1.24.7. Other
agents ran builds on the same machine (load average 2–30 during the session).

| Command | Result | Log |
|---|---|---|
| `gofmt -l .` and `go vet ./...` | PASS (no output) | `baseline-lint.log` |
| `go build ./...` | PASS | (console) |
| `go test -race -count=1 -v ./...` | PASS, 55 top-level tests, 0 failures, 75 s | `baseline-test-race.log` |
| `go test -tags lsm ./kv/lsmstore` | BLOCKED: link fails, `liblsm_capi.a` not found. The published `lsm-engine` repo has no `lsm-capi` crate. `go vet -tags lsm` passes. | `baseline-lsm.log` |
| `kvchaos -duration 30s -seed 1` (3 processes, kill/pause) | PASS: 17,915 ops, 6 SIGKILLs, 4 pauses, linearizable, converged | `baseline-chaos-seed1.{log,json}` |
| staticcheck | 2 findings (unused identifiers) | `staticcheck-govulncheck.log` |
| govulncheck | BLOCKED: vuln.go.dev refused by the egress proxy | same |
| CI | No workflow file existed anywhere in the repo, so nothing ran on GitHub | — |
