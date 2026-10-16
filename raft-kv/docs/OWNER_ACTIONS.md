# Owner actions

Things only the repository owner can do.

1. **Done (2026-10-16): the LSM integration builds from public sources.**
   Clone `siddhaantxsingh/LSM_ENGINE` next to this repo and run
   `make test-lsm && DURATION=60s make chaos-lsm` (or pass
   `LSM_CAPI=/path/to/LSM_ENGINE/capi`). Success: tests pass and `CHAOS PASS`.
   CI runs the same in the `lsm` job.
2. **Check CI.** Open the Actions tab for branch `ccr-a6e6c567-y4w7k8`
   (workflow "raft-kv CI"). Success: both jobs green, including govulncheck
   (it could not reach vuln.go.dev from the audit machine).
3. **Fix the resume.** Replace "gRPC" with "HTTP RPC", drop "C++" (or say
   "Rust via a C ABI" once item 1 is done). Supported numbers are in
   `docs/AUDIT_2026-10-14.md` ("Resume claims checked").
4. **Multi-machine numbers (optional).** Run 5 replicas on 5 VMs and
   `bin/kvctl bench` plus `kvchaos -failover-trials` style measurements
   there. Success: throughput and failover numbers measured over a real
   network.
5. **Build releases with a patched Go toolchain.** go1.24.7 has 37 reachable
   standard-library vulnerabilities (govulncheck in CI). Install the latest Go
   release and rebuild. Success: `govulncheck ./...` prints no reachable
   vulnerabilities.
6. **Merge.** Review and merge `ccr-a6e6c567-y4w7k8` into `main` yourself;
   no pull request was opened.
