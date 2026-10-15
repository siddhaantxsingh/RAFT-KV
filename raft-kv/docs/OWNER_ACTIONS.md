# Owner actions

Things only the repository owner can do.

1. **Publish the LSM C-ABI crate.** `make test-lsm` needs `../lsm-engine` with a
   workspace member `lsm-capi` that builds `liblsm_capi.a`. Push it to
   `siddhaantxsingh/lsm-engine`, then run
   `make test-lsm && DURATION=60s make chaos-lsm`.
   Success: tests pass and `CHAOS PASS`. Until then, do not claim the Rust
   integration as reproducible.
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
5. **Merge.** Review and merge `ccr-a6e6c567-y4w7k8` into `main` yourself;
   no pull request was opened.
