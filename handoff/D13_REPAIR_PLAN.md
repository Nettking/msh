# D13 isolated repair plan

Base: actual main `b7194820d8f1940ae60b8c9639e09b7f61e65c55`, verified by
`git ls-remote`. PR473 remains unchanged at `440123f6bc6dc358eef3d233236bc14f91af60e0`.
The affected responder and original test are identical between these sources.
Repair branch: `codex/fix-d13-windows-refusal-response`, separate worktree
`C:/wsl/fcp-fix-d13-windows-refusal-response-20260912`.

The unknown-path POST handler must write the existing404 response immediately,
then consume only a valid, declared request body of at most the existing4096-byte
limit before ordinary close. Bound this cleanup with one absolute deadline no
larger than the existing10-second grant budget; use partial reads and remaining
time so a trickling client cannot restart the budget. EOF, socket error and
timeout end cleanup. Invalid, negative and oversized lengths are never used as
read sizes. No identity verification, secret access or authority invocation on
this refusal path. Existing join parsing, authority rules, size limits and
response schemas remain unchanged. No retry/sleep is added to product logic.

Regression proof before draft publication:

- A valid unknown-path POST sends headers, waits for the actual404 response,
  then sends its declared body. Client must retain the complete404 response;
  this synchronization avoids depending on a lucky scheduler delay.
- Existing unknown-path and all responder tests continue to pass.
- Missing/incomplete body returns the404 before waiting and cannot hold cleanup
  beyond its absolute budget; a focused controlled-clock test checks trickle.
- Invalid, negative, zero and oversized lengths do not cause unbounded reads or
  authority access. EOF and socket errors terminate safely.
- Native Windows focused tests and available isolated Linux focused tests only;
  lint/compile/diff review. Prove the new regression fails on unmodified base.

All development executions use disposable loopback fixtures. No deployment,
production service, protected Recorder data, runner configuration, PR473 source
change or full candidate qualification. Push the repair and draft PR with exact
focused evidence, then checkpoint before further qualification decisions.
