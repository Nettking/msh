# D18 bounded original-host network diagnostic

DIAGNOSTIC ONLY; not qualification or physical acceptance. Original failure
is already durable as D18/#480 at exact5e, run34734857086/job103664331488.

Source review now confirms stage03 is logged only after successful authenticated
reviewer enrollment/join. The next bounded stage announces capabilities, refuses
forged ownership and discovers owners. RuntimeError is surfaced from require/RPC;
its actual error code is absent from retained stdout. Checked-in workflow uploads
network public output only after a successful command, so its failed Linux network
summary was not uploaded. This is a confirmed observability limitation; do not
invent a quorum/CSRF/timeout root cause. Original private-file availability is unknown.

Use one coordination-only job on existing Beast-Linux-WSL labels beast-linux and
fcp-linux-fast, not an AQG replacement or pool admission. Check clean exact candidate
5e6f184311019b9982e8544a18f3dc02c1b16e98 and existing Python3.12.13. Candidate source,
workflow contracts, dependencies and all internal deadlines remain unchanged.
Use the same unchanged constrained dependencies and original network-demo command.

An external coordination driver first attempts to read only the exact prior
CI-owned driver-failure.log if it survives runner temp cleanup. If present, retain
only a sanitized exception/stack classification and do not rerun the demo. If absent,
execute the original same-host network command ONCE with a new owned temporary output
path, preserving exit status. Capture the redacted public summary and a whitelist of
exception types, repo-relative stack frames and known error codes. Never upload raw
private-state, credentials, tokens, worker state databases or full exception messages.
Do not alter tested files, monkeypatch methods, weaken deadlines or add retry loops.

This is necessary failure classification for a newly failed required ICSE gate,
not a new acceptance gate, optional repetition of D14-D16, or restart of the sweep.
A passing diagnostic is only a non-reproduction, never exact-head gate replacement
or root-cause closure. Do not repeat it if it passes. New confirmed failure mechanism
must be checkpointed and issue480 updated before further investigation.

Existing CI workspace/owned loopback fixtures only; no deployment, Docker,
Recorder-data access, runner service/account/label/pool changes or physical timers.
Prior AQG diagnosis remains cancelled. Control workflow never enters a candidate.
Persist plan/driver before creating its own path-triggered control workflow. Verify
one startup snapshot, then inspect near expected completion (normally1-3minutes,
conservative next existing heartbeat); do not short-poll long F85 retry before05:14UTC.
