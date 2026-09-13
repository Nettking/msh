# Federation v1 current release checkpoint

Updated 2026-09-13T13:56Z. Active context: clean handoff + this file + live GitHub.
Historical coordination is evidence only; use it for a concrete current need.

## Current source / heads
- Previous frozen C=e6a9b74a1d555609eed6bf40c800e1258f1c9077 is BLOCKED and not
  releasable: a real Windows build failure is reported as success. No replacement freeze.
- Actual main=1492d925d791b9a0ebc7bcee39ce9b3b7477254b includes merged tooling PR485.
- Product repair PR486 head=fe21bdc59bc1414fd7c4cdcf1aa4f6d6aa9048fe.
- Combined qualification source=94567b0d9ac916eec9e4f094d2f6754562b966aa, pinned at
  codex/federation-v1-qualify-94567b0d. Its tree8fd60c5e2886dfedb0248959fda6cbfc60413d2f
  equals PR486 head tree; base main1492d925. No repair merge or new deployment yet.
- Repair worktree: C:/wsl/fcp-v1-windows-build-exit-status-20260913,
  branch codex/windows-build-exit-status-20260913, clean and pushed.
- H=501b528e9476878e6a6fe5cde8240b2d54b1d263 is the independently pinned P01 harness.
- Nettking and Nitro owned runtimes still run C; Recorder owned voter remains fba50818.
- No release tag or complete physical PASS. Coordination remains outside product source.

## Current green evidence
- Prior C qualified once: 37/37 required +3/3 companions PASS, including release
  34751832493 16/16 and ICSE34751934925 Windows/Linux10/10, Compose4/4, publication.
  Retain diagnostics/main-e6a9b74a-qualification/qualification.json; do not rerun C.
- Original ICSE bootstrap/reconnect findings were non-demonstrated candidate defects.
  Their bounded capture/recovery is complete; no new ICSE diagnostic sweep.
- H shared-volume accounting repair PR485: all8 applicable workflows/31 jobs PASS,
  release34756997295 16/16. Four native logs prove merge990cb8f2 tree=H=main1492d925.
  Proof: diagnostics/physical-e6a9b74a/growth-native-proof.json and ZIP.
- PR486 focused native Windows tests20/20 PASS. Exit0 succeeds, exit7 refuses,
  stdout/stderr preserved; existing cache cleanup tested. Ruff/diff PASS.
- Real Docker red-to-green: C returns0 and writes success for a missing Dockerfile;
  repaired controller returns1/core_image_build_failed:1, writes no success marker,
  and leaves running core containers unchanged. Tested controller Git blob21b611f8.
  Proof: diagnostics/physical-e6a9b74a/windows-build-exit-defect.json and red/green logs.
- C's prior native readiness gates remain evidence, not qualification of the repair.

## Actual blockers / disposition
- Windows product defect is demonstrated on clean C and also present on main1492d925.
  Start-Process can expose null ExitCode; casting to int turns failure into zero,
  then old same-commit images satisfy identity. PR486 retains the process handle,
  waits for completion and refuses unavailable/nonzero status through existing cleanup.
  No bounds, assertions, authority or security changes.
- Repair qualification is incomplete. At13:56 all13 expected workflows exist;
  three workflows PASS, others queued/running, no failure. Await all required proof.
- Physical C work stopped. Checked-in impact plan for PR486 requires all12 CF7
  physical scenarios fresh, zero carry-forward, no unknown paths. After merge,
  qualify actual resulting main once, freeze it, revalidate, then restart acceptance.
- C Windows P01 had three activations/baseline/runtime PASS. Its hourly growth FAIL
  was confounded by CI started by the tooling merge:790,487,040 shared-volume bytes,
  with451,089,550 surviving new CI-temp bytes. Images/Docker totals stable.
  No product-growth defect demonstrated. Preserve failed packets; no limit change.
- Nitro C P01 stopped after one activation; the next build succeeded but readiness
  timed out. Its owned builder is stopped, all three C cores still run, and a later
  configured HTTP check also timed out. Physical readiness remains unresolved;
  no additional defect demonstrated. Receipt: physical-e6a9b74a/nitro-post-p01-safety.json.
- P07/P12 have not started. Owned recorder corpora are empty and aged history
  insufficient. Pending user input: two approved real MTConnect endpoints and a
  non-protected aged test corpus. Protected Recorder data remains untouched.

## Active work / next action
1. Inspect PR486 and its existing exact-source runs only on meaningful change.
   Seven workflows started automatically; the six missing gates were dispatched
   once at source94567b0d. No existing job rerun. Durable receipt:
   diagnostics/physical-e6a9b74a/windows-repair-gap-dispatch.json.
   Current status: windows-repair-qualification-current.json in that directory.
   Review helper: review-windows-repair-qualification.py. Do not dispatch duplicates.
2. Retain native source/artifact proof as these runs complete, review/merge PR486
   with expected-head guard, then qualify resulting main once and freeze replacement.
   The source tree must be finalized before acceptance; tag must equal accepted main.
3. No physical executor remains active. H control records are under each harness
   .acceptance/runtime-control; campaign evidence remains evidence/v1-physical.
   Windows H: C:/wsl/fcp-v1-p01-filesystem-growth-20260913.
   Nitro H: /home/martin/fcp-v1-501b528e-harness-20260913.
4. Keep runtime contexts clean: C:/wsl/fcp-v1-73c779-nettking-runtime-20260910 and
   /home/martin/fcp-v1-73c779-nitro-20260910/source. Harness/control/evidence/venv
   files stay outside them. Both runtimes remain C until replacement qualification.
5. Resolve remaining physical prerequisites and required P01-P12, CF7/B01-B09.
   Let CI finish before Windows growth measurement. Never rewrite old observations
   or silently replace campaign identities. Recorder candidate admission is pending.
6. P07 real1h, P12 real24h with strict single-run evidence. No tag/publication or
   physical PASS before all required observations and validation actually complete.
- One owner is this task; existing30-minute heartbeat targets it. Older task stopped.
- PR485 merge receipt: diagnostics/physical-e6a9b74a/growth-repair-merge.json.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- Only credible current-candidate contract violations block v1; preserve host noise.
- No broad sweep, blanket retry, unnecessary runner/account/label change.
- AQG deliberately off and never a dependency. Protected Recorder data untouched.
- No reset, operational Docker prune, volume deletion, Arrowhead restart or protected
  data action. Supported lifecycle may bound only its own regenerable builder cache.
