# Federation v1 current release checkpoint

Updated 2026-09-13T14:40Z. Active context: clean handoff + this file + live GitHub.
Historical coordination is evidence only; consult it for a concrete current need.

## Current source / heads
- Actual main=1aac6148759d7b2fd488ec26b97e1a786bdafa80 after normal PR486 merge.
- Tree=8fd60c5e2886dfedb0248959fda6cbfc60413d2f equals the qualified repair tree.
- Main worktree: C:/wsl/fcp-v1-1aac6148-main-20260913, clean, detached at actual main.
- PR486 head=fe21bdc59bc1414fd7c4cdcf1aa4f6d6aa9048fe; qualified merge source
  94567b0d9ac916eec9e4f094d2f6754562b966aa. PR485 tooling merged beforehand.
- Previous frozen C=e6a9b74a1d555609eed6bf40c800e1258f1c9077 remains BLOCKED;
  it can falsely report a failed Windows build as successful. No replacement freeze.
- Nettking/Nitro owned runtimes still run C; Recorder owned voter remains fba50818.
  No new runtime activation, release tag, publication, or complete physical PASS.

## Current green evidence
- PR486 combined source945 qualified: 37/37 required +3/3 companions PASS.
  Release34760641401 16/16; ICSE34760641469 Windows/Linux10/10, Compose4/4.
  Native source proof:38 exact checkouts +2 aggregates;4528 disjoint Linux test IDs.
  Publication source equals exact Git archive; all digests and native artifacts checked.
  Proof: diagnostics/repair-94567b0d-qualification/qualification.json and archives.
- Windows repair: native20/20 tests PASS; real missing-Dockerfile red-to-green
  refuses the failure and writes no success marker, leaving core containers unchanged.
  Proof: diagnostics/physical-e6a9b74a/windows-build-exit-defect.json.
- One bounded PR486 failed-job recovery completed: Windows transport370 passed,
  1 skipped; ICSE10/10 and publication PASS. No source, assertion, or runner-label change.
  Original failures were storage-ingest timeout and reviewer-join timeout after quorum
  bootstrap PASS. No shared mechanism or deterministic candidate defect demonstrated.
  Retained originals/disposition: diagnostics/repair-94567b0d-qualification/.
- PR485 shared-volume accounting tooling qualified and merged normally.
  Earlier C gates and original ICSE capture are retained evidence; never rerun C
  or reopen that completed diagnostic sweep.

## Actual blockers / disposition
- Actual-main qualification is incomplete. At14:40 all13 workflows exist, queued
  or running, no reported failure. Seven auto-started; six gaps dispatched once.
  Receipt/state: diagnostics/main-1aac6148-qualification/{gap-dispatch,
  qualification-current}.json. Never dispatch duplicate workflows.
- Product repair requires all12 physical scenarios fresh: checked-in impact plan
  has zero carry-forward and no unknown paths. Freeze/revalidate new main first.
- No physical executor is active. C Windows three-activation/baseline/runtime checks
  passed, but growth failed with concurrent CI writes on the sampled shared volume.
  Host-write confounding is demonstrated; no product-growth defect demonstrated.
- Nitro C campaign stopped after a successful build followed by readiness timeout.
  Owned builder stopped; three C cores running; later configured HTTP check timed out.
  Physical readiness remains unresolved, not an additional demonstrated product defect.
  Receipt: diagnostics/physical-e6a9b74a/nitro-post-p01-safety.json. Do not rerun C.
- P07/P12 have not started. Owned recording corpora are empty; aged history insufficient.
  Pending user input: two approved real MTConnect endpoints and a non-protected aged
  test corpus. Protected Recorder data remains untouched.

## Active work / next action
1. Inspect existing actual-main runs on meaningful change:
   diagnostics/qualify-current-release-main.py --source 1aac6148759d7b2fd488ec26b97e1a786bdafa80
   Use no dispatch flag; the six missing gates were already accepted.
2. Retain newly completed native source/artifact proof. Helpers under
   diagnostics/repair-94567b0d-qualification/ accept --main-source <actual-main-SHA>:
   retain-current-native.py, fetch-current-artifacts.py, review-completed-artifacts.py.
   Run final artifact review only when all required actual-main jobs are green.
3. Then freeze that verified main once, run checked-in revalidation and start fresh
   physical acceptance P01-P12, CF7 and B01-B09. Tag must equal the accepted main.
   Let CI finish before Windows disk-growth measurement. Recorder admission pending.
4. Keep mutable controls/evidence/venv outside runtime build contexts:
   C:/wsl/fcp-v1-73c779-nettking-runtime-20260910 and
   /home/martin/fcp-v1-73c779-nitro-20260910/source.
   Old H=501b528e controls remain in C:/wsl/fcp-v1-p01-filesystem-growth-20260913 and
   /home/martin/fcp-v1-501b528e-harness-20260913; old evidence is never rewritten.
5. P07 real1h and P12 real24h with strict single-run evidence. No physical PASS
   or tag/publication before all required observations and validation finish.
- One owner is this task; existing30-minute heartbeat targets it. Older task stopped.
- PR486 merge receipt: diagnostics/repair-94567b0d-qualification/merge-receipt.json.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- Only credible current-candidate contract violations block v1; preserve host noise.
- No broad sweep, blanket retry, unnecessary runner/account/label changes.
- AQG deliberately off and never a dependency. Protected Recorder data untouched.
- No reset, operational Docker prune, volume deletion, Arrowhead restart or protected
  data action. Supported lifecycle may bound only its own regenerable builder cache.
