# Federation v1 current release checkpoint

Updated 2026-09-13T12:11Z. Active context: clean handoff + this file + live GitHub.
Historical plans are evidence only; use handoff/archive only for a current requirement.

## Current source / heads
- AUTHORITATIVE_SHA=e6a9b74a1d555609eed6bf40c800e1258f1c9077 (frozen).
- Actual main verified at freeze; tree cc9b29515d47d754f24199bf403213e7ff111315.
- Merged in order: PR475 -> a039061e; PR483 -> 02a90d3b; PR473 -> e6a9b74a.
- Clean detached checkout: C:/wsl/fcp-v1-e6a9b74a-main-20260913.
- Freeze receipt: diagnostics/AUTHORITATIVE_CANDIDATE-e6a9b74a.json.
- No release tag. Nettking owned acceptance runtime activated on frozen candidate.
- Nitro and Recorder candidate activation remain pending; protected Recorder untouched.
- Coordination stays outside product source.

## Current green gates
- Actual resulting main qualified once: 37/37 required + 3/3 companions PASS.
- Release 34751832493: 16/16; all other 12 workflows: 24/24.
- Phase 2, CF7 A/B/C, update, branding, CF8, F85, sharding, CFI2, registry PASS.
- ICSE 34751934925: Windows and Linux 10/10; Compose 4/4; publication verified.
- Native logs prove 38 exact checkouts; two aggregate jobs consume green results.
- All native artifacts/digests retained and reviewed, including complete 4503-test
  shard coverage, both full-suite orders and exact-source public ICSE bundle.
- Authoritative proof: diagnostics/main-e6a9b74a-qualification/qualification.json.
- Do not repeat qualification or inspect already completed greens again.

## Actual blockers / disposition
- ICSE disposed: original bootstrap/reconnect failures remain non-demonstrated
  candidate defects; unchanged recovery and fresh actual-main ICSE passed.
- No complete physical PASS. P07/P12 not started. Both owned data roots have zero
  recorder corpus and insufficient aged history. User was asked for two approved
  real MTConnect agents and non-protected aged test data; answer is pending.
- Initial Nettking P01: three activations PASS, hourly growth FAIL. Harness/control
  files entered COPY . build context; both data/results samples also count one
  Windows filesystem twice. Retained: diagnostics/physical-e6a9b74a/
  P01-growth-disposition.json. Do not change ceilings or speculate a product fix.
- Nitro initial candidate build aborted with build_cache_discard_failed. Its owned
  builder is stopped, old9b286f93 services still run, protected Recorder untouched.

## Active work / next action
1. Revalidation complete: full fresh set, zero carry-forward. Native Windows and
   Nitro init/preflight/readiness gates PASS. Nitro test TMPDIR now uses its owned
   disk-backed filesystem; correct resource refusal on tiny /tmp is resolved.
2. Separate harness from Docker runtime context (no source edit). Windows harness:
   C:/wsl/fcp-v1-e6a9b74a-main-20260913; clean runtime:
   C:/wsl/fcp-v1-73c779-nettking-runtime-20260910, now e6a9b74a. Clean images admitted
   and absence of harness inputs verified. Controls: harness/.acceptance/runtime-control-clean.
3. One targeted Windows P01 recovery is active. Inspect that control directory's
   P01-recovery-status.json; do not repeat it. Script .acceptance/p01-clean-recovery.py,
   shell session83119. Evidence root evidence/v1-physical-clean-runtime. Old failed
   physical attempt remains under evidence/v1-physical; do not relabel or overwrite.
4. Nitro corrected runtime admission accepted once. Harness:
   /home/martin/fcp-v1-e6a9b74a-nitro-20260913/source; clean runtime:
   /home/martin/fcp-v1-73c779-nitro-20260910/source, now e6a9b74a. Inspect harness
   .acceptance/runtime-control-clean/activation-status.json, then verify actual
   running images and absence of harness files. Do not rerun an active recovery.
5. MSH Recorder read-only source check: owned voter source fba50818, task running.
   Protected installation/data untouched. Candidate staging/admission still pending.
6. Resolve physical prerequisites and complete fresh P01-P12, CF7 and B01-B09.
   P07 real1h, P12 real24h with strict run-bound evidence; never use elapsed time
   alone as PASS. Tag/publish only after complete physical acceptance on frozen SHA.
- One active owner is this fresh task; existing30-minute heartbeat targets it.
- Merge receipts: diagnostics/release-merge-475.json, -483.json, -473.json.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- No broad diagnostic sweep, blanket retry, unnecessary runner/account/label change.
- Only credible current-candidate contract violations block v1; archive timing noise.
- AQG deliberately off and never a dependency. Protected Recorder data untouched.
- No physical PASS before real observations and strict validation complete.
- No reset, operational Docker prune, volume deletion, Arrowhead restart or protected
  data actions. Supported build lifecycle may bound its own regenerable builder cache.
