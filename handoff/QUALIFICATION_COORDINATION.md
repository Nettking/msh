# Federation v1 current release checkpoint

Updated 2026-09-13T12:30Z. Active context: clean handoff + this file + live GitHub.
Historical plans are evidence only; use handoff/archive only for a current requirement.

## Current source / heads
- AUTHORITATIVE_SHA=e6a9b74a1d555609eed6bf40c800e1258f1c9077 (frozen).
- Actual main verified at freeze; tree cc9b29515d47d754f24199bf403213e7ff111315.
- Merged in order: PR475 -> a039061e; PR483 -> 02a90d3b; PR473 -> e6a9b74a.
- Clean detached checkout: C:/wsl/fcp-v1-e6a9b74a-main-20260913.
- Freeze receipt: diagnostics/AUTHORITATIVE_CANDIDATE-e6a9b74a.json.
- No release tag. Nettking and Nitro owned acceptance runtimes now run frozen C.
- Recorder candidate activation remains pending; protected Recorder untouched.
- PR485 acceptance-only repair: 501b528e9476878e6a6fe5cde8240b2d54b1d263.
- Runtime binding permits independently pinned tooling; product freeze stays C.
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
- Corrected Windows P01: three activations PASS, growth FAIL. Confirmed deterministic
  harness defect: data/results share C: but its 41,558,016-byte delta was summed
  twice. Images and Docker totals remained stable. Unique-volume rate 693,860,673
  B/hour is below unchanged 1 GiB/hour. No product-growth defect demonstrated.
- Repair PR485 records verified opaque volume identity, counts each volume once,
  and refuses missing/contradictory/changing surfaces. Old failures are preserved;
  no retroactive physical PASS. P01-clean-recovery-disposition.json holds proof.
- PR485 native proof: initial22 regression cases RED; Windows224 CF7 tests GREEN,
  Linux23 focused tests GREEN; Ruff/diff PASS. Required PR CI remains active.
- PR485 CI at12:30: CF7-B both OS PASS; Linux CF7, campaign tooling, update and
  ICSE native/Compose PASS. Windows checks and release jobs are queued/running.
- Docs portal34756997305 never started: account billing/spending admission failure.
  Infrastructure disposition retained; no retry, runner change or candidate defect.

## Active work / next action
1. Revalidation complete: full fresh set, zero carry-forward. Native Windows and
   Nitro init/preflight/readiness gates PASS. Nitro test TMPDIR now uses its owned
   disk-backed filesystem; correct resource refusal on tiny /tmp is resolved.
2. PR485: review live CI only on meaningful change. Native campaign-tooling workflow
   34756997294 and CF7 34756997291 are required before using repaired harness.
   Do not dispatch broad qualification or rerun C's valid gates. CI receipt:
   diagnostics/physical-e6a9b74a/growth-repair-ci-current.json.
3. New clean tooling checkouts: C:/wsl/fcp-v1-p01-filesystem-growth-20260913 and
   Nitro /home/martin/fcp-v1-501b528e-harness-20260913. Both source501b528e.
   Explicit bindings to C validated at .acceptance/runtime-control in each.
   After tooling qualification collect fresh P01 growth measurements. Initialize
   through clean runtime checkout C, write evidence outside it, pin harness501.
   Never silently upgrade existing campaign harness identity or rewrite packets.
4. Both bounded runtime recoveries completed. Nettking clean runtime:
   C:/wsl/fcp-v1-73c779-nettking-runtime-20260910; Nitro clean runtime:
   /home/martin/fcp-v1-73c779-nitro-20260910/source. All core images carry C and
   exclude .acceptance/evidence/.env. Keep harness/control files outside these.
   Original C harnesses and .acceptance/runtime-control-clean retain private
   bindings/logs. Original physical attempts remain under their evidence roots.
5. MSH Recorder read-only source check: owned voter source fba50818, task running.
   Protected installation/data untouched. Candidate staging/admission still pending.
6. Resolve pending user inputs and complete fresh P01-P12, CF7 and B01-B09.
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
