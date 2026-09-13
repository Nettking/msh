# Federation v1 current release checkpoint

Updated 2026-09-13T11:21Z. Active context: clean handoff + this file + live GitHub.
Historical plans are evidence only; use handoff/archive only for a current requirement.

## Current source / heads
- AUTHORITATIVE_SHA=e6a9b74a1d555609eed6bf40c800e1258f1c9077 (frozen).
- Actual main verified at freeze; tree cc9b29515d47d754f24199bf403213e7ff111315.
- Merged in order: PR475 -> a039061e; PR483 -> 02a90d3b; PR473 -> e6a9b74a.
- Clean detached checkout: C:/wsl/fcp-v1-e6a9b74a-main-20260913.
- Freeze receipt: diagnostics/AUTHORITATIVE_CANDIDATE-e6a9b74a.json.
- No release tag or deployment. Coordination stays outside product source.

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
- No demonstrated remaining candidate defect. Original Windows bootstrap/Linux
  reconnect observations remain unresolved but non-demonstrated candidate defects.
  Recovery and fresh actual-main ICSE passed; no common mechanism proven.
- Physical acceptance remains outstanding. No physical PASS; P07/P12 not started.

## Active work / next action
1. Complete checked-in revalidation. There are no valid prior physical PASS records
   to carry forward; missing observations require fresh candidate evidence.
2. Review physical scenario side effects and exact host/runtime bindings, then run
   fresh init/preflight/gate on required physical hosts and supported startup.
3. Execute fresh P01-P12; P07 real 1h and P12 real 24h with strict run-bound evidence.
4. Tag/publish only after complete checked-in physical acceptance on frozen SHA.
- One active owner is this fresh task; existing 30-minute heartbeat targets it.
- Merge receipts: diagnostics/release-merge-475.json, -483.json, -473.json.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- No broad diagnostic sweep, blanket retry, unnecessary runner/account/label change.
- Only credible current-candidate contract violations block v1; archive timing noise.
- AQG deliberately off and never a dependency. Protected Recorder data untouched.
- No physical PASS before real observations and strict validation complete.
- No reset/prune, volume deletion, Arrowhead restart, or protected-data actions.
