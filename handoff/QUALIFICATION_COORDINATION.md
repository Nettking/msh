# Federation v1 current release checkpoint

Updated 2026-09-13T10:28Z. Active context: clean handoff + this file + live GitHub.
Historical plans are evidence only; use handoff/archive only for a current requirement.

## Current source / heads
- Actual resulting main:e6a9b74a1d555609eed6bf40c800e1258f1c9077.
- Merged in order:PR475 -> a039061e; PR483 ->02a90d3b; PR473 ->e6a9b74a.
- Actual main tree cc9b29515d47d754f24199bf403213e7ff111315 matches the reviewed
  offline combination; all three reviewed PR heads are ancestors. No deployment.
- Clean detached checkout:C:/wsl/fcp-v1-e6a9b74a-main-20260913.
- Main qualification is next. AUTHORITATIVE_SHA is not frozen yet.

## Green gates / retained evidence
- Prior combined f104038a native37/37 required plus3/3 companions PASS.
- ICSE Windows/Linux10/10, retained Compose and exact-source publication verified.
- Evidence:diagnostics/f104-final-qualification/qualification.json and archives.
- Those results retain f104 identity and are not final-main qualification.
- Source release notes are finalized; no source edit needed merely for publication.

## Actual blockers / disposition
- No demonstrated remaining candidate defect. Original Windows bootstrap/Linux
  reconnect observations remain unresolved but non-demonstrated candidate defects;
  both native recoveries passed unchanged. No common mechanism proven.
- Prior ICSE run34746641262 metadata disagreed with completed native attempt3 jobs.
  Native proof is retained; do not revisit empty attempt2 or retry passing work.
- Normal expected-head GitHub APIs accepted all merges; no policy bypass used.

## Active work / next action
1. Qualify actual main e6a9b74a once. Reuse automatic exact-source release34751832493,
   update34751832430, branding34751832432. Dispatch only ten absent workflows.
   Dispatcher/receipts:diagnostics/dispatch_actual_main_e6a9b74a.py and
   diagnostics/main-e6a9b74a-qualification-dispatch.json. Inspect existing receipt
   and live runs before any repeated request, especially after uncertain responses.
2. Retain completed native checks/artifacts on actual main; classify new failures
   before action. Never qualify intermediate main or repeat valid green results.
3. After required main qualification, freeze one authoritative SHA, run checked-in
   revalidation and scenario side-effect review, then fresh physical P01-P12.
- One active owner is this fresh task; existing30-minute heartbeat targets it.
- Merge receipts:diagnostics/release-merge-475.json,-483.json,-473.json.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- No broad diagnostic sweep, blanket retry, unnecessary runner/account/label change.
- Only credible current-candidate contract violations block v1; archive timing noise.
- AQG deliberately off and never a dependency. Protected Recorder data untouched.
- No physical PASS. P07/P12 not started; require real1h/24h and strict run-bound evidence.
- Coordination artifacts stay out of candidates. No reset/prune or protected-data actions.
