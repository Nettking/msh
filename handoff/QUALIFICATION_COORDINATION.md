# Federation v1 current release checkpoint

Updated 2026-09-13. Active context: clean release handoff + this checkpoint + live GitHub.
Historical coordination is archived in archive/QUALIFICATION_COORDINATION-c25045b9.md.

## Current source / heads
- Combined native qualification source: f104038a2b77705adaa547cdd1df7d895bf80a41.
- PR #483: 06b956787ae63215982d5afd7198dead366e05ea, draft/open, stacked on #475.
- PR #475: 5e6f184311019b9982e8544a18f3dc02c1b16e98, draft/open, targets main.
- PR #473: 440123f6bc6dc358eef3d233236bc14f91af60e0, draft/open, CI retirement.
- Main: b7194820d8f1940ae60b8c9639e09b7f61e65c55; no merge/deployment this handoff.
- f104 and #483 head have identical tree ff5af295717f6dc9dc90cea399e2d5901c38935d.
  Native evidence retains f104 identity; it is never relabelled as native 06b95678.

## Green gates (retain, never repeat without a source/contract reason)
- f104 release 34746641263: 16/16 PASS; native source/artifact review retained.
- f104 Phase 2 34746641323, CF7-B 34746641366, software update 34746641390: PASS.
- f104 branding 34746641274: PASS. ICSE compose: PASS on attempt 1.
- Prior #475/#473 valid qualification remains source-bound evidence; consult only
  the specific receipt needed to fill a current qualification/merge requirement.

## Actual blockers / disposition
- ICSE run 34746641262 attempt 1: Windows bootstrap QuorumUnavailable;
  Linux successor reconnect TimeoutError; dependent publication skipped.
- Both classified unresolved but non-demonstrated candidate defect. Distinct paths;
  neither a shared mechanism nor independent root causes are established.
- One bounded Linux capture 34749055328/job103702121271 completed: unchanged f104,
  original Beast Linux runner/Python, all 55 package versions match original,
  network 10/10 PASS, exit 0, owned children stopped, clean source.
- Captured frames are expected forgery and minority-refusal checks. They do not
  identify either original failing frame. Windows-specific cause remains untested.
- No product or deterministic harness defect demonstrated; no speculative repair.
  Diagnostic PASS is not qualification or physical acceptance.
- Evidence: diagnostics/icse-trace-34749055328/review.json and native-artifact.zip.

## Active work / next action
1. ICSE 34746641262 targeted failed-job recovery accepted: attempt 2 queued.
   Retain original failures and successful compose; do not repeat the trace capture.
2. Fill only seven absent exact-source workflows per diagnostics/f104-remaining-qualification.md.
   Review recovery native source/public artifacts; retain all valid green evidence.
3. Review and merge required release changes in dependency order (#475 before #483;
   separate #473 after its applicable qualification/review), without bypassing checks.
4. Qualify actual resulting main once, freeze one authoritative SHA, run checked-in
   revalidation, then fresh physical acceptance P01-P12.

## Immutable safety constraints
- No weakened assertions, quorum/authority rules, deadlines, security or acceptance.
- No broad diagnostic sweep, blanket CI retry or unnecessary runner/account/label change.
- Historical timing/CI observations block only with credible current-source evidence
  of a checked-in v1 contract violation. No new D-number for a changed symptom alone.
- AQG deliberately off; never a release dependency or requested recovery.
- Protected Recorder data untouched and out of scope. No physical PASS exists.
- P07/P12 have not started and must complete real durations before any PASS.
- Coordination/diagnostic artifacts stay out of candidate code and release merges.
