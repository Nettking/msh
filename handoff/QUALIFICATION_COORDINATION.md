# Federation v1 current release checkpoint

Updated 2026-09-13T10:23Z. Active context: clean handoff + this file + live GitHub.
Historical coordination/plans are evidence only; see handoff/archive when needed.

## Current source / heads
- Qualified combined source: f104038a2b77705adaa547cdd1df7d895bf80a41.
- PR483 head06b956787ae63215982d5afd7198dead366e05ea, stacked on PR475.
- PR475 head5e6f184311019b9982e8544a18f3dc02c1b16e98, targets main.
- PR473 head440123f6bc6dc358eef3d233236bc14f91af60e0, CI retirement.
- Main b7194820d8f1940ae60b8c9639e09b7f61e65c55. No release merge/deployment yet.
- f104 and PR483 head share tree ff5af295717f6dc9dc90cea399e2d5901c38935d.
  Results retain actual f104 identity; no native06/5e/final-main relabelling.

## Green gates
- f104 native required qualification COMPLETE:37/37 plus3/3 companions.
- Release16/16; Phase2, CF7-A/B/C, update, branding, CF8, F8.5, sharding,
  CFI2 and registry PASS. ICSE native4/4 including retained original Compose.
- ICSE Windows/Linux network10/10 each; exact-source publication bundle verified.
- Evidence: diagnostics/f104-final-qualification/qualification.json,
  artifact-review.json, native-review.json and retained native archives.
- Preserve every valid green result; no further f104 execution is needed.

## Actual blockers / disposition
- No demonstrated remaining candidate defect. Original Windows bootstrap and Linux
  reconnect observations remain unresolved but non-demonstrated candidate defects.
  No common mechanism proven; no speculative product repair or new diagnostic.
- ICSE run34746641262 API metadata is inconsistent: attempt2 says queued/no jobs;
  latest native jobs are attempt3 and all pass. Run aggregate still says failure.
  Native logs/source/publication prove recovery completed09:55UTC. Do not poll the
  empty attempt2 endpoint or retry green jobs to change metadata. Receipt retains
  the discrepancy; normal merge API must enforce repository policy without bypass.
- Branch-rule reads403 mean unknown policy, not absence of rules. No external
  review approval is claimed. Await an actual server rejection before inferring
  a merge permission/check blocker.

## Active work / next action
1. Persist final f104 proof, update PR validation, then guarded merges:475 before483;
   retarget483 to main after475. Merge separate473 after applicable review.
2. Prepared expected combined tree cc9b29515d47d754f24199bf403213e7ff111315;
   offline merge-tree was clean. This is not an authoritative commit.
3. Qualify actual resulting main once, reusing automatic runs on that exact SHA
   and dispatching only absent gates; never qualify intermediate main.
4. Freeze one authoritative SHA, checked-in revalidation, fresh physical P01-P12.
   Preparation: diagnostics/next_release_merge_sequence.md; no physical action yet.
- One active owner is this fresh task. Existing30-minute heartbeat targets it;
  older task stopped. Keep the checkpoint compact and evidence separate.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- No broad diagnostic sweep, blanket retry, unnecessary runner/account/label change.
- Only credible current-candidate contract violations block v1; archive timing noise.
- AQG deliberately off and never a dependency. Protected Recorder data untouched.
- No physical PASS. P07/P12 not started; require real1h/24h and strict run-bound evidence.
- Coordination artifacts stay out of candidates. No reset/prune or protected-data actions.
