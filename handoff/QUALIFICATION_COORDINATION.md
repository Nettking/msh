# Federation v1 current release checkpoint

Updated 2026-09-14T05:35Z. Active context: clean handoff + this file + live GitHub.
Historical coordination is evidence only; consult it for a specific current need.

## Current source / heads
- Frozen actual main: 0246bf8b5a2ccdba007e1baf277e5f9a06976f4d;
  tree 1e12bfc70c0bc7feecf85ed15a369e2f85bba4ce. Live refs verified 05:35Z.
- Distinct same-tree trial: 14e0193535a9fc1024bde95ae1f49b8940bfd9d9,
  branch codex/federation-v1-manifest-history-repair.
- Freeze: diagnostics/AUTHORITATIVE_CANDIDATE-0246bf8b.json, 04:08:05Z;
  SHA256 c8fd12588a527b795209b13f841d3378836f42cc5d4a429acffff74044bc5a77.
- Windows H: C:/wsl/fcp-v1-0246bf8b-main-20260914; Nitro H:
  /home/martin/fcp-v1-0246bf8b-main-20260914/source. Both clean exact source.
- Creator, native, other Windows and Nitro admitted to 0246; no replay needed.
  Native stopped after failed P06 startup; membership/data/outbox preserved.
- Checkpoint branch: codex/federation-v1-diagnostic-sweep-20260911.
  Repair d0e5b6b8993b76ccb2e49da699fb304a2684df66 on branch
  codex/federation-v1-ack-validation-repair; PR490 open, qualification active.
  PR native source148dbdb21d1f98dfb5a1db08c8e7081a299cb149, tree49c6f273.
  No full physical PASS, tag or publication.

## Current green gates
- diagnostics/main-0246bf8b-qualification/qualification.json:
  ACTUAL_MAIN_AUTOMATED_QUALIFIED, SHA256
  6c0366cd9721a02996d8930ea3ac733b3e6d4545490fa4e49baec9ac2c9e76bd.
- Release 34799746727 attempt2: 16/16 PASS; ICSE 34799746741: 4/4 PASS.
- CF7-A 34799800037/B 34799746776: 2/2 each; Phase2 34799746714: 2/2.
  Update 34799746763, branding 34799746756, registry 34799801591 PASS.
- 28 native checkout proofs + 2 aggregates; exact source, shard/order sets
  and publication source retained. Original failed attempts preserved.
- Windows native readiness PASS 04:11:15Z; Nitro PASS 04:18:21Z.
  No repeat qualification/readiness/finalizer/freeze of unchanged main.

## Actual blockers
- First real P06 startup 04:41:11-04:42:51Z failed supported-startup observation.
  Product sharing deadline 45s, observer 100s; no software POST, trial or restore.
  Eleven ACKs succeeded at about 7-8s each; initial eight-dataset pass unfinished.
  Native now 205 completed / 723 pending (928 total); honest progress retained.
  Original supervisor retry observed; clean operator stop, no live native writer.
- Current-candidate startup obstruction classified PRODUCT_DEFECT. Full-path
  baseline took76.27s for8 ACKs (CPU72.14s); repeated character validation dominates.
  Exact creator window shows stable cores, no storage rejection; late reply after
  native disconnect. Evidence: diagnostics/physical-0246bf8b/
  P06-first-startup-disposition.json; private p06-c4-first-startup-creator-window.
- Revalidation requires ALL_FRESH_NO_CARRY (safe=false, 2 unknown paths,
  all12 impacted). Fresh campaign initialized 04:22:42Z; zero physical assertions.
- P07 >=1 real hour and P12 >=24 real hours NOT STARTED; P01 hour unstarted.
  Separate real-source CF7 needs two reachable CNC sources; asked once.
- Nitro pressure preparation idle; prior memory-margin refusal is fixture
  capacity, not product OOM. Fixed floor/cap retained; no pressure activation.
- PR490 Phase2 Windows103865575559 failed; exact same-source Windows node PASS
  in release job103865575863. Inner cause unknown; no demonstrated candidate defect.
  ONE failed-only recovery accepted05:30, Linux103865575321 retained. No further retry.

## Active work / next action
1. Bounded full-ACK baseline and repaired proof DONE, no repeats. Repair changes
   only manifest._required_text to an equivalent compiled C0 predicate; all five
   validations remain. Same8-ACK cycle38.01s (CPU36.39s), all206 resulting manifest
   rows byte-identical; genuine duplicate verified.9focused tests and existing
   manifest/ACK suites PASS; independent review clear. Evidence:
   diagnostics/physical-0246bf8b/full-ack-repair-proof.json (not physical PASS).
2. Qualify exact PR490 source, then resulting main once; freeze/revalidate
   and prove fresh physical startup. No additional source change proposed.
   Do not manually drain queues, weaken deadlines or rerun unchanged qualification.
   .acceptance/ack-validation-release-preparation controls root-reviewed/enabled;
   inputs.pr.json binds clean C:/wsl/fcp-v1-148dbdb2-pr490-20260914.
   Release34808706571 active(7PASS/2running/3queued;4dependent jobs not started);
   Phase234808706541 attempt2active. ICSE34808706447, CF7-B34808706496,
   update34808706631 and branding34808706521 PASS. No merge yet.
   Require new9text tests unskipped in native fullorders/covering shards.
   Partial retention-dd144b9a:12native logs all exactsource,7artifacts; notqualified.
   CONTINUATION.md has reviewed disabled normal-merge/main qualification controls.
3. Creator admission/resident DONE: diagnostics/physical-0246bf8b/
   creator-admission-and-resident.json. All198 pre-admission revisions preserved.
4. Other Windows/Nitro admission and residency DONE; warm-transitions.json.
   Windows separate candidate-admission.verified.json preserves original refusal;
   no launcher replay. Nitro all26 raw receipts mirrored. No more C4 admissions.
5. P06 followthrough, P01/P09, P11, pressure and timed recipes remain disabled
   under .acceptance/postrepair-*-preparation. No preparation equals physical PASS.
   Any future Agent seed must follow latest honest C4 state, never reset to C3.
   .acceptance/pr490-p06-transition-preparation has disabled next transition;
   current agent captured read-only, baseline01c6b873; revalidate before quiescence.
6. P12 then P07 may overlap under retained fault observations; real minimums
   unchanged. Need final shared-runtime state, complete passive coverage and
   measured storage growth/capacity before either timer starts.
   P12 supplemental three unchanged-cap fixtures prepared, unexecuted: timed
   preparation/supplemental/test_p12_unchanged_ceilings.py, source guard14blobs.
7. Privacy-review and publish current evidence. Existing 30-minute heartbeat
   active; substantive work only on a meaningful state change or actionable step.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- Only credible current-candidate checked-contract violations block v1.
- No broad sweep, blanket retries, new D-number per symptom or runner changes.
- AQG off/nondependency. AGQ7NCC offline until tomorrow; do not wait for it.
- Protected Recorder data untouched. No reset, operational Docker prune,
  volume deletion or Arrowhead restart. Retain original evidence and partial copies.
- Windows64GiB floor plus growth margin unchanged. Real timers, one frozen source.
  Final tag only after actual physical acceptance.
