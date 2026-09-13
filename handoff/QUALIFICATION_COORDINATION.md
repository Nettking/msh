# Federation v1 current release checkpoint

Updated 2026-09-13T21:10Z. Active context: clean handoff + this file + live GitHub.
Historical coordination is evidence only; consult it for a concrete current need.

## Current source / heads
- Main=e91e521813aff67c44bae51d00dfae212b56ef7e, PR487 merged normally.
  Tree=c61eb8e838e8c3d44628af8909da4ed8ed347acf, parents oldC + repair76ad339f.
  No override/bypass. Receipt: physical-1aac6148/storage-repair-pr487-merge.json.
- New clean detached harness: C:/wsl/fcp-v1-e91e5218-main-20260913.
  This is not yet frozen or physically accepted. No tag/public release created.
- Old C=1aac6148759d7b2fd488ec26b97e1a786bdafa80 remains immutable evidence only;
  its storage-reply defect is repaired by PR487. No cross-SHA physical relabeling.
- Permanent native main checkout prepared at
  C:/wsl/fcp-v1-native-main-runtime-20260913, currently oldC; no runtime started.

## Current green gates / release decision
- PR487 head76ad339f1631e136bba7a8a85973bddb1650d570 and synthetic merge
  ec0bbd1d5ce45a97ddc10058bad38eea90f044b6 have identical reviewed trees.
- Repair: two production files, seven new regressions;77/77 related tests PASS.
  Actual authenticated successful storage reply no longer races analysis consumer.
- PR release34777368620:16/16 PASS. AQG went offline mid rotating job;
  only interrupted job and dependent verdicts recovered on unchanged pool/source.
- PR ICSE34777368640:4/4 PASS; Windows/Linux networks10/10, components4/4.
  Download action selected older failed Linux artifact by descending numeric ID.
  Exact failed ZIP preserved in Git89911820 before deleting only superseded CI copy;
  only publication reran. Verified bundle10324446083 matches all1411 source files.
- PR native proof38 checkouts +2 aggregates;39CI jobs PASS, standalone Phase2 FAIL.
  Phase2's one recovery failed in a different initial setup operation; both tests
  PASS twice in exact-source release suites. No demonstrated candidate defect;
  no third attempt. Standalone Phase2 is not a release-verdict dependency.
  Dispositions/source/archive proofs: diagnostics/physical-1aac6148/storage-repair-*.
- Actual main qualification started once; only missing workflows dispatched once.
  Snapshot: diagnostics/main-e91e5218-qualification/qualification-current.json.
  Release34780575628; ICSE34780575682. Refresh existing runs; never redispatch green.
- Main Phase2 Windows103786775814:5s failover-plan publish timeout; Linux PASS.
  Same-tree PR Windows case PASS. Await already-scheduled actual-main release case
  and full-order evidence before disposition; no Phase2 retry started.
- Old C actual-main37+3 jobs and old physical P01 10/10, P03 8/8, P04 2/6,
  P05 9/16, P10 floor/P11 prestage retained under physical-1aac6148 only.

## Actual blockers / prerequisites
- Main Windows transport-storage103786726556:6failed/14setup errors, all correct
  allocation-exhausted refusals. Our44GiB pressure-prep files lowered C: to50.9GiB
  below unchanged64GiB floor. Host interference; no candidate defect. Copying both
  unused images to Nitro, then remove verified originals; require>=72GiB free.
  Failed native log/JUnit retained under main-e91e5218/windows-capacity-failure.
  One failed-job+dependent recovery after capacity restoration; preserve green jobs.
- Actual main e91 qualification, authoritative freeze and checked-in revalidation
  still required. Expected impact-map unknown analysis_runtime denies all carry;
  run it unchanged and perform all fresh scenarios, never change map for green.
- All physical P01-P12, CF7 and B01-B09 remain incomplete as a campaign.
  P07 real1h and P12 real24h NOT STARTED. Never claim duration before completion.
- Owned aged native corpus is valid input for fresh timed measurements. Two real
  CNC endpoints are a separate CF7 requirement, not a prerequisite for P07/P12.
  QuickTurn/IG500/VTC timed out; user asked once for two reachable sources. Pending.
- Protected Recorder corpus is excluded. Newly generated dummy DPAPI material may
  use the separately available second Windows context; no protected data involved.

## Active work / next action
1. Finish actual-main CI/source/JUnit/artifact audit once; freeze e91; run checked-in
   revalidation; then activate exact-C2 runtimes and fresh physical campaign.
   Root controls: coord/.acceptance/c2-release-preparation. Nitro clean e91 source
   staged too; no runtime activation. Independent fresh P09 crash checkout prepared.
2. Existing native data/membership/checkpoints remain in place:
   C:/wsl/fcp-v1-1aac6148-main-20260913/.acceptance/native-faults.
   Supervisor/source driver stopped cleanly, no active fault. Saved signed identity
   must be reused in place. New permanent main checkout enables real update trial.
3. Existing owned creator source C:/wsl/fcp-v1-1aac6148-onboarding-runtime-20260913;
   private controls in old harness/.acceptance/onboarding-test. Preserve its owned
   coordinator volume, credentials and model mount. Other owned runtimes separate.
4. Isolated pressure fixture MIGRATING: empty private WSL daemons stopped, exact
   loops unmounted/detached; both24+20GiB files retained pending Nitro full-copy
   fsync/SHA proof. Original operational Docker unchanged. No product/filler ran.
   Native CI requires64GiB storage floor plus8GiB margin; old40GiB prep guard wrong.
   Adapt pressure controls to Nitro only after separate resource/identity checks.
5. Nitro SMB transport READY: positive13checks and empty64MiB negative share.
   New helper active; prior helper retained stopped. One first atomic WinError5
   retained; bounded unchanged follow-up PASS, no security change. No backup yet.
   P11 needs a separate legitimate C2 fixture; existing workbench lacks corpus/auth.
6. P06/P09/outbox/timed executors prepared in coord/.acceptance. Real native faults
   must precede reservation of aged runtime for P07/P12. Clock faults process-scoped
   only; no host clock change. Timed collection needs creator history/latency proof.
7. Root owns release mutations. Agents prepare bounded P02/P04/P08, P09 clock and
   P11 executors pending freeze. Existing30-minute heartbeat targets this thread.
   Keep diagnostic detail outside this checkpoint and record only meaningful changes.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- Only credible current-candidate contract violations block v1; preserve host noise.
- No broad sweep, blanket retry, unnecessary runner/account/label changes.
- AQG gate deliberately off, never a dependency. AQG7NCC host offline; no wait.
- Protected Recorder data untouched. No reset, operational Docker prune, volume
  deletion, Arrowhead restart or protected-data action. Supported lifecycle may
  bound only its own regenerable builder cache. Final tag must equal accepted SHA.
