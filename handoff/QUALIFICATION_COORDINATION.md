# Federation v1 current release checkpoint

Updated 2026-09-13T21:31Z. Active context: clean handoff + this file + live GitHub.
Historical coordination is evidence only; consult it for a specific current need.

## Current source / heads
- Main=e91e521813aff67c44bae51d00dfae212b56ef7e, PR487 merged normally.
  Tree=c61eb8e838e8c3d44628af8909da4ed8ed347acf. No override/bypass.
- Clean detached harness: C:/wsl/fcp-v1-e91e5218-main-20260913.
  C2 is NOT frozen or physically accepted. No tag/public release created.
- Old C=1aac6148759d7b2fd488ec26b97e1a786bdafa80 is immutable evidence only.
  PR487 repairs its demonstrated storage-reply ownership race. No cross-SHA PASS.
- Native main checkout prepared at C:/wsl/fcp-v1-native-main-runtime-20260913,
  still oldC/stopped. Normal update-trial target branch retained at76ad339f.

## Current green gates / release decision
- Repair: two production files, seven new regressions;77/77 related tests PASS.
- PR487 exact-tree release16/16 and ICSE4/4 PASS; immutable proofs archived.
- Actual-main qualification dispatched once. All companion workflows and ICSE PASS.
  Release34780575628: attempt1 had12PASS; Windows transport-storage failed plus
  three dependent verdicts. Both full-suite orders/all Linux shards passed.
- Windows103786726556 had6failures/14setup errors, all allocation-exhausted:
  our44GiB pressure preparation lowered C: below the unchanged64GiB reserve.
  Host-capacity interference, no demonstrated candidate defect; no source change.
  Original native log/JUnit and cause retained under diagnostics/
  main-e91e5218-qualification/windows-capacity-failure/.
- Capacity RESTORED: exact inactive images retained as sparse archives, every
  changed range verified zero, full before/after SHA+size equal;93.28GiB free.
  ARCHIVE_ONLY marker forbids pressure reuse. Originals retained, no deletion.
  Nitro partial copies retained; no complete transfer claimed. Operational Docker
  and protected data unchanged. Required recovery margin>=72GiB remains enforced.
- One failed-job+dependent recovery dispatched, run34780575628 attempt2;
  Windows103795715019 running.12successful executions retained, pool unchanged.
  POST succeeded; local success-shape assertion failed, live attempt2 confirmed
  dispatch. No duplicate request. Receipt: windows-capacity-failure/targeted-recovery.json.
- Standalone main Phase2 Windows103786775814:5s plan-publish timeout; Linux PASS.
  Hold disposition until actual-main Windows recovery and both full orders prove
  the exact test PASS. No Phase2 retry. Checked release verdict excludes Phase2;
  retain FAIL explicitly if all16mandatory jobs and3same-test proofs pass.

## Actual blockers / prerequisites
- Finish existing main recovery/source/JUnit/artifact audit; freeze actual e91.
  Checked revalidation must run unchanged. Expected unknown analysis_runtime path
  denies all12carry-forward scenarios; do all fresh, never edit map for green.
- All physical P01-P12, CF7 and B01-B09 remain incomplete as a campaign.
  P07 real1h and P12 real24h NOT STARTED. No physical PASS claim.
- Timed measurements need grouped outbox counts, actual reconcile-function time,
  and incremental file accounting: naive200k cap is insufficient for real24h.
  Passive bounded collectors being completed; product traffic/limits unchanged.
- Two reachable real CNC sources remain a separate CF7 requirement. Configured
  QuickTurn/IG500/VTC timed out; user asked once, answer pending. Timers independent.

## Active work / next action
1. Root controls: coord/.acceptance/c2-release-preparation. Complete main recovery,
   retain_main.py -> finalize_main.py -> freeze_and_revalidate.py. Then transfer
   immutable receipts with stage_nitro_freeze.py; native readiness once per host.
2. Stage/admit exact-C2 owned runtimes and activate_creator.py. Native source
   admission initializes the same-C2 campaign before P06; P01 initializer is
   checked-idempotent. P09 first-five and fresh P04/P05 can use separate fixtures.
3. Reuse native data/membership/checkpoints IN PLACE at old harness
   .acceptance/native-faults/data. No supervisor/source Agent active. P06 basic ->
   crash fence -> real branch trial/main restore, chaining latest Agent state.
   Finish shared creator clock/outbox faults before reserving it for P07/P12.
4. Existing owned creator source C:/wsl/fcp-v1-1aac6148-onboarding-runtime-20260913;
   preserve its data/coordinator volume/credentials/readonly model mounts.
   Other owned Win/Nitro workbenches remain separate; all still oldC runtime.
5. Pressure agent prepares a NEW Nitro isolated fixture after capacity disposition;
   original WSL daemons stopped/loops detached, sparse archives non-activatable.
   No pressure product/filler ran. Do not wait for44GiB archival network copies.
6. SMB transport ready:13positive checks + real64MiB negative share. Old helper
   retained stopped; new owned helper active. P11 separate C2 source/config ready
   at C:/wsl/fcp-v1-p11-e91e5218-20260913, no activation/volumes/backup yet.
   Portable dummy-only Recorder Python transfer recovering one stdin transport
   failure; exact stalled loader stopped, unrelated loader/protected data untouched.
7. P06/P09/outbox/poison executors prepared in coord/.acceptance; agents finish
   timed instrumentation and P11 controls. Root owns release mutations. Existing
   30-minute heartbeat targets this thread. Keep details outside this checkpoint.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- Only credible current-candidate contract violations block v1; preserve host noise.
- No broad sweep, blanket retry, unnecessary runner/account/label changes.
- AQG gate deliberately off, never a dependency. AQG7NCC host offline; no wait.
- Protected Recorder data untouched. No reset, operational Docker prune, volume
  deletion, Arrowhead restart or protected-data action. Supported lifecycle may
  bound only its own regenerable builder cache. Final tag must equal accepted SHA.
