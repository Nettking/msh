# Federation v1 current release checkpoint

Updated 2026-09-13T21:54Z. Active context: clean handoff + this file + live GitHub.
Historical coordination is evidence only; consult it for a specific current need.

## Current source / heads
- Main=e91e521813aff67c44bae51d00dfae212b56ef7e, PR487 merged normally.
  Tree=c61eb8e838e8c3d44628af8909da4ed8ed347acf. No override/bypass.
- Clean detached harness: C:/wsl/fcp-v1-e91e5218-main-20260913.
  C2 is FROZEN; physical acceptance incomplete. No tag/public release created.
- Old C=1aac6148759d7b2fd488ec26b97e1a786bdafa80 is immutable evidence only.
  PR487 repairs its demonstrated storage-reply ownership race. No cross-SHA PASS.
- Native main checkout prepared at C:/wsl/fcp-v1-native-main-runtime-20260913,
  now clean main C2/stopped, campaign initialized, membership preserved in place.
  Normal update-trial target branch retained at76ad339f.

## Current green gates / release decision
- Repair: two production files, seven new regressions;77/77 related tests PASS.
- PR487 exact-tree release16/16 and ICSE4/4 PASS; immutable proofs archived.
- Actual-main qualification COMPLETE: release34780575628 attempt2=16/16 PASS;
  ICSE4/4 and other companions PASS except explicit standalone Phase2 below.
  Final source/native/JUnit/publication audit PASS;38 native source proofs,
  40 logs,4535 exact test identities,4 disjoint shards equal both full orders.
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
- One failed-job+dependent recovery PASS, run34780575628 attempt2;
  Windows103795715019 PASS.12successful executions retained, pool unchanged.
  POST succeeded; local success-shape assertion failed, live attempt2 confirmed
  dispatch. No duplicate request. Receipt: windows-capacity-failure/targeted-recovery.json.
- Standalone main Phase2 Windows103786775814:5s plan-publish timeout; Linux PASS.
  FAIL retained: exact same test non-skipped PASS in recovered Windows and both
  full orders. Unresolved timing observation, no demonstrated candidate defect.
  No Phase2 retry; checked mandatory release verdict is16jobs, not legacy37.

## Actual blockers / prerequisites
- Freeze and checked revalidation DONE; unknown analysis_runtime path denies
  all12carry-forwards. ALL_SCENARIOS_FRESH_NO_CARRY_FORWARD; map unchanged.
  Exact C2 native readiness PASS on Windows and Nitro.
- All physical P01-P12, CF7 and B01-B09 remain incomplete as a campaign.
  P07 real1h and P12 real24h NOT STARTED. No physical PASS claim.
- Passive timed duration/outbox/file-ledger/history collectors prepared and
  self-checked. Review actual load/capacity before timers; traffic unchanged.
- Two reachable real CNC sources remain a separate CF7 requirement. Configured
  QuickTurn/IG500/VTC timed out; user asked once, answer pending. Timers independent.

## Active work / next action
1. Root controls: coord/.acceptance/c2-release-preparation. Creator source nowC2;
   supported start safely refused: configured host port missed by Docker publish
   filter. Existing owned stack intact. Assess current contract before recovery.
2. Nitro workbench staging stopped before source mutation: system Python lacks
   psycopg. Agent resumes reviewed partial with qualified Python, then admission.
   Agent executing separate fresh P09/P04/P05; P09 observer cost being repaired.
3. Reuse native data/membership/checkpoints IN PLACE at old harness
   .acceptance/native-faults/data. No supervisor/source Agent active. P06 basic ->
   crash fence -> real branch trial/main restore, chaining latest Agent state.
   Finish shared creator clock/outbox faults before reserving it for P07/P12.
4. Existing owned creator source C:/wsl/fcp-v1-1aac6148-onboarding-runtime-20260913;
   preserve its data/coordinator volume/credentials/readonly model mounts.
   Other owned Win/Nitro workbenches remain separate, admissions pending.
5. NEW Nitro44GiB isolated pressure fixture ready;679GiB outer free. First3s
   daemon readiness timeout retained; read-only adoption PASS, empty private
   daemon. No product/filler. Old sparse archives/partial copies retained.
6. SMB transport ready:13positive checks + real64MiB negative share. Old helper
   retained stopped; new owned helper active. P11 separate C2 source/config ready
   at C:/wsl/fcp-v1-p11-e91e5218-20260913, no activation/volumes/backup yet.
   Recorder isolated portable Python3.12 manifest/imports verified; no physical
   DPAPI/key assertion. P11 baseline awaits creator activation disposition.
7. P06/P09/outbox/poison/timed/P11 executors prepared in coord/.acceptance;
   root owns shared faults and release mutations. Existing
   30-minute heartbeat targets this thread. Keep details outside this checkpoint.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- Only credible current-candidate contract violations block v1; preserve host noise.
- No broad sweep, blanket retry, unnecessary runner/account/label changes.
- AQG gate deliberately off, never a dependency. AQG7NCC host offline; no wait.
- Protected Recorder data untouched. No reset, operational Docker prune, volume
  deletion, Arrowhead restart or protected-data action. Supported lifecycle may
  bound only its own regenerable builder cache. Final tag must equal accepted SHA.
