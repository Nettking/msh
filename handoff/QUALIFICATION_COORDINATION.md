# Federation v1 current release checkpoint

Updated 2026-09-13T19:38Z. Active context: clean handoff + this file + live GitHub.
Historical coordination is evidence only; consult it for a concrete current need.

## Current source / heads
- Recorded C=1aac6148759d7b2fd488ec26b97e1a786bdafa80; live main verified unchanged
  this continuation. Tree=8fd60c5e2886dfedb0248959fda6cbfc60413d2f. PR485 then PR486 merged.
- Freeze: diagnostics/AUTHORITATIVE_CANDIDATE-1aac6148.json. No tag/public release
  or complete physical PASS. C now has a demonstrated defect; campaign paused for repair.
- Windows harness C:/wsl/fcp-v1-1aac6148-main-20260913; Nitro harness
  /home/martin/fcp-v1-1aac6148-main-20260913/source. Both detached C.
- Existing owned runtimes installed C on both hosts. Separate Windows onboarding
  and native-fault runtimes are clean detached C; mutable controls/data are outside.
  Recorder owned voter remains fba50818; protected corpus is outside this campaign.

## Current green gates
- Actual resulting main qualified once: 37/37 required +3/3 companion jobs PASS,
  all first attempts, all13 workflows complete. Do not dispatch or retry green CI.
- Release34763230099:16/16 PASS. ICSE34763230042: Windows/Linux10/10,
  Compose4/4 and publication PASS. Native38 checkouts +2 aggregates verified;
  4528 disjoint Linux test IDs and all native/JUnit/artifact digests validated.
- Publication10318929498 exports exactly git archive C. Immutable qualification:
  diagnostics/main-1aac6148-qualification/qualification.json and archives.
- Checked-in revalidation from e6a9b74a PASS: all12 scenarios fresh, zero carry-forward
  or unknown paths. Native preflight +4/4 readiness PASS on each host; venvs reused.
- Old C physical only: P01 10/10, P03 8/8, P04 2/6, P05 9/16;
  P10 emergency-floor and P11 helper-prestaged PASS. Original failures retained.
  Proofs under physical-1aac6148; no cross-SHA physical reuse or relabeling.
  Native input/supervisor stopped cleanly. No P06 PASS or active fault executor.
- Original ICSE trace and bounded PR486 recovery dispositioned; no demonstrated
  shared/deterministic candidate mechanism. Do not reopen the broad diagnostic sweep.

## Actual blockers / disposition
- Demonstrated C product defect: creator analysis can retain the original AI message
  queue while storage is inserted behind it. Two consumers then race for storage frames.
  Exact-source reproducer: actual provider committed bytes and successful authenticated
  reply was diverted to analysis; unchanged15s request timed out. Ordered control
  committed in0.125s. This matches the captured native/creator timeout failure path.
- Proof: physical-1aac6148/P06-demonstrated-composition-defect.json and reproducer ZIP.
  It promotes the earlier unresolved observation; neither diagnostic is physical PASS.
  Native supervisor/child stopped, no active fault executor, no P06 PASS.
- PR487 OPEN: head76ad339f1631e136bba7a8a85973bddb1650d570, merge ref
  ec0bbd1d5ce45a97ddc10058bad38eea90f044b6. Equal treec61eb8e838e8c3d44628af8909da4ed8ed347acf.
  Storage-stage identity/generation repair: two production files,77 tests PASS.
  All13 qualification workflows present; automatic PR sourceec0, manual source76.
  ICSE34777368640: Windows PASS; Linux announce timeout unresolved/non-demonstrated,
  Compose missing parent snapshot is infrastructure. One failed-job recovery accepted;
  receipt physical-1aac6148/storage-repair-icse-recovery.json. No blanket retry.
  Phase2 Linux34777608170 failed; bounded log/source review active.
- All required physical scenarios, CF7 and B01-B09 remain incomplete as a campaign.
  C evidence stays immutable. Checked-in P01-P12 schema has no cross-SHA carry mechanism;
  repaired main needs new freeze/revalidation and fresh physical observations.
- Resource inventory found WSL root and Nitro rootful Docker with ample disk: isolated
  bounded ext4 pressure filesystem can be prepared without changing thresholds/accounts.
  No pressure mount/fault yet. Never fill shared system disk or overcommit tmpfs RAM.
- P07/P12 NOT STARTED. Corrected prerequisite: owned product-generated historical corpus
  is allowed by their checked-in contracts; two real CNC endpoints are a separate CF7
  requirement. Configured QuickTurn/IG500/VTC sources time out; user asked once to
  make two reachable or identify current sources. Reply pending; URLs private.
- A second real Windows context is available for newly generated test DPAPI refusal.
  Protected Recorder data remains outside the campaign.

## Active work / next action
1. Finish PR487 qualification and native source proof; merge normally; qualify actual
   main once; freeze C2;
   run checked-in revalidation, then fresh physical campaign. No generic startup retry.
2. Native runtime C:/wsl/fcp-v1-1aac6148-native-faults-20260913; controls/data/results:
   Windows harness/.acceptance/native-faults. Saved signed membership retained.
   Do not mint another grant, reset data or bypass required sharing readiness.
3. Creator runtime C:/wsl/fcp-v1-1aac6148-onboarding-runtime-20260913; controls:
   Windows harness/.acceptance/onboarding-test. Separate new coordinator volume,
   cached model storage read-only, private test owner. Pairing now advertises its
   existing58796 tailnet relay binding; only Flask operator environment was activated.
   Candidate image/credentials/mounts/authority preserved; original runtimes separate.
4. Guarded44GiB isolated WSL backing/daemon and Nitro SSHFS preparation authorized;
   retain >=40GiB outer free. P06/P09 executors prepared only. Start P07 real1h/P12
   real24h only on qualified C2 with strict
   single-run evidence. Final tag must equal the newly frozen accepted candidate.
5. Windows harness/evidence/v1-physical is combined coordinator; original native roots
   retained. Mutable inputs/evidence/venvs remain outside runtime build contexts.
- Root owns release mutations; agents audit qualification, set up isolated resources
  and prepare timed C2 runs. Existing30-minute heartbeat targets this task.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- Only credible current-candidate contract violations block v1; preserve host noise.
- No broad sweep, blanket retry, unnecessary runner/account/label changes.
- AQG deliberately off and never a dependency. Protected Recorder data untouched.
- No reset, operational Docker prune, volume deletion, Arrowhead restart or protected
  data action. Supported lifecycle may bound only its own regenerable builder cache.
