# Federation v1 current release checkpoint

Updated 2026-09-13T18:56Z. Active context: clean handoff + this file + live GitHub.
Historical coordination is evidence only; consult it for a concrete current need.

## Current source / heads
- Frozen C=1aac6148759d7b2fd488ec26b97e1a786bdafa80; live main verified unchanged
  this continuation. Tree=8fd60c5e2886dfedb0248959fda6cbfc60413d2f. PR485 then PR486 merged.
- Freeze: diagnostics/AUTHORITATIVE_CANDIDATE-1aac6148.json. No tag/public release
  or complete physical PASS. Old e6a9b74a freeze is superseded; do not qualify it again.
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
- Fresh P01 PASS:10/10, Windows5/5 +Nitro5/5. Three supported activations per OS,
  exact runtime/baseline identity, growth and safely refused real failed builds.
  Sustained Windows hour PASS820288256 B/h below unchanged1073741824 B/h ceiling;
  original short-window FAIL retained. Proof: physical-1aac6148/P01-combined-result.json.
- Fresh P03 PASS:8/8. Supported Windows tailnet launcher exit0 and cross-host HTTP200.
  One corrected operator binding VERIFY resolved source/config directory mismatch
  on the existing action. No relaunch, source repair or assertion change.
  Proof: physical-1aac6148/P03-complete-result.json and ZIP; original FAIL retained.
- P04:2/6 PASS: eight simultaneous native sources and maximum accepted ingress.
  Actual8MiB current/16MiB probe/16MiB sample,10000 observations/span, default limits.
- P05:8/16 PASS: model absence, path confinement, slow trickle, oversized bytes/count,
  trillion-sequence gap, event coalescing, real Flask crash and real relay crash.
  Each core recovered automatically once, same image/container, other cores unchanged.
- New proofs: physical-1aac6148/native-ingress-result.json, ingress ZIP,
  P05-core-crashes-status.json and P05-core-crash-evidence.zip. All originals retained.
  Native input stopped cleanly. No product source, limits or deadlines changed.
- Original ICSE trace and bounded PR486 recovery dispositioned; no demonstrated
  shared/deterministic candidate mechanism. Do not reopen the broad diagnostic sweep.

## Actual blockers / disposition
- P02, remaining P04-P12, fresh CF7 and B01-B09 incomplete. No full physical PASS.
- P06 startup stopped before deliberate faults: native signed enrollment succeeded,
  but supported child failed unchanged45s required sharing readiness. Supervisor was
  stopped by real Ctrl+C, exit0. No owned supervisor/recorder child remains; no P06 PASS.
- Bounded capture: responsive event loop; announcement/authority selection succeeded;
  native delivery awaits storage ingest reply. Creator trace shows nested timeout in
  PhaseDLogicalStorageClient.ingest -> RelayStorageEndpoint.request to local provider.
  Read-only receipts: primary-only ack policy, no assigned replicas,4 prepared intents,
  zero committed items,928 pending native rows. Cause of missing reply remains unknown.
- Classification: unresolved but non-demonstrated candidate defect. No evidence-based
  product repair yet; do not waive P06 or repeat generic startup/re-enrollment attempts.
  Detailed disposition/stacks: physical-1aac6148/P06-startup-disposition.json and peers.
- P02 needs isolated storage: Nitro checkout/data/results/Docker share system disk;
  no native sudo, Windows not elevated. User request for isolated mounts/admin-enabled
  test host pending. No pressure injected; never exhaust a shared disk as a substitute.
- P07/P12 NOT STARTED. Pending user input: two approved real MTConnect endpoints and
  a non-protected aged corpus. Owned corpora lack required history. Do not ask again.

## Active work / next action
1. No active executor. Preserve completed proof and inspect only the exact-source
   shared local-provider request/reply path implicated by P06. Obtain a focused
   reproducer before repair or further startup; no generic diagnostic loop.
2. Native runtime C:/wsl/fcp-v1-1aac6148-native-faults-20260913; controls/data/results:
   Windows harness/.acceptance/native-faults. Saved signed membership retained.
   Do not mint another grant, reset data or bypass required sharing readiness.
3. Creator runtime C:/wsl/fcp-v1-1aac6148-onboarding-runtime-20260913; controls:
   Windows harness/.acceptance/onboarding-test. Separate new coordinator volume,
   cached model storage read-only, private test owner. Pairing now advertises its
   existing58796 tailnet relay binding; only Flask operator environment was activated.
   Candidate image/credentials/mounts/authority preserved; original runtimes separate.
4. Resume pressure only with isolated resources. Resume timed acceptance only with
   approved sources/corpus. P07 real1h/P12 real24h must complete strict single-run
   evidence. Complete all required observations before public release; tag must equal C.
5. Windows harness/evidence/v1-physical is combined coordinator; original native roots
   retained. Mutable inputs/evidence/venvs remain outside runtime build contexts.
- One owner is this task; existing30-minute heartbeat targets it. Older task stopped.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- Only credible current-candidate contract violations block v1; preserve host noise.
- No broad sweep, blanket retry, unnecessary runner/account/label changes.
- AQG deliberately off and never a dependency. Protected Recorder data untouched.
- No reset, operational Docker prune, volume deletion, Arrowhead restart or protected
  data action. Supported lifecycle may bound only its own regenerable builder cache.
