# Federation v1 current release checkpoint

Updated 2026-09-13T16:00Z. Active context: clean handoff + this file + live GitHub.
Historical coordination is evidence only; consult it for a concrete current need.

## Current source / heads
- Authoritative frozen candidate C=1aac6148759d7b2fd488ec26b97e1a786bdafa80.
- Live main=C; tree=8fd60c5e2886dfedb0248959fda6cbfc60413d2f. PR485 then PR486
  merged normally. Freeze: diagnostics/AUTHORITATIVE_CANDIDATE-1aac6148.json.
- Windows harness: C:/wsl/fcp-v1-1aac6148-main-20260913, clean detached C.
- Nitro harness: /home/martin/fcp-v1-1aac6148-main-20260913/source, clean detached C.
- Both owned runtime source checkouts are C. Windows installed C, verified three
  core image labels and configured HTTP200. Nitro activation is still RUNNING;
  its final image/HTTP verification is pending. Recorder owned voter remains fba50818.
- No tag, public release, or complete physical PASS. Old e6a9b74a freeze is blocked
  and superseded; never repeat qualification or physical acceptance on that source.

## Current green gates
- Actual resulting main qualified once: 37/37 required +3/3 companion jobs PASS,
  all first attempts. All 13 workflows complete; no additional dispatch or retry.
- Release34763230099:16/16 PASS. ICSE34763230042: Windows/Linux10/10,
  Compose4/4 and publication PASS. Native38 checkouts +2 aggregates verified;
  4528 disjoint Linux test IDs and every native/JUnit/artifact digest checked.
- Publication artifact10318929498 exports exactly git archive C. Proof:
  diagnostics/main-1aac6148-qualification/qualification.json and immutable archives.
- Checked-in revalidation from e6a9b74a PASS: all 12 physical scenarios fresh,
  zero carry-forward, no unknown paths. Proof: diagnostics/physical-1aac6148/.
- Both native hosts: preflight and 4/4 readiness gates PASS. Python3.12.10 Windows,
  3.12.13 Linux; existing venvs reused with unchanged dependency declarations.
- Windows fresh P01: three activations, resource baseline, runtime state PASS.
  These are partial observations, not complete P01 or physical acceptance.
- Original ICSE trace and bounded PR486 failed-job recovery have valid dispositions;
  no shared or deterministic candidate mechanism demonstrated. Evidence remains in
  prior diagnostics and repair-94567b0d-qualification; do not reopen the broad sweep.

## Actual blockers / disposition
- Windows P01 short-window hourly growth FAIL:135884800 bytes over 3 activations,
  45294933 bytes/activation; projected2324568721 B/h exceeds1073741824 B/h.
  Core images, Docker totals, owned raw-file count0 and history32768 bytes stable.
  Current CI had already finished. Whole-volume attribution remains incomplete:
  unresolved but non-demonstrated candidate defect; no product repair justified yet.
- Exactly one passive hour-window follow-up is active. All 4 original samples and
  FAIL retained; same1GiB/hour ceiling. No Windows activation/fault work until terminal.
  Final sample not before 2026-09-13T16:41:05.365965Z; PID31872.
  Status: Windows harness/.acceptance/runtime-control/P01-hour-status.json.
- Nitro admission dispatched once, PID3010269, started15:45:29Z. Latest read15:57Z:
  build finished, cores restarted, optional model step running; final result pending.
  Status: Nitro harness/.acceptance/runtime-control/candidate-admission.json.
- Fresh P01 failed-build cleanup and remaining physical scenarios are incomplete.
  Previous focused repair proof cannot substitute for fresh physical acceptance.
- P07/P12 have NOT STARTED. Owned corpora are empty; aged history insufficient.
  Pending user input already requested: two approved real MTConnect endpoints and
  a non-protected aged test corpus. Do not ask again or access protected Recorder data.

## Active work / next action
1. Inspect the existing Nitro admission on meaningful change using
   diagnostics/physical-1aac6148/read-nitro-admission.py. Never redispatch an uncertain
   activation. If stopped, retain terminal status and inspect its bounded failure.
2. After Nitro admission completes with launcher0, three C labels and HTTP200,
   dispatch one fresh Nitro P01 three-activation campaign using run-qualified-p01.py.
   Retain a durable dispatch receipt; do not overlap physical voter activations.
3. At/after16:41Z read existing Windows P01-hour terminal packets. Preserve the
   initial FAIL and every sample, classify the result, then resume required P01 work.
   Never restart the measurement, filter samples, or relax the unchanged ceiling.
4. Complete fresh physical P01-P12, CF7 and B01-B09 under the checked-in contract.
   P07 real1h and P12 real24h require strict single-run evidence. No PASS or public
   release before all required observations and validation finish; tag must equal C.
5. Mutable controls/evidence/venv stay outside runtime build contexts:
   C:/wsl/fcp-v1-73c779-nettking-runtime-20260910 and
   /home/martin/fcp-v1-73c779-nitro-20260910/source.
   Both new harness controls preserve exact live configuration and existing owned
   data mounts; staging held the checked-in host mutation lock. Old evidence untouched.
- One owner is this task; existing 30-minute heartbeat targets it. Older task stopped.
- Current detailed evidence/scripts: diagnostics/physical-1aac6148/.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- Only credible current-candidate contract violations block v1; preserve host noise.
- No broad sweep, blanket retry, unnecessary runner/account/label changes.
- AQG deliberately off and never a dependency. Protected Recorder data untouched.
- No reset, operational Docker prune, volume deletion, Arrowhead restart or protected
  data action. Supported lifecycle may bound only its own regenerable builder cache.
