# Federation v1 current release checkpoint

Updated 2026-09-13T17:26Z. Active context: clean handoff + this file + live GitHub.
Historical coordination is evidence only; consult it for a concrete current need.

## Current source / heads
- Authoritative frozen candidate C=1aac6148759d7b2fd488ec26b97e1a786bdafa80.
- Live main=C; tree=8fd60c5e2886dfedb0248959fda6cbfc60413d2f. PR485 then PR486
  merged normally. Freeze: diagnostics/AUTHORITATIVE_CANDIDATE-1aac6148.json.
- Windows harness: C:/wsl/fcp-v1-1aac6148-main-20260913, clean detached C.
- Nitro harness: /home/martin/fcp-v1-1aac6148-main-20260913/source, clean detached C.
- Both owned runtimes installed C: launcher0, three core image labels and configured
  HTTP200 verified on each. Recorder owned voter remains fba50818.
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
- Fresh combined P01 PASS: 10/10 native assertions; Windows5/5 and Nitro5/5.
  Three supported activations, baseline, runtime state, growth and one real failed
  build on each OS passed. Both failed builds refused safely and stopped their writer.
- Windows single declared hour follow-up PASS at16:41:15Z:820288256 B/h below the
  unchanged1073741824 B/h ceiling. All5 samples and the original short-window FAIL
  retained byte-for-byte; no restart amplification. No sustained defect demonstrated.
- Nitro growth decreased43319296 bytes across4 samples; its build fault passed16:32Z.
- Portable hosts/observations merged per checked-in multi-host instructions, without
  overwrites. Checked-in scenario_status: PASS, no missing/failing P01 assertions.
  Proof: diagnostics/physical-1aac6148/P01-combined-result.json and evidence ZIP.
- Original ICSE trace and bounded PR486 recovery are dispositioned. No demonstrated
  shared/deterministic candidate mechanism. Do not reopen the completed broad sweep.
- Fresh P03:7/8 PASS. Both native concurrency probes, retired update.cmd, launcher
  versus update, Windows start.cmd, Linux start.sh, and model failure isolation pass.
- P05 Ollama absence PASS from the same bounded model outage. Same model container
  restored healthy; core IDs/images/restart counts unchanged; workbench HTTP200.
  Proof: diagnostics/physical-1aac6148/P03-progress-result.json and portable archives.

## Actual blockers / disposition
- P02, the last P03 assertion, P04-P12, fresh CF7 and B01-B09 remain incomplete.
- P02 needs independent test storage: Nitro checkout/data/results/Docker share one
  system filesystem. No pressure injected. Native sudo is unavailable; Windows is
  not elevated. User input requested once for isolated mounts or an admin-enabled
  test host; keep pending. Never exhaust the shared system disk to bypass isolation.
- P03 start-tailscale.cmd is not attempted. Existing Windows fixture is loopback-only
  and workbench status is identity-missing. Tailnet input delta is reviewed/staged
  at Windows harness/.acceptance/p03-tailnet, NOT activated; no identity fabricated.
  Need product-level onboarding in an isolated owned test installation, preserving
  existing data/authority and the no-reset constraint. AQG remains off.
- P07/P12 have NOT STARTED. Owned corpora are empty; aged history insufficient.
  Pending user input already requested: two approved real MTConnect endpoints and
  a non-protected aged test corpus. Do not ask again or access protected Recorder data.
- Nitro P03 launcher exited0; supplemental HTTP10s timed out. One read-only recovery
  returned200 in1.381s with the same deadline; exact core labels/zero restarts verified.
  Original timeout retained, no launch repeated, no demonstrated candidate defect.
- Windows P01 wrapped-error parser defect was repaired in the local evidence helper;
  existing real fault verified without repeating it. Detailed original evidence retained.
- Windows short-window growth remains an archived observation, not a demonstrated
  candidate defect. The one declared follow-up completed; no further growth diagnosis.

## Active work / next action
1. No physical executor remains active. Never repeat green P01/P03 checks.
   Combined coordinator evidence: Windows harness/evidence/v1-physical, containing
   both native host records and all original packets. Native roots remain retained.
2. Prepare isolated product-level onboarding for the remaining P03 tailnet path.
   Existing P01/P03 fixture records remain evidence. Do not reset or overwrite their
   identity/data to manufacture onboarding, and do not run AQG or a broad diagnostic sweep.
   Native P03 recovery is terminal. Windows staged tailnet inputs are not live defaults.
3. Resume P02 when isolated owned storage/admin test host is available; define bounded
   filler and recovery against the real resource before injection. Pending request is
   missing setup information, not authorization to lower thresholds or fill unrelated disks.
4. Complete fresh P02-P12, CF7 and B01-B09 under the checked-in contract. P07 real1h
   and P12 real24h require strict single-run evidence and approved source/corpus inputs.
   No full PASS or public release before every required observation/validation finishes;
   tag must equal C. Preserve valid exact-source evidence instead of retesting it.
5. Mutable controls/evidence/venv stay outside runtime build contexts:
   C:/wsl/fcp-v1-73c779-nettking-runtime-20260910 and
   /home/martin/fcp-v1-73c779-nitro-20260910/source.
   Both harness controls preserve exact live configuration and existing owned mounts;
   staging held the checked-in host mutation lock. Old evidence remains untouched.
- One owner is this task; existing 30-minute heartbeat targets it. Older task stopped.
- Detailed current evidence/scripts: diagnostics/physical-1aac6148/.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- Only credible current-candidate contract violations block v1; preserve host noise.
- No broad sweep, blanket retry, unnecessary runner/account/label changes.
- AQG deliberately off and never a dependency. Protected Recorder data untouched.
- No reset, operational Docker prune, volume deletion, Arrowhead restart or protected
  data action. Supported lifecycle may bound only its own regenerable builder cache.
