# Federation v1 current release checkpoint

Updated 2026-09-14T06:22Z. Active context: clean handoff + this file + live GitHub.
Historical coordination is evidence only; consult it for a specific current need.

## Current source / heads
- PR490 merged normally, no bypass. Actual main:
  f00f13013b58e0023f1a506e32daa86df37af7ae, tree49c6f273afa82059d45dbc2bdffb5168b083f25d.
- Repair head/trial d0e5b6b8993b76ccb2e49da699fb304a2684df66,
  branch codex/federation-v1-ack-validation-repair; same tree as resulting main.
- Clean H C:/wsl/fcp-v1-f00f1301-main-20260914. Nitro H planned:
  /home/martin/fcp-v1-f00f1301-main-20260914/source. Native bundle retained.
- Existing runtime/freeze remains0246bf8b; no newmain activation/freeze yet.
  Native stopped after P06 startup failure;205completed/723pending preserved.
- Checkpoint branch codex/federation-v1-diagnostic-sweep-20260911.
  No full physical PASS, final tag or publication.

## Current green gates
- PR native148dbdb21d1f98dfb5a1db08c8e7081a299cb149 qualified, tree49c6:
  diagnostics/pr490-148dbdb2-qualification/qualification.json,
  SHA2568cb643f1bcf054bf22303564708fe0365fc90f75a458cf58cb866eefbcc9c5f2.
- PR release16/16; ICSE4/4; Phase2, CF7-B, update, branding PASS.
  25 native checkout proofs; complete shard/order/publication source audited.
- All9new tests unskipped in both fullorders and disjoint shard union:
  manifest-text-focused-review.json SHA256f90284ba7993b19eefc546190853357f84d1982baa205d5f92b93b9c163b66cc.
- Phase2 sole failed-job recovery PASS381/1unrelated skip; original failednode
  passed, originalLinux execution retained. Cause unknown/non-demonstrated defect.
  diagnostics/pr490-148dbdb2-phase2-recovery/recovery-review.json. No more retry.
- New main branding PASS; other required gates running/queued, no failures06:19Z.

## Actual blockers
- Fresh physical acceptance outstanding. Old0246 P06 startup failed45s sharing/
  100s observation; full-path proof demonstrated repeated C0 validation cost.
  Minimal equivalent predicate repair merged490.8ACK profile76.27s ->38.01s,
  all206 manifest rows byte-identical; no live startup PASS inferred.
  Evidence: diagnostics/physical-0246bf8b/full-ack-repair-proof.json.
- Main must qualify/freeze/revalidate before fresh campaign; no physical carry.
- P01 hour, P07>=3600s and P12>=86400s NOT STARTED. Real durations required.
- Separate real-source CF7 needs two reachable CNC sources; asked once.
- Nitro pressure needs unchanged fresh capacity preflight after builds settle.
  Prior memory-margin refusal is fixture capacity, not product OOM.

## Active work / next action
1. Qualify resulting main once using .acceptance/ack-validation-release-preparation.
   inputs.main.json + authorization.root-reviewed-main.json bind exactsource.
   Release34812799504, ICSE34812799419, Phase234812799566,
   CF7-B34812799532, update34812799444, branding34812799431 automatic.
   Only missing CF7-A34812886619 and registry34812889190 dispatched06:18Z.
   inspect_main.py is read-only; retain/finalize when terminal, then9caseaudit.
2. Prepare native readiness/freeze/revalidation in pr490-main-physical-preparation;
   next P06 in pr490-p06-transition-preparation; general/P11 in
   pr490-general-physical-preparation. Execution disabled until bound review.
   Latest C4 agent seed and current-agent capture01c6b873 retained;
   revalidate identity/quiescence before transition. Never reset queues/state.
3. Warm, pressure and timed postrepair recipes remain available. Windows C4
   admission uses separate candidate-admission.verified.json, preserving original.
   P12/P07 may overlap only after final shared-runtime state, complete passive
   coverage and measured whole-volume growth/capacity. Supplemental unchanged-cap
   fixtures prepared, not executed. No preparation equals physical PASS.
4. Privacy-review/publish new evidence. Existing30-minute heartbeat active;
   substantive work only on a meaningful state change or actionable step.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- Only credible current-candidate checked-contract violations block v1.
- No broad sweep, blanket retries, new D-number per symptom or runner changes.
- AQG off/nondependency. AGQ7NCC offline until tomorrow; do not wait for it.
- Protected Recorder data untouched. No reset, operational Docker prune,
  volume deletion or Arrowhead restart. Retain original evidence/partial copies.
- Windows64GiB floor plus growth margin unchanged. Real timers, one frozen source.
  Final tag only after actual physical acceptance.
