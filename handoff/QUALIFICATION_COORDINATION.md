# Federation v1 current release checkpoint

Updated 2026-09-14T07:02Z. Active context: clean handoff + this file + live GitHub.
Historical coordination is evidence only; consult it for a specific current need.

## Current source / heads
- PR490 merged normally, no bypass. Actual main:
  f00f13013b58e0023f1a506e32daa86df37af7ae, tree49c6f273afa82059d45dbc2bdffb5168b083f25d.
- Repair head/trial d0e5b6b8993b76ccb2e49da699fb304a2684df66,
  branch codex/federation-v1-ack-validation-repair; same tree as resulting main.
- Clean H C:/wsl/fcp-v1-f00f1301-main-20260914. Nitro H staged clean/detached:
  /home/martin/fcp-v1-f00f1301-main-20260914/source. Source-only stage DONE06:33Z;
  diagnostics/physical-f00f1301/nitro-source-stage.json. Do not replay staging.
- Authoritative freeze f00f1301 DONE07:00Z, SHA256
  0c5127ae2d2dc8885182f38c908dc544789f38cf3e9de69bc12d1f1e56366d25.
  Existing runtime remains0246bf8b; no newmain activation yet.
  Native stopped after P06 startup failure;205completed/723pending preserved.
- Checkpoint branch codex/federation-v1-diagnostic-sweep-20260911.
  No full physical PASS, final tag or publication.

## Current green gates
- Actual main AUTOMATED_QUALIFIED: release16/16, ICSE4/4, Phase2, CF7-A/B,
  software update, branding and registry PASS first attempt. No reruns.
  diagnostics/main-f00f1301-qualification/qualification.json SHA256
  9d4c1c55046ca86110e728e6638a2e4a7d15e36c9cd4db377af82a0984ae6ff4.
- 28 native checkout proofs; complete shard/order/publication source audited.
  All9new tests unskipped in both fullorders and disjoint shard union;
  manifest-text-focused-review.json SHA256
  5717eaf3598a190e8d334e5c9ebeb404e7c7822991ca5cd0796be900bd5ffb39.
- Checked revalidation DONE: all fresh/no carry; expected unknown product path.
  physical-f00f1301/revalidation-from-0246bf8b.json SHA256
  341b26ba7aa104e2cec0df6e27798767212032d3870b2d7f6fbb9348b7ba6d1b.
  Prior PR qualification/recovery evidence retained separately; do not repeat.

## Actual blockers
- Fresh physical acceptance outstanding. Old0246 P06 startup failed45s sharing/
  100s observation; full-path proof demonstrated repeated C0 validation cost.
  Minimal equivalent predicate repair merged490.8ACK profile76.27s ->38.01s,
  all206 manifest rows byte-identical; no live startup PASS inferred.
  Evidence: diagnostics/physical-0246bf8b/full-ack-repair-proof.json.
- Fresh native readiness running on both hosts; runtime admission still required.
- P01 hour, P07>=3600s and P12>=86400s NOT STARTED. Real durations required.
- Separate real-source CF7 needs two reachable CNC sources; asked once.
- Nitro pressure needs unchanged fresh capacity preflight after builds settle.
  Prior memory-margin refusal is fixture capacity, not product OOM.

## Active work / next action
1. Main retention/finalization/focused audit DONE once; no CI work outstanding.
   Release34812799504; seven companion IDs in immutable qualification receipt.
2. pr490-main-physical-preparation freeze/revalidation DONE. Generated readiness
   controls installed byte-identically both hosts; one native readiness started
   each07:02Z. Inspect status/results, do not relaunch. Nitro source-stage raw
   receipts match aad507928e30c9a35a6c70ce7ac5ba7d18fe4e57ca88c3a3a21d3915c79e1c28.
   P06 prepared-f00/INVOCATIONS.md under pr490-p06-transition-preparation gives
   exact next actions; root reviewed diffs, offline history-guard12cases verified
   (4accepted/8refused). Prepared invoke_reviewed.py preserves child-only
   PSModulePath removal; bind its actual generated HTTP/config hashes before use.
   Latest C4 agent seed and current-agent capture01c6b873 retained;
   revalidate identity/quiescence before transition. Never reset queues/state.
3. pr490-general-physical-preparation/generated-f00f1301-disabled generated once,
   generation ea4546ca; inert only, all receipt/state bindings unset. P11 adapter
   still needs actual C3-to-f00 five-path revalidation and populated continuity.
   pr490-warm-physical-preparation independently reviewed; no generation/activation.
   Windows C4 uses candidate-admission.verified.json, preserving original.
   Current warm-agent identity capture DONE once/host: both idle, no extra
   descendants/signals/changes. Concrete quiescence controls being prepared.
   pr490-timed-physical-preparation has13 helper recipe and14 unchanged supplemental
   blobs; no execution tree/fixtures/timers. Pressure recipe still unbound.
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
