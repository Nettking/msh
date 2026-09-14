# Federation v1 current release checkpoint

Updated 2026-09-14T03:14Z. Active context: clean handoff + this file + live GitHub.
Historical coordination is evidence only; consult it for a specific current need.

## Current source / heads
- Actual main after normal PR489 merge:0246bf8b5a2ccdba007e1baf277e5f9a06976f4d;
  tree1e12bfc70c0bc7feecf85ed15a369e2f85bba4ce. NOT YET QUALIFIED OR FROZEN.
- Qualified PR synthetic:a158465bedae7979a4ec5f86d2402b4303ec42b5;
  repair/trial head14e0193535a9fc1024bde95ae1f49b8940bfd9d9, same tree.
- New clean Windows H:C:/wsl/fcp-v1-0246bf8b-main-20260914.
  Planned Nitro H:/home/martin/fcp-v1-0246bf8b-main-20260914/source; NOT STAGED.
- New H/.acceptance/native-source.bundle:14841bytes, established1aac..HEAD;
  SHA256ace9f6ab15db0b1ef17fca476176a4e751453c0524f97e67d1c57551740296f1.
- Runtime sources/images still C3:48154445d380980047bb5cbc39ae5b4c1a6889c0.
  C3 frozen acceptance PAUSED after demonstrated aged-manifest startup defect.
  Old freeze diagnostics/AUTHORITATIVE_CANDIDATE-48154445.json remains unchanged.
- Checkpoint/evidence branch:codex/federation-v1-diagnostic-sweep-20260911.
  No0246 runtime activation, physical PASS, tag or publication.

## Current green gates
- PR489 release34795691356 attempt2:16/16; ICSE34795691398 attempt2:4/4.
  Phase2, CF7-B, update and branding PASS;25native checkout proofs+2aggregates.
  Exact shard/full-order sets, artifact digests and publication source verified.
- PR qualification:diagnostics/manifest-repair-pr-qualification/qualification.json;
  SHA256dfe8e891ec28f3622afa03bb46af8954f26232a3ae1626fb6ed361b0b530ed8c.
- PR prior failures retained in diagnostics/pr489-qualification/; no further PR actions.
- Main0246 ICSE34799746741:4/4 first-attempt PASS; CF7-A34799800037 and
  CF7-B34799746776:2/2 PASS; update34799746763, branding34799746756 and registry
  34799801591 PASS. Phase2 34799746714 attempt2:2/2 PASS after ONE Linux recovery;
  282/282 in original failing group; Windows execution reused. Original FAIL retained.
- C3 automated qualification/readiness and28physical assertions retained as C3.
  No full physical scenario/campaign PASS; no relabeling or carry to0246.

## Actual blockers / prerequisites
- Main release34799746727 attempt1 active:9PASS/1running/2queued/1FAIL at03:14.
  Windows journal103839826327:initial duplicate HTTP409, type-only QuorumUnavailable;
  exact current node unskippedPASS10.402s in Linux shard0. No demonstrated defect.
  Evidence/root plan:diagnostics/main-0246bf8b-qualification/windows-journal-first-failure/.
  ONE failed-only recovery planned AFTER terminal failed-set review; NOT SENT.
- Checked prospective impact:C3->repair has unknown manifest_store/phase_d_control
  paths, all12CF7 scenarios impacted,0carry. Rerun exact planner after qualification.
  safe=false forbids carry; it does not forbid fresh acceptance/admission.
- P07>=1 real hour and P12>=24 real hours NOT STARTED. Early faults, complete
  source-bound coverage and measured capacity precede shared-runtime timers.
- Separate CF7 needs two reachable real CNC sources. Asked once, answer pending.
- Nitro pressure fixture idle: containment PASS, memory margin refused00:47.
  Host3.26GiB total/1.84GiB available; no pressure product activation. Do not repeat
  until resource state changes; no limit/cap/host-pool changes or unrelated stops.
- C3 P01 per-hour growth FAIL retained; new passive hour after measured-host builds.
  P09 outbox windows and P06 authenticated update trial still unperformed.
- P06 stopped native cleanly; creator retains198manifest revisions/native pending
  work in place. Repair22focused+141existing tests PASS,197-revision5.44->0.99s;
  full repaired live45s startup remains unproven. No manual drain or reset.

## Active work / next action
1. PREP=.acceptance/manifest-repair-release-preparation. inspect_actual_main.py;
   inputs-actual-main.json and read-only authorization now bind0246. Wait on current
   release run; review terminal failures, execute its one failed-only recovery.
   Then final retain_qualification/finalize_qualification; reuse retained cache.
   Do not rerun merge489.py, dispatch helpers or recover_main_phase2_once.py.
2. After actual-main qualification, freeze_qualified_repair.py under
   .acceptance/postrepair-freeze-preparation (SHA25624dfbaf260958cd9749c5d942fe54cb32ae7d97e3a28ea20fbd64e3d86569acf).
   Use exact qualification hash and output diagnostics/AUTHORITATIVE_CANDIDATE-0246bf8b.json.
   Truthful checked exit2/unknown2/all12fresh/0carry, raw outputs and policy hashes.
3. Generate reviewed readiness controls there with new H/Nitro H/freeze hash/bundle;
   stage source-only Nitro H, transfer identical freeze/qualification bytes, then
   use existing qualified Windows/Nitro3.12venvs for exact-source native readiness.
4. Disabled admission:.acceptance/postrepair-admission-preparation, current helper
   SHA10a10cc977496a53aa18a4c4ee5e70ff0459d0a46a15b5b0b73e71461d2fe682.
   Final-source review before execution; preserve all data/mount/env/user/identity.
   Agent transition:.acceptance/postrepair-agent-transition-preparation, disabled;
   resident-verifier scope correction in progress; normal start.cmd restores agent.
   Quiesce exact owned pair before staging; child env removes only PSModulePath.
   Final bindings derive actual Compose labels; original preparations stay retained.
5. .acceptance/postrepair-p06-preparation preserves45/100/600/600s and uses latest
   Agent seed c3-physical-48154445-20260914/run-p06-update-trial-recovery/agent-observations.json,
   SHAca35d3afaba47b33c0e8705ce80011ba527e10b5cc0a8a0f9c40591bfebb468c.
   Admit BOTH creator and native, use distinct same-tree14e01935 trial and new campaign.
6. Populated P11 preparation:.acceptance/p11-next-candidate-preparation, corrected
   renderer d62d8931...; baseline in C3 H/.acceptance/p11-control-internal-model-r4.
   Real16records/uploadREADY,AI disabled,5owned containers,0host writers at baseline.
   Preserve populated state; never empty-fixture reset/rebind. New updater inventory
   is required after normal activation. No backup/key/restore PASS yet.
7. Remaining Windows/Nitro/P01/P09/pressure controls are C3-bound and disabled;
   require final-source bindings. postrepair-general-preparation remains disabled;
   P01 paired ordering correction reviewed in p01-sequencing-r2; no execution.
   Pressure preparation paused; postrepair-pressure-preparation/PAUSED_PREPARATION.md.
   Existing30-minute heartbeat active; advance only on meaningful changes/actions.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- Only credible current-candidate checked-contract violations block v1.
- No broad sweep/blanket retry/new D-number per symptom or needless runner changes.
- AQG off/nondependency. AGQ7NCC offline until tomorrow; do not wait for it.
- Protected Recorder data untouched. No reset, operational Docker prune, volume
  deletion or Arrowhead restart. Retain old evidence/images/partial copies.
- Windows64GiB floor plus growth margin unchanged. Timers use actual durations
  and one frozen source. Final tag only after physical acceptance.
