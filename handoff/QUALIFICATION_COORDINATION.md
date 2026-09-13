# Federation v1 current release checkpoint

Updated 2026-09-13T23:17Z. Active context: clean handoff + this file + live GitHub.
Historical coordination is evidence only; consult it for a specific current need.

## Current source / heads
- PR488 merged normally, no bypass. Actual main/C3:
  48154445d380980047bb5cbc39ae5b4c1a6889c0; tree 4f45748e55e7d976ef9936b8f77933eae8a5aa29.
- Clean C3 source staged: Windows C:/wsl/fcp-v1-48154445-main-20260914 (H);
  Nitro /home/martin/fcp-v1-48154445-main-20260914/source. NOT FROZEN/ACTIVATED.
- Native runtime: clean main C2=e91e5218, stopped; membership/data retained in
  old 1aac6148 H/.acceptance/native-faults/data. Trial ref 76ad339f retained.
- Creator source C2/live images 1aac; other Windows source/images 1aac;
  Nitro workbench source/images C2. No C3 product admission yet.

## Green gates / release decision
- PR488 synthetic 1bd199b3: release 16/16, ICSE 4/4, current product gates PASS;
  audit verified 29 native source proofs, two aggregates and publication bytes.
  Real C2 warm-activation defect repaired; 43 related tests + live red-to-green.
- C3 release 34787907931: 9 PASS, 0 FAIL at 23:16, remaining jobs running/queued.
  CF7-B/C, CF8, software update, branding and registry PASS. CF7-A running.
- ICSE 34787907928 attempt 2: ONE failed Linux job recovery accepted; Windows
  and Compose successes retained. Linux recovery running. NEVER dispatch again.
  Original Linux job 103806642856 timed out during initial reviewer join after
  quorum PASS, before failover. No underlying/shared mechanism demonstrated;
  exact private trace absent. Native/public failure evidence retained.
- F85 Linux job 103806642710: one 20s terminal wait timeout, no demonstrated
  driver death/cause. Same-tree PR has four mandatory-suite PASS proofs.
  Use already-running C3 shard2/fixed/rotating/Windows-capability node results;
  reviewed exact-disposition helper requires all four non-skipped C3 PASS.
  F85 Windows PASS; original Linux FAIL stays recorded. No F85 retry.
- Hosted bootstrap 34787907975/job 103806642746 never started (billing,
  no runner/steps): exact infrastructure disposition, no account/pool changes.
- No physical campaign PASS, release or tag. C2 physical evidence stays C2.

## Actual blockers / prerequisites
- Finish current C3 qualification: all 16 release jobs, ICSE 4, permanent
  CF7-A/B both OS, registry and native/artifact audit. Freeze and revalidate.
- All P01-P12/CF7/B01-B09 fresh: checked C3 impact is 12 scenarios, zero carry.
  P07 >=1 real hour and P12 >=24 real hours NOT STARTED. Complete measurement
  coverage and measured capacity baseline required before timers.
- Separate CF7 input: two reachable real CNC sources; configured sources timed
  out. Asked user once, answer pending. Timed campaign is independent.
- Nitro pressure fixture idle: containment PASS, host memory margin REFUSED
  at 23:01. Recheck only after meaningful load change; keep limits unchanged.

## Active work / next action
1. Root owns qualification/freeze/admission and shared creator/native faults.
   .acceptance/c3-release-preparation: snapshot_actual_main.py, reviewed
   freeze_qualified.py/admit_owned.py and actual-main-source.json.
2. .acceptance/c3-qualification-preparation: partial main retention completed
   (24 logs/15 artifacts), label main-48154445-final; retain final completion once.
   Final audit also needs --f85-disposition pointing
   to public main-48154445-qualification/f85-first-failure/disposition.json.
   Retention/finalizer and exact F85 guards reviewed; 42 refusal checks PASS.
3. General controls: c3-general-physical-preparation/generated-48154445-r2,
   copied/hash-verified into H/.acceptance/physical-controls (16 files).
   P06/timed/P09: .acceptance/c3-physical-48154445-20260914.
   Nitro transfer: c3-release-preparation/NITRO_CONTROL_TRANSFER.md, reviewed;
   package/transfer only after exact freeze and qualification exist.
4. After freeze/readiness, admit creator/native/Windows/Nitro; initialize fresh
   campaign. Independent P04/P05/P09 and dedicated P11 work can then proceed.
   Root P06 basic -> crash fence -> actual trial/main restore; chain latest
   Agent state. Finish shared clock/outbox/creator faults before timed reservation.
5. Nitro private pressure 44GiB fixture/guardian empty, no product/filler;
   18 pinned controls and 32 guard checks ready. Actual core containment required
   after startup, before initialization. Windows archives remain ARCHIVE_ONLY.
6. P11 fixture empty; C3 rebind/entrypoints reviewed. SMB transport/share and
   Recorder isolated Python ready; no protected-data/key assertion/backup run.
7. Existing 30-minute heartbeat ACTIVE in this thread. Advance on meaningful
   state changes/actions; no historical recap, repeated green CI or extra gates.

## Immutable safety constraints
- Never weaken assertions, quorum/authority, deadlines, security or acceptance.
- Only credible current-candidate checked-contract violations block v1.
- No broad sweep/blanket retry or needless runner/account/label changes.
- AQG off/nondependency. AGQ7NCC offline until tomorrow; do not wait for it.
- Protected Recorder data untouched. No reset, operational Docker prune, volume
  deletion or Arrowhead restart. Retain old evidence/images/partial copies.
- Windows 64GiB floor unchanged; require growth margin. Timers use actual
  durations and one frozen source. Final tag only after physical acceptance.
