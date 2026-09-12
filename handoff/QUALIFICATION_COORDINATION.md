# Federation v1 qualification coordination

**Current actionable checkpoint (2026-09-12T05:43:20.975418+00:00):** D08/#466 repair is
pushed as draft **PR467**, exact head `84c66f8185c1411d9dc8c5c33244a2f564845ce7`,
branch codex/analysis-content-resolve-race, clean isolated worktree
C:/wsl/fcp-analysis-content-resolve-race-20260912. Five baseline regressions fail;
repair focused suite70pass/4symlink-privilege skips; final formatted extended-path
run13pass/4skips, including4 real junction boundary cases. No qualification dispatch.
Next: review exact draft head, containment/reparse/reader contracts and current
review findings; preserve automatically scheduled evidence, then decide required
qualification from the completed repair review. Do not qualify merely because a
patch exists or retry the failed F85 job on unchanged main.
Actual main2a9c9b8eb53edff74c2de23570ec56e054d29b22 remains NOT QUALIFIED
(36/37 successful, F85-Windows D08 failed;3/3 companions successful).
All prior repairs463/465 are merged; native2a9 release/ICSE artifact reviews retained.
Physical runtime stays M9b286f931497bf6291e215f6340443c5162826b0; no candidate
freeze, physical PASS, P07/P12 or protected Recorder-data changes.

Current user direction, September 11 2026: execute on state changes, using this
checkpoint and the completed diagnostic sweep report; do not repeat the sweep.

Sequence: qualify required exact PR heads -> merge456/457/461 as separate fixes
-> qualify actual final merged main once -> resolve/verify D04 through the
documented controlled host procedure -> freeze one AUTHORITATIVE_SHA -> clean
checked-in revalidation -> fresh formal physical acceptance.

| PR | Intended exact head | Phase |
|---|---|---|
|456|1a0c634f47f8a247b6d1d2d1a219f5c12590587d|Qualified37/37 plus CFI2/registry; merged as63976fb3|
|457|143fe7a9082193114af3d34dc437b85f845849a2|Qualified37/37 plus CFI2/registry; merged as8948e953|
|461|5b826c6806ab1bdb960412ba20ca78192971fb1d|Qualified37/37 plus CFI2/registry; merged as9b286f93|

**Current stage:** actual final merged main
`9b286f931497bf6291e215f6340443c5162826b0` is qualified37/37 plus CFI2/registry,
release-verdict and native artifact review. All fix heads are ancestors.
Preserve `diagnostics/merged-main-final-qualification.json`; no software rerun.
D04 host remediation and local/real Nettking peer verification are complete on N;
issue459 is closed. AUTHORITATIVE_SHA is now frozen as
`9b286f931497bf6291e215f6340443c5162826b0` (M).
Clean checked-in revalidation and scenario side-effect review are complete.
Nettking supported activation and all three actual M core image/env/repaired-source
hashes are verified. [Runtime receipt](diagnostics/nettking-m-runtime-admission.json).
**PHYSICAL ADMISSION STOPPED: new confirmed D06 product defect on M.** Checked-in
Nitro responder replacement stopped old instance then failed bind with EADDRINUSE.
Preserve [D06 / issue462](diagnostics/D06.md). Repair is ready for review in PR463,
head7286f30d30e20c94c13cc9acebb2fd6d61bc1163, with focused development tests PASS.
Next: review repair/native exact-head CI state. No dependent admission, physical
retry workaround or PASS; no repair deployed and no new candidate frozen.
**PR463 corrected head83955f65b7e6bb36de8e90f34608b97070fed33b pushed.** D06-R1
is resolved in branch: signal-only boolean default restored, only replacement
opts in to stable-handle exit waiting. Backup source/deadline/error unchanged.
Focused Windows52PASS/8skips and isolated Linux11PASS; full new-head qualification
pending. Preserve all7286f30d CI/native/artifact evidence as historical, including
its review blocker; never carry those results to83955f65. Next: update PR review
and qualification tracking to new head, then run only necessary exact-head gates.
Nitro supported startup and actual M core image/env/repaired-source hashes are
verified. Control identity preserved; LEADER/ready term9/index1553. The attempted
checked-in responder replacement exposed D06; no further physical startup now.

Nettking and Nitro cores run M; Nitro responder replacement failed, current
listener is absent: both old/new processes exited and5151 is free. Do not restart
to hide D06. Core services remain M and stable; secret metadata unchanged.
Recorder protected production6fb77c/separate voterfba508 have not been changed.
No physical PASS; P07/P12
not started. Protected Recorder data remains out of bounds. The user's latest
D04 instruction authorizes controlled removal/disablement of the stale Nitro
responder when that stage becomes actionable; this supersedes the earlier
dedicated-port proposal. Verify ownership immediately before acting and retain
evidence. D04 is environment work, never folded into a product PR.

Retain valid exact-head PASS, native checkout/log proofs, registry and ICSE
artifacts. Poll only these three PRs and required qualification; inspect logs only
for newly completed/failed jobs or contradictions. No duplicate dispatches. No
merge before exact-head qualification and relevant correctness findings resolve.
No intermediate merged-main qualification or per-PR physical restart.

## September11 13:04UTC — checkpoint branch recovery

Git fetch and targeted ls-remote found the diagnostic branch absent on GitHub.
The three intended repair refs and main were unchanged. The clean local
checkpoint was6483e95fed889b05553e4d2e311d0eedb56722ff, previously pushed and
referenced by the diagnostic artifacts. Restored that exact branch with a normal
non-force push. No source/runtime/host/CI state changed. Cause of remote branch
absence is unknown and is not a product finding.

Next exact action: read only current PR heads/state and required workflow job
state; compare against diagnostics/post-sweep-qualification-checkpoint.json.
Retain new completed evidence, classify failures before changing code, and
dispatch only proven required gaps. Publish each meaningful transition here.

## September11 13:06UTC — required CI advanced, no head change

All three intended heads and main remain unchanged. Newly completed jobs are
successful:456 ICSE Compose/Linux entrypoint;457 Windows phase2, both retirement
jobs and Linux operator surface;461 three Linux release shards/PostgreSQL,
registry metadata, branding and Linux acceptance harness. Other required jobs
remain queued/in progress; no new failure observed. These are API states pending
new native checkout proof, not an exact-head PASS declaration. Detailed delta:
[qualification-last-transition.json](diagnostics/qualification-last-transition.json).

State cache and [poll helper](diagnostics/poll_required_state.py) now constrain
recurring reads to the requested PRs/workflows and skip unchanged job fetches.
The existing ten-minute follow-up was updated to low-token state-driven rules
and the user's controlled D04 procedure. Next: retain only newly completed native
logs, then handle proven exact-head gaps for completed461 workflows; no duplicate
dispatch, active-job cancellation, merge or host change.

## September11 13:09UTC — new native proofs retained

Downloaded only13 newly completed job logs; existing logs were not reread.
Seven new successful jobs prove intended exact heads:456 two,457 four,461 one.
Six other461 successes prove synthetic checkout
e03addedbc4d10e39e7c6a592977f45995ec6b8e, including its completed branding and
registry workflows; they cannot qualify5b826c68. No new test failure.
[Accumulating native provenance](diagnostics/qualification-native-provenance.json)
retains prior and new records with log digests. [Delta retention helper](diagnostics/retain_changed_jobs.py)
uses only newly completed jobs on subsequent polls.

Next: dispatch branding and registry on the unchanged461 branch only if no exact
run exists; both prior runs are complete and their source mismatch is proved.
Other active/queued jobs remain untouched. No merge/D04/runtime action yet.

## September11 13:11UTC — two proven PR461 gaps dispatched

Branding and immutable registry metadata were dispatched on exact
5b826c6806ab1bdb960412ba20ca78192971fb1d after rechecking PR/ref and absence of
existing exact runs. Both previous runs had completed with proved synthetic
checkout. [Dispatch ledger](diagnostics/completed-head-gap-dispatches.json)
contains native proofs, workflow digests and204 receipts. No passing exact-head
job was rerun; no active job cancelled. No PR is yet declared merge-ready.

Next actionable trigger: completion/failure of required pending checks. Use
poll_required_state.py; if material_change=false, return a minimal no-change
status and stop substantial work. If true, first persist the transition, then
retain_changed_jobs.py for only new completed logs. Terminal-run job lists are
cached; non-terminal jobs are refreshed because the parent can remain queued
while jobs progress. Continue exact-head gap reconciliation for461 after its
remaining PR runs complete; preserve456/457 evidence. Do not touch D04 until
final merged-main qualification is complete. The recurring follow-up remains
active and quiet on unchanged state.

## September11 13:21UTC — required checks advanced

Heads unchanged. New successful completions:456 Windows release checks and branding;457 ICSE publication bundle, Windows operator/CFI2 and Linux CF7B;461 additional Linux release work, exact registry run34602732775 and Linux sharding. No new failure; required jobs still pending and no merge-ready verdict. The detailed state/delta are persisted. Next: retain only new logs, then verify newly completed registry/ICSE artifacts; no host action or duplicate dispatch.


New native logs retained:8 additional successful exact-head proofs (456 two,457 four,461 two);4 further461 jobs used synthetic e03added and are excluded from exact-head qualification. Prior proofs remain preserved. No new failures. Registry461 and ICSE457 artifact review is next.


## September11 13:24UTC — completed artifact review retained

PR457 ICSE run34595242291 is verified against exact143fe7a9: the public source
export matches all1409 immutable Git archive files; both ZIP/checksum layers,
metadata and six retained GitHub artifact digests agree. Component results are
4/4 on Linux, Windows and Compose; both native network results have all10
required checks and complete owned-process teardown. No rebuild or test rerun.
PR461 registry run34602732775 proves exact5b826c68 and successful pinned digest /
architecture verification; this workflow intentionally emits no artifact.

[Artifact review](diagnostics/qualification-artifact-review-1321.json),
[artifact retention receipt](diagnostics/pr457-icse-artifact-retention.json), and
[review procedure](diagnostics/review_completed_icse_registry.py) preserve the
results. Required jobs remain pending; no merge-ready verdict, merged-main
qualification, candidate freeze, D04 action or physical acceptance claim.

Next actionable trigger: required job completion/failure or intended PR head
change. Run poll_required_state.py on the next scheduled check, persist any
meaningful transition first, then retain only new evidence. Preserve existing
exact-head successes; no duplicate dispatch. Runtime remains
0536f03d67eb277e11573c2188d8e820399627e3, protected Recorder data untouched,
P07/P12 not started.

## September11 13:38UTC — required checks advanced

Intended PR heads are unchanged; no new failures. Completed required workflows:
456 ICSE run34595194849 and phase2 run34593925179;457 phase2 run34595440682;
461 exact branding run34602729385 and synthetic phase2 run34600817414.
Additional successful leaf jobs are retained in qualification-last-transition.json;
other required jobs remain pending. These new API results require native source
proof before an exact-head verdict. No PR is yet declared merge-ready.

Next: retain only newly completed native logs, verify newly completed456 ICSE
artifacts, then reconcile the completed461 phase2 source gap. Preserve all valid
prior evidence and leave active CI untouched. Runtime remains0536f03d, protected
Recorder data untouched; no host, D04, candidate or physical acceptance action.

Native delta retained:16 new logs. Six successful jobs prove exact intended
heads (456 two,457 two,461 two); nine461 leaves prove synthetic e03addedbc4
and are excluded. Its suite-order aggregate has no independent checkout and
cannot qualify the intended head. No new failures. Completed461 phase2 has
both leaf proofs for the synthetic checkout, establishing a required exact-head
gap; no exact phase2 run is presently recorded.

PR461 exact phase2 was dispatched on unchanged5b826c68 after checking current
PR/ref, absence of an exact run and native source mismatch in both completed
prior leaves. The updated completed-head-gap-dispatches.json records both
proof digests, workflow digest and204 receipt. No active job was cancelled or
valid exact-head PASS rerun. Next: retain/audit completed456 ICSE artifacts;
remaining qualification awaits state changes.

## September11 13:41UTC — PR456 ICSE artifacts verified

Completed exact-head run34595194849 has six retained native GitHub ZIPs with
matching API digests. The bundle source equals all1408 public immutable Git
archive files at1a0c634f; both checksum layers and embedded metadata agree.
Component evidence is4/4 on Linux, Windows and Compose; Linux/Windows network
evidence has10 required checks each and complete owned-process teardown.
[Review](diagnostics/pr456-icse-artifact-review.json),
[artifact receipt](diagnostics/pr456-icse-artifact-retention.json), and
[review procedure](diagnostics/review_pr456_icse.py) are retained. No rebuild,
software rerun or physical acceptance claim.

Next scheduled check: poll_required_state.py; preserve unchanged evidence and
retain only new native job results after persisting meaningful transitions.
Required gates are still pending across the fix set, including the newly
dispatched exact461 phase2. No PR merge/candidate freeze yet. Runtime remains
0536f03d67eb277e11573c2188d8e820399627e3; D04 deferred until final merged-main
qualification; protected Recorder data untouched; P07/P12 not started.

## September11 13:52UTC — new required job completions

Heads unchanged; no new failures. PR457 capability/product Windows regressions
completed successfully; transport/storage is now running. PR461 exact phase2
run34605553188 has Linux success and Windows queued; exact harness34601191261
is complete/success. Its prior synthetic CF7B34600817477 and ICSE34600817567
are now complete/success. Detailed state delta is retained; remaining required
gates still pending, no merge-ready verdict. Next: retain only new native proofs,
then fill completed CF7B/ICSE exact-head gaps only if the proofs confirm mismatch
and no exact run exists. No candidate, host or protected-data change; runtime
remains0536f03d67eb277e11573c2188d8e820399627e3. P07/P12 not started.

Six new native logs retained: three successful exact-head proofs (457 Windows
capability/product,461 Linux phase2 and Windows harness); three461 CF7B/ICSE
leaves prove e03addedbc4 and are excluded. Together with prior retained leaves,
both completed workflows lack intended5b826c68 checkout proof. Exact harness
now has both platform proofs. Next: dispatch only these two demonstrated461
CF7B/ICSE gaps after live absence/head checks; no synthetic artifact review or
rerun of existing exact-head PASS.

## September11 13:53UTC — two proven exact-head gaps dispatched

PR461 CF7B and ICSE were dispatched on unchanged5b826c6806ab1bdb960412ba20ca78192971fb1d.
Live PR/ref checks and absence checks passed; every prior matching run was
completed with native synthetic-source proof. The ledger retains proof digests,
workflow digests and204 receipts. No active job was cancelled or valid exact-head
PASS repeated. Both runs will be picked up by the next compact scheduled poll.

Next actionable trigger: completion/failure of required jobs or intended-head
change. Persist the transition first, retain only new proofs, reconcile remaining
461 exact-head gaps after prior workflows complete. No merge-ready verdict yet;
final merged-main qualification/D04/physical campaign remain deferred. Runtime
and protected Recorder state unchanged; no P07/P12 timer or physical PASS.

## September11 14:04UTC — qualification continues without new failures

Heads unchanged. PR457 Windows transport/storage regressions and CF7B Windows
completed successfully; CF7B run34595260905 is complete. PR461 new exact
CF7B34606865363 has Linux
success; exact ICSE34606870244 has Compose/Linux success, Windows pending.
PR456 Windows software-update is now running. Required gates remain pending.
Next: retain only these five newly completed native proofs, then await the next
required job transition. No merge, duplicate dispatch, candidate or host change;
physical runtime remains0536f03d67eb277e11573c2188d8e820399627e3; protected
Recorder data untouched; P07/P12 not started.

All five newly retained successful native logs prove their intended exact PR
heads (457 two,461 three); no mismatch or new failure. The accumulating
qualification-native-provenance.json preserves log digests and source proof.
No additional exact-head gap became actionable in this check. Next scheduled
action: poll_required_state.py; act only on material changes, preserve all valid
PASS, and do not dispatch duplicates. Final merged-main/D04/physical acceptance
remain deferred while required PR gates finish.

## September11 14:16UTC — three required workflows completed

Intended heads unchanged; no new failure. PR456 software-update34594937633
completed successfully. PR461 exact CF7B34606865363 and readiness34601194107
completed successfully. PR457 Windows software-update is running. Detailed
state delta is retained. Next: retain the three new native leaf proofs and
assess remaining gates from the cached state. No new dispatch, merge or host
change; runtime remains0536f03d67eb277e11573c2188d8e820399627e3; protected
Recorder data untouched; P07/P12 not started.

Three new successful native logs prove the exact intended heads (456 one,
461 two); both461 CF7B/readiness now have platform proofs. No new failure.
Cached remaining gates:456 three Windows release regressions;457 Windows
software-update, release checks and shard contract;461 Windows phase2, ICSE
and shard contract plus still-pending synthetic release/software-update/
retirement/CFI2/operator workflows, which will need exact-head reconciliation
after completion. No PR is merge-ready. Existing valid evidence is preserved.

Next scheduled action: poll_required_state.py. Act on new completions/failures
only; persist first, retain new proofs and dispatch demonstrated missing exact
workflows without rerunning valid PASS. No new artifact review is actionable.
Final-main qualification, D04 and physical acceptance remain deferred.

## September11 14:27UTC — PR457 required workflows complete

PR457 software-update34601202338, release34594990947 (including both matrices
and final verdict), and sharding34595257295 completed successfully. The cached
required workflow set is now complete by API status; exact-head final proof and
release artifact/correctness reconciliation remain required before merge.
Heads unchanged, no new failure;461 Windows retirement is now running.

Next: retain six new457 job logs, audit newly produced release artifacts and
reconcile all required exact-head evidence plus unresolved release-relevant
correctness findings. Do not infer merge readiness from API greens alone.
Runtime remains0536f03d67eb277e11573c2188d8e820399627e3; no host/D04/physical
change, protected Recorder data untouched, P07/P12 not started.

PR457 six new native logs retained: five successful jobs prove143fe7a9,
including both release matrices; the successful final aggregate has no checkout
step and must be reconciled against its required dependency jobs. No new error.
Next: audit native release artifacts and the completed gate graph, then review
current release-relevant PR findings before any readiness/merge declaration.

PR457 final native release artifact review is complete: all9 GitHub ZIP digests
match; four shards cover4397 unique tests, and both full-suite orders contain
4397 cases. All JUnit failures/errors are zero. Windows groups contain872,
371 and298 cases. Exact native source proof is retained for every source-running
job; the only two jobs without checkout are declared dependency aggregates.
[Release artifact review](diagnostics/pr457-final-release-artifact-review.json)
and [receipts](diagnostics/pr457-final-release-artifact-retention.json) are stored.
The live PR review snapshot has no inline correctness findings or review objects;
its review summary is complete at the intended head. Next: reconcile intentional
platform skips and aggregate dependencies, then publish exact-head qualification
verdict. No physical evidence or host state changed.

PR457 gate graph and test identities are verified:37/37 required jobs plus
CFI2 and registry3/3 are successful at the intended source; both checkout-free
aggregates assert their successful same-run dependencies. Full-order JUnit test
identities exactly match4397 collected shard identities. Thirteen skipped
platform cases have explicit passing JUnit evidence elsewhere; remaining
Windows-only/PostgreSQL skips need reconciliation against native non-artifact
jobs before the final audit is closed. This is evidence review, not a newly
confirmed defect or justification to rerun tests. The intermediate report and
repeatable verifier are persisted. Next: inspect only retained native commands/
results covering those named cases; keep all hosts unchanged.

## September11 14:35UTC — PR457 required exact-head qualification PASS

Exact143fe7a9082193114af3d34dc437b85f845849a2 has37/37 required jobs PASS,
CFI2 both platforms PASS, immutable registry metadata PASS, release-verdict PASS,
and reviewed release/ICSE artifacts. The two checkout-free aggregates are bound
to verified same-run source dependencies. No reported correctness finding on
the current head; review summary is completed. Full qualification receipt:
[pr457-final-qualification.json](diagnostics/pr457-final-qualification.json).

Skip reconciliation is complete within the existing required gate scope:
13 distinct skipped cases pass in other JUnit results; seven PostgreSQL cases
are covered by the dedicated11/11 native job, and three Recorder launcher cases
by the Windows discovery boundary145/145 job. Eleven remaining POSIX exclusions
have explicit unchanged Windows-only guards; no all-platform execution claim
is made for them. Native update suite also includes migration/volume-selector
modules (311 PASS, five skips). No skipped case was silently relabeled PASS,
no assertion changed and no additional software execution was needed.

Preserve this qualification; do not rerun unchanged457. Follow the requested
sequence by completing456/461 qualification before the separate merge batch.
Immediately before each merge recheck exact head, repository merge gates and
relevant findings. Main pushes automatically trigger release CI, so do not
intentionally qualify an intermediate main; reconcile the final push's actual
main run once the full fix set is merged. No new candidate is selected now.

Next scheduled action: compact poll for456/461 required job changes (457 state
only for head/merge changes); persist transitions and retain only new native
proofs. Runtime remains0536f03d67eb277e11573c2188d8e820399627e3; no deployment,
D04 action or protected Recorder data change. Physical acceptance remains
incomplete; P07/P12 have not started.

## September11 14:46UTC — PR461 workflow transitions

Heads unchanged; PR457 qualification remains valid, no new failures. PR461 exact
phase2 run34605553188 completed successfully. Prior synthetic software-update
34600817276 and retirement34600817428 completed successfully; journal/artifacts
Windows regressions are running. Next: retain three new native leaf proofs,
then dispatch only demonstrated completed exact-head gaps after live absence/
head checks. No merge, host or protected-data change; runtime remains
0536f03d67eb277e11573c2188d8e820399627e3. P07/P12 not started.

New461 native proofs: Windows phase2 proves exact5b826c68, completing both
platforms. Software-update and retirement Windows leaves prove e03addedbc4;
with retained Linux proofs, both completed workflows are demonstrated exact-head
gaps. No test failure. Next: dispatch those two gaps only after live checks;
retain current456/457 evidence and leave active release CI untouched.

## September11 14:47UTC — two PR461 source gaps dispatched

Exact software-update and retirement were dispatched on unchanged
5b826c6806ab1bdb960412ba20ca78192971fb1d after live PR/ref and absence checks.
Both prior workflows were complete; every leaf had native synthetic-source
proof. completed-head-gap-dispatches.json retains proofs, workflow digests and
204 receipts. No active job cancelled or valid exact-head PASS rerun.

Next scheduled action: poll_required_state.py. Preserve457 qualification and
all other valid evidence; retain only new completions. Remaining461 synthetic
release/CFI2/operator paths need reconciliation after completion. No new ICSE
review is actionable yet. Runtime and protected Recorder state remain unchanged;
no merge/final-main qualification/D04 action/physical acceptance or P07/P12.

## September11 14:58UTC — PR461 exact retirement completed

Heads unchanged;457 qualification remains valid. PR461 exact retirement
34612131170 is complete/success on both platforms. Exact software-update
34612126881 has Linux success and Windows queued. Synthetic release journal/
artifacts completed successfully; capability/product is running. No new failure.
Next: retain four new native logs; no new completed exact-head gap is actionable
from this delta. Preserve all existing PASS. No merge or host action; runtime
remains0536f03d67eb277e11573c2188d8e820399627e3, protected Recorder untouched,
P07/P12 not started.

Four new native proofs retained: exact5b826c68 for both retirement platforms
and Linux software-update; synthetic e03addedbc4 for release journal/artifacts,
which remains excluded. No errors or new dispatch requirement. Next scheduled
action: poll_required_state.py, preserving457 qualification and all exact PASS;
retain only new completions and reconcile461 release/CFI2/operator after their
prior runs complete. No physical evidence, candidate or host state changed.

## September11 15:09UTC — PR461 exact ICSE completed

Heads unchanged;457 qualification remains valid, no new failure. PR461 exact
ICSE34606870244 completed successfully, including native Windows entrypoint and
publication bundle. Synthetic release capability/product completed successfully.
PR456 transport/storage Windows regressions are running. Next: retain three new
native proofs, then retain/review only the newly completed exact461 ICSE artifacts.
No merge or host action; runtime remains0536f03d67eb277e11573c2188d8e820399627e3,
protected Recorder untouched, no physical PASS and no P07/P12 timers.

New461 native proofs retained: both ICSE completions prove exact5b826c68,
completing four native ICSE source proofs. Release capability/product proves
synthetic e03addedbc4 and remains excluded. No test failure. Next: retain/review
the exact ICSE bundle; do not inspect synthetic publication artifacts or rerun
any qualification job.

## September11 15:11UTC — PR461 ICSE artifacts verified

Exact5b826c6806ab1bdb960412ba20ca78192971fb1d ICSE run34606870244 has six
retained native ZIPs matching GitHub digests. Public source equals all1408
immutable Git export files; both checksum layers and embedded metadata agree.
Component evidence is4/4 on Linux, Windows and Compose; both native network
results have all10 required checks and complete owned-process teardown.
[Review](diagnostics/pr461-icse-artifact-review.json),
[receipts](diagnostics/pr461-icse-artifact-retention.json), and
[procedure](diagnostics/review_pr461_icse.py) retain the evidence. No rebuild,
software rerun or physical acceptance claim.

Next scheduled action: poll_required_state.py, preserving qualified457 and all
other valid evidence. Await456 release and461 pending gates; reconcile remaining
synthetic release/CFI2/operator workflows after completion. No new candidate or
merge yet; no D04/host/protected Recorder action; P07/P12 not started.

## September11 15:22UTC — required Windows jobs advanced

Heads unchanged;457 qualification remains valid, no new failure. PR456 exact
release34595427316 has transport/storage and journal/artifacts success;
capability/product is running. PR461 exact sharding34601196761 completed
successfully on both platforms. Next: retain three new native proofs; wait for
456 release completion before its complete artifact review. No duplicate jobs,
merge or host action; runtime remains0536f03d67eb277e11573c2188d8e820399627e3,
protected Recorder untouched, no physical PASS and no P07/P12 timers.

All three new successful native logs prove intended exact heads:456 two Windows
release groups and461 Windows sharding. No mismatch or new failure. Preserve
these proofs and457 qualification. Next scheduled action: poll_required_state.py;
on456 release completion retain the remaining logs, review complete native
release artifacts and reconcile its final required gate verdict. For461 retain
new evidence and reconcile synthetic release/CFI2/operator only when complete.
No host, protected-data or physical acceptance change.

## September11 15:33UTC — PR456 release and PR461 synthetic release complete

Heads unchanged;457 qualification preserved. PR456 exact release34595427316
completed successfully, including both matrices and verdict. Its required set
is now complete by API; final native/artifact/gate review remains before the
qualification verdict. PR461 prior synthetic release34600817254 also completed
successfully; Windows operator surface is running. No new failure.

Next: retain eight new native logs and reconcile checkout-free aggregates.
Dispatch461 exact release only after proving all source-running prior leaves
used the synthetic checkout and no exact release run exists. Then complete456
native release/artifact/correctness review. No merge or host action; runtime
remains0536f03d67eb277e11573c2188d8e820399627e3, protected Recorder untouched,
no physical PASS and no P07/P12 timers.

Eight new native logs retained: three456 jobs prove exact1a0c634f and its final
verdict is checkout-free; three461 leaves prove e03addedbc4 and its final verdict
is checkout-free. Combined461 release provenance has14 source-running leaves
at the synthetic checkout and two dependency-only aggregates with no checkout.
None qualifies5b826c68. Next: guarded exact461 release dispatch; preserve all456/
457 proofs and perform no synthetic release artifact review.

## September11 15:34UTC — exact PR461 release dispatched

Exact release was dispatched on unchanged5b826c6806ab1bdb960412ba20ca78192971fb1d
with current head/ref and absence checks. Completed prior release has14 proved
synthetic source leaves and exactly two known checkout-free aggregates; all16
native log digests are recorded in completed-head-gap-dispatches.json with the
workflow digest and204 receipt. This fills a missing exact-head gate; no valid
PASS was rerun and no active CI cancelled. Next: finalize456 using its completed
native release artifacts and existing ICSE/registry evidence. Hosts unchanged.

PR456 complete release artifact review: all9 native ZIP digests match;
four disjoint shards cover4381 tests and both full-suite orders contain4381
cases. All JUnit failures/errors are zero. Windows groups contain872,371,298
cases. Source proof and full artifact review/receipts are retained. No current
inline findings or review objects. Automated review summary coversa225a41;
manual comparison to intended1a0c634f shows only five workflow lines adding the
focused POSIX readiness regression step, with no product change since that
review. Next: reconcile final aggregate/test identities and expected skips,
verify the new step's native results, and publish456 exact-head verdict.

## September11 15:38UTC — PR456 required exact-head qualification PASS

Exact1a0c634f47f8a247b6d1d2d1a219f5c12590587d has37/37 required jobs PASS,
CFI2 both platforms and registry3/3 PASS, release-verdict PASS, reviewed complete
release/ICSE artifacts, and both checkout-free aggregates bound to same-run
successful source dependencies. Both full-suite orders match all4381 collected
test identities. [Final receipt](diagnostics/pr456-final-qualification.json)
and [verifier](diagnostics/finalize_pr456_qualification.py) preserve the result.

Expected skip reconciliation:13 distinct cases have passing JUnit elsewhere;
seven PostgreSQL cases are covered by its native11/11 job. Fourteen remaining
POSIX exclusions retain their explicit unchanged Windows-only guards; no
execution claim is made for skipped cases. No new code/test/guard change.
Automated reviewa225a41 reported no findings; the only later change adds five
workflow lines for readiness tests. That delta was manually reviewed and the
exact native release steps prove18/18 readiness cases on both platforms.

PR456 and457 are now qualified; preserve both unchanged heads and results.
PR461 is the remaining required fix qualification, including the just-dispatched
exact release and remaining software-update/CFI2/operator gates. Next scheduled
action: poll_required_state.py, retain only new proof and dispatch only completed
source-mismatch gaps. Complete461, then perform separate merges with live head/
merge-gate/correctness checks and qualify actual final merged main once.

No PR merged or new candidate selected in this check. Physical runtime remains
0536f03d67eb277e11573c2188d8e820399627e3; D04 deferred, protected Recorder
untouched, physical acceptance incomplete and P07/P12 not started.

## September11 15:49UTC — PR461 final source gaps became actionable

Heads unchanged;456/457 qualification remains valid. Exact461 release
34616905954 has fixed-order success and rotating/Windows transport running;
other release leaves are queued. Exact software-update34612126881 completed
successfully. Prior synthetic CFI2 run34600817433 and operator34600817582 are
now complete/success. No new failure. Next: retain four new native proofs, then
dispatch only demonstrated completed CFI2/operator exact-head gaps after live
checks. No merge or host action; runtime remains0536f03d67eb277e11573c2188d8e820399627e3,
protected Recorder untouched, physical acceptance incomplete, no P07/P12 timers.

Four new native proofs retained: exact5b826c68 for fixed full-suite order and
Windows software-update; synthetic e03addedbc4 for Windows CFI2/operator.
Together with their prior Linux proofs, both completed workflows are demonstrated
exact-head gaps. Software-update is now proved on both platforms. Next: guarded
CFI2/operator exact dispatches, preserving all qualified heads and active CI.

## September11 15:50UTC — final PR461 source gaps dispatched

CFI2 and operator surface were dispatched on unchanged5b826c6806ab1bdb960412ba20ca78192971fb1d
after live head/ref/absence checks and proof that every prior completed leaf
used the synthetic checkout. Ledger stores native/workflow digests and204
receipts. Every required461 workflow now has an exact-head dispatch; no valid
PASS rerun or active CI cancellation. No additional dispatch is presently needed.

Next scheduled action: poll_required_state.py; retain only new required results.
Await461 release/CFI2/operator completion, then reconcile complete release
artifacts and final qualification using already retained ICSE/registry proofs.
Preserve456/457 qualified verdicts. After461 qualification, separate merge batch
with live checks, then actual final-main qualification once. Hosts/Recorder data
remain untouched; no new candidate, D04 action or physical acceptance claim.

## September11 16:01UTC — exact PR461 release advanced

Heads unchanged;456/457 qualifications remain valid. PR461 exact release
34616905954 gained successful Windows transport/storage, release checks,
capability/product, Linux shard1 and rotating full-suite order. Shard0 and
Windows journal/artifacts are running. Exact operator34618403669 has Linux
success; CFI2 run34618400138 is queued. No new failure. Next: retain six new
native proofs, then await remaining gates; no duplicate dispatch or partial
artifact review needed. Runtime remains0536f03d67eb277e11573c2188d8e820399627e3,
protected Recorder untouched, no merge/candidate/physical acceptance action.

All six newly retained successful native logs prove exact5b826c68. No mismatch
or new failure. Next scheduled action: poll_required_state.py; retain only new
results, finalize complete461 release artifacts when ready, then reconcile its
full required gate verdict. Preserve qualified456/457 and existing461 ICSE/
registry proofs. No further dispatch presently required; all hosts/protected
Recorder data unchanged, P07/P12 absent and physical acceptance incomplete.

## September11 16:12UTC — PR461 release regression leaves complete

Heads unchanged;456/457 qualifications remain valid. Exact461 release
34616905954 gained successful shards0/2/3, Linux release checks, PostgreSQL
and Windows journal/artifacts. All regression leaves are now complete; release
matrices/order aggregate/verdict remain pending. CFI2 runs on both platforms.
No new failure. Next: retain six new native proofs, then await final gates before
complete artifact/verdict review. No duplicate dispatch, merge or host action;
runtime remains0536f03d67eb277e11573c2188d8e820399627e3, protected Recorder
untouched, physical acceptance incomplete and P07/P12 absent.

All six new successful native logs prove exact5b826c68. No mismatch or new
failure. Next scheduled action: poll_required_state.py; retain new aggregate/
CFI2/operator results, then finalize461 complete release artifacts and required
qualification. Preserve456/457 PASS and existing461 ICSE/registry proofs.
No additional workflow dispatch is needed. Runtime/protected Recorder state
unchanged; no physical acceptance or P07/P12 action.

## September11 16:24UTC — PR461 required workflows complete

Heads unchanged;456/457 qualifications remain valid. Exact461 release34616905954
completed successfully, including both matrices/order aggregate/verdict. CFI2
34618400138 and operator34618403669 are complete/success. No new failure.
Next: retain seven new native proofs, audit complete461 release artifacts and
current correctness findings, reconcile final qualification. If complete,
proceed to live premerge checks and the three separate merges, then qualify
actual resulting main once. No head/result inference from synthetic runs.
Runtime remains0536f03d67eb277e11573c2188d8e820399627e3; hosts and protected
Recorder untouched, physical acceptance incomplete, P07/P12 absent.

Seven new native proofs retained: five jobs prove exact5b826c68; the successful
order/verdict jobs are the two known checkout-free dependency aggregates.
No mismatch or new failure. Next: complete native release/artifact/gate review
and current PR findings before recording461 qualification or merging.

PR461 complete release artifact review: all9 native ZIP digests match; four
shards cover4368 test identities and both full orders contain4368 cases. All
JUnit failures/errors are zero. Current PR has no comments/reviews/findings.
Manual source review confirms only one Compose environment line plus five
real-render regression cases; default/custom ports preserve the shared resolver.
No authority/storage change. Final gate/skip/test-identity audit is next.

Branch-protection/rules metadata reads returned403 with the existing credential;
this is an audit visibility limitation, not a product failure. Use normal merge
APIs with expected-head guards and server enforcement; never bypass protections.
Persisted snapshot: merge-branch-protection.json. No merge attempted yet.

## September11 16:28UTC — complete required fix set qualified

PR461 exact5b826c6806ab1bdb960412ba20ca78192971fb1d has37/37 required jobs PASS,
CFI2/registry3/3 PASS, release-verdict PASS, reviewed ICSE/release artifacts and
source-bound dependency aggregates. Both full orders match all4368 collected
test identities; all five new Compose cases passed unskipped in each order.
Expected skip reconciliation preserves13 cross-platform JUnit passes, seven
PostgreSQL cases in its native11/11 job, and14 unchanged POSIX platform exclusions.
Manual review of the two changed files found no correctness issue; no reported
findings exist. [Final receipt](diagnostics/pr461-final-qualification.json) and
[verifier](diagnostics/finalize_pr461_qualification.py) retain the result.

All three intended PR heads are now qualified. Next: live expected-head and
review checks; mark drafts ready and merge456,457,461 separately through normal
GitHub enforcement, persisting each transition immediately. Do not update/rebase
qualified PR heads or bypass required gates. Then use actual final merged main
and its automatic push qualification, filling only genuinely missing required
workflows once. No intermediate main is a candidate or physical evidence.

Runtime remains0536f03d67eb277e11573c2188d8e820399627e3; no host/D04/Recorder
action yet, no physical acceptance PASS and no P07/P12 timers.

Live batch preflight confirms all intended heads unchanged, no inline/review
findings, and mergeable source trees. Main remains0536f03d. PR456 was marked
ready on exact1a0c634f through GitHub at16:27:57UTC; receipt retained. Next:
normal expected-head merge456 with GitHub's protection enforcement. No host or
physical acceptance changes.

## September11 16:29UTC — PR456 merged normally

GitHub merged qualified1a0c634f as63976fb3aa24fffd25d9ee407572e61180ab5315
with an expected-head guard and normal merge enforcement. Receipt: pr456-merge.json.
This is intermediate main, not a new candidate or qualified merged main. No
explicit intermediate-main qualification will be dispatched. Next: refresh457
head/findings, mark ready and merge it separately; preserve its exact-head PASS.
Runtime/Recorder state unchanged, physical acceptance not restarted.

PR457 metadata still reports the old base SHA, while git ls-remote, a fresh Git
ref API read and PR456 merged record all confirm actual main63976fb3. This is
metadata freshness, not a failed merge or product defect. Qualified457 head
remains143fe7a9; no findings. PR457 marked ready at16:29:53UTC. Next: normal
expected-head merge457 against actual current main; no head rewrite or gate bypass.

## September11 16:30UTC — PR457 merged normally

GitHub merged qualified143fe7a9 as8948e953bd2f0cb063124ff2065ed5dd90f5f4a3
with expected-head guard and normal enforcement. Receipt: pr457-merge.json.
This remains intermediate main. Next: fresh461 head/findings check, mark ready
and merge separately; then verify actual final main and complete fix ancestry.
No intermediate qualification dispatched; runtime/protected Recorder unchanged.

Fresh461 premerge snapshot confirms unchanged5b826c68, no findings, mergeable
source and actual main8948e953. PR461 marked ready at16:31:22UTC. Receipts retain
actual ref separately from stale PR base metadata. Next: normal expected-head
merge461; then verify final main ancestry and select its qualification runs.
No candidate or physical state change.

## September11 16:32UTC — all three required fixes merged

GitHub merged qualified461 head5b826c68 as9b286f931497bf6291e215f6340443c5162826b0
with expected-head guard and normal enforcement. All required fixes are merged
separately; receipts retained for456/457/461. This final main still requires its
own complete qualification; no new AUTHORITATIVE_SHA is frozen yet.

Next: verify actual main and qualified-head ancestry, retain the automatic final
push runs and dispatch only required workflows absent for this final SHA. Do not
reuse individual PR PASS as merged-main qualification or qualify intermediate
main intentionally. D04 remains deferred until final main qualifies. Runtime
and protected Recorder unchanged; physical acceptance/P07/P12 not restarted.

## September11 16:33UTC — actual final main verified; qualification in flight

Git fetch confirms actual main9b286f931497bf6291e215f6340443c5162826b0. All three
qualified PR heads are ancestors; final parent pair is8948e953 +5b826c68. A clean
detached audit worktree exists atC:\wsl\fcp-v1-9b286f93-merged-main-20260911;
no runtime deployment occurred. The final push already created release34622528054
and nine other required/additional workflows. Preserve these; do not duplicate.

The exact final snapshot lacks only harness, readiness and sharding workflows.
Next: guarded dispatch of those three missing workflows at current main9b286f93,
then compact final-main polling and native source retention. PR-head software
PASS remains separate. Final main is not yet qualified or frozen as a physical
candidate. Runtime/Recorder unchanged; D04/P07/P12 not started.

## September11 16:35UTC — final-main qualification gaps filled once

Only missing harness, readiness and sharding were dispatched on verified actual
main9b286f931497bf6291e215f6340443c5162826b0. Each dispatch rechecked main/ref and
absence of any matching final-SHA run; ledger stores workflow digests and204
receipts. Existing final push runs are preserved; no duplicate release job or
intermediate-main qualification was requested. Next: compact final-main poll and
new-only native retention, then final artifact/verdict review when complete.

## September11 16:37UTC — final-main delta polling established

The required final-main set is present: automatic push runs plus guarded
harness34622777814/readiness34622782096/sharding34622786434 dispatches. Exact
main remains9b286f93. New successful API results are ICSE Compose and Linux
operator; branding was already successful in the initial snapshot. Final release
34622528054 remains pending. Next: retain only these new completed native logs.

Use poll_merged_main_state.py from now on; it reads only actual main and the
selected final SHA's required workflows, caches terminal jobs and writes only
material transitions. retain_merged_main_jobs.py preserves source-bound proof
without rereading completed logs. No PR polling or duplicate qualification is
needed. The automated follow-up will be updated to this stage after persistence.

Three completed final-main jobs now have native exact9b286f93 proof: ICSE Compose,
Linux operator and branding. Log digests are preserved in
merged-main-native-provenance.json; no new failure. Qualification is still
pending. Next: update recurring follow-up to final-main-only state polling;
then await required completions without duplicate runs or host changes.

## September11 16:39UTC — recurring execution moved to final-main qualification

Automation fortsett-federation-v1-p-nettking remains ACTIVE every10 minutes and
now follows poll_merged_main_state.py / retain_merged_main_jobs.py. It preserves
all PR qualification, selects only actual final9b286f93 runs, and stays quiet on
ordinary pending progress. No duplicate qualification or PR merge will be started.

Exact next command from this diagnostic checkout:
C:\wsl\fcp-v1-fba508-nettking-20260910\.venv\Scripts\python.exe -B handoff/diagnostics/poll_merged_main_state.py
On a material change persist before retaining/reviewing new evidence. Once all
final-main gates and artifacts qualify, D04 is the next controlled host action,
followed by candidate freeze, checked-in revalidation and fresh physical tests.
Runtime remains0536f03d67eb277e11573c2188d8e820399627e3; protected Recorder data
untouched. No physical PASS, no P07/P12 timers, no candidate freeze yet.

## September11 16:49UTC — final-main qualification advanced

Actual main remains9b286f931497bf6291e215f6340443c5162826b0. New successes:
CF7B/phase2/software-update/harness/readiness/sharding Linux, ICSE Windows,
both CFI2 platforms and registry metadata. Release34622528054 now exposes its
queued leaf jobs; other required work is pending/running. No new failure.
Next: retain ten new native proofs, preserving all completed evidence. Registry
emits no artifact by design; its native exact-source successful verification
is the evidence. No duplicate dispatch, host or protected Recorder action;
runtime remains0536f03d, candidate not frozen, physical acceptance incomplete.

All ten newly retained successful native logs prove actual final9b286f93,
including both CFI2 platforms and registry. No source mismatch or new error.
Thirteen final-main job proofs are now retained; required qualification remains
pending. Next scheduled action: poll_merged_main_state.py; retain only new
completions and review complete ICSE/release artifacts when available. No test
reruns, further dispatches, D04 or physical acceptance actions are needed now.

## September11 17:01UTC — final-main Windows gates advanced

Actual main remains9b286f931497bf6291e215f6340443c5162826b0. CF7B, phase2 and
readiness now complete/success on both platforms. New Linux successes: ICSE
entrypoint, retirement and PostgreSQL release check. ICSE publication is queued;
rotating suite and Windows harness are running. No new failure. Next: retain
six new exact-source proofs; await complete artifacts/final gates. No duplicate
qualification, host or protected Recorder action; runtime remains0536f03d,
physical acceptance incomplete and candidate not frozen.

All six new successful native logs prove exact final9b286f93. Nineteen final-main
proofs are retained; no source mismatch or new failure. Next scheduled action:
poll_merged_main_state.py, retaining only new results and reviewing ICSE once
its publication job finishes. Preserve all prior qualification; no new dispatch,
D04, candidate freeze or physical acceptance action while final gates remain.

## September11 17:13UTC — final-main release and Windows checks advanced

Actual main remains9b286f931497bf6291e215f6340443c5162826b0. New successes:
Linux shards1/3 and rotating full order; Windows harness and sharding complete,
finishing both workflows. Fixed order and Windows retirement are running.
No new failure. Next: retain five new native proofs; await complete final gates
and ICSE publication. Preserve all existing PASS, with no duplicate dispatch,
D04/host/protected Recorder action or physical acceptance claim.

Five new successful native logs prove exact final9b286f93;24 final-main proofs
are retained. No mismatch or new failure. Next scheduled action remains
poll_merged_main_state.py, followed by new-only retention and completed-artifact
review when actionable. Runtime remains0536f03d; no candidate freeze or physical
acceptance/P07/P12 action while required final qualification is pending.

## September11 17:24UTC — final-main operator/retirement completed

Actual main remains9b286f931497bf6291e215f6340443c5162826b0. Operator surface
and retirement now complete/success on both platforms. Release gained Linux
shard0, Linux release checks and fixed full order successes; shard2 and Windows
software-update are running. No new failure. Next: retain five new native
proofs, then await remaining gates/publication. No duplicate qualification or
D04/host/protected Recorder action; runtime0536f03d remains unchanged.

Five new successful native logs prove exact final9b286f93;29 final-main proofs
are retained. No mismatch or new failure. Next scheduled action:
poll_merged_main_state.py; retain only new results and review complete ICSE/
release artifacts when actionable. Preserve all prior PASS. No candidate freeze,
physical acceptance/P07/P12 action or further workflow dispatch at this stage.

## September11 17:36UTC — final-main ICSE and software-update complete

Actual main remains9b286f931497bf6291e215f6340443c5162826b0. ICSE34622527886
and software-update34622527937 completed successfully. Release gained Windows
capability/product, Linux shard2 and the order aggregate; Windows transport is
running. No new failure. Next: retain five new native logs, then retain/review
newly complete final-main ICSE artifacts. Preserve all prior PASS; no duplicate
workflow, D04/host/protected Recorder action or physical acceptance claim.

Five new logs retained: four prove exact9b286f93, while the successful suite-order
aggregate has no independent checkout and will be bound to its same-run order
jobs in final review.34 native records now retained. No mismatch/new error.
Next: review the actual final-main ICSE bundle without rebuilding or rerunning
software. Remaining release checks are still pending; no physical PASS inferred.

## September11 17:37UTC — actual final-main ICSE artifacts verified

ICSE run34622527886 at9b286f931497bf6291e215f6340443c5162826b0 has six retained
native GitHub ZIPs with matching digests. Public source equals all1411 immutable
Git export files; both checksum layers and nested metadata agree. Component
results are4/4 on Linux, Windows and Compose; both native network results have
all10 required checks and complete owned-process teardown. No rebuild or rerun.
[Review](diagnostics/merged-main-icse-artifact-review.json),
[receipts](diagnostics/merged-main-icse-artifact-retention.json), and
[procedure](diagnostics/review_merged_main_icse.py) preserve the evidence.

Next scheduled action: poll_merged_main_state.py. Remaining release leaves/
matrices/verdict must complete before final release artifact/qualification review.
Preserve verified ICSE and all existing PASS. No candidate freeze, D04/host/
protected Recorder action or physical acceptance claim; P07/P12 not started.

## September11 17:48UTC — final-main Windows regressions complete

Actual main remains9b286f931497bf6291e215f6340443c5162826b0. Windows transport/
storage and journal/artifacts succeeded; final Windows release checks are now
running. No new failure. Next: retain two new native proofs, then await release
checks/matrices/verdict before final artifact review. No rerun, dispatch or
host/protected Recorder change; physical acceptance remains incomplete.

Both new successful native logs prove exact final9b286f93;36 native records
retained. No source mismatch/new failure. Next scheduled action:
poll_merged_main_state.py. When final Windows release checks and dependent
matrices/verdict complete, review complete release artifacts and final37/37
qualification; reuse verified ICSE/registry evidence. Runtime0536f03d, protected
Recorder data and P07/P12 state remain unchanged. No candidate freeze yet.

## September11 17:59UTC — final merged-main workflows complete

Actual main remains9b286f931497bf6291e215f6340443c5162826b0. Final release
34622528054 completed/success, including Windows release checks, both matrices
and verdict. Required workflow set is complete by API; no new failure. Next:
retain four new native proofs, audit complete native release artifacts and final
gate/source/skip lineage, then persist the merged-main qualification verdict.
Only after that may controlled D04 remediation proceed. No candidate freeze or
physical acceptance inferred yet; runtime0536f03d and Recorder remain untouched.

Four final native logs retained: Windows release checks and both matrices prove
exact9b286f93; the successful verdict is checkout-free. All40 required/additional
job records are now retained (38 source-running jobs, two dependency aggregates).
No source mismatch or new failure. Next: final native artifact/gate review,
using existing ICSE/registry proofs and without rerunning software.

## September11 18:01UTC — complete final-main release artifacts verified

All9 native release ZIPs match GitHub digests. Four disjoint shards cover4420
tests; both full-suite orders contain4420 cases. All JUnit failures/errors are
zero, with recorded expected platform skips. Windows groups contain872,371,298
cases. Exact-source artifact review and receipts are persisted. Next: final
aggregate dependency/test-identity/skip reconciliation and37/37 qualification
receipt, reusing verified ICSE and registry evidence. No tests rerun, no host
changes or physical acceptance claim.

## September11 18:03UTC — actual final merged-main qualification PASS

Exact9b286f931497bf6291e215f6340443c5162826b0 is qualified:37/37 required jobs,
CFI2/registry3/3 and release-verdict PASS.38 actual-source native proofs and two
same-run dependency aggregates are reconciled. Four disjoint shards and both
full orders match4420 test identities. Native readiness is18/18 on each platform;
discovery boundary Windows145/145 covers the three platform skips on Linux.
Expected remaining platform exclusions are recorded, not relabeled as passes.
Complete release and ICSE artifact hashes/source/metadata/teardown are verified.
Actual main ref, clean detached checkout and all three fix ancestors are verified.

[Final qualification](diagnostics/merged-main-final-qualification.json) and
[repeatable audit](diagnostics/finalize_merged_main_qualification.py) are durable.
This completes the one actual final-main software qualification; do not rerun it.
No new AUTHORITATIVE_SHA is frozen and no physical acceptance is claimed yet.

Next highest-value action: controlled D04 Nitro responder remediation. Read the
documented host procedure, verify current identity/port ownership, remove/disable
only the stale5151 responder under the user's authorization, verify expected
service/port/state and persist evidence. Then freeze exact9b286f93, run checked-in
clean revalidation and restart fresh formal physical acceptance under scenario
safety rules. Runtime remains0536f03d67eb277e11573c2188d8e820399627e3; protected
Recorder data untouched, P07/P12 absent. No host mutation has occurred yet.

## September11 18:08UTC — D04 exact stale instance reverified

[Read-only preflight](diagnostics/d04-admission-preflight.json) proves the same
Nitro PID1422341/start120268097/socket31225091, orphan session601, owning5151.
Clean N source, fingerprint and all current core container starts are unchanged.
No host mutation yet. [Controlled procedure](D04_CONTROLLED_HOST_PROCEDURE.md)
rechecks process identity through a PID file descriptor, sends only SIGTERM,
and starts the unchanged checked-in N responder with its existing campaign state.
This resolves current host ownership without prematurely deploying M or rerunning
software. After local/real-peer ownership/health verification and persistence,
freeze qualified M and run clean checked-in revalidation. M needs new runtime
admission/verification before any physical evidence. No Recorder-host access.

### D04 first control precondition stopped without mutation

The controlled script stopped before SIGTERM or any write: the assumed existing
Nitro campaign secret was absent or unreadable. [Exact guard evidence](diagnostics/d04-control-guard-attempt1.json).
The earlier sweep's `secret_file_exists` concerned the legacy responder, not the
current campaign. This is a maintenance precondition, not a new product defect.
Next: distinguish missing versus unreadable candidate secret; if absent, review
normal checked-in responder creation within the verified Nitro-only campaign
mount as an explicit configuration step. Never copy the legacy identity/secret,
overwrite an existing secret or touch Recorder-host data. D04 is still pending.

###18:12UTC — missing Nitro onboarding files isolated

[Path metadata](diagnostics/d04-state-path-preconditions.json) proves both current
campaign secret/PID files absent; their parent directories are private UID1000,
and Flask resolves exactly the same mounted secret path. The procedure now
explicitly permits normal checked-in helper creation of these absent Nitro-only
files, while refusing any overwrite, symlink or legacy identity copy. This is
controlled host configuration, not a product assertion/deadline change. Next:
execute the persisted guarded procedure once, then retain exact results.

##18:13UTC — D04 stale responder replaced, local ownership verified

The guarded PID-file-descriptor SIGTERM stopped only legacy1422341. The unchanged
checked-in N responder now runs as PID1174977/start185384331/native Python3.12.13,
owns socket56386768 on5151, and returns local health200/ready/Tailscale in0.103s.
No stale respawn observed during10s. All core container IDs/images/start times
are unchanged. Normal helper startup created only the previously absent Nitro
campaign secret0600 and PID record; no existing secret replaced or identity copied.
No enrollment/grant request and no Recorder-host access. Source remains clean N.
[Durable result](diagnostics/d04-controlled-replacement.json).

Next: verify health from real Nettking peer, persist D04 resolution in#459 and
freeze qualified9b286f93. New M runtime admission/verification remains required;
this N host configuration observation is not M physical acceptance evidence.

##18:14UTC — D04 environment blocker resolved on current runtime

[Real Nettking peer](diagnostics/d04-real-peer-health.json) receives Nitro5151
health200/ready/Tailscale in0.156s. Combined with exact new process/socket/source
proof and unchanged core starts, this resolves the stale-listener host blocker.
Issue459 receives the durable evidence and is closed as environment remediation.
M main still exactly9b286f93 on GitHub. Next: freeze that qualified SHA, then run
the checked-in impact/revalidation procedure from its clean detached checkout.
All physical observations must be fresh; P07/P12 remain unstarted.

##18:16UTC — one qualified AUTHORITATIVE_SHA frozen

Frozen M=`9b286f931497bf6291e215f6340443c5162826b0`, after all three exact-head
qualifications/merges, the one final merged-main qualification and D04 resolution.
GitHub main and clean detached local checkout reverified. [Freeze receipt](diagnostics/authoritative-candidate-9b286f93.json)
binds the preserved final qualification digest. No software rerun, source repair
or physical deployment. No old physical observations carried forward.

Next exact action: native Python3.12 from clean M runs
`-m catalog.federation.tests.cf7_acceptance.physical_revalidation --baseline 0536f03d67eb277e11573c2188d8e820399627e3 --candidate 9b286f931497bf6291e215f6340443c5162826b0 --repo-root .`.
Retain planner output, including any fail-closed unknown paths. Then reconcile
the checked-in scenario/precondition contract and stage state-preserving runtime
admission. All M physical observations fresh; P07/P12 real durations remain.

##18:18UTC — clean revalidation and M scenario safety review complete

The exact checked-in M planner executed once from clean M with native Python3.12.
Exit2 denies carry-forward due to unknown discovery impact path and requires all12
fresh CF7 observations. [Plan](diagnostics/main-9b286f93-revalidation-plan.json)
and [review](diagnostics/main-9b286f93-revalidation-review.json) preserve that
fail-closed result; no map weakening or software rerun. All prior physical PASS0.

Nine execution/contract files are unchanged N-to-M. [M admission/safety plan](PHYSICAL_M_ADMISSION.md)
records each untimed scenario's stateful/disruptive boundary and prohibits blanket
automate. Startup readiness and Compose port changes were separately reviewed.
Next: fresh host/runtime/updater/pending-request and protected-container metadata,
then stage exact-M source/configuration under existing mutation locks. Do not
touch protected corpus, change voters or start timers before actual admission.

##18:22UTC — Nettking activation preconditions and Recorder invariant

[Nettking read-only metadata](diagnostics/m-nettking-admission-preflight.json):
clean N source, all three exact-N cores running, no OOM/restarts, no ignored
build files, no pending update request or configured capture workload. Existing
model manifest unchanged;95.35GB free disk,6.21millionKiB free RAM, GPU currently
idle. Windows launcher/updater sources are unchanged N-to-M; existing updater
processes remain, so source advance/start will share their checked-in mutex.
The process collector also matched its own PowerShell command; that self-match
does not establish an actual responder or third updater.

[Recorder metadata](diagnostics/m-recorder-admission-metadata.json) reverified
the protected container/image/start/mount invariant without reading record files.
Separate voter remains running FOLLOWER term8/index1551 on clean fba508 source.
Its fingerprint field was not serialized correctly by this audit and must be
rechecked; this is not evidence of a changed physical host or product defect.

Next: run `diagnostics/launch_nettking_m_runtime.py` once. It rechecks source,
owned resolved configuration/mounts/model/headroom, then executes unchanged
checked-in `start.cmd` after a clean N-to-M fast-forward under the same Windows
host mutation mutex. No fresh/reset/volume prune; no other host change. Durable
native receipt/log are under private `.acceptance/nettking-9b286f93-supported-start.*`.
Inspect operation state before retrying, persist every transition, then verify
actual M runtime before continuing Nitro/Recorder admission. Physical PASS0.

### Nettking N-to-M activation started

All preconditions passed. Native supported startup is running once as PID26896,
after serialized source advancement, with the persisted original owned Compose
configuration. [Operation receipt](diagnostics/nettking-m-supported-start.json).
No second launch, timer or acceptance probe while activation is active. Next:
read private `.acceptance/nettking-9b286f93-supported-start.json` on a completion
transition and inspect the native log only if needed; then verify actual runtime.

##18:25UTC — Nettking supported M startup completed; runtime verification next

Supported launcher exit0 at18:24:53; [completed operation receipt](diagnostics/nettking-m-supported-start.json).
This is startup completion, not physical PASS. Next verify all three actual M
images/env/source bytes, preserved mounts/model and fresh runtime provenance.

[Nitro preflight](diagnostics/m-nitro-activation-preflight.json) is clean N,
LEADER/ready/index1551, no pending updater/capture request, native N responder
still owned, about1.93millionKiB RAM available. No Nitro source/containers changed.
Recorder fingerprint retry identified that the audit selected the old per-Martin
Windows Store-backed venv, which cannot launch under current SSH context.
[Exact audit failure](diagnostics/m-recorder-legacy-python-context-attempt.json).
This does not yet establish a new independent defect: select the already documented
N-native interpreter/context before diagnosis. Protected container invariant remains
verified; no protected record files read, no service-account or host changes.

##18:27UTC — Nettking actual M core runtime verified

[Exact admission receipt](diagnostics/nettking-m-runtime-admission.json) proves
all three running core images/env commits and four repaired source file hashes
match M; runtime and harness checkouts are clean M. Actual Flask port5151 agrees.
All data/model mounts are preserved, pinned Ollama model reused without download,
no OOM. Fingerprint0efb6f56566ac0eb. A new explicit M runtime binding was created
only after verification; old physical evidence was not relabeled. Membership and
host responder admission remain pending, physical PASS0.

Recorder context failure is narrowed to an audit selecting the legacy venv:
the retained N-native provenance names the correct existing executable at
`C:\Users\Utlån\fcp-v1-0536f03d-20260911\tooling\python312\python.exe`.
Next: verify that existing operative interpreter, then stage exact M for Nitro
and owned Recorder campaign source. Never modify protected production Recorder.

### Recorder operative-context audit timed out; no new product defect established

The probe using the documented portable native interpreter timed out at its
outer40s SSH bound before returning a result. [Exact unresolved observation](diagnostics/m-recorder-context-timeout.json).
It contains native fingerprint plus Git metadata, so the failing subcommand is
not isolated. Do not repeatedly rerun it or alter service accounts. Next inspect
only the audit child process metadata and distinguish interpreter/Git/transport.
No product/data mutation was requested; check whether a read-only child remains.
Nitro source staging can safely continue independently. M source bundle was
created from clean M with N prerequisite and HEAD exactlyM; it is not deployed.

##18:31UTC — Recorder operative preflight resolved without host repair

No leftover audit child process exists. Separate bounded native and Git probes
all return exit0: existing Python3.12.10 in0.096s, expected physical fingerprint
8589698c32d44b1f in0.510s, staged source exact clean N in0.050s each.
[Native proof](diagnostics/m-recorder-native-bounded.json),
[source proof](diagnostics/m-recorder-source-bounded.json).
The earlier combined transport timeout is retained as an unreproduced audit
observation; no independent product/environment defect is demonstrated. No host
repair, identity copy or service-account change; protected data untouched.
Next: stage the qualified M bundle for Nitro, then controlled source/build/start
using the existing per-service image configuration and mutation lock.

##18:33UTC — qualified M source bundle staged on Nitro

[Bundle receipt](diagnostics/m-nitro-bundle-staged.json):22304bytes,
SHA256af475a0f31a8aed6827feb6318ac3dd2d5e7ab218d753824db70bac424a83d29,
Git verifies prerequisite N and exact HEAD M. Stored only under Nitro campaign
inputs; its clean source/core/responder still N. No product/data mutation.
Next: guarded source fast-forward under the checked-in host mutation lock,
compare actual resolved Compose before/after (only candidate images/build args
and new default responder port may differ), then supported normal startup.
Preserve data/results/models/control identity; verify leader recovery afterward.

### Controlled Nitro operation staged for reviewable execution

`diagnostics/activate_nitro_m.py` rechecks clean N, native Python, exact current
services, idle capture/update state, model mounts, current ready leader/index and
qualified bundle. Under the existing Git-scoped mutation lock it fast-forwards
to M and compares complete resolved Compose: only core candidate image/build/env
SHA and Flask default5151 may differ. Then it runs checked-in normal `bash start.sh`.
No fresh/reset/resume enrollment action. Owned services may briefly lose authority
during restart; fail-closed behavior and fixed voter identities are preserved.
No protected Recorder process/data action. The N responder remains explicitly N
until a separate guarded M replacement after core verification.

The audit controller will run detached with a durable host receipt/log under
Nitro inputs, so session loss does not discard the operation. Never relaunch if
either the staged controller or receipt already exists; inspect it first.

##18:36UTC — Nitro detached M activation controller dispatched

[Dispatch receipt](diagnostics/nitro-m-controller-dispatch.json) binds controller
PID1205079 and reviewed script SHA256fd20d511fe7c3ac468b815ea279ce1861d7be2be9bae9eb6784c17351bee55cd.
It survives this session with native logs and operation receipt on Nitro inputs.
Next exact read: `/home/martin/fcp-v1-73c779-nitro-20260910/inputs/supported-start-9b286f93.json`.
If absent inspect `m-activation-controller.log` for precondition failure; otherwise
react only to status transitions. Never duplicate the controller/startup. Fresh
runtime and control identity/recovery verification remains before acceptance.

###18:36UTC — Nitro configuration guard stopped before runtime mutation

[Operation receipt](diagnostics/nitro-m-supported-start.json) reports
`INSPECT_OPERATION_BEFORE_RETRY`: `Resolved deployment changes exceeded candidate image/build/env SHA and default5151`.
The guarded source fetch/fast-forward reached M; supported startup was NOT
launched (no child PID). Current cores/responder still N, no data reset or
protected Recorder action. This proves an audit configuration mismatch, not
yet a product defect. Next: compare exact resolved N/M configurations and
classify each changed key. Do not loosen checks or retry blindly. Main software
qualification and all retained evidence remain valid; physical acceptance0.

##18:38UTC — exact Nitro configuration discrepancy isolated

[Resolved delta](diagnostics/nitro-m-config-delta.json) identifies only four audit
expectation mismatches: three nonexistent Compose FCP_BUILD_COMMIT environment
entries (runtime inherits that value from the checked-in Dockerfile image ENV),
and the private host's old custom responder port. D05 now correctly propagates
that value; current D04-owned responder uses5151. These are audit/configuration
issues, not product regressions or reasons to rerun software qualification.

Next: explicitly align the new M private host environment with owned port5151,
and compare against the actual checked-in image-ENV contract without inserting
fictional Compose environment fields. Require the complete resolved config to
match otherwise, preserve old operation/log, then run a separately named guarded
continuation from clean M. No service startup or protected-data action occurred.

###18:39UTC — guarded Nitro continuation prepared

The private old host port is exactly5152. [Readable numeric delta](diagnostics/nitro-m-config-delta-readable-port.json).
The new M environment explicitly aligns it to the D04-owned5151. Checked-in
Dockerfile lines26–28 prove build SHA is inherited via image ENV/label, not a
Compose environment entry. The separate `activate_nitro_m_guarded.py` preserves
the first stopped controller/evidence, requires that exact guard result and clean
M, compares the entire configuration allowing only actual candidate image/build
changes and the explicit5152-to5151 host-port alignment, then starts unchanged M.
No assertions, deadlines, authority or product source were weakened/changed.
Next exact command: run `diagnostics/dispatch_nitro_m_guarded.py` once; inspect
`supported-start-9b286f93-guarded.json` before any further action/retry.

##18:40UTC — guarded Nitro continuation dispatched

[New controller receipt](diagnostics/nitro-m-guarded-controller-dispatch.json)
binds PID1211377/script22c0c161b7ff2fc6b18acb5288062816cb708005ea870e72556b342942325b2c.
The original stopped operation/evidence remain untouched. Next read only the
new operation receipt and react to completion/failure; no duplicate startup.

### Nitro supported M startup RUNNING; next completion action prepared

[Operation state](diagnostics/nitro-m-guarded-supported-start.json) proves complete
configuration equality and supported `bash start.sh` now running as PID1211658.
New private environment explicitly uses M and port5151; old inputs untouched.
No known product defect. Next read `read_nitro_m_guarded_operation.py`; if success,
execute `verify_nitro_m_runtime.py` once and retain actual image/env/repaired-source
hashes, unchanged mounts/model and recovered control identity/index. Do not claim
host responder M yet: its imported N process must be replaced and verified next.
No P07/P12 or physical PASS. Other voters/Recorder protected production untouched.

##18:53UTC — Nitro supported startup completed successfully

The operation finished18:43:16UTC with exit0. [Completed native operation](diagnostics/nitro-m-guarded-supported-start.current.json).
No duplicate startup or software rerun. Next exact action: stream the already
reviewed `verify_nitro_m_runtime.py` through authenticated Nitro native Python;
verify actual M core source/images, preserved mounts/model and control recovery.
This is admission, not physical PASS. Host responder still imports N until its
separate checked-in guarded replacement; protected Recorder data unchanged.

##18:54UTC — Nitro actual M runtime and control recovery verified

[Runtime admission](diagnostics/nitro-m-runtime-admission.json) proves all three
M core image/env commits and four repaired source hashes, clean M checkout,
preserved mounts and existing pinned model manifest. Fingerprintad885120af1f11cf.
Control cluster/federation/voter identity preserved; LEADER/ready term9,
commit_index=last_applied1553. No OOM or protected Recorder action. No PASS claim.
Next: bind a fresh M responder to the verified M app/5151 through the checked-in
process-identity replacement; preserve the current shared secret and verify real
peer health. Do not count old imported N responder bytes as M.

### Nitro M responder replacement procedure staged

`admit_nitro_m_responder.py` rechecks current N PID1174977/start185384331 and
canonical process record/socket, exact M app/source/port and existing secret.
Under the shared host mutex it invokes unchanged M responder main, including
its own checked-in stable-process-handle replacement guard. No manual pre-stop
or alternative bind/retry workaround. Preserve secret, native log and receipt;
on failure stop dependent admission and classify before any retry. No grants,
enrollment, protected Recorder action or physical evidence claim.

##18:56UTC — new confirmed D06 stops physical admission

Unmodified M responder main reported successful exact-instance termination of
N PID1174977, then its new PID1234730 exited1 with `[Errno98] Address already in use`
on5151. [D06 full finding](diagnostics/D06.md) and [native receipt](diagnostics/nitro-m-responder-admission.json)
preserve exact candidate, stage/procedure, before identity, result and native-log
digest. This is a product asynchronous termination/rebind defect; no workaround
or repeated launch. Physical qualification stops; software PASS is not rerun.

Next: publish durable issue linked here before substantial investigation, then
one read-only process/socket snapshot and narrow isolated repair/regression.
No protected Recorder data touched; no grant/enrollment request; P07/P12 absent.

###18:58UTC — D06 durable issue published

[Issue462](https://github.com/Nettking/msh/issues/462) contains exact evidence,
source/stage/host, state effects and repair boundary. The accumulating blocker
table now includes D06. Next read-only process/socket snapshot; then separate
repair branch/draft PR and focused regression. Do not resume M physical admission.

##18:59UTC — D06 post-failure state preserved

[Snapshot](diagnostics/d06-post-failure.json): both processes exited,5151 is now
free, all M core identities/start times unchanged, shared secret mtime remains
D04 creation18:13:48UTC. Exact native log digest matches. No listener retry.
This supports the asynchronous signal/rebind mechanism. Next: isolated repair
branch from M, real-process regression reproducing delayed exit, bounded exit
wait on the same stable process handle, and draft PR linked to#462. No host deploy.

##19:09UTC — D06 narrow repair pushed as draft463

[Draft463](https://github.com/Nettking/msh/pull/463), exact
7286f30d30e20c94c13cc9acebb2fd6d61bc1163, waits for confirmed process exit on the
same verified OS handle, bounded5s. Unconfirmed exit refuses before bind/PID write.
Real Linux child reproduced M EADDRINUSE before repair. Focused Windows37PASS,
7Linux skips; isolated Linux host-only11PASS including delayed exit/timeout;
ruff/diff PASS. [Development evidence](diagnostics/d06-repair-focused-evidence.json).
No grant/discovery deadline, PID-reuse protection or authority requirement relaxed.

No full qualification manually dispatched merely because repair exists; native
PR workflows may run automatically. Next: inspect only463 current exact head,
relevant correctness findings and required native runs on a state transition.
Preserve all prior software evidence. Before any future physical restart, this
new repair needs the required PR-head qualification, normal merge, actual new
merged-main qualification and clean revalidation/candidate selection. Physical
hosts stay on M cores; Nitro has no responder; Recorder protected data untouched.

##19:12UTC — PR463 initial native CI state retained, source review complete

Exact head remains7286f30d. [State cache](diagnostics/pr463-qualification-state.json)
and [delta](diagnostics/pr463-qualification-last-transition.json) cover only463.
Seven workflows auto-started; PostgreSQL release check and Linux shard contract completed, other
jobs queued/running. No failed job. Four required workflows were not triggered
by paths: acceptance harness, physical readiness, retirement, operator surface.
CFI2/registry are also absent. No jobs have been duplicated or manually dispatched.

Source review finds no unresolved correctness issue: Linux poll retains the same
pidfd through signal/exit confirmation, Windows retains the same verified handle
with SYNCHRONIZE, both waits are bounded5s, failed wait refuses before bind/write,
and all handles close in finally. PID mismatch/no strong handle remain fail-closed.
Real process and timeout regressions plus native Windows handle tests support
the narrow repair. Prior source/authority/deadline contracts remain unchanged.

Next: mark463 ready for review; qualify this reviewed exact head under existing
37-job/CFI2/registry contracts. First retain completed native checkout proofs,
then dispatch only proven absent gates; let existing jobs finish before resolving
any synthetic-checkout gaps. Do not qualify old candidates or deploy the repair.

###19:13UTC — reviewed PR463 ready; required qualification now justified

Normal GitHub ready-for-review transition succeeded for exact7286f30d, base M;
PR is open/unmerged and mergeable. Local source review and focused regressions
are complete, so the required exact-head qualification is now the next gate.
No physical deployment or merge yet. Retain existing jobs before filling only
proven absent workflow gaps. No repeated old qualification or blanket dispatch.

###19:14UTC — first463 native proofs retained; synthetic checkout identified

The two completed successful jobs (PostgreSQL release check and Linux shard
contract) actually checked out syntheticbfec91c9, not7286f30d. [Native receipts](diagnostics/pr463-initial-native-provenance.json).
They cannot qualify the PR head. Their parent workflows still run; do not cancel
or duplicate them. Required absent harness/readiness/retirement/operator plus
CFI2/registry can be dispatched now after fresh absence/head checks, independently
of active parent runs. Exact-head release/sharding gap resolution waits until
those parents complete and all relevant native proofs are retained.

##19:14UTC — six proven absent PR463 gates dispatched once

Fresh head/ref and run absence checks preceded each dispatch: acceptance harness,
physical readiness, retirement, operator surface, CFI2 and registry. [Ledger](diagnostics/pr463-gap-dispatch-ledger.json)
retains204 responses and timestamps. Existing seven automatic workflows remain
untouched. Exact-head qualification is incomplete; no merge or new candidate.

Next exact command: native audit Python runs `handoff/diagnostics/poll_pr463_state.py`.
If unchanged, stay quiet. On newly completed jobs, retain only their full native
logs and actual checkout commits. Preserve the two existing synthetic proofs.
Resolve exact-head gaps only once a parent has finished and source mismatch is
demonstrated. No physical runtime action; M cores stay running, Nitro5151 empty,
protected Recorder data unchanged. P07/P12 unstarted and physical acceptance0.

##19:18UTC — dispatched gates visible; five additional jobs completed

PR463 head is unchanged and ready for review. All13 expected workflows now have
active parents, including the six dispatches recorded19:14UTC. Five additional
automatic jobs succeeded; no failures observed. Exact-head qualification remains
incomplete. [State/delta](diagnostics/pr463-qualification-last-transition.json).
Next: retain only these newly completed native logs, verify actual checkout SHA,
then await parent completion without duplicate dispatch. Physical state unchanged.

Native log retention is complete for all seven completed jobs (five new), with
digests in [cumulative provenance](diagnostics/pr463-native-provenance.json).
All seven actually checked out synthetic `bfec91c9c0ab3581645bc4c575bf4a056b2b80c9`,
not head7286f30d. Preserve this evidence; exact-head gaps now demonstrated for
phase2, release, sharding, ICSE and CF7B. Their parents are still active, so do not
dispatch replacements yet. Six explicit dispatches remain queued and unqualified.
Next heartbeat: run `poll_pr463_state.py`; only on new terminal jobs run
`retain_pr463_new_native_logs.py`. On parent completion, review required results
and fill demonstrated exact-head gaps once. No product/runtime changes or physical
claims. This is source provenance accounting, not a newly discovered product defect.

##19:30UTC — two automatic parents complete; review exact-head gaps

PR463 head7286f30d is unchanged. CF7B34637251493 and software-update34637251568
completed successfully. Other parents remain active; no failed jobs observed.
New completed jobs are checkpointed in the state/delta. Next: retain only new
native logs, verify source provenance, and dispatch an exact-head replacement
only for a completed parent with a demonstrated mismatch and no existing dispatch.
Physical admission remains stopped by D06; no runtime mutation or new candidate.

Native provenance retained for six new jobs. Harness Windows and readiness Linux
are the first two verified exact-head PASS jobs. Both completed CF7B jobs and both
completed software-update jobs used syntheticbfec91c9; replacement dispatch for
each completed parent is justified by source mismatch. Existing passing logs are
preserved. Next: dispatch these two exact-head gaps once with fresh guards and
persist each response; other active parents remain untouched.

19:32UTC: CF7B exact-head replacement dispatched once (HTTP204), with both native
source mismatch proofs in the dispatch ledger. Next: software-update completed
gap through `dispatch_pr463_completed_gap.py federation-software-update.yml`.

19:32UTC: software-update exact-head replacement also dispatched once (HTTP204),
with both native proofs persisted. Eight dispatches total are recorded for463;
do not repeat them. Two exact-head native PASS jobs verified so far; qualification
is incomplete. Next exact action: native audit Python runs
`handoff/diagnostics/poll_pr463_state.py` on the next state check, then retains only
new terminal logs with `retain_pr463_new_native_logs.py`. Other automatic parents
remain active; use the guarded completed-gap helper only after successful parent
completion and native source mismatch proof. Keep physical admission stopped.

##19:43UTC — phase2 parent complete; qualification continues

PR463 head7286f30d unchanged. Phase2 parent34637251366 completed successfully;
other selected parents remain active. CF7B34639368409 and software-update34639396004
are the already-requested exact-head replacements, not additional dispatches.
No failed jobs observed. Next: retain newly completed logs, verify checkout SHA,
then fill the phase2 gap only if both jobs prove source mismatch. Physical state
remains unchanged and admission stopped by D06.

Five new native logs retained. Retirement Windows and CFI2 Windows are verified
exact-head PASS, bringing the current native total to four (three required37
jobs plus one companion job). Phase2 Windows and both release full-order jobs
used syntheticbfec91c9. Both phase2 jobs now have complete mismatch proofs;
next dispatch its exact-head gap once with the guarded helper. Release remains
active; preserve all completed evidence without restarting it.

19:44UTC: phase2 exact-head replacement dispatched once (HTTP204), with both
native mismatch proofs recorded. Nine dispatches now recorded for463; no repeats.
Next: `poll_pr463_state.py` on the next state check; only new completed jobs need
`retain_pr463_new_native_logs.py`. Fill remaining gaps only after parent completion
and native source verification. No merge, new candidate or physical restart yet.

##19:54UTC — four dispatched gates and branding parent complete

Head7286f30d unchanged. Harness34637782331, retirement34637788283,
CFI2 34637793677, registry34637796846 and automatic branding34637251468
completed successfully. Other selected parents remain active; no failed jobs.
Phase2 replacement34640438781 is visible. Next: retain only newly completed
native logs, verify exact source, preserve valid gates and fill branding only
if its completed native proof establishes a mismatch. Physical admission stopped.

Eight new native logs retained. Exact-head verified PASS now6/37 required jobs
plus CFI2/registry3/3. Harness and retirement both platforms are complete and
preserved. Branding's only job used syntheticbfec91c9; its completed parent has
a demonstrated source gap. Next: dispatch branding exact-head once. Release/ICSE
new successes also used syntheticbfec91c9; those parents are still active.

19:55UTC: branding exact-head replacement dispatched once (HTTP204); ten total
dispatches for463 now recorded. Preserve6/37+companion3/3 verified native PASS.
Next: `poll_pr463_state.py` on the next state check; retain only newly completed
logs. Remaining synthetic-source parents are release, sharding and ICSE; wait for
completion, then inspect native/aggregate provenance before a justified dispatch.
No merge, new candidate, physical restart or protected-data operation.

##20:06UTC — ICSE parent and further dispatched gates complete

Head7286f30d unchanged. ICSE34637251466, branding34641464390,
CF7B34639368409 and readiness34637785139 completed successfully. No failed jobs
observed. Other selected parents are active. Next: retain only new native logs,
verify checkout source and fill the completed ICSE gap if all native proofs
establish a mismatch. Keep valid gates and all active parents; physical stopped.

Ten new native logs retained. Exact-head PASS now12/37 required jobs plus
CFI2/registry3/3. CF7B, readiness and branding are fully verified and preserved.
All four completed ICSE jobs, including publication-bundle, checked out
syntheticbfec91c9; all mismatch proofs are retained. Next: dispatch ICSE exact-head
once. Release/sharding parents remain active; do not duplicate them.

20:07UTC: ICSE exact-head replacement dispatched once (HTTP204), with four native
proofs in the ledger; eleven total dispatches now recorded for463. Next:
`poll_pr463_state.py` on next state check, retain only new terminal logs. The only
remaining synthetic-source parents are release and sharding; wait for completion
and inspect their native/aggregate provenance before any justified replacement.
Preserve12/37+3/3 exact-head PASS. No merge or physical state change.

##20:18UTC — sharding parent and operator gate complete

Head7286f30d unchanged. Sharding34637251420 and operator34637791132 completed
successfully; no failed jobs observed. ICSE replacement34642522512 is active
with two completed jobs. Release parent remains active. Next: retain new native
logs and verify checkout provenance; resolve sharding only if its completed
native proofs establish the expected source gap. Physical admission stays stopped.

Seven new native logs retained. Exact-head PASS now15/37 required plus companion3/3.
Operator both platforms and two ICSE jobs are verified. Both completed sharding
jobs prove syntheticbfec91c9; exact-head replacement is justified. Release's
order-independence aggregate has no checkout, as expected for an aggregate; retain
its native log and review dependency provenance when its parent completes. Do not
treat a missing checkout in an aggregate as a product failure or standalone proof.
Next: guarded sharding dispatch once; release remains active.

20:19UTC: sharding exact-head replacement dispatched once (HTTP204), with both
native proofs persisted; twelve total dispatches for463. Next state check:
`poll_pr463_state.py`, then retain only new terminal logs. Only the release parent
still needs source-gap resolution after completion; its aggregates require
dependency review before adapting the guarded dispatch helper. Preserve15/37+3/3
verified exact-head PASS. No merge, physical restart or protected-data operation.

##20:29UTC — release parent complete; final source gap actionable

Head7286f30d unchanged. Release34637251378 completed16/16 successfully;
phase2 and software-update exact-head parents also completed successfully.
No failed jobs observed. Sharding replacement34643583922 and ICSE remain active.
Next: retain new native logs, then review release aggregate dependencies and all
source proofs before dispatching its exact-head replacement once. Physical stopped.

Seven new native logs retained. Exact-head PASS now18/37 required plus companion3/3;
phase2 and software-update both platforms verified. Release14 source-bearing jobs
all prove syntheticbfec91c9, while two successful aggregates have no checkout.
Next: inspect exact checked-in aggregate dependency definitions and native logs,
persist that provenance review, then dispatch the demonstrated release source gap.

20:31UTC: [Aggregate review](diagnostics/pr463-release-aggregate-review.json)
confirms the actual synthetic workflow is byte-identical to head7286f30d; the
two no-checkout aggregates only require the reviewed successful dependencies.
All14 source jobs used syntheticbfec91c9. Synthetic Git object fetched for this
read-only review; no checkout/runtime change. Guarded helper now accepts only
these two explicitly reviewed aggregate IDs/digests for this run. Next: one
release exact-head dispatch, preserving all native evidence and existing valid jobs.

20:32UTC: final release exact-head replacement dispatched once (HTTP204), with
14 native proofs and two reviewed aggregate dependencies. All13 qualification
workflows now have one justified dispatch. No source gaps remain undispatched;
do not repeat any dispatch. Next state check: `poll_pr463_state.py`, then retain
only new native logs. Preserve18/37+3/3 exact-head PASS. Once all required jobs
and release verdict pass, review exact-head artifacts and current correctness
findings before normal merge; then qualify actual new merged main once.
No new candidate or physical action before that sequence completes.

##20:43UTC — only exact-head release remains active

Head7286f30d unchanged. Sharding34643583922 and ICSE34642522512 completed
successfully. Every selected workflow except release34644764330 is now complete;
release has four successful jobs so far. No failed jobs. Next: retain new native
logs, verify source and review completed ICSE artifacts while release proceeds.
No duplicate jobs, merge, candidate selection or physical action.

Seven new native logs all verify exact head7286f30d. Required checks now25/37
plus companion3/3; all21 non-release required jobs are complete and preserved.
Release4/16 verified so far. Next: review completed exact-head ICSE artifacts;
then continue on release job state changes only. Physical admission remains stopped.

20:45UTC: six immutable ICSE artifact ZIPs retained from34642522512; every GitHub
digest matches. [Retention receipt](diagnostics/pr463-icse-artifact-retention.json).
Next: inspect the publication manifest, source export, checksums, native component
and network evidence; do not rebuild artifacts or rerun completed jobs.

20:45UTC: [Exact-head ICSE artifact review](diagnostics/pr463-icse-artifact-review.json)
PASS: all1412 exported source files byte-match Git head7286f30d; manifest/ref/run,
19 checksums and publication digest verified. Native Linux/Windows/Compose each
4/4; both network executions10 required checks with owned-process teardown.
No artifact rebuild, source change or physical acceptance claim. Preserve this
review and25/37+3/3 native qualification; only release remains active.
Next exact action: audit Python runs `handoff/diagnostics/poll_pr463_state.py` on
the next state check. On release completion retain only new logs, review its
aggregates and native release artifacts, then assess exact-head merge readiness.

##20:56UTC — four more release jobs completed

Head7286f30d unchanged. Release34644764330 now8/16 successful; parent remains
active and no failed jobs observed. All other gates and ICSE review are preserved.
Next: retain only newly completed native logs and verify checkout source; no
duplicate dispatch, merge, candidate selection or physical action.

Four new native logs all verify exact head7286f30d. Required PASS now29/37 plus
companion3/3, including release8/16. Preserve every completed check and ICSE
artifact review. Next exact action: `poll_pr463_state.py` on the next state check;
retain only newly completed logs. On release completion review aggregate/source
and immutable release artifacts before final qualification verdict and merge.
Physical admission remains stopped by D06; protected Recorder data untouched.

##21:07UTC — three more release jobs completed

Head7286f30d unchanged. Release34644764330 now11/16 successful; parent remains
active with no failed jobs observed. Other gates and ICSE review remain complete.
Next: retain only new native logs and verify source/aggregate provenance before
counting new qualification evidence. No duplicate jobs or physical action.

All three new logs verify exact head7286f30d. Required PASS now32/37 plus
companion3/3; native Linux and Windows release checks are both verified.
All four Linux regression shards have completed. Preserve these proofs and ICSE
review. Next state check: `poll_pr463_state.py`, retain only new terminal logs,
then review final release aggregates/artifacts once the parent completes.
No merge or candidate selection yet; physical admission remains stopped by D06.

##21:18UTC — all PR463 qualification workflows completed successfully

Head7286f30d unchanged. Release34644764330 completed16/16; all required37 jobs
and companion3 jobs now report success. Native provenance/artifact review remains
the final qualification step. Next: retain five new release logs, verify aggregate
dependencies, retain/review immutable release artifacts, inspect current review
findings and exact head before normal merge. Physical admission remains stopped.

Five final native logs retained: three source-bearing jobs verify exacthead7286f30d;
two expected no-checkout aggregates await dependency review. All38 source-bearing
jobs (35 required plus3 companion) now have native exact-head proofs. Next: finish
the two aggregate reviews and immutable release artifact review before verdict.

21:19UTC: nine immutable release artifacts retained from34644764330, all GitHub
digests match. Receipt `diagnostics/pr463-final-release-artifact-retention.json`.
Next: run retained-artifact review against exacthead7286f30d, reconcile aggregates,
test identities/skips and current review findings. No completed test is rerun.

21:20UTC: all nine source-bound release artifacts reviewed successfully: four
disjoint shards cover4431 collected identities; both full orders contain4431 cases;
all artifact failures/errors0. Review persisted as
`diagnostics/pr463-final-release-artifact-review.json`. Next: reconcile exact
full-order identities, intentional/native-covered skips, aggregate logs and live
PR review findings before recording qualification PASS and normal merge.

21:21UTC: MERGE BLOCKED by unresolved correctness review on exacthead7286f30d.
Review thread PRRT_kwDOPZM3cc6hmxQd reports that the new5s replacement exception
changes backup_recovery's existing10s stop/error contract. This is a repair review
finding, not physical evidence or a newly observed host failure. [Review input](diagnostics/pr463-review-input.json).
All CI is green but does not resolve this finding. Next: inspect the helper's
backup caller, reproduce safely in isolated tests, then repair PR463 if confirmed.
Do not merge or deploy the reviewed head. Preserve all completed qualification.

21:24UTC: review confirmed by two isolated red regressions at7286f30d;
[D06-R1](diagnostics/D06-R1.md) records exact procedure, error, SHA and state boundary.
No physical execution/data access. Next: publish confirmation on existing PR463,
then restore signal-only default helper semantics and opt in to5s exit waiting
only at responder replacement. Backup's10s deadline/error contract stays unchanged.

21:28UTC: correction pushed on existing PR463, head
`83955f65b7e6bb36de8e90f34608b97070fed33b`. [Focused evidence](diagnostics/pr463-backup-contract-repair.json)
records red-to-green backup tests and preserved real Linux exit-wait regressions.
Only isolated source/test files changed; backup implementation is untouched.
Old qualification state/delta/native proof archived under `pr463-head-7286f30d-*`.
Next: publish correction on PR463, retarget checked-in polling/retention helpers,
review new auto qualification state and absent gates. No merge/physical deployment.

21:29UTC: checked-in polling/retention/dispatch helpers now target83955f65.
Seven automatic workflows exist and are active; no completed/failed jobs yet.
Four required gates plus CFI2/registry are absent and may be dispatched once
after fresh absence/ref guards. Old7286f30d ledger/receipts remain preserved.
The addressed backup review thread is resolved after red-to-green verification
and correction comment5640841676. Future polling now detects review-comment count
changes so new correctness feedback is investigated before qualification finishes.
Next: dispatch only the six proven absent gates for83955f65, then persist responses.

21:29UTC: six proven absent gates dispatched once for83955f65 (all HTTP204),
with fresh head/ref/absence guards. Ledger retains each response separately by
SHA. Seven automatic parents remain active; no old-head rerun or host action.
Next exact command: audit Python runs `handoff/diagnostics/poll_pr463_state.py`.
If no material change, stay quiet. On review-comment changes inspect review
threads promptly; on new terminal jobs retain only their native logs. Resolve
synthetic-source gaps only after completed-parent proof, preserving valid work.
Do not reuse the old release aggregate review without verifying its source/run.
No merge until this exact new head satisfies all required gates/artifacts and
has no relevant unresolved correctness findings; actual new main qualifies once.

##21:41UTC — corrected-head qualification parents progressing

Head83955f65 unchanged. Automatic CF7B34649416554, branding34649416642 and
sharding34649416672 completed successfully; dispatched harness34649600106 also
complete. Other parents remain active; no failed jobs observed. All six21:29
dispatches are visible. Next: retain newly completed native logs, verify checkout
SHA, and resolve only completed-parent source gaps. No physical action or merge.

Fifteen native logs retained. Exact83955f65 PASS4/37 plus CFI2 Linux1/3 companion.
Ten automatic job logs instead prove synthetic68f6e72c45bf1b0f709efb4ac90ff05fbde37ec7.
Completed CF7B, branding and sharding parents have full source-mismatch proofs;
next dispatch each exact-head gap once with fresh guards and persist each response.
Other automatic parents remain active. Review-comment count is unchanged.

21:42UTC: cf7b-product-physical-acceptance.yml exact-head83955f65 gap dispatched once (HTTP204); native proofs and fresh guards are in the ledger. Preserve valid work and do not repeat this dispatch.

21:42UTC: product-branding.yml exact-head83955f65 gap dispatched once (HTTP204); native proofs and fresh guards are in the ledger. Preserve valid work and do not repeat this dispatch.

21:42UTC: ci-test-sharding.yml exact-head83955f65 gap dispatched once (HTTP204); native proofs and fresh guards are in the ledger. Preserve valid work and do not repeat this dispatch.

Nine dispatches now recorded for83955f65. Preserve4/37 required and1/3 companion
native exact-head PASS. Remaining automatic parents needing later source review:
software-update, phase2, ICSE and release. Next exact action on next state check:
`poll_pr463_state.py`, then only new native log/review transitions. No duplicate
dispatch, old-head evidence carry-forward, merge or physical state change.

##21:53UTC — software-update and phase2 automatic parents complete

Head83955f65 unchanged. Software-update34649416668 and phase234649416744
completed successfully; other incomplete parents remain active, no failed jobs.
The three21:42 exact-head replacements are visible. Next: retain newly completed
logs, verify source and fill only demonstrated completed-parent gaps. Review
comment count unchanged; no physical state change or merge.

Four new logs retained. Operator Linux verifies exact83955f65, bringing required
native PASS to5/37 plus companion1/3. Both software-update jobs and phase2 Windows
prove synthetic68f6e72c; phase2 Linux proof was already retained. Both completed
parents have full source-gap proof. Next: one guarded exact-head dispatch per gap.

21:54UTC: federation-software-update.yml exact-head83955f65 gap dispatched once (HTTP204); native proofs and fresh guards persisted in the ledger. Next state check uses poll_pr463_state.py; no duplicate dispatch or physical action.

21:54UTC: phase2-federation.yml exact-head83955f65 gap dispatched once (HTTP204); native proofs and fresh guards persisted in the ledger. Next state check uses poll_pr463_state.py; no duplicate dispatch or physical action.

##22:05UTC — readiness and retirement exact-head parents complete

Head83955f65 unchanged. Readiness34649602701 and retirement34649604991 completed
successfully. Other incomplete parents remain active; no failed jobs observed.
Software-update34651625551 and phase234651637810 are the already-requested
replacements. Next: retain only new native logs and verify source; no dispatch
is currently justified for the still-active automatic release/ICSE parents.

Seven new logs retained: five verify83955f65, two release logs prove synthetic
68f6e72c. Required exact-head PASS now10/37 plus companion1/3. Harness, readiness
and retirement are complete on both platforms. Review-comment count unchanged.
Next: `poll_pr463_state.py` on the next state check, retaining only new terminal
logs/review transitions. Preserve eleven dispatches and every valid result; wait
for release/ICSE parent completion before resolving their source gaps. No merge,
new candidate or physical action. Protected Recorder data remains untouched.

##22:17UTC — software-update and operator exact-head parents complete

Head83955f65 unchanged. Software-update34651625551 and operator34649607489
completed successfully; other incomplete parents remain active with no failed
jobs observed. Next: retain new native logs and verify source. Release/ICSE
automatic parents remain active; no additional dispatch or physical action.

Five new logs retained. Software-update Windows and operator Windows verify
exact83955f65, bringing required PASS to12/37 plus companion1/3. Both gates are
complete on both platforms. Three new release logs prove synthetic68f6e72c;
preserve them without counting against the PR head. Review-comment count unchanged.
Next exact action: `poll_pr463_state.py` at the next state check, then retain only
new terminal/review evidence. Release/ICSE source gaps wait for parent completion.
No merge, candidate selection, physical restart or protected-data operation.

##22:28UTC — more exact-head gates and automatic ICSE complete

Head83955f65 unchanged. CF7B34650691556, branding34650703317,
phase234651637810, registry34649613062 and automatic ICSE34649416778 completed
successfully. Other incomplete parents remain active; no failed jobs observed.
Next: retain new native logs, verify source and resolve ICSE only if all four
completed jobs prove a source mismatch. No duplicate jobs or physical action.

Eleven new logs retained. Exact83955f65 PASS16/37 plus companion2/3; CF7B,
branding and phase2 are fully verified, registry also verifies exact head.
All four completed ICSE jobs prove synthetic68f6e72c. Next: dispatch the ICSE
exact-head gap once. Release remains active; its new no-checkout aggregate
is retained for later dependency review. Review-comment count unchanged.

22:29UTC: ICSE exact-head83955f65 replacement dispatched once (HTTP204), with
all four source proofs in the ledger. Twelve dispatches now recorded for this
head. Only automatic release still needs later source-gap resolution. Next:
`poll_pr463_state.py` on next state check; retain only new terminal/review evidence.
Preserve16/37+2/3 native PASS. No merge, candidate or physical state change.

##22:40UTC — automatic Windows capability-product release job failed

Head83955f65 unchanged. Automatic release34649416713 reports failed Windows
regressions(capability-product); parent remains active. Other selected jobs show
no failures. ICSE replacement34654263201 has two successful jobs. Failure is
UNCLASSIFIED pending native evidence; do not modify product code or rerun jobs.
Next: retain new native logs, inspect the failing job first and classify its
mechanism/source before deciding safe qualification continuation. Physical stopped.

22:42UTC: independent failure D07 persisted with exact native trace/digests.
The concurrent analysis scheduling test raises WinError5 at content_store.py:138
while replacing plan.json in isolated CI temp state. Actual checkout68f6e72c;
root cause unresolved, no product change justified yet. [Finding](diagnostics/D07.md).
Next: publish its issue before further investigation, then compare source and
perform focused isolated reproduction. Two new ICSE successes verify83955f65;
preserve required18/37 plus companion2/3. Physical/protected state unchanged.

22:43UTC: D07 published as issue464. Next: read the failing concurrent test,
content store and caller boundaries; compare their source identities againstM
and83955f65, then reproduce only in disposable local test state. No product edit
or CI retry until classification supports it. Existing independent CI may continue.

22:45UTC: D07 is a confirmed pre-existing product concurrency defect. On clean
83955f65, a real public content-store stream held after first chunk prevents an
identical-content write with WinError5; closing reader permits the same write.
[Reproduction/source identity receipt](diagnostics/d07-open-reader-reproduction.json)
proves affected files identical acrossM/head/synthetic checkout. Original CI handle
owner is unknown; reproduced mechanism matches its error. One existing-test pass
does not dismiss the race. No protected state or source changed. Next: publish
classification on464, then inspect existing platform helpers for a separate narrow
repair. Keep PR463 separate and preserve valid qualification; no new candidate.

22:50UTC: separate D07 repair worktree created from M at
`C:/wsl/fcp-analysis-reader-sharing-20260912`, branch
`codex/analysis-content-reader-sharing`. Existing `stable_directory().open_read()`
already supports Windows delete sharing and stable POSIX handles. Next: add a
deterministic public-stream replacement regression, retain its red result against
unchanged M, then use this existing reader API for content-store reads. No physical
deployment or change to PR463. Heartbeat now includes D07/#464 and this next action.

22:54UTC: D07 regression against unchanged M product source is red: two deterministic
real Windows open-stream replacement cases fail with WinError5 (identical and
different payload); integrity-negative case passes. Native JUnit retained at
diagnostics/d07-red-regression.xml. Next: implement shared-delete stable readers
and bind size validation to the opened handle, then run focused regressions.

22:56UTC: shared-delete reader-only experiment is insufficient: deterministic
replacement cases still fail WinError5;60 other focused cases pass. Retained
diagnostics/d07-reader-only-insufficient.xml. This refines D07, not a new blocker.
Next: inspect Windows atomic replacement semantics before choosing a complete
repair. Work-in-progress is isolated on the D07 worktree; no deployment or CI retry.

22:58UTC: D07 repair pushed as draft PR465, head
f095f6cc96cc95c2a67d287b189e88267d95ae79. Complete fix uses shared-delete readers
plus explicit native POSIX replacement; prior replacement callers unchanged.
Focused native Windows tests67 PASS/3 platform skips in33.73s, receipt at
diagnostics/d07-final-focused-regression.xml. No physical operation or full retry.
Next: review the repair and required exact-head CI gaps; preserve PR463 proofs.
Both fixes remain separate. Final merged main must be qualified once after merges.

23:01UTC: PR465 current head4749ab6689315a7ad953c11ce4b2ec942433d71a handles
minimum-size native rename buffers; added one-character artifact-name coverage.
Targeted current-head native regressions10 PASS/3 platform skips, lint/diff PASS.
Prior67-test evidence remains bound to f095f6cc. Updated D07 and accumulating table.
Next: inspect current PR state, review, and start only absent required gates.

23:02UTC: PR463 automatic release34649416713 completed failure; Windows capability
D07 plus both release-matrix jobs and verdict are red. Do not assume aggregates
are independent defects; next retain only new terminal native logs and inspect
those failures. All other selected PR463 workflows now report success. PR465
head4749ab66 has seven automatic workflows queued/pending, no review threads.
Missing required workflows:CF7,CF7C,CF8,sharding; companions absent. Do not
dispatch duplicates. State polls and exact next actions persisted before diagnosis.


23:03UTC: new native logs verify PR463 sharding-Windows, ICSE-Windows/publication
and CFI2-Windows at83955f65: preserve21/37 plus3/3. Both red release matrices
explicitly see CHECKS=success,LINUX=success,WINDOWS=failure; verdict propagates
that dependency. No independent aggregate defect. Receipt:
diagnostics/pr463-d07-aggregate-classification.json. No release retry dispatched.

PR465 head4749ab66 qualification has six absent gates dispatched once at23:02UTC:
sharding,CF7,CF7C,CF8,CFI2,registry (all HTTP204); ledger persisted. Automatic
workflows remain pending/queued at last check. Next exact command:
python -B handoff/diagnostics/poll_pr465_state.py . Retain new completed logs and
verify checkout before counting evidence. Preserve PR463 proofs. D07 must be in
the required merged fix set before final merged-main qualification. Prefer
qualifying/merging the independent465 repair first; resolve any463 source/head
integration need from then-current checks/contracts, without retrying known
broken source merely to get green. Physical admission remains stopped on M.

23:14UTC: PR465 head4749ab66 unchanged; qualification jobs progressed without
reported failure. Sharding and CFI2 completed success; branding automatic run
completed success. All required workflows now exist. Next: retain only new
terminal native logs and verify checkout SHAs before counting; inspect any
completed synthetic-only source gap once. No physical or PR463 state change.

23:15UTC: native source verification retains PR465 required5/37 plus companion
2/3 PASS on exact4749ab66. Twelve new terminal logs preserved with digests.
Completed branding34656348285 proves synthetic2be67d18, not intended head.
Other synthetic-source workflows still have active jobs. Next: dispatch only
the completed branding exact-head gap once using the guarded helper; preserve
all verified proof and wait for remaining independent CI. Review count unchanged.

23:16UTC: branding exact-head gap dispatched once (HTTP204), with synthetic
checkout/digest proof in the ledger. PR465 now has seven justified dispatches
(six absent plus branding source gap), required5/37 plus companion2/3 native
PASS retained. No failures reported or review/head changes. Next actionable
command on the next state check: python -B handoff/diagnostics/poll_pr465_state.py;
then retain_pr465_new_native_logs.py only if new terminal jobs exist. PR463 and
physical M unchanged; no physical acceptance PASS or active timers.

23:26UTC: PR465 head4749ab66 unchanged, no reported failures. Branding exact-head
replacement, CF7C and registry completed success; new Linux successes in CF7B
and F85. Remaining workflows active/queued. Next: retain only newly terminal
logs and verify source. No duplicate dispatch or physical state change.

23:27UTC: five new native logs retained. Branding and CF7C-Linux verify4749ab66,
bringing required proof to7/37; registry brings companions to3/3. CF7B/F85 Linux
use synthetic2be67d18 and their parent runs remain incomplete. No new dispatch
is justified yet. Next: poll_pr465_state.py at next state check; preserve all
verified evidence, inspect only new completions/failures. Physical M unchanged.

23:37UTC: unchanged PR465 head4749ab66, no reported failures. Phase2 automatic
and CF7 exact-head runs completed; four automatic release jobs now success.
Next: retain new terminal logs, verify source and fill only a proven completed
phase2 source gap. All prior evidence preserved; no physical state change.

23:38UTC: CF7-Windows native checkout verifies4749ab66: required8/37 plus3/3
companions retained. Four release jobs and the final phase2 job prove synthetic
2be67d18. Both phase2 jobs now have retained source mismatch proof; next dispatch
its exact-head gap once. Release remains active; no release dispatch justified.

23:38UTC: phase2 exact-head4749ab66 dispatched once (HTTP204), both synthetic
job proofs recorded in the ledger. Eight justified dispatches total; preserve
required8/37 plus3/3 native PASS. Next: poll_pr465_state.py on next state check;
retain only newly completed evidence. No new defect, merge or physical change.

23:49UTC: PR465 head4749ab66 unchanged. Automatic F85 and software-update workflows
completed success; two additional release jobs succeeded. No failure reported.
Next: retain newly terminal native logs; verify both completed workflow source
gaps before dispatch. Preserve required8/37 plus3/3 exact-head proofs.

23:50UTC: four new native logs prove synthetic2be67d18. Completed F85 and update
workflows each have both source proofs retained; no exact-head run exists for
either. Next: guarded one-time dispatch of these two source gaps. Release remains
active; preserve8/37 plus3/3 native PASS. No additional defect or physical change.

23:50UTC: F85 and software-update exact-head4749ab66 dispatches succeeded
(HTTP204), each bound to both native synthetic proofs in the ledger. Ten justified
dispatches total. Next: poll_pr465_state.py at next state check; retain only new
terminal evidence. Remaining automatic CF7B/ICSE/release still active at last
check; no duplicate jobs, merge, deployment or physical acceptance claim.

2026-09-12 00:00UTC: PR465 head4749ab66 unchanged, no reported failures. New Linux
successes in exact-head F85/update/phase2 and three more automatic release jobs.
No newly completed synthetic-source workflow. Next: retain new terminal native
logs and update exact-head counts; do not duplicate active jobs.

00:01UTC: six new native logs retained. Exact4749ab66 verified for F85-Linux,
update-Linux and phase2-Windows (correcting the preceding phase2 platform label).
Required proof is11/37 plus3/3 companions. Three release shards use synthetic
2be67d18; release parent still active. No further dispatch justified. Next:
poll_pr465_state.py at next state check, retaining only new terminal evidence.
Physical M, PR463, protected data and timers remain unchanged.

00:11UTC: PR465 head4749ab66 unchanged, no reported failures. Phase2 exact-head
run completed; automatic ICSE has three successful jobs and release twelve.
Next: retain new terminal native logs, verify source before counting. No new
dispatch while ICSE/release remain active; preserve all earlier evidence.

00:12UTC: five new logs retained. Phase2-Linux proves exact4749ab66, bringing
required native PASS to12/37 plus3/3 companions. Windows capability-product
(including D07 regressions), journal-artifacts and ICSE-Windows succeeded on
synthetic2be67d18; these are not exact-head qualification. Order-independence
aggregate has no checkout and awaits dependency review with completed release.
Next: poll_pr465_state.py on next state check; retain only new completions.
No active-source gap dispatch, merge or physical change is justified now.

00:22UTC: PR465 head4749ab66 unchanged. Automatic release34656348410 completed
success16/16; automatic CF7B and ICSE also completed success. No failed jobs
reported. Next: retain new terminal native logs, inspect source/aggregate proof,
then fill only demonstrated completed exact-head gaps. Preserve12/37 plus3/3.

00:24UTC: completed CF7B/ICSE source jobs all prove synthetic2be67d18. Release
review confirms14 source-bearing jobs use that synthetic SHA; its two no-checkout
aggregates have verified successful dependency logs and identical workflow source.
Receipt: diagnostics/pr465-release-aggregate-review.json. Required12/37 plus3/3
exact-head PASS preserved. An audit helper retarget typo was caught by its run
count assertion and corrected before producing the receipt; no gate was weakened.
Next: dispatch the three demonstrated source gaps once (release,ICSE,CF7B), then
resume state-change polling. No physical acceptance or candidate freeze.

00:24:33UTC: PR465 exact-head4749ab66 gap dispatched: federation-v1-release.yml (HTTP204).
Native source proof and dispatch ledger committed; no duplicate or physical change.

00:24:40UTC: PR465 exact-head4749ab66 gap dispatched: icse-tool-demo.yml (HTTP204).
Native source proof and dispatch ledger committed; no duplicate or physical change.

00:24:47UTC: PR465 exact-head4749ab66 gap dispatched: cf7b-product-physical-acceptance.yml (HTTP204).
Native source proof and dispatch ledger committed; no duplicate or physical change.

00:35UTC: PR465 head4749ab66 unchanged, no reported failures. Exact-head F85/CF8
completed; final release34661528639 has two successful jobs. CF7B34661541571 and
ICSE34661535011 are queued. All13 required/companion workflows now have one
justified exact-head dispatch. Next: retain only new terminal native evidence.

00:36UTC: four new native logs verify exact4749ab66: F85-Windows, CF8-Windows,
release Windows checks and rotating full-suite order. Required native evidence
now16/37 plus3/3 companions; none inferred from synthetic runs. Next: poll
pr465_state.py via the existing poll_pr465_state.py helper on next state check,
then retain newly completed logs/artifacts only. Remaining CI active; no new
dispatch, defect, merge, candidate or physical state change.

00:46UTC: PR465 head4749ab66 unchanged, no reported failures. Exact-head update
workflow completed; CF7B gained one success and release gained two. Next: retain
only new terminal native logs and update source-verified counts. No dispatch,
merge or physical change; remaining qualification runs are active.

00:47UTC: four new native logs verify4749ab66: CF7B-Windows, update-Windows,
release Linux checks and fixed suite order. Required native proof now20/37 plus
3/3 companions. Preserve every completed job; no further dispatch is needed.
Next: python -B handoff/diagnostics/poll_pr465_state.py at next state check;
retain only newly completed evidence. Physical M and protected data unchanged.

00:57UTC: PR465 head4749ab66 unchanged, no reported failure. Exact-head release
now has eight successful jobs and ICSE three; both parent runs remain active.
Next: retain newly terminal native evidence only. Preserve existing20/37 plus3/3
proofs and do not dispatch duplicates. No physical or PR463 state change.

00:58UTC: seven new native logs all verify4749ab66, including Windows
capability-product, PostgreSQL, Linux shards1/3 and all three ICSE execution jobs.
Required native evidence now27/37 plus3/3 companions. Release and ICSE publication
remain incomplete; no final qualification verdict. Next: poll_pr465_state.py on
next state check; retain new native evidence and completed publication artifacts.
No duplicate jobs, physical acceptance claim, deployment or Recorder-data change.

01:08UTC: PR465 exact head4749ab66 now reports all37 required jobs plus3 companion
jobs successful. Final release34661528639 and ICSE34661535011 completed. This is
not yet a final qualification verdict: next retain remaining native logs, audit
release/ICSE artifacts and current reviews, then record the exact-head verdict
before normal merge. No rerun, candidate freeze or physical change.

01:10UTC: all final native logs retained;38 source-bearing checks verify4749ab66,
with two no-checkout aggregates awaiting final dependency reconciliation. Nine
release ZIPs plus six ICSE ZIPs retained with matching GitHub digests. Release
artifact review confirms4426 collected identities across four disjoint shards
and both full orders, zero failures/errors and complete nine-artifact set.
Current GitHub reviews/threads are empty. Next: validate native ICSE publication
and audit skipped-case coverage/current-head review before qualification verdict.

01:11UTC: ICSE source export verifies all1412 files against exact4749ab66, all19
checksum entries, Linux/Windows/compose4/4 scenarios, both native10-check network
sets and teardown. Receipt: diagnostics/pr465-icse-artifact-review.json. Next:
audit current-head skipped-case/native coverage and final source-bound aggregates;
no qualification or physical PASS inferred merely from green parent runs.

01:14UTC: PR465 head4749ab6689315a7ad953c11ce4b2ec942433d71a is QUALIFIED_REQUIRED_PR_HEAD_SCOPE:
37 required plus3 companion jobs, exact native source/aggregate proof, nine
release artifacts and ICSE publication reviewed. All six D07 cases execute on
Windows; five platform-neutral cases also execute in both full Linux orders.
Skips reconcile to14 passes elsewhere,10 dedicated-native coverage and11 unchanged
intentional platform exclusions matching M; no new exclusion. Manual review of
the three changed files has no correctness findings; current reviews are empty.
Receipt: diagnostics/pr465-final-qualification.json. Next: publish this verdict,
mark PR465 ready, and inspect live review/merge requirements before normal merge.
No candidate freeze or physical qualification; D06/PR463 remains required.


01:14:39UTC: PR465 marked ready for review at the qualified exact head4749ab66.
Published final qualification in its PR description. Next: inspect current
review/check state and normal merge eligibility; no override of branch policy.

01:16UTC: live premerge input confirms exact qualified465 head, ready state and
no review/comments/requested reviewers. GitHub reports mergeable but unstable
due to28 failed legacy hosted checks outside the retained required set; commit
status has zero entries. Do not infer product regressions or ignore these blindly.
Next: inspect their annotations/execution status and compare the documented
legacy-hosted disposition before normal merge. Required37+3 proof remains valid.

01:17:47UTC: all28 non-required hosted-check annotations confirm the known
GitHub billing/spending refusal before job execution, matching the completed
sweep report. No independent defect or product test failure. Fresh review inputs
still show no findings/requested reviewers and the exact qualified465 head.
Main branch metadata/policy-read result retained; use only the normal merge API,
without settings changes or bypass. Next: normal merge465 guarded by exact head;
then persist its actual merge SHA before considering463 integration.

01:18:40UTC: PR465 normal exact-head-guarded merge succeeded:
b6a96b218a513fe241ef4d6f051cf166444643ac. No bypass or repository-policy change.
Next: verify463 live head/clean worktree, integrate this merged main so463 also
contains D07, then qualify the resulting new PR head. Preserve historical proofs;
no deliberate intermediate-main qualification and no physical deployment.

01:19:47UTC: PR463 clean83955f65 integrated actual mainb6a96b21 without conflicts
and pushed new head2c1a8d9389a75fcaf4dd224ba63c3f83f02a0cee. The merge added only
the three qualified D07 files; PR diff against current main remains the five D06
files. No repair deployed. Next: archive83955f65 proof, verify unchanged D06/D07
source blobs, initialize new-head qualification state and dispatch only absent
required gates. New head evidence starts empty; no cross-SHA qualification carry.

01:20:28UTC: integration verified by identical Git blobs for all five D06 files
against83955f65 and all three D07 files against qualified4749ab66. Historical
83955f65 state/native proof archived as diagnostics/pr463-head-83955f65-*.json.
PR463 polling/retention/gap helpers now require2c1a8d93; current native proof is
empty. Next: poll_pr463_state.py and fill only absent new-head workflows.

01:21:38UTC: first new-head463 snapshot confirms2c1a8d93 and seven automatic
workflows queued, no completed proof. Missing:CF7,CF7C,CF8,F85 plus CFI2/registry
companions. Next: dispatch only these six absent workflows through the guarded
helper; existing seven automatic workflows remain untouched.

01:21:52UTC: six absent463 gates for exact2c1a8d93 dispatched once (HTTP204).
New-head ledger entries preserve the prior heads separately. Next: monitor only
PR463 state changes using poll_pr463_state.py; retain new native evidence and
fill demonstrated completed source gaps only. Do not repeat465 qualification.
Physical M unchanged; final merged-main qualification waits for463 merge.

01:22:50UTC: handoff top, D06/D07 table and automation now target integrated463
head2c1a8d93. All six absent-gate dispatches confirmed/pushed; no active local
execution remains. Next: state-change poll463, retain new native evidence, then
normal merge when qualified/reviewed and final actual-main qualification once.
Do not poll or requalify merged465. Physical M/protected Recorder data unchanged.

01:33UTC: PR463 integrated head2c1a8d93 unchanged. Automatic sharding completed
success2/2; one CFI2 companion succeeded. All required workflows exist, no
reported failure. Next: retain new native logs and verify source before counting
or deciding whether completed sharding needs an exact-head dispatch.

01:34UTC: three native logs retained. CFI2-Linux verifies exact2c1a8d93 (required
0/37, companion1/3). Both completed sharding jobs prove synthetic2cc004b3. Next:
dispatch that sole demonstrated exact-head gap once; leave active runs untouched.

01:34:15UTC: sharding exact-head2c1a8d93 dispatched once (HTTP204), with both
synthetic source proofs in the ledger. Seven new-head dispatches total. Next:
poll_pr463_state.py on next state check; retain only new terminal evidence.
No new defect, merge or physical change;465 remains qualified/merged.

01:44UTC: PR463 head2c1a8d93 unchanged, no reported failures. Automatic branding
completed success; two release jobs, one exact-head sharding job and one CF7 job
now successful. Next: retain new native logs and verify checkout before counting
or filling the completed branding source gap. No duplicate or physical action.

01:45UTC: native logs verify exact2c1a8d93 for sharding-Windows and CF7-Linux:
required2/37 plus companion1/3. Completed branding proves synthetic2cc004b3;
release jobs also synthetic and parent remains active. Next: dispatch only the
completed branding exact-head gap; preserve all verified work.

01:45:46UTC: branding exact-head2c1a8d93 dispatched once (HTTP204). Eight
new-head dispatches now recorded with source evidence. Next: poll_pr463_state.py
on next state check and retain only new completed jobs. No new defect or physical
change; required2/37 plus companion1/3 native PASS preserved.

01:56UTC: PR463 head2c1a8d93 unchanged, no reported failure. Automatic phase2
and release each gained one successful job; parent runs remain active. Next:
retain only these newly terminal native logs; no new dispatch justified.

01:57UTC: both new logs prove synthetic2cc004b3 (journal-artifacts and phase2
Linux); no exact-head count change. Preserve required2/37 plus companion1/3.
Next: poll_pr463_state.py at next state change check, retaining new terminal
evidence only. Active workflows remain untouched; physical M unchanged.

02:07UTC: PR463 head2c1a8d93 unchanged, no reported failures. Exact-head sharding
completed; CF7C/F85 and automatic CF7B/update/release gained successes. Next:
retain new terminal logs and verify source. No completed automatic source gap
or new dispatch is indicated by this snapshot.

02:08UTC: six new logs retained. Exact2c1a8d93 verified for sharding-Linux,
CF7C-Linux and F85-Linux: required5/37 plus companion1/3 native PASS. CF7B/update
Linux and release Windows checks use synthetic2cc004b3; their parents remain
active. Next: poll_pr463_state.py at next state check; no additional dispatch,
merge or physical change. All completed exact-head evidence preserved.

02:18UTC: PR463 head2c1a8d93 unchanged. Automatic CF7B and exact-head registry
completed success; no failed jobs. Next: retain their new native logs, verify
source, and fill CF7B only if its completed source gap is demonstrated.

02:19UTC: registry verifies exact2c1a8d93; preserve required5/37 plus companion2/3.
Both completed CF7B jobs prove synthetic2cc004b3. Next: guarded one-time CF7B
exact-head dispatch; all active/passing runs remain untouched.

02:19:43UTC: CF7B exact-head2c1a8d93 dispatched once (HTTP204), both native
source proofs retained in ledger. Nine new-head dispatches total. Next: poll
via poll_pr463_state.py at next state check; retain new terminal evidence only.
No new defect, merge, candidate or physical state change.

02:30UTC: PR463 head2c1a8d93 unchanged, no reported failure. Exact branding
completed success; CF8 and automatic ICSE gained one successful job. Next: retain
only new terminal native logs and verify checkout; no source-gap dispatch is
justified while remaining automatic parents are active.

02:31UTC: branding and CF8-Linux native logs verify2c1a8d93, bringing required
proof to7/37 plus companion2/3. ICSE-Linux uses synthetic2cc004b3 and parent
remains active. Next: poll_pr463_state.py at next state check; retain only new
terminal evidence. No extra dispatch, merge, defect or physical state change.

02:41UTC: PR463 head2c1a8d93 unchanged, no reported failures. Automatic phase2
and exact-head CF7 completed; CF7B and release each gained one success. Next:
retain new terminal native evidence and verify completed phase2 source gap before
any dispatch. Preserve required7/37 plus companion2/3.

02:42UTC: CF7B-Linux and CF7-Windows verify exact2c1a8d93: required9/37 plus
companion2/3 native PASS. Both completed phase2 jobs prove synthetic2cc004b3;
rotating release order also synthetic. Next: guarded phase2 exact-head dispatch
once; active release remains untouched.

02:42:12UTC: phase2 exact-head2c1a8d93 dispatched once (HTTP204), both native
source proofs recorded. Ten new-head dispatches total. Next: poll_pr463_state.py
at next state check; retain newly completed native evidence only. Physical M,
protected data and candidate status unchanged; no physical PASS or timed runs.

02:52UTC: PR463 head2c1a8d93 unchanged, no reported failure. Automatic release
has nine successes and ICSE two, with both parent runs still active. Next:
retain newly terminal native logs; preserve9/37 plus2/3 exact-head proof and
leave active workflows untouched. No new dispatch justified.

02:53UTC: five new native logs all prove synthetic2cc004b3 (release shards2/3,
Windows capability/transport and ICSE compose); exact-head count stays9/37 plus
2/3 companions. No failed tests or completed new source-gap parent. Next:
poll_pr463_state.py at next state check; retain only new terminal evidence.
No extra jobs, merge, deployment or protected Recorder-data operation.

03:03UTC: PR463 head2c1a8d93 unchanged. CFI2 exact-head companion run completed;
phase2 gained one success and automatic release two. No failed jobs. Next:
retain new terminal native logs and verify checkout. Other parents remain active;
no additional dispatch is justified yet.

03:04UTC: phase2-Linux and CFI2-Windows native logs verify2c1a8d93, bringing
required proof to10/37 and companions to3/3. Two release jobs prove synthetic
2cc004b3; parent remains active. Next: poll_pr463_state.py at next state check;
retain newly completed evidence only. No rerun, merge or physical state change.

03:14UTC: PR463 head2c1a8d93 unchanged, no reported failure. Automatic release
34664467237 completed16/16, ICSE34664467253 and update34664467309 also success.
Exact CF7B completed. Next: retain new terminal native logs and verify source/
aggregate dependencies before filling the remaining demonstrated exact-head gaps.

03:16UTC: nine new native logs retained; CF7B-Windows verifies2c1a8d93, bringing
required exact-head proof to11/37 plus3/3 companions. Completed release review
confirms14 synthetic2cc004b3 source jobs and two successful dependency-only
aggregates with identical checked-in workflow. ICSE/update source jobs also
prove synthetic. Old aggregate review archived by SHA. Next: dispatch only
these three completed source gaps once (release,ICSE,update). No new defect.

03:16:47UTC: integrated PR463 exact-head2c1a8d93 gap dispatched once:
federation-v1-release.yml (HTTP204). Native proof and ledger preserved; no duplicate job or
physical action. Next: state-change poll463 and retain new terminal evidence.

03:16:54UTC: integrated PR463 exact-head2c1a8d93 gap dispatched once:
icse-tool-demo.yml (HTTP204). Native proof and ledger preserved; no duplicate job or
physical action. Next: state-change poll463 and retain new terminal evidence.

03:17:01UTC: integrated PR463 exact-head2c1a8d93 gap dispatched once:
federation-software-update.yml (HTTP204). Native proof and ledger preserved; no duplicate job or
physical action. Next: state-change poll463 and retain new terminal evidence.

03:27UTC: integrated PR463 exact-head2c1a8d93 poll shows new successful jobs in release, ICSE and update; CF7C and CF8 now completed successfully. No failing required job or head change. All13 workflows already have one justified exact-head dispatch. Next: retain newly terminal native logs and update verified counts. Physical admission remains stopped; no physical action or timers.

03:27UTC evidence retention: seven new successful native job logs all verify exact2c1a8d9389a75fcaf4dd224ba63c3f83f02a0cee. Required proof now18/37 plus3/3 companions. Receipts preserve log hashes; no rerun, new defect or physical evidence. Next exact command: audit Python -B handoff/diagnostics/poll_pr463_state.py; inspect only material changes, retain newly completed logs, then final artifact/review checks when the remaining required runs finish.

03:41UTC: PR463 head remains2c1a8d9389a75fcaf4dd224ba63c3f83f02a0cee. Release now has six successful jobs; update and F85 completed successfully. Phase2 is running; no required failures. All13 exact-head dispatches remain accounted for. Next: retain six newly terminal native logs, verify checkout provenance, then await remaining required checks. Physical admission remains stopped.

03:41UTC evidence retention: all six new successful native logs verify exact2c1a8d9389a75fcaf4dd224ba63c3f83f02a0cee; required proof now24/37 plus3/3 companions. Full-suite rotating, journal-artifacts and two Linux shards are among new receipts. No duplicate dispatch or new defect. Next command: audit Python -B handoff/diagnostics/poll_pr463_state.py; retain only changes and perform final artifact/current-review verification after the remaining required runs finish. No physical action or timers.

03:53UTC: PR463 remains exact2c1a8d9389a75fcaf4dd224ba63c3f83f02a0cee. Release now has nine successful jobs; phase2 completed successfully. Only release and ICSE remain active; no required failure or new dispatch. Next: retain four newly terminal native logs and verify exact checkout before updating proof count. Physical admission remains stopped.

03:53UTC evidence retention: four new successful native logs all verify exact2c1a8d9389a75fcaf4dd224ba63c3f83f02a0cee; required proof now28/37 plus3/3 companions. Both full-suite orders and Windows capability-product now have successful native receipts; final artifact/skip review remains required. Next exact command: audit Python -B handoff/diagnostics/poll_pr463_state.py; only release and ICSE remain active, then complete current-head artifact/correctness review before normal merge. No new defect, physical action or timers.

04:04UTC: all13 selected PR463 workflows completed successfully on recorded head2c1a8d9389a75fcaf4dd224ba63c3f83f02a0cee; release16/16 and ICSE4/4 now terminal. This is CI completion, not yet the final qualified verdict. Next: retain nine new native logs, verify two dependency-only aggregates, current release/ICSE artifacts, skips and correctness review; then normal merge if all contracts pass. No physical actions.

04:05UTC native retention complete:38 source-bearing required/companion jobs prove exact2c1a8d93; two successful release jobs perform dependency-only aggregation and require the final workflow/log reconciliation. All40 native log hashes retained. Next: inspect raw native release/ICSE artifacts and current review state; qualification remains pending final reconciliation.

04:07UTC: native release9 and ICSE6 raw artifacts retained with every GitHub digest verified. Prior7286 artifact receipts archived by SHA before current aliases updated. Live review thread PRRT_kwDOPZM3cc6hmxQd is resolved/outdated, with no new review findings; sole historical review is COMMENTED on7286. Next: final exact-source artifact/skip review and D06/backup regression reconciliation. No product or physical state changed.

04:07:40UTC: exact-head release artifact review verified9/9 artifacts, zero failures/errors,4440 disjoint collected tests and both full-order testcase sets. ICSE native publication source equals1413 exported files from2c1a8d93;19 checksum entries,4/4 component scenarios on Linux/Windows/compose and10/10 network checks on each native platform with teardown. Skip map:35 unique skipped cases,14 pass elsewhere;21 require final native-log/unchanged-exclusion reconciliation. D06 Linux11/11 exit tests,32/32 responder tests and17/17 backup tests pass in both full orders, including both backup deadline/error regressions. Windows current focused group150 pass/7 Linux-specific skips. Final review/merge remains next; no physical evidence.

04:10:38UTC: PR463 exact2c1a8d9389a75fcaf4dd224ba63c3f83f02a0cee QUALIFIED_REQUIRED_PR_HEAD_SCOPE,37/37+3/3. Final receipt reconciles38 native source checkouts, two dependency-only aggregates,4440 test identities,9 release artifacts and ICSE source1413 files. Audit-only inherited zero-skip assumption was corrected by exact AST/collection/native-summary accounting:157 focused Windows cases=150 pass+7 explicit Linux-only skips, proving three Recorder-launcher cases executed. No changed acceptance assertion/test/deadline or product defect;11 unchanged intentional POSIX exclusions match qualified M. Sole historical correctness thread resolved/outdated, backup source10s deadline unchanged. Next: fresh head/review/merge-policy check and normal PR463 merge; then qualify actual final main once. Physical admission still stopped onM, no timers.

04:11UTC premerge: PR463 remains exact2c1a8d93, ready/non-draft, mergeable=true/clean, no nonpassing checks or requested reviewers. Sole historical comment/review matches the resolved D06-R1 finding; no unresolved current correctness findings. Actual main remainsb6a96b21. Main metadata reports unprotected; rules endpoint403 is the documented plan restriction. Normal merge only, no policy override. Next: publish exact-head qualification receipt on463 and merge with expected_head_sha guard.

2026-09-12T04:12:06.620483+00:00: PR463 normal guarded merge succeeded as2a9c9b8eb53edff74c2de23570ec56e054d29b22;
qualification comment5643376319 and merge result retained. Next exact action:
fetch origin main, verify it equals the merge result, create a clean detached audit
worktree, archive old M qualification aliases, and poll actual merged-main workflows.
Only required absent gates may be dispatched; no physical deployment or candidate freeze yet.

04:13UTC actual final main2a9c9b8eb53edff74c2de23570ec56e054d29b22 fetched and verified; clean detached audit checkout C:/wsl/fcp-v1-2a9c9b8e-merged-main-20260912 created. Both qualified repair heads are ancestors and tree equals qualified463; this does not carry qualification across SHA. All old M merged-main JSON receipts archived under diagnostics/archive-main-9b286f93 before current target/state/native/dispatch aliases reset. Six push workflows already exist (release34672364135, ICSE34672364156, CF7B34672364171, update34672364185, branding34672364194, phase234672364216), one update job completed. Seven remaining gates absent under current diff triggers. Next: retain the new native update proof, inspect those checked-in triggers, dispatch only proven absent required gates once and persist each dispatch. Automation now targets final main. No candidate freeze/deployment.

04:14UTC: current-main update-Linux native job103496010690 verifies exact2a9c9b8e (1/37 native proof so far). Seven missing workflows are explained by checked-in path filters/no push trigger; all support workflow_dispatch: CF7,CF7C,CF8,CFI2,sharding,F85,registry. Guarded gap helper now takes exactly one named missing workflow and rechecks actual main, remote absence and ledger before dispatch. Next: invoke it once for each absent gate, committing/pushing its ledger before the next invocation. No existing workflow will be rerun.

04:14:28UTC: absent final-main2a9c9b8e gate cf7-acceptance-harness.yml dispatched once; guarded request and response persisted in merged-main-gap-dispatches.json. Existing push evidence preserved; no physical action. Next: remaining absent gates, then state-change poll and native retention.

04:14:34UTC: absent final-main2a9c9b8e gate cf7c-physical-test-readiness.yml dispatched once; guarded request and response persisted in merged-main-gap-dispatches.json. Existing push evidence preserved; no physical action. Next: remaining absent gates, then state-change poll and native retention.

04:14:39UTC: absent final-main2a9c9b8e gate cf8-role-retirement.yml dispatched once; guarded request and response persisted in merged-main-gap-dispatches.json. Existing push evidence preserved; no physical action. Next: remaining absent gates, then state-change poll and native retention.

04:14:45UTC: absent final-main2a9c9b8e gate cfi2-onboarding-composition.yml dispatched once; guarded request and response persisted in merged-main-gap-dispatches.json. Existing push evidence preserved; no physical action. Next: remaining absent gates, then state-change poll and native retention.

04:14:50UTC: absent final-main2a9c9b8e gate ci-test-sharding.yml dispatched once; guarded request and response persisted in merged-main-gap-dispatches.json. Existing push evidence preserved; no physical action. Next: remaining absent gates, then state-change poll and native retention.

04:14:55UTC: absent final-main2a9c9b8e gate phase-f85-operator-federation-surface.yml dispatched once; guarded request and response persisted in merged-main-gap-dispatches.json. Existing push evidence preserved; no physical action. Next: remaining absent gates, then state-change poll and native retention.

04:15:00UTC: absent final-main2a9c9b8e gate release-image-metadata.yml dispatched once; guarded request and response persisted in merged-main-gap-dispatches.json. Existing push evidence preserved; no physical action. Next: remaining absent gates, then state-change poll and native retention.

04:15UTC: all13 required/companion final-main workflows now present (six existing push runs plus seven single guarded dispatches); no missing workflow, duplicate dispatch or failure. Actual main still2a9c9b8e. Three jobs completed successfully; next retain two new native logs, then wait for actual state changes via poll_merged_main_state.py. Retained final-main artifact/skip/final-verdict aliases not yet updated still describe old M and cannot qualify2a9; exact old evidence is archived under archive-main-9b286f93. Future final review should adapt the current PR463 review logic (including explicit seven Linux-only Windows skips) to actual main and its own native artifacts; no cross-SHA proof.

04:15UTC native retention: final-main2a9c9b8e has3/37 verified required successes (update-Linux, release-Linux, ICSE-compose); each native checkout equals the actual merged SHA, log hashes persisted. Remaining required/companion jobs are queued/running; no failure. Next exact command: C:/wsl/fcp-v1-fba508-nettking-20260910/.venv/Scripts/python.exe -B handoff/diagnostics/poll_merged_main_state.py from this diagnostic worktree; only on changes persist state, retain new logs with retain_merged_main_jobs.py, and progress to complete exact-main artifact review when terminal. Physical runtime/protected Recorder invariant unchanged; no candidate freeze or timers.

04:26UTC: actual final main remains2a9c9b8eb53edff74c2de23570ec56e054d29b22; eight new successful terminal jobs across release,CF7B,update,phase2,CF7C,CFI2. Update workflow completed; no failing required job or missing workflow. Next: retain new native logs and verify source before counting. Existing qualification runs preserved; no physical action.

04:26UTC native retention: all eight new terminal logs prove exact2a9c9b8eb53edff74c2de23570ec56e054d29b22; required proof now10/37 plus1/3 companions. Hash receipts committed; no source mismatch or new defect. Next exact command: audit Python -B handoff/diagnostics/poll_merged_main_state.py; retain only changed terminal jobs with retain_merged_main_jobs.py and await full required completion before final artifact/skip/ICSE review. No dispatch, candidate freeze, physical operation or timers.

04:37UTC: actual final main remains2a9c9b8eb53edff74c2de23570ec56e054d29b22; seven new successful terminal jobs (release2,ICSE1,CF7,CF8,F85,registry). Registry workflow completed; no failure or missing workflow. Next: retain those native logs and verify exact source before counting. No dispatch or physical action.

04:37UTC native retention: all seven new terminal logs prove exact2a9c9b8eb53edff74c2de23570ec56e054d29b22; required proof now16/37 plus2/3 companions, including immutable registry metadata. No source mismatch or new defect. Next exact command: audit Python -B handoff/diagnostics/poll_merged_main_state.py; retain only new terminal jobs with retain_merged_main_jobs.py. Final artifact/skip/ICSE review remains pending full completion; physical admission stopped, no timers.

04:49UTC: actual main remains2a9c9b8eb53edff74c2de23570ec56e054d29b22; six new successful terminal jobs. ICSE,CF7B,CFI2 completed successfully; full-suite rotating and Linux sharding contract also passed. No failed or missing required workflow. Next: retain new terminal native logs, verify source, then continue state-change checks; final artifact reconciliation remains pending. No dispatch or physical action.

04:49UTC native retention: all six new terminal logs verify exact2a9c9b8eb53edff74c2de23570ec56e054d29b22; required proof now21/37 plus3/3 companions. ICSE4/4 native source proofs complete; publication artifact review still required. No new defect or duplicate job. Next exact command: audit Python -B handoff/diagnostics/poll_merged_main_state.py; retain only new terminal jobs with retain_merged_main_jobs.py and complete current-main artifact/skip/ICSE review once required runs finish. Physical admission remains stopped; no timers.

05:00UTC: actual main remains2a9c9b8eb53edff74c2de23570ec56e054d29b22; seven new successful terminal jobs (release3,branding,phase2,CF7C,CF8). Both full-suite orders now completed successfully; no failed or missing required workflow. Next: retain new native logs and verify exact checkout before updating count. No dispatch or physical action.

05:00UTC native retention: all seven new terminal logs prove exact2a9c9b8eb53edff74c2de23570ec56e054d29b22; required proof now28/37 plus3/3 companions. Both full-order native receipts retained. No source mismatch or new defect. Next exact command: audit Python -B handoff/diagnostics/poll_merged_main_state.py; retain only newly terminal jobs with retain_merged_main_jobs.py, then complete exact-main artifact/skip/ICSE review when required jobs finish. No rerun, physical action, freeze or timers.

05:12UTC: final main remains2a9c9b8eb53edff74c2de23570ec56e054d29b22. Release16/16 including verdict,CF7 and sharding completed successfully; only F85-Windows remains running. Eight new terminal logs await retention, including two dependency-only release aggregates. Next: retain native logs and current release/ICSE raw artifacts; perform exact-main artifact/skip review without reruns while the remaining job finishes. No physical action or candidate freeze.

05:13:49UTC: eight new native logs retained: six exact2a9 source checkouts and two successful dependency-only aggregates. Native raw artifacts retained once: release9 and ICSE6, every GitHub digest verified. Old artifact receipts remain archived by M SHA; current retention aliases now2a9. F85-Windows is the only unfinished job in the selected snapshot. Next: verify current-main4440-node artifact sets, skips, aggregate dependencies and ICSE source export; do not infer final qualification until F85 native success also verifies.

05:15:11UTC: exact-main artifact review verifies9/9 release artifacts,4440 test identities, both full-order sets and zero failures/errors. ICSE current native bundle equals1413 exported source files,19 checksums;4/4 components on three runtimes and10/10 network checks with teardown on both platforms. Skip map35 unique/14 pass elsewhere; final review script includes explicit7 Linux-only skips in Windows focused group, previous11 intentional exclusions and D06/D07 regressions. Current-main helpers created; no test rerun. Next: check last F85-Windows transition, retain its native log if terminal, then run finalize_current_main_qualification.py only after all37+3 complete.

05:15:33UTC: required F85-Windows job103496400575 in run34672498480 failed on actual main2a9c9b8eb53edff74c2de23570ec56e054d29b22; all other selected required/companion jobs completed successfully. Final-main qualification cannot pass; source/cause not yet classified. Next exact action: retain_merged_main_jobs.py for the failed native log, inspect its failing stage/traceback, classify before any retry or edit. Current artifact reviews remain valid on this SHA. No candidate freeze, product edit or physical action.

2026-09-12T05:17:05.862654+00:00: D08 confirmed required qualification failure persisted with exact command, native checkout/hash and traceback. F85-Windows actual2a9 main reports artifact-object-key-escape from concurrent analysis;1 failed,728 passed,1 skipped. Independent product root cause not established; no blind rerun. Next: publish GitHub issue referencing diagnostics/D08.md and D08-evidence.json, then inspect exact-source mechanism. Protected Recorder data and all physical runtimes untouched.

05:17UTC: D08 published as GitHub issue466 with source-bound evidence and impact. Next: exact-main path-validation/concurrent scheduling analysis; only isolated owned-fixture repro if justified. Do not retry F85 or create/qualify a new candidate until failure classification establishes the appropriate action.

05:19UTC D08 product mechanism confirmed on clean actual main: public store concurrent writes/resolve with old stream open returns NTFS $Extend/$Deleted path from Path.resolve for valid key, triggering artifact-object-key-escape. Focused13 writes/1396 resolves/two captured errors; exact frames retained. No source modification, external state, protected data or physical runtime touched. Next: update issue466 with focused evidence, inspect resolve callers/symlink guard contracts, then isolated D08 regression/repair branch. No F85 retry, final-main PASS, candidate freeze or physical admission.

2026-09-12T05:22:38.789489+00:00: D08 issue466 comment5643728004 contains confirmed public-API mechanism.
Automation retargeted to D08 repair; routine qualification polling is no longer
useful until the repair/classification stage changes. No blanket F85 retry.
Source caller inspection: only production external resolve caller is
analysis/scheduler.py:_ensure_slice_archive; all content-store reads/writes also
call resolve. Existing containment test is test_analysis_artifact_access.py near379.
Proposed parent/leaf-validation approach is a hypothesis, not a selected repair:
preserve true root-escape/symlink rejection and stable-handle/identity semantics.
Exact next read: from clean C:/wsl/fcp-v1-2a9c9b8e-merged-main-20260912,
read catalog/capabilities/tests/test_analysis_artifact_access.py lines350-390,
catalog/capabilities/artifact_contracts.py:_logical_key, and checked-in stable
filesystem directory/leaf reparse rules. Then create isolated worktree from2a9,
add a deterministic Windows deleted-name interleaving regression plus concurrent
public-API and real root-escape negative controls, confirm red on unchanged source,
and repair only after the safe path contract is concrete. No full qualification
merely because a patch exists; push branch/draft PR/focused results first.
Private retained repro root is .acceptance/d08-resolve-ku191byw; no cleanup needed.
All current work is pushed; product/physical checkouts remain clean and unchanged.

2026-09-12T05:37:55.084846+00:00: D08 red test checkpoint pushed on isolated repair branch ata9f8271feb361856e234c2db2d391278fda39a6b. Four native deleted-name interleavings (plain/extended roots and short/nested keys) plus concurrent public-API reproduction fail on unchanged2a9 product source;4 basic cases pass,4 symlink cases skip for Martin lacking Windows symlink privilege. Next: narrow Windows non-reparse leaf resolution fix, preserve full resolution for actual links/reparse entries, add unprivileged real junction containment tests and focused validation. No CI qualification dispatched; no physical changes.

2026-09-12T05:43:20.975418+00:00: D08 repair84c66f81 pushed clean; draft467 published with baseline/red and focused/green evidence. Current exact source blobs, test lists, log hashes and explicit privilege/formatting limitations persisted in diagnostics/D08-repair-focused.json. No full qualification dispatch or physical operation. Next exact action: read current draft467 reviews/threads, inspect diff against2a9 and its two changed files; if no correctness issue remains, plan minimum exact-head qualification under checked-in policy without repeating old successful SHA evidence.

05:44:25UTC: issue466 updated with draft467 repair and focused-evidence links; automation now targets exact84c66f81 review before qualification decisions. Next action remains current PR467 review/containment audit, then only justified exact-head gates; no blind reruns. Repair branch and diagnostic handoff are both pushed clean.

05:56UTC: exact84c66f81 prequalification correctness review complete, no findings. AST verifies only resolve method changed; key validation, reader/identity handling, stable filesystem and scheduler callers unchanged. Actual links retain full target resolution/containment; normal Windows leaves avoid renamed-object final names. Focused red/green proof preserved with privilege limitation explicit. New product-boundary repair after confirmed F85 failure is the concrete reason for37+3 exact-head qualification, after review; this is not a repeat of prior passing SHA evidence. Next: mark467 ready, poll existing auto runs, retain source proofs, fill only demonstrated gaps.

05:56:37UTC: PR467 marked ready for review at exact84c66f8185c1411d9dc8c5c33244a2f564845ce7 after persisted correctness/qualification-scope review. Normal review process, no merge or policy override. Next: poll467 only and retain already completed native logs before selecting any missing gate.
