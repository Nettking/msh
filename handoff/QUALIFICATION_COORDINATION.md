# Federation v1 qualification coordination

Current user direction, September 11 2026: execute on state changes, using this
checkpoint and the completed diagnostic sweep report; do not repeat the sweep.

Sequence: qualify required exact PR heads -> merge456/457/461 as separate fixes
-> qualify actual final merged main once -> resolve/verify D04 through the
documented controlled host procedure -> freeze one AUTHORITATIVE_SHA -> clean
checked-in revalidation -> fresh formal physical acceptance.

| PR | Intended exact head | Phase |
|---|---|---|
|456|1a0c634f47f8a247b6d1d2d1a219f5c12590587d|Qualification incomplete at previous checkpoint; refresh only state delta|
|457|143fe7a9082193114af3d34dc437b85f845849a2|Required exact-head qualification PASS; preserve evidence, hold for complete fix-set qualification and live premerge checks|
|461|5b826c6806ab1bdb960412ba20ca78192971fb1d|Draft repair;50 focused tests pass; required qualification pending|

Runtime remains0536f03d67eb277e11573c2188d8e820399627e3. No physical PASS; P07/P12
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
