# Federation v1 qualification coordination

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

**Current stage:** qualify actual final merged main
`9b286f931497bf6291e215f6340443c5162826b0` once. All fix heads are ancestors.
Use `diagnostics/poll_merged_main_state.py` and
`diagnostics/retain_merged_main_jobs.py`; PR qualification is complete and must
not be repeated. Final main is not yet qualified or frozen as a physical candidate.

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
