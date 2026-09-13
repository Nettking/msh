# PR475 exact-head qualification reconciliation

Recorded: 2026-09-13T03:09:47.232615+00:00

## Scope decision and source gap

The standing product-fix campaign requires qualification of the intended exact PR
head before merge, followed by actual final merged-main qualification once. The
established required-head scope is37 jobs plus3 CFI2/registry companions, retained
in diagnostics/pr467-final-qualification.json (status QUALIFIED_REQUIRED_PR_HEAD_SCOPE)
and prior PR463/465/467 coordination entries. The checked-in V1-F permanent release
workflow protects the broad product boundaries; CF7 acceptance documentation also
requires exact-commit matrices. No new optional diagnostic gate is being added.

Current intended PR475head is5e6f184311019b9982e8544a18f3dc02c1b16e98.
All27 automatic PR checks now pass, but their retained native checkout is
 a5e743fee24e27bfd8d6d4f57c8efd589c6c3a42, the GitHub PR merge source.
Both trees are1c671f446fa215c99a6a58a155806de394aa569a. These results are valid and
retained for that source; they are not relabelled as native5e qualification.
The concrete reason for native dispatch is the exact-head source gap, not timer
cadence, disappearing historical red statuses, or a failed assertion retry.
The prior documentation-only native F7 replacement reuse remains untouched.

## Exact required mapping

| Checked-in workflow | Jobs | Existing proof and action |
|---|---:|---|
|cf7-acceptance-harness.yml|2|Absent from PR automatic path; required-head gap|
|phase2-federation.yml|2|Automatic a5e PASS retained; native5e gap|
|cf7b-product-physical-acceptance.yml|2|Automatic a5e PASS retained; native5e gap|
|product-branding.yml|1|Automatic a5e PASS retained; native5e gap|
|federation-software-update.yml|2|Automatic a5e PASS retained; native5e gap|
|cf7c-physical-test-readiness.yml|2|Absent from PR automatic path; required-head gap|
|icse-tool-demo.yml|4|Automatic a5e PASS retained; native5e gap|
|cf8-role-retirement.yml|2|Absent from PR automatic path; required-head gap|
|phase-f85-operator-federation-surface.yml|2|Absent from PR automatic path; required-head gap|
|federation-v1-release.yml|16|Automatic a5e attempt2 PASS retained; native5e gap|
|ci-test-sharding.yml|2|Absent from PR automatic path; required-head gap|
|cfi2-onboarding-composition.yml|2|Standing companion source gap|
|release-image-metadata.yml|1|Standing metadata companion source gap|

Each existing unchanged workflow supports workflow_dispatch with checkout of its
selected ref. Dispatch only at codex/fix-d13-windows-refusal-response after checking
remote ref/PR head5e and absence of existing exact-head manual runs. Sharding uses
the default shared-pool; no host-admission target. Existing workflow/runner labels,
commands, dependencies, service assumptions, guards and deadlines remain unchanged.
Release metadata is a read/verification gate, no image publication/deployment.
No intermediate main qualification, branch merge or candidate selection occurs.

## Correctness/status review

Exact repair review: product diff26 additions and test diff84 additions, no other
files. Refusal404 sent before bounded declared-body drain; no JOIN/identity/grant
path modification. Monotonic budget10s, maximum4096 bytes; EOF/errors terminate.
The regression closes owned loopback sockets/server in finally; clock monkeypatch
is scoped to the responder module and restored by pytest. Focused base-red/new-green
and native Windows/Linux46PASS each remain preserved; no new review issue identified.

D11/D14/D15/D16 original observations and unknown causes remain recorded. The latest
same-source native required suite passes their cases without a repair. This does
not establish environmental attribution or close those issues. Reconcile any new
exact-head failure before further action. No arbitrary cancelled-AQG diagnosis is
required. GitHub reviews/comments are empty; the recorded local review is not an
external approval. Branch protection/rules read endpoints returned403, not an empty
policy. Do not bypass GitHub merge checks or interpret that403 as product failure.

## Persistence and next step

Plan and all new native evidence must be pushed before dispatch. Dispatcher records
each accepted or uncertain operation durably; on resumption inspect existing runs
before any retry. Retain startup once, then no regular qualification poll before
2026-09-13T04:01:37UTC (unless an actual completion/failure event arrives sooner).
No new candidate is frozen and no main qualification is started while product repair
and separate retirement PR473 remain unmerged. Final merged-main scope runs once
when the intended fix/cleanup set is complete. Physical runtime9b286f93, protected
Recorder data and P07/P12 remain untouched; CI is not physical evidence.
