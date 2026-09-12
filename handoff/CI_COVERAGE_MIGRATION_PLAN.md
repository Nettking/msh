# Coverage-preserving self-hosted CI migration

Status: **STAGE1 IMPLEMENTED in draft [PR468](https://github.com/Nettking/msh/pull/468); native equivalence proof pending.**
Plan/matrix were pushed at41d89c63 before source edits. Exact source: f3abe5452db2f21593a688bc62bc5f4b22d5c40e.
Qualified baseline: `17ab3a05c9c506e0f92adfaa4fa0bac231ac2c05`.
[Final main qualification](diagnostics/merged-main-final-qualification.json) is
complete37/37 plus3companions with native/artifact review. Preserve it unchanged.
Physical runtime remains9b286f93; no physical PASS, deployment or timed campaign.

## Minimal proposal

Extend **one existing workflow**, `phase-f7-closeout.yml`, keeping its current
Linux/Windows matrix: **zero new workflows and zero new job definitions**.
Keep release, F6 closeout, F8 closeout, the Python setup action and runner pools
unchanged. The [exact equivalence matrix](diagnostics/ci-migration-equivalence-matrix.json)
includes all old jobs/commands, OS, trigger paths and service assumptions.

1. Add eight missing product/test paths to both PR and main-push triggers: the
   two F6 transfer implementations, their three test modules, AI JavaScript,
   server setup service and AI candidate-visibility test. Preserve existing paths.
2. Add a separate `ruff check catalog/capabilities --select UP035` step on both
   native OS jobs. Keep the existing broad lint step unchanged; this restores
   precisely the missing rule without enabling it for unrelated AI scopes.
3. Add one strict lint invocation for relay dispatch/retry tests with the old
   F7.4/F7.5 ignore list `I001,RUF022,B008,C408,PLC0206`.
4. Add the three F6 transfer test modules to the existing F7 pytest invocation.
   Capability-only changes need this cross-boundary execution and do not trigger
   F6 closeout. The existing F7 selection already supplies all12 missing Windows
   modules once the AI-JavaScript trigger is included. Release already covers the
   candidate-visibility module on both OS; do not duplicate that addition.
5. Add `workflow_dispatch` for controlled exact-source validation. Dispatch is
   not evidence that automatic path filters work.

Use `fcp-linux-fast` and native `fcp-windows`; preserve current shell, Python3.12,
Linux storage preconditions, Windows long-path temporary roots, dependencies,
Compose validation, timeouts and assertions. No actual Recorder/service data is
used. No product/runtime code or dependency change belongs to the migration PR.

## Old to new decision table

| Legacy gates | Replacement and trigger relationship | Retirement condition |
|---|---|---|
| F7.1–F7.3 | F7 existing capability/Phase0–1 tests on both OS; separateUP035 step; same capability-path trigger | Complete green replacement evidence and references checked |
| F7.4/F7.5 | F7 existing relay tests plus strict two-file relay lint, preserving capability and relay test paths | Same; relay lint explicitly proven |
| F7.6 | F7 on all five transfer paths plus capability paths; three transfer modules join existing capability/relay/Phase0–1 tests | Same; capability-only and transfer-only representative paths proven |
| F7.7 | F7 trigger union now includes AI-JavaScript/server-setup/candidate-test-only changes; existing F7 native selection restores12 missing modules; release supplies candidate-visibility tests | Real JS-only automatic event plus complete green replacement proof |
| F8.4 | Existing F8 closeout plus release cover its remaining product/docs paths, both OS, lint and tests | May retire independently after exact-source two-green equivalent evidence/reference check; not inferred from static overlap |

All original product path conditions are preserved; broadening F7 only fills the
missing cross-boundary triggers. Deleted workflow self-paths have no future input
contract. The retirement diff must still receive release/workflow validation and
targeted F7 validation; do not leave dangling legacy filename filters/references.
F7/F8 Compose and release compile/full-Linux/selected-Windows/diff hygiene remain.

## Proof before deletion

**Stage1: extension only.** Create an isolated CI branch from the qualified main;
leave all eight hosted workflows present. Implement the single F7 extension and
focused workflow-contract tests covering every mapped path, matrix OS/labels,
lint rules/scopes and module inclusion. Update the cleanup manifest to link the
in-progress equivalence proof, without claiming retirement complete.

Review actual edited YAML against the matrix, including representative capability,
relay, transfer, JS-only, setup-only and candidate-test-only changes. A checked-in
trigger-contract test is useful but does not replace the actual GitHub event proof.
Use a disposable non-release canary PR whose diff contains only an inert/comment
change to the existing AI JavaScript path; never merge that canary change. Record
GitHub's changed-file list, event, run and actual workflow/job/source provenance.
The canary must use the same replacement workflow definition; a manual dispatch
or a PR also changing the workflow file does not prove the JS-only trigger.

Require **two complete green native replacement matrix executions** with reviewed
workflow/command blobs and exact source IDs, as required by cleanup manifestG.
Use scoped F7 runs, not another37-job campaign merely to satisfy migration proof.
Preserve auto CI triggered by the change; inspect failures before any retry.
If a native run exposes an existing strict-lint or environment failure, retain
the finding and stop that retirement group rather than relaxing rules or changing
product behavior to make migration easy.

**Stage2: retirement.** Only after stage1 evidence is durable, remove proven gates
individually or as a coherent group. Recheck executable consumers, workflow names,
required-status contracts, cleanup manifest and OSL documentation references.
Update the two F7.7 references to the retained workflow at verified new locations.
Validate the exact final deletion SHA with the changed CI contracts, native
replacement scope and applicable release policy; do not relabel17ab evidence.
If another full candidate qualification is contractually required, queue it once
for the final coherent cleanup state rather than once per deleted workflow.

## Runner capacity and execution prerequisites

[Live metadata receipt](diagnostics/ci-migration-runner-capacity.json),10:01UTC:
AQG7NCC-Windows id30 has the correct `Windows`/`fcp-windows` labels but is currently
**offline**, with API OS `unknown`. Separate Linux runner id31 is also offline.
The available-label claim is verified; current native runtime availability is not.
Do not change labels/accounts or promote either runner to a release pool.

Nettking native Windows id22 with `fcp-windows`, and Nettking-Linux id27 with
`fcp-linux-fast`, are online and scheduler-compatible by metadata. A checked-in
job must still verify the existing setup action's Python3.12.10/3.12.13, native
shell/Git Bash, writable owned temp area and Docker Compose prerequisites.
AQG can participate when actually online and those same job preconditions pass;
it is not a dependency for the design or current qualified-main evidence.
Nitro's online status is not admission to any requested or release runner pool.

## Current action

The first gate is finished and this minimal proposal is now durable. Next:
review PR468 native run34689990990 and create the planned JS-only canary;
then obtain two complete source-bound green replacement executions. Focused29
contract checks passed; see diagnostics/ci-f7-extension-checkpoint.json. Keep the acceptance campaign paused
while the source decision is pending. The23 hosted admission failures remain
infrastructure observations; no successful job is restarted to erase old red checks.
