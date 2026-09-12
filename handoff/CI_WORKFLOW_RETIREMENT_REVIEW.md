# CI workflow retirement review — stopped on non-equivalence

Source reviewed: `17ab3a05c9c506e0f92adfaa4fa0bac231ac2c05` (clean actual main).
PR467 evidence remains bound to `84c66f8185c1411d9dc8c5c33244a2f564845ce7`.
This is a subordinate read-only review, not physical acceptance or a product defect.

**Decision: STOP the proposed eight-workflow deletion. Redundancy is false at the
command/OS/trigger boundary. No cleanup branch, PR, deletion, runner migration,
source change or CI rerun was made.** Current main qualification continues unchanged.

## Verified hosted failures

All eight candidates still select `ubuntu-latest` and `windows-latest`; seven also
have an Ubuntu repository-hygiene job. Their23 PR467 jobs all failed with zero
steps and runner ID0. Every job annotation says account-payment/spending-limit
admission prevented startup. These are CI infrastructure failures, not product
test failures. No checkout/test result is inferred from them.

[Exact API evidence](diagnostics/phase-workflow-hosted-failures.json) contains every
run/job ID, runner/step metadata and original annotation. The retained37+3 PR467
qualification receipt is unchanged; no red aggregate is used to invalidate it.

## Exact coverage comparison

[Command/OS/path inventory](diagnostics/phase-workflow-coverage-audit.json) preserves
workflow blobs/hashes, job blocks, commands/line numbers, trigger paths and scopes.
The reference set is `federation-v1-release.yml` and F6/F7/F8 closeouts. All retained
workflows and their runner labels, bounds and assertions are unchanged.

| Candidate | Explicit test modules | Static test/compile overlap | Non-equivalence / disposition |
|---|---:|---|---|
| F7.1 job contracts |60|Covered by retained union|Legacy capability lint enablesUP035; retained capability scopes ignore it. Keep.|
| F7.2 provider selection |60|Covered by retained union|Same capability-lint difference. Keep.|
| F7.3 durable ownership |60|Covered by retained union|Same capability-lint difference. Keep.|
| F7.4 worker dispatch |61|Covered by retained union|UP035 plus relay-dispatch test lint absent from four retained scopes. Keep.|
| F7.5 retry/cancellation |62|Covered by retained union|UP035 plus dispatch/retry relay-test lint absent from four retained scopes. Keep.|
| F7.6 artifact authorization |65|Covered by retained union|UP035; trigger-specific cross-boundary Windows equivalence not proven. Keep.|
| F7.7 AI runtime integration |75|Covered by retained union|Concrete Windows trigger gap below. Keep.|
| F8.4 worker activation |64|Covered by retained union|No static module/lint-scope gap identified; whole eight-file deletion fails, so no partial deletion prepared.|

The release workflow compiles all `catalog`, selects all4457 test identities in
four Linux shards and both Linux full-suite orders, and runs selected Windows
groups. Existing PR467 native artifacts prove that full-suite identity inventory;
the source trees are byte-identical. This use is static coverage analysis only:
PR artifacts do not count toward current merged-main qualification.

The full Linux suite does not substitute for missing Windows execution. A change
only to `catalog/flask_app/static/js/ai-explainer.js` triggers legacyF7.7, release
andF8.5, but none of F6/F7/F8 closeout workflows. Twelve legacy test modules are
absent from the triggered retained Windows selections: seven AI modules, the
AI chat/connected-provider/model-Compose modules, and two relay lifecycle modules.
The exact12 paths and the trigger truth table are in the inventory. Eleven other
possible provider workflows were inspected; even conservatively treating all their
triggered self-hosted tests as Windows coverage leaves those12 modules missing.
Thus static test-set inclusion is insufficient to retireF7.7 safely.

F7.1–F7.6 omitUP035 from their command-local ignore lists; retained capability
lint explicitly ignoresUP035. Effective Ruff settings were compared read-only
with available Ruff0.16.3, identical file/config and only the two ignore lists differing;UP035 is the
enabled-rule delta. No lint rule was relaxed. F7.4/F7.5 also lint relay test files
outside the four retained lint scopes; Phase2 lints relay but does not trigger on
capability-only changes. No new product lint failure is alleged by this review.

Compose validation is retained in release andF7/F8 closeouts. OldF7.1–F7.7 hygiene
uses `HEAD^ HEAD`; release retains the same push range and base-to-head PR diff
validation. F8.4 and closeouts use base/main-to-head comparison. Release trigger
paths cover every candidate path for PRs/main pushes, but that broad trigger alone
does not restore the missing OS-specific selections. Same Python3.12 family and
dependency installation do not erase scope/flag differences. Release has pinned
dependencies; phase jobs use phase2 constraints and unpinned pytest/Ruff installs.

## References and contracts

Tracked-file, exact-filename and workflow-display-name searches found self path
filters, the cleanup manifest, and two F7.7 references in the proposed OSL documents:
`02_current_fcp_architecture.md:505` and `08_validation_testing_and_ci.md:63`.
Historical branch names in F7/F8 closeout documentation are archival references.
No workflow-call/run consumer, runtime import, script dispatch or acceptance-policy
reference to these eight filenames was found. The separate coordination branch's
pre-existing handoff files had no exact filename references. Dynamic workflow
consumer searches found no dependency requiring a replacement filename. Removing
workflows would change the repository export/index content, but no deletion or
index/cache invalidation was performed.

GitHub's current main branch response reports protection disabled and empty
required contexts/checks. Detailed protection and ruleset APIs return403 with a
plan-upgrade message; that visibility limit is retained rather than interpreted
as proof of every possible external policy. None of these eight belongs to the
persisted37-job Federation v1 qualification set. Nevertheless, coverage equivalence
is independently false, so deletion cannot satisfy the user's condition.

The checked-in cleanup manifest sectionG explicitly requires a command/OS/service
matrix and two green equivalent replacements, and says KEEP until equivalence is
proved. No equivalent-replacement claim is made. No repository-owner approval
question is needed: the user explicitly required stopping on a false assumption.

## Recovery / next gate

No independent product defect was found and no speculative repair issue/PR opened.
The durable audit is this report plus the two JSON receipts; the cleanup blocker
is non-equivalence, not a new physical/runtime failure. Any future consolidation
must first preserve the unique lint and Windows trigger coverage in a separately
reviewed design; do not silently broaden current release work during this campaign.

Primary next gate remains actual-main17ab3a05 qualification, already in progress.
Last progress snapshot08:52:42UTC; next routine CI check09:53UTC or later unless an
actionable completion/failure is independently delivered. Preserve all running
jobs and source receipts. No runner admission/account/label change, no Nitro pool
admission, no physical deployment, no protected Recorder-data access, no P07/P12
and no physical PASS. Read [current coordination](QUALIFICATION_COORDINATION.md).
