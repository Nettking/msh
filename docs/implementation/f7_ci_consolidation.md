# F7 CI coverage consolidation

Status: extension merged in PR #468; proven F7.1–F7.7 and F8.4 hosted gates
retired in a separate change, subject to that change's final-source validation.

The F7 closeout keeps its existing native Linux/Windows matrix on
`fcp-linux-fast` and `fcp-windows`. It consolidates unique coverage from the
historical F7.1–F7.7 gates without creating additional workflows or job definitions.
The Federation v1 release gate and F6/F8 closeouts remain unchanged.

| Historical requirement | Retained execution |
|---|---|
| Capability changes and F7.1–F7.6 lint | Existing F7 capability/Phase0–1 tests and broad lint, plus an explicit capability-onlyUP035 check on both OS |
| F7.4/F7.5 relay dispatch/retry lint | One strict two-file Ruff invocation, preserving the historical ignore list |
| F7.6 transfer/capability interaction | F7 triggers on transfer implementations/tests and selects all three transfer modules alongside capability/relay tests |
| F7.7 JavaScript/setup changes | F7 triggers for AI JavaScript, server setup and candidate-visibility tests, preserving native Windows AI/chat/provider/Compose/relay selection |
| Candidate-visibility test | Existing release Linux/Windows coverage remains; no duplicate addition to F7 |
| Compile, Compose and diff hygiene | Existing retained checks; new CI contract module is also compiled/linted/tested |
| F8.4 activation boundary | Unchanged native F8 matrix covers the complete former test selection; release checks preserve its capability/activation-test lint scopes on both OS |

Both PR and main-push path filters retain their existing paths and add the eight
missing product/test paths. The CI contract module itself also triggers F7.
`workflow_dispatch` permits controlled exact-source validation; it does not prove
automatic path selection. JUnit artifacts expose native module outcomes and skips.

The Python setup action, dependencies, native shells, Linux storage precondition,
Windows long-path test temporary directory,30-minute limit and assertions are
unchanged. No runtime source, runner labels/accounts or release-pool admission
changes are included. An online runner label alone is not runtime qualification.

Before retiring a legacy workflow, require the cleanup manifest's two complete
green equivalent replacement executions, exact source/workflow/command provenance,
and a real AI-JavaScript-only automatic event. Static contract tests and manual
dispatch alone are insufficient. Reconcile native Windows/Linux outcomes and
service assumptions, then recheck filename/status-policy/documentation consumers.
Retire only proven gates, with exact final-source validation; retain valid earlier
qualification under its original SHA. CI evidence is never physical acceptance.

The accepted audit, migration plan and per-run receipts are maintained on branch
`codex/federation-v1-diagnostic-sweep-20260911`, in
`handoff/CI_COVERAGE_MIGRATION_PLAN.md` and
`handoff/diagnostics/ci-migration-equivalence-matrix.json`.

The F7 replacement proof uses runs 34689990990 and 34690234286; the second
is the real JavaScript-only event from the closed, unmerged canary PR #469.
The later documentation-only repair preserves all executable/workflow bytes
and retains those runs under their original source IDs. Final-head branding,
focused contracts and applicable native checks passed before PR #468 merged.

F8.4 replacement proof uses existing native F8 runs 34689990982 and 34695331207,
plus Linux/Windows release lint checks in runs 34689991000 and 34695331201.
All F8/release workflow bytes are identical across those sources. This claims
the reviewed passing replacement jobs only; unrelated earlier release failures
remain preserved separately. The exact inventory, reference review and retained
native logs are recorded in
`handoff/diagnostics/phase-workflow-retirement-preflight.json` on the same branch.

Retirement removes only the eight superseded workflow files and updates their
documentation references. The four retained workflows, test selections, path
filters, dependencies, runtime source and runner configuration remain unchanged.
Repository indexing excludes `.github` files; changed indexed documentation
invalidates its cache through the existing content fingerprint. No live index,
physical runtime or protected Recorder data is modified by this change.
