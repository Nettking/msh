# F7 CI coverage consolidation

Status: extension under validation; legacy workflows remain in place.

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

The accepted audit, migration plan and per-run receipts are maintained on the
coordination branch:
[migration plan](https://github.com/Nettking/msh/blob/codex/federation-v1-diagnostic-sweep-20260911/handoff/CI_COVERAGE_MIGRATION_PLAN.md),
[equivalence matrix](https://github.com/Nettking/msh/blob/codex/federation-v1-diagnostic-sweep-20260911/handoff/diagnostics/ci-migration-equivalence-matrix.json).
