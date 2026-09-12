# D16 original-host CI diagnostic plan

Reviewed 2026-09-12. This is diagnostic CI, never release or physical acceptance
evidence. D16/#478 remains unresolved. PR475 head
`5e6f184311019b9982e8544a18f3dc02c1b16e98` and PR473 head
`440123f6bc6dc358eef3d233236bc14f91af60e0` remain unchanged.

The user-requested ready-state check at 18:44 UTC found no new PR475 results:
all runs completed, release still fails in the previously preserved two jobs.
Beast is online/idle with its existing `beast-windows` and `fcp-windows` labels.
Both AQG runners are offline; AQG Linux still lacks `fcp-linux-fast`. No label,
account or admission changes are authorized or needed for this diagnostic.
Receipt: `diagnostics/pr475-user-ready-state.json`.

## Controlled execution

1. Add a coordination-branch-only, path-triggered diagnostic workflow. It must
   never merge into a candidate. Use existing Beast Windows qualification labels
   and a 15-minute job bound. No rerun of the failed 298-test job or full suite.
2. Check out the exact unchanged PR475 source in the ordinary CI workspace.
   Assert clean tracked source, exact SHA and tree, physical host BEAST, runner
   Beast, native Windows and the existing NetworkService SID. Use the source's
   checked-in self-hosted Python action and constrained release dependencies.
3. Fetch only the reviewed diagnostic observer/driver from the control commit
   into a new job-owned directory under RUNNER_TEMP, outside the candidate.
   Do not install a product patch, change source, deploy or invoke product hosts.
4. Execute the one original D16 test once without instrumentation, then once
   with a forwarding observer, in separate Python processes and unique pytest
   base directories. Each process is bounded at 180 seconds. Stop after a new
   unexpected failure or timeout. Passing retries do not invalidate original CI.
5. The observer records population entry/exit/failure and the runtime's existing
   leader/quorum refusal, with passive role/term/readiness/commit/lifecycle
   snapshots. Forward original calls and re-raise original exceptions. No extra
   quorum probes, election, retries, altered deadline, assertion or return value.
   Timing perturbation is explicitly a limitation; no repair inference from green.
6. Preserve exact command, native host/Python/source provenance, both exit codes,
   stdout/stderr, JUnit, observer events and final source hygiene in an artifact.
   Persist and classify the result before another substantial diagnostic step.

## Side effects and exclusions

Only normal CI checkout, a fresh action-owned virtual environment, dependency
installation and test-owned temporary databases/keys/listeners are permitted.
The original fixture owns loopback sockets and closes its voters/relays/clients.
The external timeout terminates only the dedicated diagnostic child process.
No Docker invocation, cleanup/prune, production service restart, host networking,
runner configuration, global Git trust, physical campaign or protected Recorder
data access. In particular `C:\msh\git\data` is never opened or inspected.

Expected observations: original test passes through initial population and
refuses current authority after intentional voter isolation; or a reproduction
locates D16's earlier refusal with unchanged guards. A non-reproduction leaves
D16 unresolved and shifts investigation to its original predecessor context.

Next exact action: implement and statically validate the bounded control workflow
and observer, commit/push them, confirm native startup once, then check near the
bounded diagnostic's expected completion. Preserve all existing expensive CI.
