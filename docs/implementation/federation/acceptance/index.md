# Federation acceptance documentation

| Metadata | Value |
| --- | --- |
| Status | Active acceptance workspace |
| Audience | Acceptance operators, reviewers, maintainers, and release owners |
| Scope | Automated gates, physical evidence contract, readiness instructions, machine execution handoff, scenario status, and commit-bound decisions |
| Authority | Procedures in this directory do not change acceptance flags; acceptance truth comes from the manifest and a separate evidence-backed review |
| Entry point | [CF7 acceptance harness](cf7_acceptance_harness.md) |
| Parent | [Federation implementation documentation](../) |
| Reviewed | 2026-08-11 Europe/Oslo |
| Retention | Retain through the supported release lifecycle |

## Documents

1. [CF7 capability-first acceptance harness](cf7_acceptance_harness.md) — automated foundation and product-composition coverage plus the acceptance truth boundary.
2. [CF7-B product and physical acceptance](cf7b_product_physical_acceptance.md) — physical scenario and evidence contract.
3. [CF7-C physical test readiness](cf7c_physical_test_readiness.md) — machine topology, preflight, gate, execution, redaction, and stop conditions.
4. [CF7-C machine handoff](cf7c_test_handoff.md) — exact same-commit handoff for the physical machine profiles.

## Current acceptance state

**Federation v1 acceptance is finalized and tagged per operator confirmation (2026-10-23), following a physical campaign PASS on 2026-10-20.** This is a completed acceptance test, not a test still waiting to run. See [acceptance and release status](../../../releases/federation_v1_acceptance_status.md).

The machine-readable source remains:

```text
catalog/federation/tests/cf7_acceptance/scenarios.json
```

Its three end-to-end/physical acceptance flags are still `false` in the tracked repository, so **the manifest is out of sync with the operator-confirmed closeout**. Do not reinterpret those stale flags as a failed campaign, or change their machine-enforced meaning without the retained signed-off evidence, exact candidate identity and appropriate review. The acceptance-tag identifier is not yet linked in these files.

Physical evidence remains bound to the actually tested commit and must distinguish automated, simulated, browser, physical, multi-host, service, and human-review evidence. The product Git tag `v1.0.0` is a separate publication identity and was not resolvable when checked on 2026-10-23.

## Post-plan implementation reconciliation

CF8 is no longer a future acceptance prerequisite: the role-first installed-product runtime was retired on `main` after the original acceptance sequencing was written. Neither that merge nor the new operator-reported PASS automatically edits the historical manifest flags shown above.

Likewise, the current product now contains additional post-plan behavior that the physical campaign must account for when freezing a candidate:

- verified manual Federation-wide runtime updates;
- supported Windows/POSIX host update agents and Windows legacy migration bootstrap;
- headless standalone-recorder Federation pairing and logical-storage publication;
- startup MTConnect discovery/first-selection for the standalone recorder; and
- Federation-wide bounded recorder scan/source control.

Before executing a physical campaign against a new exact candidate, verify that the detailed runbooks still match these current product surfaces. If a runbook assumes the old role-first runtime, assumes Federation pages are entirely read-only, or omits the new distributed controls, update the runbook in a separate reviewed documentation/acceptance change before treating the campaign as complete.

Green CI, a successful software rollout, or successful recorder operation is useful evidence but does not independently flip an acceptance flag.
