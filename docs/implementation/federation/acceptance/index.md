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

Complete physical CF7 acceptance is not accepted.

The machine-readable source is:

```text
catalog/federation/tests/cf7_acceptance/scenarios.json
```

The following claims remain false until a separate reviewed evidence decision changes them:

```json
{
  "federation_v1_end_to_end_accepted": false,
  "capability_first_onboarding_end_to_end_accepted": false,
  "physical_evidence_accepted": false
}
```

All physical evidence must remain bound to one exact commit and distinguish automated, simulated, browser, physical, multi-host, service, and human-review evidence.

## Post-plan implementation reconciliation

CF8 is no longer a future acceptance prerequisite: the role-first installed-product runtime was retired on `main` after the original acceptance sequencing was written. That merged code change does **not** make CF7 accepted and does not change any manifest flag above.

Likewise, the current product now contains additional post-plan behavior that the physical campaign must account for when freezing a candidate:

- verified manual Federation-wide runtime updates;
- supported Windows/POSIX host update agents and Windows legacy migration bootstrap;
- headless standalone-recorder Federation pairing and logical-storage publication;
- startup MTConnect discovery/first-selection for the standalone recorder; and
- Federation-wide bounded recorder scan/source control.

Before executing a physical campaign against a new exact candidate, verify that the detailed runbooks still match these current product surfaces. If a runbook assumes the old role-first runtime, assumes Federation pages are entirely read-only, or omits the new distributed controls, update the runbook in a separate reviewed documentation/acceptance change before treating the campaign as complete.

Green CI, a successful software rollout, or successful recorder operation is useful evidence but does not independently flip an acceptance flag.
