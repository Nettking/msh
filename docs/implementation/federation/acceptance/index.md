# Federation acceptance documentation

| Metadata | Value |
| --- | --- |
| Status | Active acceptance workspace |
| Audience | Acceptance operators, reviewers, maintainers, and release owners |
| Scope | Automated gates, physical evidence contract, readiness instructions, machine execution handoff, scenario status, and commit-bound decisions |
| Authority | Procedures in this directory do not change acceptance flags; acceptance truth comes from the manifest and a separate evidence-backed review |
| Entry point | [CF7 acceptance harness](cf7_acceptance_harness.md) |
| Parent | [Federation implementation documentation](../) |
| Reviewed | 2026-08-07 Europe/Oslo |
| Retention | Retain through the supported release lifecycle |

## Documents

1. [CF7 capability-first acceptance harness](cf7_acceptance_harness.md) — automated foundation and product-composition coverage plus the acceptance truth boundary.
2. [CF7-B product and physical acceptance](cf7b_product_physical_acceptance.md) — physical scenario and evidence contract.
3. [CF7-C physical test readiness](cf7c_physical_test_readiness.md) — machine topology, preflight, gate, execution, redaction, and stop conditions.
4. [CF7-C machine handoff](cf7c_test_handoff.md) — exact same-commit handoff for the three physical machine profiles.

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

All physical evidence must remain bound to one exact commit and distinguish automated, simulated, browser, physical, multi-host, service, and human-review evidence. CF8 remains blocked until CF7 is accepted.