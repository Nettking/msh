# FCP digital twin and what-if analysis v1 — Product brief

Status: delivered on `codex/digital-twin-what-if-v1`

## Product intent

Give an authenticated workbench user a machine-specific, evidence-backed view
of historical active time and a bounded way to explore how the reported share
would change if some observed non-active time were reallocated to active time.
V1 is descriptive telemetry accounting. It is not a causal predictor,
production scheduler, cycle-time estimator, or machine-control surface.

## Scope

- Provide the feature in the workbench Monitor area.
- Support one machine and an inclusive UTC date range.
- Show observed active/non-active time and share, state breakdown, telemetry
  continuity, and a 0–50% non-active-time recovery scenario in 5-point steps.
- Require at least 30 classified intervals for scenario output.
- Read local JSONL telemetry and preserve the existing read-only access model.

The detailed, testable behavior is specified in the Kiro-style feature spec:

- [Requirements and EARS acceptance criteria](../../.kiro/specs/digital-twin-what-if-v1/requirements.md)
- [Technical design](../../.kiro/specs/digital-twin-what-if-v1/design.md)
- [Implementation task breakdown and evidence](../../.kiro/specs/digital-twin-what-if-v1/tasks.md)
- [Existing-data evaluation](digital_twin_what_if_v1_evaluation.md)
