# Operator strategy capture

MSH includes a field-capture page for recording operator strategy decisions while visiting or working with MSH.

```text
/operator-strategies
```

The page is designed to support the Operator Strategy Language idea used in the Systems paper work: an operator decision should be captured together with the situation, observations, trigger, context, hypothesis, goal, rationale, expected outcome, risk, trade-off, confidence, evidence, and possible trace target.

## Why this is separate from telemetry

Operator strategy records are research and field notes. They are not machine telemetry samples.

The records are stored as JSON under:

```text
data/operator_strategy_records/operator_strategies.json
```

This is deliberately not JSONL. MSH recursively scans `data/**/*.jsonl` as telemetry, so strategy notes must not be written as `.jsonl` under `data/`.

## Time model

The page stores two different times:

| Field | Meaning |
| --- | --- |
| `captured_at` | When the form was saved. This is always set automatically by the server. |
| `decision_time` | When the operator decision/action happened. This may be the same as capture time or a custom time entered afterwards. |
| `decision_time_mode` | `now` or `custom`. |
| `decision_time_local` | Original local datetime entered for a custom decision/action time. |
| `decision_timezone` | IANA timezone used to convert a custom local time to UTC, default `Europe/Oslo`. |

This distinction matters when strategy records should later be compared with telemetry. A decision may be recorded at 14:42 even though the action happened at 13:15.

## Required fields

The form requires:

- a strategy name or strategy situation/path name.
- `goal`.
- `decision`.

The goal is required because an operator decision without a goal is difficult to compare or trace. The decision/action is required because the page is meant to record what was actually decided or done.

## OSL-aligned fields

The capture form includes:

- strategy name.
- strategy situation/path.
- machine, process, and operation.
- observation.
- trigger.
- context.
- hypothesis.
- goal.
- decision/action.
- rationale.
- expected outcome.
- risk.
- trade-off.
- confidence.
- evidence/source.
- trace target or possible Digital Twin artefact.
- free notes.

These fields intentionally match the OSL concepts enough to support later modelling, comparison, and traceability work, while still being fast enough to use during a site visit.

## Intended workflow at MSH

1. Open `/operator-strategies` on the server.
2. Select whether the decision happened **Now** or at a **Custom time**.
3. Record the operator statement as close to the original wording as practical.
4. Fill the structured fields where possible.
5. Leave unknown fields empty rather than inventing information.
6. Use the recent-record table to check what has been captured during the visit.

## Later use

The stored records can later support:

- comparison of alternative operator strategies.
- traceability from operator reasoning to monitoring rules, recommendation services, dashboards, explanations, and validation cases.
- alignment between decisions and telemetry windows using `decision_time`.
- research coding for the Systems paper.

The current page is a capture tool. It does not yet export LaTeX, SysML v2, or OSL keyword syntax automatically.
