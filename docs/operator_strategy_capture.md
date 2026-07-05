# Operator knowledge capture

MSH includes a knowledge-capture flow for recording operator statements during field work and structuring them later.

The current pages are:

```text
/ operator-strategies/capture   -> capture one raw statement quickly
/ operator-strategies/review    -> review captured notes later
/ operator-strategies/structure/<id> -> map one note to OSL/paper fields
```

In the app menu these live under:

```text
Knowledge -> Capture
Knowledge -> Review Notes
Knowledge -> Strategies
Knowledge -> Intervention Logic
Knowledge -> SysML Export
```

## Why capture is now statement-first

During a site visit, the user should not be forced to complete a formal model. The first step is only to preserve the statement while it is fresh.

Example raw statement:

```text
When this machine is cold, the first part can drift. I usually wait before changing offsets unless the first part is clearly outside tolerance.
```

This creates a captured note. It is not yet a structured strategy.

## Why this is separate from telemetry

Operator knowledge records are research and field notes. They are not machine telemetry samples.

The records are stored as JSON under:

```text
data/operator_strategy_records/operator_strategies.json
```

This is deliberately not JSONL. MSH recursively scans `data/**/*.jsonl` as telemetry, so strategy notes must not be written as `.jsonl` under `data/`.

## Time model

The service stores both capture time and decision/action time:

| Field | Meaning |
| --- | --- |
| `captured_at` | When the form was saved. This is always set automatically by the server. |
| `decision_time` | When the statement, decision, or action is considered to have happened. |
| `decision_time_mode` | `now` or `custom`. |
| `decision_time_local` | Original local datetime entered for a custom decision/action time. |
| `decision_timezone` | IANA timezone used to convert a custom local time to UTC, default `Europe/Oslo`. |

The simple Capture page uses `now`. More detailed timing can still be stored when structuring or extending a note.

## Capture fields

The capture page intentionally stays small:

- raw operator statement.
- optional machine.
- optional quick tag / situation.
- optional confidence.

Only a statement is required.

## Review and structuring fields

Later, the user opens Review Notes and structures the note. The original statement should remain visible and unchanged. Missing fields are allowed.

The structuring page supports OSL/paper fields such as:

- context.
- trigger.
- observation.
- hypothesis / possible cause.
- goal.
- decision / strategy action.
- rationale.
- expected outcome.
- trade-off.
- risk.
- alternative strategy.
- evidence.
- confidence.
- outcome.
- DT/SysML trace target.

## Review status

A note can move through these states:

```text
captured   -> raw statement saved, not interpreted yet
structured -> mapped to OSL/paper fields
reusable   -> good enough for comparison, support cards, and SysML export
```

## Intervention logic is separate

A structured strategy is interpreted operator knowledge. Intervention logic is a technical YAML rule used to detect candidate situations from telemetry.

Example:

```text
Raw statement: The machine drifts while cold.
Structured strategy: wait before changing offsets during cold-start drift.
Intervention logic: detect early-run drift or related signal changes from telemetry.
```

Do not create intervention logic unless the situation can reasonably be detected from telemetry signals such as state, load, vibration, alarms, or measurement events.

## SysML export

Reusable structured strategies can be exported under Knowledge -> SysML Export.

The export follows the paper method:

```text
coded CNC strategy statement
  -> OSL keywords
  -> SysML artefact
```

The exporter should stay aligned with `Nettking/systems-paper/sysml/osl-core.sysml` and the keyword-style SysML example. See `docs/agent_notes/osl_sysml_alignment.md` before changing the exporter.

## Intended MSH field workflow

1. Open Knowledge -> Capture during the site visit.
2. Save one raw statement without analysing it.
3. After the visit, open Knowledge -> Review Notes.
4. Structure the note only where the interpretation is justified.
5. Mark good structured notes as reusable.
6. Compare reusable strategies under Knowledge -> Strategies.
7. Add intervention logic only when telemetry can detect a candidate situation.
8. Export reusable strategies to SysML.

## Later use

The stored records can later support:

- comparison of alternative operator strategies.
- support cards in Assist.
- traceability from operator reasoning to monitoring rules, recommendation services, dashboards, explanations, and validation cases.
- alignment between decisions and telemetry windows using `decision_time`.
- research coding for the Systems paper.
