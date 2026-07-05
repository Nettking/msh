# Knowledge Flow

The Knowledge area is for capturing and structuring operator experience.

## Pages

```text
Knowledge
  Capture
  Review Notes
  Strategies
  Intervention Logic
  SysML Export
```

## Flow

### 1. Capture

Use Capture during a site visit or while talking to an operator.

Write one raw statement in plain text. Do not analyse it yet.

Example:

```text
When this machine is cold, the first part can drift. I usually wait before changing offsets.
```

### 2. Review Notes

After the visit, review captured statements. A note can stay incomplete if the interpretation is not clear.

### 3. Structure the note

Map the raw statement into OSL/paper fields, such as:

- context
- trigger
- observation
- hypothesis / possible cause
- goal
- decision / strategy action
- rationale
- expected outcome
- trade-off
- risk
- alternative strategy
- evidence
- confidence
- outcome
- DT/SysML trace target

### 4. Compare strategies

Use Strategies to compare structured notes by situation, action, evidence, confidence, outcome, and trade-off.

### 5. Add intervention logic when detection is possible

Intervention Logic is not the same as a strategy. It is the technical YAML rule layer for detecting candidate situations from telemetry.

Only add intervention logic when the situation can reasonably be detected from signals such as state, load, vibration, alarms, or measurements.

### 6. Export to SysML

Reusable structured strategies can be exported to SysML using the paper method:

```text
coded CNC strategy statement
  -> OSL keywords
  -> SysML artefact
```
