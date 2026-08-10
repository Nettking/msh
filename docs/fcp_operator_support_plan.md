# FCP operator support direction

FCP should not only be a dashboard. The app should become an operator support system that helps capture, structure, reuse, and validate operator strategies.

## Core idea

FCP is the practical collection and use layer for operator knowledge:

```text
operator experience
  -> FCP captures a raw statement
  -> the statement is reviewed later
  -> the note is structured with OSL/paper keywords
  -> FCP can compare strategies and define intervention logic
  -> reusable strategies export to SysML
  -> Digital Twin / recommender uses the strategy for support
  -> operator validates the outcome
  -> the strategy improves
```

## Main workflow

### 1. Capture on site

The first capture step must stay fast. The user should be able to write one raw statement in text and save it without analysing it.

Example:

```text
On this machine, thermal drift is common during the first 30 minutes. Do not adjust offsets too early unless the first part is clearly outside tolerance.
```

This creates a captured note. It is not yet a strategy.

### 2. Review later

After the site visit, the user opens Review Notes and decides what each statement means. A note can remain incomplete. Missing fields are allowed.

### 3. Structure into OSL/paper fields

A structured note should preserve the original statement and map it into fields such as:

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

Structured notes can be grouped by situation so the user can compare actions, evidence, confidence, outcomes, and trade-offs.

### 5. Define intervention logic

Intervention logic is not the same as an operator strategy. Intervention logic is the technical YAML rule layer that detects candidate situations from telemetry signals.

Example:

```text
Structured strategy: wait before offset correction during cold-start drift.
Intervention logic: detect early-run dimensional drift or vibration/load change that might indicate this situation.
```

### 6. Export to SysML

Only reusable structured strategies should be exported. The export should follow the paper method:

```text
coded CNC strategy statement
  -> OSL keywords
  -> SysML artefact
```

## Three support modes

### Monitor mode

Answers: what is happening now?

Examples:

- live machine state
- current telemetry trends
- playback readiness
- assist cards

### Assist mode

Answers: what could the operator do next?

Assist mode should show support cards with:

- problem or observation
- possible causes
- suggested next step
- rationale
- risk
- alternative action
- whether operator confirmation is required

The app should avoid overclaiming. It should use language such as `possible explanation` and `suggested next step`, not `known cause` unless validated.

### Learning mode

Answers: what happened before, and which strategy worked?

Learning mode should reuse structured operator notes, quality outcomes, first-part approvals, and SysML exports to compare strategies across similar situations.

## Design rules

1. Keep capture fast. The user should be able to save one useful raw statement without deciding the structure immediately.
2. Add structure after capture. Outcome, quality result, reusable strategy, intervention logic, and SysML export can be added later.
3. Keep raw statements separate from interpretation. The original text should remain visible when structuring the note.
4. Do not automate unsafe CNC decisions. Actions such as tool changes, offset changes, feed reduction, pause production, part inspection, and supervisor calls should support operator confirmation.
5. Keep diagnostics separate from operator support. Diagnostics explains why the system is not ready; Assist explains what can be done.
6. Connect strategy to quality. A strategy becomes more valuable when the app stores whether it improved surface finish, dimensional accuracy, or production stability.
7. Treat FCP as the OSL data collector. OSL is the formal strategy structure. FCP is where strategies are captured, validated, compared, and exported.

## Minimum useful milestone

The first useful version should include:

1. Raw statement capture.
2. Review Notes inbox.
3. Note structuring using OSL/paper keywords.
4. Strategy comparison.
5. Intervention logic as separate detection-rule layer.
6. SysML export for reusable strategies.
7. In-app Guide page for users.

This moves FCP from a dashboard toward a practical operator support system while keeping the app understandable during field use.
