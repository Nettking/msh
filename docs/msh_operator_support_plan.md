# MSH operator support direction

MSH should not only be a dashboard. The app should become an operator support system that helps capture, structure, reuse, and validate operator strategies.

## Core idea

MSH is the practical collection and use layer for operator knowledge:

```text
operator experience
  -> MSH captures structured strategy notes
  -> OSL represents the strategy formally
  -> Digital Twin / recommender uses it for support
  -> operator validates the outcome
  -> the strategy improves
```

## Three support modes

### Monitor mode

Answers: what is happening now?

Examples:

- live machine state
- alarms
- current telemetry trends
- playback readiness
- diagnostics

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

Learning mode should reuse structured operator notes, quality outcomes, first-part approvals, and OSL exports to compare strategies across similar situations.

## Design rules

1. Keep capture fast. The operator should be able to save a useful note with only the action/decision.
2. Add structure after capture. Outcome, quality result, reusable strategy, and OSL export can be added later.
3. Do not automate unsafe CNC decisions. Actions such as tool changes, offset changes, feed reduction, pause production, part inspection, and supervisor calls should support operator confirmation.
4. Keep diagnostics separate from operator support. Diagnostics explains why the system is not ready; Assist explains what can be done.
5. Connect strategy to quality. A strategy becomes more valuable when the app stores whether it improved surface finish, dimensional accuracy, or production stability.
6. Treat MSH as the OSL data collector. OSL is the formal representation. MSH is where strategies are captured, validated, and reused.

## Minimum useful milestone

The first useful version should include:

1. OSL-aligned operator note schema.
2. Quick note capture with optional outcome update.
3. Assist support cards.
4. Operator confirmation workflow.
5. First-part approval checklist.
6. OSL export for reusable notes.

This moves MSH from a dashboard toward a practical operator support system while keeping the app understandable during field use.
