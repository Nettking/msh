# SysML Export

The **Knowledge → SysML Export** page generates a keyword-style SysML text projection of reusable operator strategies already recorded in FCP.

## Procedure

1. Capture an operator statement.
2. Review and structure the note without changing the original statement.
3. Mark the strategy as reusable only after human assessment.
4. Open **Knowledge → SysML Export**, inspect the preview, and export if appropriate.

The preview includes up to 50 reusable records. Export reads up to 500 reusable records and writes the generated text to `data/sysml/operator_strategies.sysml`. These are implementation limits, not evidence-quality or acceptance thresholds.

## Generated content

The exporter creates an `FCP_OperatorStrategies` package with strategy and situation elements. It maps available note fields into keyword-style context, observation, trigger, hypothesis, goal, decision, operator action, rationale, expected outcome, risk, evidence, and related entries.

Missing fields are generally omitted; some fields receive fallback labels. An evidence-status field is marked `SourceBacked` when non-empty evidence text is present; **this only records the presence of a note field**. It does not authenticate the underlying source or validate a proposition.

## Limitations and authority

- Generated text imports a language profile **not bundled in this repository**. The export is not a standalone, fully resolved SysML model.
- This feature does not perform SysML parsing, well-formedness verification, semantic equivalence checking, or operational-state validation.
- A reusable flag is not research validation, safety approval, or authorization to execute a machine action.
- The exporter confers no Federation, provider, storage, compute, job, or machine authority.
- No second source repository is required to use FCP's capture/review workflow.

See [Operator knowledge and modeling](../implementation/operator_knowledge_modeling.md) and [Operator knowledge capture](../operator_strategy_capture.md).
