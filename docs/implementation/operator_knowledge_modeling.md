# Operator knowledge and SysML export in FCP

| Metadata | Value |
| --- | --- |
| Status | Maintained FCP reference; not an implementation plan |
| Scope | Existing FCP operator-note workflow and generated SysML text |
| Audience | Operators, maintainers, and reviewers |
| Authority | Describes implemented scope only; does not grant release or machine authority |
| Reviewed | 2026-10-20 Europe/Oslo |

## Implemented workflow

FCP provides statement-first operator-note capture, later human review and structuring, reusable-strategy comparison, intervention logic, and one-way keyword-style SysML text generation. These capabilities live inside FCP; users do not need a second source repository to operate them.

- [Operator capture and review](../operator_strategy_capture.md)
- [Knowledge-flow guide](../wiki/Knowledge-Flow.md)
- [SysML export](../wiki/SysML-Export.md)
- [Operator support workflow](../fcp_operator_support_plan.md)

## What the representation does not establish

The exporter creates a text projection from note fields. It is not a complete semantic strategy-language implementation, formal validator, proof of SysML v2 conformance, operational-state estimator, or digital-twin state validator. Generated text contains profile imports not supplied by FCP; formal consumers must resolve these separately.

Structured records, confidence labels, evidence text, and reusable status are inputs to a human workflow. Evidence text alone is not independently verified evidence; human approval is not inferred from a reusable flag. No strategy note or generated model can command a machine or grant Federation/provider/storage/compute/job authority.

## Future extension gate

A future modeling capability must first specify its **in-repository** vocabulary, accepted subset, evidence provenance, review decisions, failure semantics, export contract, and testable cases. Only then should an implementation plan be authorized. An external design must not silently become normative FCP documentation.

Historical research-planning drafts were removed from the maintained documentation tree because external source pins, old FCP snapshots, missing internal links, and an unimplemented delivery roadmap did not establish current FCP behavior. They remain recoverable from Git history. Federation release acceptance remains governed by its separate evidence and release processes.
