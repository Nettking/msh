# Agent note: keep MSH SysML export aligned with the OSL paper

This repository generates SysML from MSH Operator Notes. The exporter must stay aligned with the paper repository, but the paper repository must not be changed from MSH tasks unless the user explicitly asks for it.

## Source of truth

Before changing `catalog/flask_app/services/osl_export_service.py`, fetch these files from the read-only paper repository:

- `Nettking/systems-paper/sysml/README.md`
- `Nettking/systems-paper/sysml/osl-core.sysml`
- `Nettking/systems-paper/sysml/cnc-chatter-keywords-example.sysml`

The current paper method is:

```text
coded CNC strategy statement
-> OSL keywords
-> SysML artefact
```

## Required export style

MSH must export reusable operator notes using the paper keyword style, not a flat generic attribute dump.

Use imports:

```sysml
import OSLCore::*;
import OSLMetadata::*;
```

Use OSL semantic metadata keywords such as:

```text
#operator_strategy
#strategy_situation
#situation
#observation
#trigger
#context
#hypothesis
#goal
#decision
#operator_action
#rationale
#expected_outcome
#trade_off
#risk
#evidence
#dt_artifact
#requirement_artifact
#monitoring_rule
#recommendation
#dashboard
#explanation
#validation_case
#provenance
```

Confidence is currently modelled as:

```sysml
attribute confidence: ConfidenceLevel = High;
```

Evidence support is currently modelled as:

```sysml
attribute evidenceStatus: EvidenceStatus = SourceBacked;
```

## Drift guard

After changing the exporter, update and run:

```bash
python -m pytest catalog/flask_app/test_osl_sysml_export.py catalog/flask_app/test_operator_strategy_lifecycle.py
```

The exporter test should fail if the output moves back to:

- YAML output
- `part def OSLStrategy` flat attribute export
- `osl.context` comment-style pseudo-fields
- non-paper keywords such as `strategy_action`, `alternative_strategy`, or `dt_artifact_link` as first-class export keywords

## When the paper changes

If the keyword set or SysML structure changes in `Nettking/systems-paper`, update MSH in this order:

1. Read the paper SysML files again.
2. Update this agent note.
3. Update `osl_export_service.py`.
4. Update `test_osl_sysml_export.py`.
5. Only then update UI copy on the OSL/SysML export page.
