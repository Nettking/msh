# SysML Export

Knowledge -> SysML Export exports reusable structured strategies to SysML.

## Method

The export follows the paper method:

```text
coded CNC strategy statement
  -> OSL keywords
  -> SysML artefact
```

## What gets exported

Only reusable structured strategies should be exported.

A raw captured statement is not enough. The note should first be reviewed and mapped to OSL/paper fields.

## Keyword style

The exporter should stay aligned with the SysML files in the paper repository:

```text
Nettking/systems-paper/sysml/osl-core.sysml
Nettking/systems-paper/sysml/cnc-chatter-keywords-example.sysml
```

The expected style uses OSL metadata keywords such as:

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
#recommendation
#validation_case
```

Before changing the exporter, read:

```text
docs/agent_notes/osl_sysml_alignment.md
```
