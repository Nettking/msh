# corrolation_machine_pairs

## Status
**Experimental**

## Script path
`catalog/corrolation_machine_pairs/corrolation_machine_pairs.py`

## Behavior observed via static code inspection
- Reads `data/*.jsonl` from `DATA_DIR = Path("data")`.
- Generates `correlation_matrix.csv` in repo root.

## Runtime/path assumptions (not runtime-tested)
- Relative path assumes repository root as current working directory.
- Recommended invocation: `python catalog/corrolation_machine_pairs/corrolation_machine_pairs.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
