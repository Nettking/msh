# analyze_missing_sequence_number

## Status
**Active**

## Script path
`catalog/analyze_missing_sequence_number/analyze_missing_sequence_number.py`

## Behavior observed via static code inspection
- Reads `data/*.jsonl` from `DATA_DIR = "data"`.
- Writes `missing_per_day.csv` in repo root.

## Runtime/path assumptions (not runtime-tested)
- Uses relative paths; run from repository root.
- Recommended invocation: `python catalog/analyze_missing_sequence_number/analyze_missing_sequence_number.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
