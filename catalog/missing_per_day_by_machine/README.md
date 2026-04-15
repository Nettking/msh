# missing_per_day_by_machine

## Status
**Active**

## Script path
`catalog/missing_per_day_by_machine/missing_per_day_by_machine.py`

## Behavior observed via static code inspection
- Reads `data/*.jsonl`; requires `timestamp`, `sequence`, and `machine`.
- Writes `missing_per_day_by_machine.csv` and plots under `plots_per_machine/`.

## Runtime/path assumptions (not runtime-tested)
- Relative paths assume repository root execution.
- Recommended invocation: `python catalog/missing_per_day_by_machine/missing_per_day_by_machine.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
