# machines_active_per_day

## Status
**Active**

## Script path
`catalog/machines_active_per_day/machines_active_per_day.py`

## Behavior observed via static code inspection
- Reads `data/*.jsonl`; requires `timestamp` and `machine`.
- Writes `machines_active_per_day.csv` and `machines_active_per_day.png`.

## Runtime/path assumptions (not runtime-tested)
- Relative paths assume repo root execution.
- Recommended invocation: `python catalog/machines_active_per_day/machines_active_per_day.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
