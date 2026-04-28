# find_stops

## Status
**Active**

## Script path
`catalog/find_stops/find_stops.py`

## Behavior observed via static code inspection
- Reads `data/*.jsonl` from `DATA_DIR = Path("data")`.
- Writes hourly stop intervals to `results/find_stops/hourly_stop_intervals.csv`.

## Runtime/path assumptions (not runtime-tested)
- Relative paths require root-based execution context.
- Recommended invocation: `python catalog/find_stops/find_stops.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
