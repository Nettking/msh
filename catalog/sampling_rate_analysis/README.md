# sampling_rate_analysis

## Status
**Active**

## Script path
`catalog/sampling_rate_analysis/sampling_rate_analysis.py`

## Behavior observed via static code inspection
- Reads `data/*.jsonl` from `DATA_DIR = "data"`.
- Writes `sampling_rate_summary.csv`.

## Runtime/path assumptions (not runtime-tested)
- Relative path resolution expects root execution.
- Recommended invocation: `python catalog/sampling_rate_analysis/sampling_rate_analysis.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
