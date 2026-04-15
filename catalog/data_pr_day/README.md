# data_pr_day

## Status
**Active**

## Script path
`catalog/data_pr_day/data_pr_day.py`

## Behavior observed via static code inspection
- Reads `data/*.jsonl` and requires `timestamp` + `machine` fields.
- Writes plots under `graphs/` (`GRAPH_BASE_DIR = Path("graphs")`).

## Runtime/path assumptions (not runtime-tested)
- Relative `data/` and `graphs/` paths are root-based.
- Recommended invocation: `python catalog/data_pr_day/data_pr_day.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
