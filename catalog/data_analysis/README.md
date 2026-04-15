# data_analysis

## Status
**Experimental**

## Script path
`catalog/data_analysis/data_analysis.py`

## Behavior observed via static code inspection
- Reads `./data/*.jsonl` from `FOLDER = "./data"`.
- Primarily console analysis output (no fixed export path in constants).

## Runtime/path assumptions (not runtime-tested)
- Relative input path resolves correctly when run from repository root.
- Recommended invocation: `python catalog/data_analysis/data_analysis.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
