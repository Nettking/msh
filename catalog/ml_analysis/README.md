# ml_analysis

## Status
**Experimental**

## Script path
`catalog/ml_analysis/ml_analysis.py`

## Behavior observed via static code inspection
- Reads `data/*.jsonl` from `DATA_DIR = Path("data")`.
- Writes model artifacts and reports under `ml_results/`.

## Runtime/path assumptions (not runtime-tested)
- Relative paths and scikit-learn dependencies required.
- Recommended invocation: `python catalog/ml_analysis/ml_analysis.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
