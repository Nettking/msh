# data_simulator

## Status
**Experimental**

## Script path
`catalog/data_simulator/data_simulator.py`

## Behavior observed via static code inspection
- Reads `data/*.jsonl` from `DATA_DIR = "data"`.
- Streamlit UI only (no guaranteed file output).

## Runtime/path assumptions (not runtime-tested)
- Run Streamlit from repository root so `data/` resolves.
- Recommended invocation: `streamlit run catalog/data_simulator/data_simulator.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
