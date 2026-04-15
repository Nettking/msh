# interventions

## Status
**Legacy (env-specific)**

## Script path
`catalog/interventions/interventions.py`

## Behavior observed via static code inspection
- Reads from `DATA_DIR = Path(r"C:\wsl\msh\data")` (hardcoded absolute path).
- Writes `intervention_states.csv` and `override_changes.csv` in current working directory.

## Runtime/path assumptions (not runtime-tested)
- Must edit `DATA_DIR` before use on non-Windows/other environments.
- Recommended invocation: `python catalog/interventions/interventions.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
