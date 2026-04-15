# auto_connect

## Status
**Legacy (utility)**

## Script path
`catalog/auto_connect/auto_connect.py`

## Behavior observed via static code inspection
- Requires desktop session and template image (e.g., `record data/button.png`) for OpenCV matching.
- No structured files; performs mouse movement/click automation.

## Runtime/path assumptions (not runtime-tested)
- Interactive desktop required; not part of MTConnect data pipeline.
- Recommended invocation: `python catalog/auto_connect/auto_connect.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
