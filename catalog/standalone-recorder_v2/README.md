# standalone-recorder_v2

## Status
**Active**

## Script path
`catalog/standalone-recorder_v2/standalone-recorder_v2.py`

## Behavior observed via static code inspection
- Polls MTConnect sources defined in `SOURCES` over HTTP.
- Writes machine-partitioned JSONL under `data/` and `recorder_state.json`.

## Runtime/path assumptions (not runtime-tested)
- Network access to configured endpoints required.
- Recommended invocation: `python catalog/standalone-recorder_v2/standalone-recorder_v2.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
