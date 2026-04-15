# standalone_recorder

## Status
**Legacy**

## Script path
`catalog/standalone_recorder/standalone_recorder.py`

## Behavior observed via static code inspection
- Polls MTConnect sources defined in `SOURCES` over HTTP.
- Writes flat daily JSONL files under `data/`.

## Runtime/path assumptions (not runtime-tested)
- Kept for compatibility with older flat-file analysis workflows.
- Recommended invocation: `python catalog/standalone_recorder/standalone_recorder.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
