# `record data/` directory

Recorder and automation utilities used to capture MTConnect telemetry (plus one UI automation helper).

## Files

- `standalone_recorder.py`
  - Original/simple recorder.
  - Polls sources and appends daily records to `data/YYYY-MM-DD.jsonl`.

- `standalone-recorder_v2.py`
  - Improved recorder with:
    - buffered writes
    - per-source backoff
    - persisted sequence state
    - graceful shutdown handling
  - Writes `data/<machine>/<YYYY-MM-DD>.jsonl`.

- `auto_connect.py`
  - Desktop automation utility (template matching + auto-click behavior).
  - Not part of the MTConnect parsing/analysis pipeline.

- `button.png`
  - Template image used by `auto_connect.py`.

## Notes

- Keep both recorder versions for history and comparison.
- Prefer `standalone-recorder_v2.py` for ongoing recording unless legacy output shape is required by existing analysis scripts.
- Because this folder name includes a space, always quote paths in shell commands.
