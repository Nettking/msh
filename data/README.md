# Data directory

This directory is the expected local location for MTConnect JSONL inputs used by analysis scripts.

## Supported layouts currently used in this repository

1. **Flat files** (used by most analysis scripts):

```text
data/
  2026-01-10.jsonl
  2026-01-11.jsonl
```

2. **Per-machine subfolders** (written by `record data/standalone-recorder_v2.py`):

```text
data/
  VTC/
    2026-01-10.jsonl
  IG500/
    2026-01-10.jsonl
```

## Important note

Many current analysis scripts only scan `data/*.jsonl` and do **not** recurse into machine subfolders. If your data was recorded with recorder v2, adjust scripts or pre-process layout before running those analyses.

## Version-control policy

- Data files are ignored by default in `.gitignore`.
- This README is tracked to preserve onboarding context.
