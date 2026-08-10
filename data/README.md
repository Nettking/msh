# Data directory

This directory is the expected local location for MTConnect JSONL inputs used by the runtime, cache rebuilds, and analysis scripts.

## Supported layouts

1. **Flat files**

```text
data/
  2026-01-10.jsonl
  2026-01-11.jsonl
```

2. **Per-machine subfolders**

```text
data/
  VTC/
    2026-01-10.jsonl
  IG500/
    2026-01-10.jsonl
```

3. **Browser upload batches**

```text
data/
  imports/
    uploads.sqlite3
  uploads/
    upload-<opaque-id>/
      first.jsonl
      second.jsonl
```

The Data upload page accepts several JSONL files in one batch. Every nonblank line is validated as a JSON object and stored transactionally in `data/imports/uploads.sqlite3`. The corresponding JSONL files are published under `data/uploads/` only after the full database import succeeds. A temporary `.fcp-importing` marker hides incomplete publication directories from supported recursive discovery.

## Current support level

The current runtime, date discovery, workflow-session filtering, and telemetry analytics cache paths are intended to discover JSONL recursively under `data/**/*.jsonl`.

Some older manual, exploratory, or legacy scripts may still assume a flat `data/*.jsonl` layout. When maintaining those scripts, prefer shared helpers from `catalog/common/` and `catalog/runner/` instead of implementing new ad hoc file discovery.

## Version-control policy

- Data files are ignored by default in `.gitignore`.
- This README is tracked to preserve onboarding context.
