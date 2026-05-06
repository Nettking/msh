# Troubleshooting

Use `/status` first. It shows runtime phase, discovered date bounds, processed/pending counts, last failure, and readiness hints.

## Flask starts but no data appears

Possible causes:

- `data/` is empty or mounted incorrectly.
- JSONL files are not under a scanned/input root.
- records do not contain parseable timestamps and filenames do not contain usable dates.
- startup is still in discovery/bootstrap.

Checks:

```bash
find data -name '*.jsonl' -print | head
cat results/workflows/runtime_state.json
```

Open `/status` and verify `latest_available_source_date`, `total_available_days`, and `last_failure`.

## Playback page has no selectable data

Playback requires session-filtered data and a timeline export. Confirm that a session exists under `results/workflows/` and that it contains:

```text
data/
exports/timeline/timeline_rows.csv
exports/timeline/manifest.json
```

If filtered data exists but no export exists, run the workflow or `data_visualizer` from `/control`, or trigger a refresh and wait for bootstrap/catch-up.

## Playback export is stale or not reused

Playback cache reuse requires the manifest's session config signature and filtered-data generation timestamp to match the current session metadata. If a session was edited manually or filtered data was regenerated, the export should be recreated.

## A script failed during bootstrap

The runtime is best-effort. One script failure does not prevent Flask from starting or other scripts from running. Use `/status` for `last_failure` and `/control` for recent stdout/stderr snippets. You can rerun a failed script from `/control` after fixing data or environment problems.

## A control action will not start

Only one control action can run at a time. Wait for the active action to finish and refresh `/control`. If the process restarted, remember that recent run history is in memory and may be cleared.

## Date range produces no records

Check whether records have parseable `timestamp` values. For same-day hour filtering, records without parseable timestamps are skipped. Filename-date fallback is only for files where no records have timestamps and applies to date-range filtering, not hour filtering.

## Docker rebuild or dependency issues

Rebuild the Flask service:

```bash
docker compose up --build flask
```

For local development, reinstall Python dependencies in your environment:

```bash
python -m pip install -r requirements.txt
```

## Runtime appears stuck after restart

Open `/startup` if a startup decision is required. Otherwise inspect `results/workflows/runtime_state.json` and `/status`. You can use `/control` to request a refresh or create/reuse a specific session scope.

## Hidden scripts do not appear in `/control`

This is expected for runner internals, recorders, simulator, desktop automation, and environment-specific tools. They are intentionally excluded from workflow discovery to avoid accidental execution as analysis scripts.
