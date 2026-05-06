# Troubleshooting

Use `/status` first. It shows runtime phase, discovered date bounds, processed/pending counts, last failure, and readiness hints.

## Flask starts but no data appears

Possible causes:

- `data/` is empty or mounted incorrectly.
- JSONL files are not under a scanned/input root.
- records do not contain parseable timestamps and filenames do not contain usable dates.
- bootstrap is still in discovery/filtering.

Checks on macOS/Linux:

```bash
find data -name '*.jsonl' -print | head
python -m json.tool results/workflows/runtime_state.json | head -80
```

PowerShell equivalents:

```powershell
Get-ChildItem -Path data -Filter *.jsonl -Recurse | Select-Object -First 10 FullName
Get-Content results/workflows/runtime_state.json -Raw | ConvertFrom-Json | Format-List
```

Open `/status` and verify `latest_available_source_date`, `total_available_days`, and `last_failure`.

## Playback page has no selectable data

Playback requires a workflow session with filtered data and a timeline export. Confirm that a session exists under `results/workflows/` and that it contains:

```text
data/
exports/timeline/timeline_rows.csv
exports/timeline/manifest.json
```

Useful checks:

```bash
find results/workflows -path '*/exports/timeline/timeline_rows.csv' -print | head
find results/workflows -path '*/session_state.json' -print | head
```

```powershell
Get-ChildItem results/workflows -Recurse -Filter timeline_rows.csv | Select-Object -First 10 FullName
Get-ChildItem results/workflows -Recurse -Filter session_state.json | Select-Object -First 10 FullName
```

If filtered data exists but no export exists, run the workflow or `data_visualizer` from `/control`, or trigger a refresh and wait for bootstrap/catch-up.

## Playback export is stale or not reused

Playback cache reuse requires the manifest's session config signature and filtered-data generation timestamp to match the current workflow session metadata. If a session was edited manually or filtered data was regenerated, recreate the export from `/control` by rerunning the workflow or `data_visualizer`.

To compare the relevant fields:

```bash
python - <<'PY'
import json
from pathlib import Path
session = Path('results/workflows/<session-id>')
state = json.loads((session / 'session_state.json').read_text())
manifest = json.loads((session / 'exports/timeline/manifest.json').read_text())
print('state signature:', state.get('session_config_signature'))
print('manifest signature:', manifest.get('session_config_signature'))
print('state filtered generated_at:', state.get('filter_result', {}).get('generated_at'))
print('manifest filtered generated_at:', manifest.get('filtered_generated_at'))
PY
```

## A script failed during bootstrap or catch-up

The runtime is best-effort. One automatic script failure does not prevent Flask from starting or other scripts from running. Use `/status` for `last_failure` and `/control` for recent stdout/stderr snippets.

Operational next steps:

1. Fix the data or dependency issue shown in the snippet.
2. Open `/control`.
3. Select the affected workflow session.
4. Rerun the failed script, startup-safe checks, or the workflow.

For local dependency issues, verify imports with:

```bash
python -m pip install -r requirements.txt
python -m catalog.flask_app.app
```

## A control action will not start

Only one control action can run at a time. Wait for the active action to finish and refresh `/control`. If the process restarted, remember that recent run history is in memory and may be cleared.

If the UI still shows an active action after a restart, check the current process logs rather than deleting workflow session artifacts; run history is not durable state.

## Date range produces no records

Check whether records have parseable `timestamp` values. For same-day hour filtering, records without parseable timestamps are skipped. Filename-date fallback is only for files where no records have timestamps and applies to date-range filtering, not hour filtering.

Quick timestamp sample:

```bash
python - <<'PY'
import json
from pathlib import Path
for path in sorted(Path('data').rglob('*.jsonl'))[:5]:
    with path.open(encoding='utf-8') as fh:
        for line in fh:
            try:
                print(path, json.loads(line).get('timestamp'))
            except json.JSONDecodeError:
                print(path, 'malformed json line')
            break
PY
```

PowerShell timestamp sample:

```powershell
Get-ChildItem data -Recurse -Filter *.jsonl | Select-Object -First 5 | ForEach-Object {
  $line = Get-Content $_.FullName -TotalCount 1
  try { [pscustomobject]@{ File = $_.FullName; Timestamp = ($line | ConvertFrom-Json).timestamp } }
  catch { [pscustomobject]@{ File = $_.FullName; Timestamp = 'malformed json line' } }
}
```

## Docker rebuild or dependency issues

Rebuild the Flask service:

```bash
docker compose up --build flask
```

For local development, reinstall Python dependencies in your environment:

```bash
python -m pip install -r requirements.txt
```

If Docker cannot see data that exists on the host, inspect the compose volume mount and compare host/container paths:

```bash
docker compose exec flask sh -lc "pwd; find data -name '*.jsonl' -print | head"
```

## Runtime appears stuck after restart

Open `/startup` if a startup decision is required. Otherwise inspect `results/workflows/runtime_state.json` and `/status`. You can use `/control` to request a refresh or create/reuse a specific workflow session scope.

A common safe check is to compare runtime state with discovered sessions:

```bash
python - <<'PY'
from pathlib import Path
print('runtime state exists:', Path('results/workflows/runtime_state.json').exists())
print('session count:', len([p for p in Path('results/workflows').glob('*/session_state.json')]))
PY
```

## Hidden scripts do not appear in `/control`

This is expected for runner internals, recorders, simulator, desktop automation, and environment-specific tools. They are intentionally excluded from workflow discovery to avoid accidental execution as analysis scripts. See [catalog/README.md](../catalog/README.md#hidden-or-non-workflow-folders).
