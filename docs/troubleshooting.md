# Troubleshooting

Status: **current operator guide**
Reviewed: **2026-08-11**

Use `/status` first for local runtime/data problems and **Federation** first for membership, distributed update, contribution, or standalone-recorder control problems.

Do not delete identity, Federation databases, Docker volumes, recorder checkpoints, or capability evidence merely to clear a warning. Use the documented reset/migration boundary only when you intentionally want that state change.

## Federation device is missing or will not reconnect

Check:

1. the device is using its existing `data/` directory and stable identity;
2. relay/Flask networking is reachable on the trusted LAN/VPN;
3. the joining device was paired through a current signed `FCP1-...` code or already has a saved trusted binding; and
4. the issuing FCP installation was opened through a LAN/VPN address reachable by the other physical machine before the pairing code was generated.

Current browser pairing codes are one-use and valid for up to 10 minutes. If a code expired or was already redeemed, generate another code; do not try to turn the old code into a persistent credential.

## Federation update check times out

On a normal FCP device, verify that the host-owned update agent is running. The supported launchers start it automatically:

```cmd
start.cmd
```

or:

```bash
bash start.sh
```

Then inspect the local result:

```cmd
type data\federation\update-agent\result.json
```

or:

```bash
cat data/federation/update-agent/result.json
```

Common safe failure states include:

- `dirty` — tracked/untracked local changes must be reviewed before updating;
- `ahead` or `diverged` — the checkout is not a safe fast-forward target;
- unapproved remote/branch/checkout state;
- target unavailable;
- runtime activation/verification failure; or
- report timeout after the host could not finish in the bounded window.

Do not use `git reset --hard`, `git clean`, or delete Federation state to bypass these checks.

A standalone recorder launched directly with `python start_recorder.py` does not run the normal Flask update-event processor/host update agent. It currently requires its own host checkout/process update path.

## Update is stuck on Activation Queued

`Activation Queued` means the device accepted the update request but has not yet proved the exact running target commit.

Wait for build/restart/verification to finish before pressing **Update all devices** again. The coordinator may briefly be unavailable while its Flask/relay runtime restarts.

If the state does not resolve, inspect:

```bash
docker compose ps
```

and the update-agent result file above.

The successful terminal state is shown in the UI as **Updated** with a green indicator. Internally this is `runtime_verified`, which requires `running_commit == target_commit`.

## Older Windows installation cannot participate in Update all

An installation from before the current updater may need one safe migration bootstrap:

```cmd
migrate.cmd
```

The migration preserves existing identity, Federation state, evidence, data, models, and retained relay volume while fast-forwarding only approved `main` and starting the current launcher/update agent.

If migration reports ambiguous retained relay volumes, do not delete volumes to guess. The migration deliberately fails closed when it cannot uniquely identify the saved device's relay state.

## Standalone recorder does not find machines on first start

Normal first start is:

```bash
python start_recorder.py FCP1-...
```

The launcher attempts the bounded private-network scan automatically. If no suitable private IPv4 `/24` can be inferred, either:

- join the Federation and request a scan later from `/federation/recorders`; or
- pass an explicit validated private network:

```bash
python start_recorder.py FCP1-... --scan-cidr 192.168.200.0/24
```

Verify the MTConnect Agent is reachable on the expected port (default 5000) from the recorder host itself.

## Remote recorder scan/source change does not complete

Open **Federation -> Recorders** and verify:

- the target is a connected standalone recorder;
- the request targets the recorder's current Federation membership;
- the recorder process is still running its Federation control worker; and
- the recorder can reach the requested private network/MTConnect port.

Source additions are intentionally limited to opaque IDs from the recorder's latest scan. If an addition is rejected because the scan is stale or mismatched, run a new scan and select from that result.

A remote Federation member cannot inject an arbitrary `http://...` source. For explicit source URLs, administer the recorder host directly or use the local source configuration path.

Removing a source does not delete previously captured telemetry/checkpoints.

## Standalone recorder records locally but Federation storage is empty

This can be expected during a Federation/storage outage. Local recording is the primary commit boundary.

Check that:

- the recorder remains paired/reconnected;
- a ready logical-storage authority exists;
- the intended storage group is available (or exactly one group is ready for auto-selection); and
- the recorder publication outbox can reconnect and retry.

Do not move the recorder checkpoint backward to force publication. Checkpoint-committed data is reconciled into a durable publication outbox and should retry independently.

## Flask starts but no data appears

Possible causes:

- `data/` is empty or mounted incorrectly;
- JSONL files are not under a scanned/input root;
- records do not contain parseable timestamps and filenames do not contain usable dates; or
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

Only one local control action can run at a time. Wait for the active action to finish and refresh `/control`.

If the UI still shows an active action after a restart, check current process logs rather than deleting workflow session artifacts; recent run history may be in memory while durable session artifacts remain on disk.

## Filtering opens too many files

Filtering uses `results/runner/data_index.json` to prune source JSONL files by cached timestamp bounds before parsing candidate files. If pruning looks ineffective, the index may contain files with missing/invalid timestamp metadata or legacy files that only have filename dates.

The runner is intentionally conservative: files with unknown metadata are opened rather than silently skipped. Let date discovery or filtering complete once after source files change so the incremental index can refresh changed/new files and remove deleted entries.

## Date range produces no records

Check whether records have parseable `timestamp` values. For same-day hour filtering, records without parseable timestamps are skipped. Filename-date fallback is only for files where no records have timestamps and applies to date-range filtering, not hour filtering.

## Docker rebuild or dependency issues

For the supported product, prefer the launcher/update path. For targeted development troubleshooting, rebuild a service explicitly:

```bash
docker compose up -d --build flask
```

For local development, reinstall Python dependencies:

```bash
python -m pip install -r requirements.txt
```

If Docker cannot see host data, inspect the volume mount and compare host/container paths:

```bash
docker compose exec flask sh -lc "pwd; find data -name '*.jsonl' -print | head"
```

## Runtime appears stuck after restart

The supported installed-product startup is capability-first. Open `/onboarding` when setup is incomplete; otherwise inspect `results/workflows/runtime_state.json` and `/status` and use `/control` to request a refresh or select a workflow session.

Do not restore old role-first startup behavior to fix a current runtime problem. Legacy setup state is migration input only.

## Hidden scripts do not appear in `/control`

This is expected for runner internals, recorders, simulator, desktop automation, and environment-specific tools. They are intentionally excluded from workflow discovery to avoid accidental execution as analysis scripts. See [catalog/README.md](../catalog/README.md#hidden-or-non-workflow-folders).

## Related guides

- [Federation operations](federation_operations.md)
- [Standalone recorder](standalone_recorder.md)
- [Server setup](server_setup.md)
- [Quick start](quick_start.md)
