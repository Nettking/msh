# Troubleshooting

Status: **current operator guide**
Reviewed: **2026-08-12**

Use `/status` first for local runtime/data problems and **Federation** first for membership, leadership, distributed update, contribution, Tailscale discovery, or standalone-recorder control problems.

Do not delete identity, Federation databases, Docker volumes, recorder checkpoints, human-auth state, or capability evidence merely to clear a warning. Use the documented reset/migration boundary only when you intentionally want that state change.

## Fresh installation has no account

A fresh local authority has no default username/password. Open the FCP web interface. While the local human-user database is empty, normal browser requests should redirect to:

```text
/admin/users/bootstrap
```

Create the first administrator there with a valid email address and a confirmed password of at least 12 characters. After that account commits, the anonymous bootstrap closes and normal sign-in takes over.

A remotely paired Federation member with an empty local shadow-user database intentionally does **not** reopen first-admin bootstrap. Use Federation human sign-in instead.

If a first-user bootstrap claim exists while the user table is unexpectedly empty, FCP fails closed with 503 rather than reopening anonymous admin creation. Investigate the auth database/state; do not delete individual guard files merely to bypass the protection.

## Federation device is missing or will not reconnect

Check:

1. the device is using its existing `data/` directory and stable identity;
2. relay/Flask networking is reachable on the trusted LAN/VPN/Tailscale path;
3. the joining device was paired through a current signed `FCP1-...` code or already has a saved trusted binding; and
4. the issuing FCP installation was opened through a LAN/VPN address reachable by the other physical machine before the pairing code was generated.

Current browser pairing codes are one-use and valid for up to 10 minutes. If a code expired or was already redeemed, generate another code; do not try to turn the old code into a persistent credential.

## Tailscale discovery finds no Federation

On the host, check the already signed-in Tailscale client:

```bash
tailscale status
tailscale ip -4
```

Then verify:

- both FCP hosts are signed in to the intended same tailnet;
- the existing FCP host is online;
- its FCP web port is reachable through its Tailscale IPv4 address;
- the firewall permits that trusted Tailscale path; and
- the advertising FCP installation has local Federation authority. A remotely paired member deliberately does not advertise itself as the Federation authority.

Discovery is a pre-start snapshot. Run `start-tailscale.cmd` or `bash start-tailscale.sh` again to refresh it after peers come online.

If the Federation is discovered but onboarding still requires `FCP1-...`, that is expected. Tailscale proves reachability, not Federation membership.

See [Tailscale Federation discovery](tailscale_federation_discovery.md).

## Leader-only controls disappeared from this device

Open Federation Overview and inspect the **creator**, **current leader**, and **leadership term**.

Immutable creator provenance and current operational leadership are separate. After a valid coordinator-authored leader transition, the former leader is fenced from current-leader controls such as Federation-wide updates, leader capability requests, and reviewed member/provider administration.

A brief disconnect does not immediately churn leadership. Promotion occurs only after the bounded offline/heartbeat timeout and only when a valid connected successor exists.

If the authoritative coordinator/relay service itself is unavailable, automatic leader promotion cannot replace it. The current implementation does not provide replicated coordinator/quorum failover.

Human credential/password authority remains creator-backed even if another device becomes current operational leader.

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

A standalone recorder launched directly with `python start_recorder.py` does not run the normal Flask host update agent. It currently requires its own host checkout/process update path.

## Update is stuck on Activation Queued

`Activation Queued` means the device accepted the update request but has not yet proved the exact running target commit.

Wait for build/restart/verification to finish before the current leader presses **Update all devices** again. The coordinator service may briefly be unavailable while its Flask/relay runtime restarts.

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

The migration preserves existing identity, Federation state, evidence, data, human-auth state, models, and retained relay volume while fast-forwarding only approved `main` and starting the current launcher/update agent.

If migration reports ambiguous retained relay volumes, do not delete volumes to guess. The migration deliberately fails closed when it cannot uniquely identify the saved device's relay state.

## A member does not act on a benchmark/contribution request

Current-leader capability requests target only remote members that are reachable when the request is issued. Offline members are not silently queued.

A reachable member may legitimately contribute nothing when:

- no locally registered benchmark definition is runnable;
- a candidate is not locally `ALLOWED`;
- prerequisites are missing; or
- the contribution still requires separate provider-enrollment approval.

Issue another request after a member reconnects. The leader request cannot inject arbitrary benchmark code or bypass member-local/provider policy.

## Device display name is rejected

A trusted member can rename itself under **Federation -> This device**. The current leader can rename other members under **Federation -> Devices**.

Names must satisfy bounded validation, case-insensitive uniqueness, and reserved/public-safe-name rules. The stable cryptographic `node_id` remains the technical identity even when the display name changes.

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

### Recorder data from another Federation device does not appear

Remote recorder telemetry is not found by scanning another device's files. The local product must be connected to the same Federation and must discover a ready session-owner storage authority with the intended logical storage group. It then lists only committed manifest batches, verifies each read, and rebuilds them below `data/federation/shared/telemetry` for the normal catalog and live views.

Check that:

- both devices are current members and connected to the authenticated relay;
- the recorder reports a committed Federation watermark rather than only a local checkpoint;
- the session owner advertises a ready `fcp.storage-control` authority;
- the selected storage group matches on the recorder and Flask device when more than one group exists; and
- the local mirror has free space below its configured quota.

Do not point the scanner at a provider's batch directory. It contains internal storage envelopes and may include prepared or stale data. A hash, schema, or sequence conflict is rejected and quarantined instead of being shown.

### Generic JSONL from another Federation device does not appear

Supported non-recorder `data/**/*.jsonl` is handled by a separate generic Federation logical-storage bridge. Browser-uploaded JSONL is included by default. Authenticated remote files are hash/size verified and materialized below the normal `data/federation/shared/` scan boundary so existing recursive JSONL consumers can discover them.

Check that:

- both devices are current Federation members;
- the source is valid JSONL under the supported `data/` corpus;
- the generic Federation storage synchronization path is running;
- the local generic mirror has space below `FCP_FEDERATED_JSONL_MAX_MIRROR_BYTES`; and
- the publishing installation has not explicitly set `FCP_FEDERATED_JSONL_PUBLISH_UPLOADS=0` when the source is a browser upload.

This does not expose arbitrary host files outside the reviewed FCP data corpus.

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
- [Tailscale Federation discovery](tailscale_federation_discovery.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Standalone recorder](standalone_recorder.md)
- [Server setup](server_setup.md)
- [Quick start](quick_start.md)
