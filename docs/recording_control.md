# MTConnect recording control

MSH runs the MTConnect recorder as an independent Docker service. The service is
normally alive but idle. Recording is turned on and off from the MSH startup UI.

This is separate from the runtime session choice:

- **Resume session / Start new session** controls analysis and playback progress.
- **Start recording / Stop recording** controls collection from MTConnect sources.

## Start MSH

From the repository root:

```powershell
git pull origin main
docker compose up -d --build flask recorder
```

Or use:

```powershell
.\start.cmd
```

The recorder container starts in standby mode. It does not contact machine
sources until recording is enabled.

## Turn recording on

1. Open `http://localhost:5000/startup`.
2. Confirm the device role is **Full server** or **Recorder only**.
3. Confirm at least one recorder source is configured, for example:

   ```text
   IG500=http://192.168.200.251:5000/current
   ```

   Multiple sources use semicolons:

   ```text
   QuickTurn=http://192.168.200.249:5000/current;IG500=http://192.168.200.251:5000/current;VTC=http://192.168.200.252:5000/current
   ```

4. Click **Start recording** in the startup header.

The web app writes the desired state to:

```text
data/source_state/mtconnect_recorder_control.json
```

The independent recorder service watches that file and reports its heartbeat to:

```text
data/source_state/mtconnect_recorder_status.json
```

## Output

Telemetry is written per machine and day:

```text
data/sources/mtconnect_recorder/jsonl/<machine>/<YYYY-MM-DD>.jsonl
```

Duplicate snapshots are suppressed using the MTConnect `lastSequence` value.
Sequence state survives restarts in:

```text
data/source_state/mtconnect_recorder_state.json
```

The rotating recorder log is:

```text
data/source_state/mtconnect_recorder.log
```

## Verify the recorder service

```powershell
docker compose ps recorder
docker compose logs --tail 100 recorder
Get-Content .\data\source_state\mtconnect_recorder_status.json
Get-Content .\data\source_state\mtconnect_recorder.log -Tail 50
```

Check for recent data:

```powershell
Get-ChildItem .\data\sources\mtconnect_recorder\jsonl -Recurse
```

Read the newest rows:

```powershell
$latest = Get-ChildItem .\data\sources\mtconnect_recorder\jsonl\*.jsonl -Recurse |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

$latest.FullName
Get-Content $latest.FullName -Tail 5
```

## Verify VPN/network access from Docker

The Windows computer may reach the machine network while Docker cannot. Test from
the Flask container because it uses the same Docker networking environment as the
recorder:

```powershell
docker compose exec flask python -c "import requests; u='http://192.168.200.251:5000/current'; r=requests.get(u, timeout=5); print(r.status_code, len(r.text)); print(r.text[:200])"
```

Expected result: HTTP `200` and MTConnect XML.

- If Windows and Docker both fail, inspect VPN, route, machine address, or firewall.
- If Windows works but Docker fails, inspect Docker Desktop VPN routing.
- If Docker succeeds but no JSONL appears, inspect `lastSequence`, recorder status,
  and recorder logs.

## Stop recording

Click **Stop recording**. The worker flushes buffered rows and returns to standby.
The Docker service remains alive so it can be enabled again without restarting
MSH.

## Architecture

Flask does not create or own the recorder process. It writes a durable desired
state. The recorder container owns polling and disk writes. This prevents duplicate
recorders with multiple web workers and allows Flask to restart without silently
stopping data capture.
