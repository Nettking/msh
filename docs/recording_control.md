# MTConnect recording control

FCP runs the MTConnect recorder as an independent Docker service. The service is
normally alive but idle. Recording is turned on and off from the FCP startup UI.

This is separate from the runtime session choice:

- **Resume session / Start new session** controls analysis and playback progress.
- **Start recording / Stop recording** controls collection from MTConnect sources.

## Start FCP

On the Windows FCP machine, update and start from Command Prompt:

```cmd
cd /d C:\path\to\fcp
git pull --ff-only origin main
start.cmd
```

`start.cmd` starts both containers in the background and opens:

```text
http://localhost:5000/startup
```

The launcher waits until the page answers before opening it and binds the web
port to `127.0.0.1` by default. This keeps recorder setup and private-network
scan controls on the FCP machine.

Do not run `setup_fcp.py` for an ordinary restart. Browser-managed settings are
already stored under `data\` and rerunning command setup could replace them.
The recorder container starts in standby mode until recording is enabled.

For a fresh checkout:

```cmd
cd /d "%USERPROFILE%\Documents"
git clone https://github.com/Nettking/msh.git
cd fcp
start.cmd
```

If `git pull --ff-only` reports local changes or a branch conflict, stop and
inspect those changes. Do not use a hard reset on a recorder machine.

## Discover machines from MTConnect data

1. During first-time setup choose **Recorder station** (or **Full server**).
2. In the Recorder step, enter the private machine subnet,
   such as `192.168.200.0/24`, and port `5000`.
3. Click **Scan network**.
4. Keep the required MTConnect Agents checked, continue, and save setup.

The selected role and checked Agents are saved together. On the Recorder status
page, **Add or rescan machines** opens this same Recorder setup step again.

Discovery reads each Agent's `/probe` document. The stable recorder key comes
from the MTConnect Device UUID, with serial number, Device id, and address used
as fallbacks. The displayed machine name also comes from `/probe` and includes
the serial number or UUID. This avoids merging several machines under the
generic name `Mazak`.

## Turn recording on

1. Open `http://localhost:5000/status`. A configured Recorder station is sent
   here automatically when `start.cmd` opens FCP.
2. Confirm at least one recorder source is configured, for example:

   ```text
   MAZAK-M7ZDA13010Z=http://192.168.200.249:5000
   ```

   Multiple sources use semicolons:

   ```text
   MAZAK-M7ZDA13010Z=http://192.168.200.249:5000;IG500-UUID=http://192.168.200.251:5000
   ```

3. Click **Start recording** on Recorder status.

Recorder totals, last-save time, machine health, and sequence numbers refresh
about every two seconds while the page is visible. Live refresh pauses in a
hidden tab and resumes when you return.

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

The following host-mounted paths also survive `git pull`, image rebuilds, and
future `start.cmd` runs:

```text
data/server_setup/server_settings.json
data/source_state/mtconnect_recorder_control.json
data/source_state/mtconnect_network_scan.json
data/sources/mtconnect_recorder/
```

If discovery replaces an old generic alias with the UUID for the same Agent
URL, the recorder moves that source's checkpoint to the new alias before
continuing. It does not intentionally restart at sequence zero.

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
FCP.

## Architecture

Flask does not create or own the recorder process. It writes a durable desired
state. The recorder container owns polling and disk writes. This prevents duplicate
recorders with multiple web workers and allows Flask to restart without silently
stopping data capture.
