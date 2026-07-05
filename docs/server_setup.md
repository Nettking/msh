# Server setup and deployment modes

MSH is intended to run as an always-on telemetry server. A server machine can collect data, expose the Flask workbench on the local network, or do both at the same time.

The normal deployment shape is:

```text
server computer
  -> Docker Compose
  -> selected MSH components
  -> data/ and results/ persisted on host disk

client computer
  -> browser
  -> http://<server-ip>:5000
```

Do not expose the Flask port directly to the public internet. Use a trusted LAN, VPN, or a reverse proxy with authentication and HTTPS.

## First setup

From a fresh checkout on the server:

```bash
git clone https://github.com/Nettking/msh.git
cd msh
python setup_msh.py
docker compose up -d --build
```

The setup helper writes a local `.env` file. That file is ignored by git and controls which Docker Compose profiles are active.

## Deployment modes

| Mode | Compose profile | Purpose |
| --- | --- | --- |
| Full server | `full` | Run the Flask workbench and the MTConnect recorder together. |
| Web workbench | `web` | Run the Flask workbench, orchestration, playback, sources page, and analysis UI. |
| Web UI only | `web` with `MSH_SKIP_ORCHESTRATION=1` | Run the UI without background processing. Useful for inspection/debugging. |
| Recorder only | `recorder` | Run only the MTConnect data recorder. No web UI. |
| Prep only | `prep` | Run one-shot preparation/orchestration. |
| Observer sync only | `observer-sync` | Run one-shot Observer Phoenix synchronization. |

You can rerun setup at any time:

```bash
python setup_msh.py
docker compose up -d --build
```

## Manual profile control

Instead of using `.env`, profiles can also be selected explicitly:

```bash
docker compose --profile web up -d --build
docker compose --profile recorder up -d --build
docker compose --profile full up -d --build
```

The `.env` approach is preferred for a server because the selected role is remembered.

## Connecting from another computer

Find the server IP address:

```bash
hostname -I
```

On Windows PowerShell:

```powershell
ipconfig
```

Then open this on the client computer:

```text
http://<server-ip>:5000
```

If the page does not load, check that:

- the server and client are on the same network or VPN.
- Docker is running.
- the `flask` service is running.
- port `5000/tcp` is allowed by the server firewall.

## Recorder configuration

The recorder service polls MTConnect `/current` endpoints and writes normalized source JSONL under:

```text
data/sources/mtconnect_recorder/jsonl/<machine>/<YYYY-MM-DD>.jsonl
```

During setup, recorder sources can be entered as:

```text
IG500=http://192.168.200.251:5000/current;VTC=http://192.168.200.252:5000/current
```

The same value can be placed in `.env` manually:

```text
MSH_RECORDER_SOURCES=IG500=http://192.168.200.251:5000/current;VTC=http://192.168.200.252:5000/current
```

The recorder state file is stored under:

```text
data/source_state/mtconnect_recorder_state.json
```

This keeps sequence tracking persistent across container restarts.

## Common server commands

Start the selected role:

```bash
docker compose up -d --build
```

View logs:

```bash
docker compose logs -f
```

Restart:

```bash
docker compose restart
```

Stop:

```bash
docker compose down
```

Update after pulling new repo changes:

```bash
git pull
docker compose up -d --build
```

## Notes

- `data/` and `results/` are mounted into containers and persist on the host.
- `.env` is local and ignored by git.
- Observer Phoenix credentials can be configured in the Flask UI at `/sources/observer-phoenix` when the web profile is active.
- The Parquet/DuckDB cache remains derived from JSONL and can be rebuilt from `/control`.
