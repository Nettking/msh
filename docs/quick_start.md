# Quick start

Status: **current user guide**  
Reviewed: **2026-08-06**

This guide describes the normal supported way to start MSH and complete capability-first onboarding.

An MSH device is not assigned one permanent product role during first setup. The required first-run flow is:

```text
Identity
  -> Federation
  -> Inspect
  -> finish setup
  -> open Federation
```

A current device inspection is sufficient to finish setup. Benchmarks and contribution decisions are optional follow-up work and do not block the normal workbench.

## Prerequisites

- Git.
- Docker Desktop on Windows, or Docker Engine with Docker Compose on Linux/macOS.
- Internet access the first time the configured Ollama model must be downloaded.
- JSONL telemetry under `data/` only when you want to analyze real machine data. Development examples are available under `example-data/`.

## Windows: supported launcher

From a fresh checkout:

```cmd
git clone https://github.com/Nettking/msh.git
cd msh
start.cmd
```

The launcher builds and starts the current core services:

- Federation relay;
- Ollama;
- Flask workbench;
- managed recorder.

It also checks that the configured Ollama model is installed and downloads it when necessary. Existing identity, Federation, recorder, model, and data state are preserved during normal starts.

The web interface is limited to the local MSH computer by default. Open:

```text
http://localhost:5000
```

For a device without completed onboarding, open:

```text
http://localhost:5000/onboarding
```

The actual web port may differ when port `5000` is already occupied. `start.cmd` prints the resolved address before opening the browser.

### Reconnect and refresh an existing setup

Use the explicit resume path when you want startup to reconnect the saved Federation, refresh inspection, run the benchmark plan, and reconcile saved contribution intent before opening the workbench:

```cmd
start.cmd --resume
```

This does not replace the device identity or create a new Federation.

### Start as a fresh device

To remove this checkout's device identity and Federation setup before starting:

```cmd
start.cmd --fresh
```

The launcher displays the reset boundary and requires typing `RESET` before it changes anything.

The reset removes:

- the local device identity and keys;
- Federation membership, pairing, onboarding, inspection, and benchmark state;
- local Federation relay authority state;
- saved legacy server-role and setup choices.

It preserves:

- recorded telemetry and imported data;
- source configuration and recorder checkpoints;
- analysis and workflow results;
- Docker images;
- downloaded Ollama models.

After the reset, MSH verifies that authoritative setup state is empty and opens capability-first onboarding.

## Linux or macOS

Start the same default service set from the repository root:

```bash
docker compose up -d --build relay ollama recorder flask
```

Install the configured model the first time, or after changing `MSH_AI_MODEL`:

```bash
docker compose --profile model-install run --rm ollama-pull
```

Then open:

```text
http://localhost:5000/onboarding
```

Normal `docker compose` starts preserve device, Federation, recorder, model, and data state.

## Complete first-run onboarding

The mandatory setup is intentionally short:

1. **Identity** — create or load the stable identity for this MSH device.
2. **Federation** — reconnect, join, or create the user-facing Federation through an authenticated path.
3. **Inspect** — inspect the device's supported local capabilities.
4. **Finish setup** — enable the normal workbench without granting optional contribution authority.
5. **Open Federation** — review connected devices and available capabilities.

Finishing setup does not automatically grant recorder, language-model, compute, or storage contribution authority. Optional benchmarks and contribution choices remain available from the Federation pages.

## First pages to open

- <http://localhost:5000/onboarding> — complete or repair capability-first onboarding.
- <http://localhost:5000/federation> — inspect the Federation, devices, capabilities, benchmarks, and contribution state.
- <http://localhost:5000/status> — verify discovery, bootstrap, catch-up, failures, and readiness.
- <http://localhost:5000/control> — select datasets, refresh data, and run workflows or scripts.
- <http://localhost:5000/operator-strategies> — capture structured operator decisions.
- <http://localhost:5000/sources/observer-phoenix> — configure and test Observer Phoenix access.
- <http://localhost:5000/ai> — ask read-only system-understanding questions.
- <http://localhost:5000/playback> — inspect playback-ready exports.
- <http://localhost:5000/analyses> — browse discovered CSV and JSON artifacts.
- <http://localhost:5000/docs> — browse repository documentation.

## Access from another trusted device

`start.cmd` binds the Flask workbench to `127.0.0.1` by default. To allow access from another computer on a trusted LAN or VPN, set the bind address before starting.

From Windows Command Prompt:

```cmd
set MSH_WEB_BIND=0.0.0.0
start.cmd
```

From PowerShell:

```powershell
$env:MSH_WEB_BIND = "0.0.0.0"
.\start.cmd
```

For Docker Compose on Linux/macOS, place this in `.env` or export it before startup:

```text
MSH_WEB_BIND=0.0.0.0
```

Then open:

```text
http://<server-ip>:5000
```

Use the reachable LAN or VPN address when pairing another physical MSH device so the pairing material can advertise a reachable relay address. Do not expose Flask, the relay, or Ollama directly to the public internet.

See [Server setup](server_setup.md) for network, deployment, recorder, and provider details.

## Advanced deployment profiles

`setup_msh.py` and Compose profiles remain available for advanced or compatibility deployments such as a headless model provider, a recorder-only station, or a one-shot preparation job. They are not the normal first-run product flow and do not define a permanent device identity or grant capability authority.

See [Server setup](server_setup.md) before selecting a non-default deployment shape.

## Optional one-shot jobs

Run preparation explicitly:

```bash
docker compose --profile prep run --rm prep
```

Run Observer Phoenix synchronization explicitly:

```bash
docker compose --profile observer-sync run --rm observer-sync
```

## Start without Docker

Use this for local development or troubleshooting only:

```bash
python -m catalog.flask_app.app
```

Useful environment variables include:

- `FLASK_RUN_HOST` — defaults to `0.0.0.0` for direct Flask startup.
- `FLASK_RUN_PORT` — defaults to `5000`.
- `FLASK_DEBUG=1` — enables Flask debug mode.
- `MSH_FLASK_SECRET` — Flask secret key.
- `MSH_SKIP_ORCHESTRATION=1` — starts Flask without the background runtime.
- `MSH_SCAN_DIRS` — comma-separated artifact scan roots.
- `MSH_AI_MODEL` — selected Ollama model.
- `OLLAMA_BASE_URL` — Ollama API URL.
