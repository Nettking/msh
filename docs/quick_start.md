# Quick start

Status: **current user guide**  
Reviewed: **2026-08-09**

This guide describes the normal supported way to start MSH and complete capability-first onboarding.

An MSH device is not assigned one permanent product role during first setup. The required first-run flow is:

```text
Identity
  -> Federation
  -> Inspect
  -> finish setup
  -> open Federation
```

Accepted device inspection evidence is sufficient to finish setup. Benchmarks and contribution decisions are optional follow-up work and do not block the normal workbench.

Inspection and benchmark results are durable device evidence. Run them when establishing the device, or explicitly again after a relevant hardware, provider, model, service, or configuration change. Normal starts and updates reuse the saved evidence instead of rerunning probes. Historical time-only expiry metadata does not by itself force another run in the installed product.

## Prerequisites

- Git.
- Docker Desktop on Windows, or Docker Engine with Docker Compose on Linux/macOS.
- Python 3 on Linux/macOS for the bounded host-owned software update agent.
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

It also checks that the configured Ollama model is installed and downloads it when necessary. Existing identity, Federation, inspection, benchmark, recorder, model, and data state are preserved during normal starts. The launcher bakes the exact Git commit into the MSH runtime images and starts the bounded host-owned update agent used by **Federation → Update all devices**.

The web interface is limited to the local MSH computer by default. Open:

```text
http://localhost:5000
```

For a device without completed onboarding, open:

```text
http://localhost:5000/onboarding
```

The actual web port may differ when port `5000` is already occupied. `start.cmd` prints the resolved address before opening the browser.

### Reconnect an existing setup after an update

Use the explicit resume path when you want startup to reconnect the saved Federation and verify that the existing local capability evidence can be reused before opening the workbench:

```cmd
start.cmd --resume
```

Resume is evidence-preserving. It does **not** rerun device inspection or benchmarks, and it does not replace the device identity or create a new Federation. The long-running Flask app reconciles saved contribution intent once; saved benchmark evidence may support an explicitly enabled contribution only while its benchmark identity, implementation version, and declared dependency inputs still match. A dependency/version change is not treated as fresh authority.

`update.cmd` uses this resume path after a successful fast-forward update. Federation-wide updates use the same resume behavior through the host update agent, but additionally rebuild and verify the exact running image before reporting success.

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

## Linux or macOS: supported launcher

From a fresh checkout:

```bash
git clone https://github.com/Nettking/msh.git
cd msh
bash start.sh
```

The POSIX launcher starts the same default core services, verifies the configured Ollama model, bakes the exact Git commit into the MSH images, and starts the single-instance host update agent. Normal starts preserve device, Federation, inspection, benchmark, recorder, model, and data state.

To reconnect an existing saved Federation before opening the workbench:

```bash
bash start.sh --resume
```

Then open:

```text
http://localhost:5000/onboarding
```

Inspection and benchmark execution remain explicit browser actions. Python 3 is required on the host only for the bounded update agent; MSH application dependencies still run in Docker.

### Manual Compose startup

Direct Compose remains useful for development and troubleshooting:

```bash
export MSH_BUILD_COMMIT="$(git rev-parse --verify HEAD^{commit})"
docker compose up -d --build relay ollama recorder flask
```

However, **Federation → Update all devices** requires the host-owned update agent. If you deliberately bypass `start.sh`, run the agent separately from the same checkout and data directory:

```bash
python3 scripts/posix/msh_update_agent.py \
  --repo-root "$PWD" \
  --data-directory "${MSH_DATA_DIR:-$PWD/data}"
```

The agent accepts only the bounded local handoff contract. It does not expose a network endpoint or accept arbitrary commands from Federation messages.

## Complete first-run onboarding

The mandatory setup is intentionally short:

1. **Identity** — create or load the stable identity for this MSH device.
2. **Federation** — reconnect, join, or create the user-facing Federation through an authenticated path.
3. **Inspect** — inspect the device's supported local capabilities and persist that evidence.
4. **Finish setup** — enable the normal workbench without granting optional contribution authority.
5. **Open Federation** — review connected devices and available capabilities.

Finishing setup does not automatically grant recorder, language-model, compute, or storage contribution authority. Optional benchmarks and contribution choices remain available from the Federation pages.

The installed product uses run-once capability evidence. Explicit **Inspect again** and **Run again** actions remain available when the device or a relevant dependency changes, but elapsed time alone does not force either action. The frozen evidence records retain their original `expires_at` metadata without being rewritten. Benchmark identity, implementation-version, and declared dependency changes still invalidate saved benchmark evidence and require an explicit new run before it can support contribution reconciliation.

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

`start.cmd` and `start.sh` bind the Flask workbench to `127.0.0.1` by default. To allow access from another computer on a trusted LAN or VPN, set the bind address before starting.

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

From Linux/macOS:

```bash
export MSH_WEB_BIND=0.0.0.0
bash start.sh
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

Direct Flask startup does not provide the host-owned Docker activation boundary required by Federation-wide software updates.
