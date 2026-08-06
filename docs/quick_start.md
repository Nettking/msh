# Quick start

This guide gets MSH running as a server-oriented Docker deployment. The same checkout can run the full workbench, the web UI only, the MTConnect recorder only, a headless language-model provider, or one-shot preparation/synchronization jobs.

## Prerequisites

- Docker and Docker Compose for the recommended path.
- Python 3 only to run the local setup helper.
- Internet access on first AI setup so Docker can pull the Ollama image and selected model.
- JSONL telemetry files in `data/` when you want real analysis results. A small example file is available under `example-data/` for development.

## Select what this server should run

Run the setup helper from the repository root:

```bash
python setup_msh.py
```

Choose one of the deployment modes:

- **Full server** — Flask workbench plus MTConnect recorder.
- **Web workbench** — Flask, orchestration, playback, source settings, and analysis UI.
- **Web UI only** — Flask UI without background orchestration.
- **Recorder station** — MTConnect recorder with setup, controls, and diagnostics UI.
- **Language-model provider** — headless model capability for another connected MSH device.
- **Prep only** — one-shot preparation/orchestration.
- **Observer sync only** — one-shot Observer Phoenix synchronization.

When a web-capable mode is selected, setup can use Ollama on the MSH computer or connect to Ollama contributed by another computer. The three standard model choices are:

| Choice | Model | Device target |
| --- | --- | --- |
| Edge small | `smollm2:360m` | Small CPU, Raspberry Pi class, or very low memory testing. |
| Laptop standard | `llama3.2:3b` | Normal laptop or small server. Default balance. |
| Workstation strong | `qwen2.5:7b` | Gaming laptop, workstation, or GPU server. Stronger answers. |

The helper writes `.env`, which is ignored by git and read automatically by Docker Compose. If you choose to pull the model during setup, it starts the `ollama` service and runs the one-shot `ollama-pull` installer.

For an Android/phone MSH instance using a laptop model, see [Connected capabilities](connected_capabilities.md).

## Small laptop provider

To make a Docker-capable laptop contribute the default small model without installing Ollama separately:

```bash
git clone https://github.com/Nettking/msh.git
cd msh
docker compose --profile provider run --rm model-provider-install
```

This starts the provider and downloads `smollm2:360m` once into a persistent Docker volume. Connect the other MSH device to `http://<laptop-ip>:11434`.

## Start with Docker

On Windows, use the supported launcher:

```cmd
start.cmd
```

The launcher starts the current capability-first core baseline in detached mode:

- the Federation relay;
- the bundled Ollama service;
- the Flask workbench;
- the managed recorder.

It waits for `/onboarding` to become available and then opens that page. It does not download a model automatically; model installation remains an explicit setup action.

### Start as a fresh device

To remove the current MSH device identity and Federation setup before starting:

```cmd
start.cmd --fresh
```

The launcher displays the exact reset boundary and requires typing `RESET` before changing anything. It stops the current Compose deployment and removes only:

- `data/federation/`, including the local device identity and keys, onboarding state, saved binding, pairing state, benchmark/contribution evidence, and capability-transition state;
- the Compose `relay_state` volume containing the local Federation coordinator authority database;
- `data/server_setup/server_settings.json`, containing the saved server role and legacy device setup choices.

The fresh-device option intentionally preserves:

- recorded telemetry and imported data;
- source configuration and recorder checkpoints;
- analysis/workflow results;
- `.env` deployment configuration;
- Docker images;
- downloaded Ollama and model-provider models.

After the reset, the same command starts the normal core services and opens `/onboarding` with Identity and Federation empty. Running `start.cmd` without `--fresh` continues to preserve all existing state.

On Linux, macOS, or for manual control, start the default Compose deployment:

```bash
docker compose up -d --build
```

Open capability-first onboarding:

```text
http://localhost:5000/onboarding
```

After onboarding, the Federation overview is available at:

```text
http://localhost:5000/federation
```

From another computer on the same trusted network, open:

```text
http://<server-ip>:5000/onboarding
```

Opening MSH through the reachable LAN or VPN address also allows generated pairing codes to advertise the relay at `ws://<server-ip>:8765`. Opening through `localhost` is intentionally insufficient for pairing another physical device.

For server/firewall details, see [Server setup](server_setup.md).

## Start without Docker

Use this for local development or troubleshooting only:

```bash
python -m catalog.flask_app.app
```

Flask reads these useful environment variables:

- `FLASK_RUN_HOST` — defaults to `0.0.0.0`.
- `FLASK_RUN_PORT` — defaults to `5000`.
- `FLASK_DEBUG` — set to `1` for Flask debug mode.
- `MSH_FLASK_SECRET` — Flask secret key; generated by setup for Docker deployments.
- `MSH_SKIP_ORCHESTRATION=1` — starts Flask without the background runtime.
- `MSH_SCAN_DIRS` — comma-separated artifact scan roots; defaults are supplemented with `data` and `results` by the runtime.
- `MSH_AI_MODEL` — selected Ollama model.
- `OLLAMA_BASE_URL` — Ollama API URL. In Docker this is normally `http://ollama:11434`.

## What web-workbench startup does

The default web-workbench path is webapp-first:

1. Flask starts and registers the operator routes.
2. The runtime manager records app/runtime milestones in `results/workflows/runtime_state.json`.
3. Source dates are discovered from JSONL telemetry in `data/`.
4. A deterministic automatic workflow session is created or reused for the latest discovered day.
5. Session-filtered data is prepared under `results/workflows/<session-id>/data/`.
6. A compact derived metrics artifact is created at `data/_derived/basic_metrics.csv` inside the session.
7. The automatic playback-ready script set runs in best-effort mode.
8. Playback timeline exports are generated or reused under `exports/timeline/` inside the session.
9. Historical catch-up proceeds one day at a time in the background, then the runtime polls for newly arriving source days.

The automatic script set is intentionally bounded: `machines_active_per_day`, `analyze_missing_sequence_number`, `missing_per_day_by_machine`, `sampling_rate_analysis`, and `data_visualizer`.

## First pages to open

- <http://localhost:5000/onboarding> — create/load identity, connect or create the Federation, and inspect the device.
- <http://localhost:5000/federation> — inspect the connected Federation and device capabilities.
- <http://localhost:5000/status> — verify discovery, bootstrap, catch-up, failures, and readiness.
- <http://localhost:5000/control> — select datasets, trigger refreshes, and run workflows or scripts.
- <http://localhost:5000/operator-strategies> — record OSL-style operator strategy decisions during field work.
- <http://localhost:5000/sources/observer-phoenix> — configure and test Observer Phoenix credentials.
- <http://localhost:5000/ai> — ask read-only system-understanding questions using the selected local or connected model provider.
- <http://localhost:5000/playback> — inspect playback-ready exports after filtered session data exists.
- <http://localhost:5000/analyses> — browse discovered CSV/JSON artifacts.

## Optional one-shot preparation

Select `prep-only` during setup, or run explicitly:

```bash
docker compose --profile prep run --rm prep
```

## AI model retry

If model installation fails or you change `MSH_AI_MODEL` in `.env`, retry with:

```bash
docker compose up -d ollama
docker compose run --rm ollama-pull
```

## Windows helper

`start.cmd` is the primary Windows launcher for the capability-first core services. `ops/start-system.ps1` remains a lower-level host-side wrapper around Docker Compose and can optionally launch a VPN monitor script if you pass a valid `-VpnReconnectScript`.
