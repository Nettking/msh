# Server setup and deployment modes

MSH is intended to run as an always-on telemetry server. A server machine can collect data, expose the Flask workbench on the local network, provide an AI capability, consume that capability from another connected machine, or combine these roles.

The normal deployment shape is:

```text
server computer
  -> Docker Compose
  -> selected MSH components
  -> data/ and results/ persisted on host disk
  -> optional Ollama model volume for /ai

client computer
  -> browser
  -> http://<server-ip>:5000
```

Do not expose the Flask port or the Ollama port directly to the public internet. Use a trusted LAN, VPN, or a reverse proxy with authentication and HTTPS.

## First setup

From a fresh checkout on the server:

```bash
git clone https://github.com/Nettking/msh.git
cd msh
python setup_msh.py
docker compose up -d --build
```

The setup helper writes a local `.env` file. That file is ignored by git and controls which Docker Compose profiles are active.

When a web-capable mode is selected, setup asks whether to enable the AI explainer. The default is yes. Ollama can run beside MSH or on a connected computer on a trusted LAN/VPN. Setup then offers three standard model choices, tests the provider, and can pull the selected model immediately.

The browser setup runs in a focused shell without the normal Monitor, Knowledge, and System menus. It shows one decision at a time: device role, AI connection, model, recorder when required, and finish. After the first setup and any required runtime-start choice, MSH shows a focused first-task handoff: capture operator knowledge, connect machine data, or deliberately open the full workbench. Normal navigation appears only after the user chooses a task.

Technical bootstrap defaults are not treated as a user-completed setup. A session-resume choice is shown only after the browser wizard has been saved, and it is presented separately from device configuration.

See [Connected capabilities](connected_capabilities.md) for the phone-to-laptop setup.

## Deployment modes

| Mode | Compose profile | Purpose |
| --- | --- | --- |
| Full server | `full` | Run the Flask workbench and the MTConnect recorder together. |
| Web workbench | `web` | Run the Flask workbench, orchestration, playback, sources page, strategy capture, analysis UI, and optional AI page. |
| Web UI only | `web` with `MSH_SKIP_ORCHESTRATION=1` | Run the UI without background processing. Useful for inspection/debugging. |
| Recorder station | `recorder` | Run MTConnect capture with the setup, recorder controls, and diagnostics UI. |
| Language-model provider | `provider` | Run a headless MSH node that contributes an Ollama model to another MSH device. |
| Prep only | `prep` | Run one-shot preparation/orchestration. |
| Observer sync only | `observer-sync` | Run one-shot Observer Phoenix synchronization. |
| AI model server | `ai` | Run Ollama and the one-shot model installer used by `/ai`. Usually combined with `web` or `full`. |

You can rerun setup at any time:

```bash
python setup_msh.py
docker compose up -d --build
```

## Language-model provider node

Use this mode when one MSH device, such as a phone, should use a model contributed by a Docker-capable laptop. The laptop is installed from the same MSH repository, but it runs only the provider services rather than a second web workbench.

From a fresh checkout on the laptop, the Docker-only installation is:

```bash
git clone https://github.com/Nettking/msh.git
cd msh
docker compose --profile provider run --rm model-provider-install
```

This starts `model-provider`, waits for it to become healthy, and installs the default `edge-small` model (`smollm2:360m`). Port `11434` is published on the laptop so another MSH device on the trusted LAN/VPN can connect. The model is stored in the persistent `model_provider_models` volume and is not downloaded again during ordinary starts or repository updates.

The setup-helper equivalent is:

```bash
python setup_msh.py --mode language-model-provider --ai-profile edge-small --start --pull-model
```

After the first install:

```bash
# Verify
docker compose --profile provider ps model-provider

# Start again without reinstalling the model
docker compose --profile provider up -d model-provider

# Stop
docker compose --profile provider down
```

Do not expose port `11434` through the public internet. Restrict it to a trusted private network or VPN.

## AI model choices

`setup_msh.py` offers three standard model choices for different devices:

| Setup choice | Model | Intended device |
| --- | --- | --- |
| `edge-small` | `smollm2:360m` | Small CPU, Raspberry Pi class, or very low memory testing. |
| `laptop-standard` | `llama3.2:3b` | Normal laptop or small server. Default balance. |
| `workstation-strong` | `qwen2.5:7b` | Gaming laptop, workstation, or GPU server. Stronger answers. |

When AI is enabled, setup writes these values to `.env`:

```text
COMPOSE_PROFILES=web,ai
MSH_AI_PROFILE=laptop-standard
MSH_AI_MODEL=llama3.2:3b
OLLAMA_BASE_URL=http://ollama:11434
```

For a full server the profile line is normally:

```text
COMPOSE_PROFILES=full,ai
```

Local AI uses two services:

```text
ollama       persistent local model server
ollama-pull  one-shot installer that pulls MSH_AI_MODEL into the Docker volume
```

The provider profile uses separate services and storage:

```text
model-provider          persistent provider reachable on the trusted LAN/VPN
model-provider-install  one-shot installer for MSH_PROVIDER_MODEL
```

If setup cannot pull the model immediately, retry after Docker is running:

```bash
docker compose up -d ollama
docker compose run --rm ollama-pull
```

## Manual profile control

Instead of using `.env`, profiles can also be selected explicitly:

```bash
docker compose --profile web up -d --build
docker compose --profile recorder up -d --build
docker compose --profile full up -d --build
docker compose --profile web --profile ai up -d --build
docker compose --profile provider up -d model-provider
```

The `.env` approach is preferred for a server because the selected role and AI model are remembered.

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

For a recorder station, choose **Recorder station** in main Setup and use the
network scan in its Recorder step. Enter the private machine subnet, for
example `192.168.200.0/24`, and keep the MTConnect port at `5000`. MSH requests
`/probe` only from that bounded private range and proposes source keys from the
MTConnect UUID. The visible label includes Device name plus serial number or
UUID, so two Mazak machines remain distinct. Checked Agents and the role are
saved together.

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
- Ollama models are stored in the Docker volume `ollama_models`.
- Observer Phoenix credentials can be configured in the Flask UI at `/sources/observer-phoenix` when the web profile is active.
- The Parquet/DuckDB cache remains derived from JSONL and can be rebuilt from `/control`.
