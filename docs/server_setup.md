# Server setup and deployment

Status: **current administrator guide**  
Reviewed: **2026-08-06**

MSH is designed to run as a persistent device that may host several independent capabilities. The normal product setup does not assign the device one permanent role.

The supported default deployment starts:

```text
MSH device
  -> Federation relay
  -> Flask workbench and runtime
  -> managed recorder
  -> Ollama service
  -> persistent data, results, model, and Federation state
```

A browser on the same computer, or on another trusted LAN/VPN device when explicitly enabled, connects to the Flask workbench.

Do not expose Flask, the Federation relay, or Ollama directly to the public internet. Use a trusted private network, VPN, or an authenticated HTTPS reverse proxy.

## Normal first installation

### Windows

Install Docker Desktop, then run:

```cmd
git clone https://github.com/Nettking/msh.git
cd msh
start.cmd
```

`start.cmd`:

- verifies that Docker is available and running;
- resolves safe host paths, ports, and persistent volume names;
- builds the relay, Flask, and recorder images;
- starts the relay, Ollama, recorder, and Flask services;
- installs the configured Ollama model when it is missing;
- preserves existing device and data state;
- opens the workbench after readiness checks pass.

Windows web access is local-only by default through `127.0.0.1`. The launcher prints the resolved URL and may select a different host port if `5000` is occupied.

### Linux or macOS

From a fresh checkout:

```bash
git clone https://github.com/Nettking/msh.git
cd msh
docker compose up -d --build relay ollama recorder flask
```

Install the configured model when it is not already present:

```bash
docker compose --profile model-install run --rm ollama-pull
```

Open:

```text
http://localhost:5000/onboarding
```

## Capability-first browser setup

The mandatory first-run flow is:

```text
Identity
  -> Federation
  -> Inspect
  -> finish setup
  -> open Federation
```

The steps have these boundaries:

1. **Identity** creates or loads the stable identity of this MSH device.
2. **Federation** reconnects, joins, or creates a Federation through an authenticated path.
3. **Inspect** records the device's supported local capabilities.
4. **Finish setup** opens the normal workbench after a current inspection.
5. **Federation** provides the read-only overview and entry points for optional follow-up work.

Benchmarks and contribution decisions are optional after setup. Completing onboarding does not automatically grant recorder, language-model, compute, or storage contribution authority.

The older role-first settings carrier remains in the repository for compatibility until its separately reviewed retirement work is accepted. It is not the canonical user-facing onboarding model.

## Starting an existing device

A normal start preserves all existing state:

```cmd
start.cmd
```

On Windows, use the explicit resume operation when startup should reconnect the saved Federation, refresh inspection, run the benchmark plan, and reconcile saved contribution intent before opening the workbench:

```cmd
start.cmd --resume
```

The resume operation retains the existing identity and Federation. Depending on the result, it opens the Federation overview, benchmark review, guided repair, or onboarding page.

On Linux/macOS, ordinary `docker compose up -d` preserves the same mounted directories and named volumes. Capability refresh and reconciliation remain available from the Federation product surface.

## Fresh-device reset

On Windows:

```cmd
start.cmd --fresh
```

The launcher shows the exact reset boundary and requires typing `RESET`.

It removes device identity, Federation membership and pairing state, onboarding progress, inspection and benchmark state, local relay authority state, and retained legacy setup choices.

It intentionally preserves:

- recorded and imported data;
- source configuration;
- recorder checkpoints;
- workflow and analysis results;
- Docker images;
- downloaded Ollama and provider models.

Do not manually delete only part of the Federation state. Partial deletion can leave contradictory identity, membership, or authority records.

## Network access

### Local-only Windows default

`start.cmd` defaults to:

```text
MSH_WEB_BIND=127.0.0.1
```

This is the safest normal setting when only the MSH computer needs the browser UI.

### Trusted LAN or VPN access

To allow another trusted computer to reach Flask, set the bind address before startup.

Windows Command Prompt:

```cmd
set MSH_WEB_BIND=0.0.0.0
start.cmd
```

PowerShell:

```powershell
$env:MSH_WEB_BIND = "0.0.0.0"
.\start.cmd
```

Linux/macOS `.env`:

```text
MSH_WEB_BIND=0.0.0.0
```

Then open:

```text
http://<server-ip>:5000
```

Find the address with:

```bash
hostname -I
```

or on Windows:

```powershell
ipconfig
```

When pairing another physical device, open MSH using the reachable LAN or VPN address. Opening through `localhost` cannot advertise a relay address reachable from the other device.

If access fails, verify:

- both devices are on the same trusted LAN or VPN;
- Docker is running;
- the `flask` and `relay` services are healthy;
- the selected host port is allowed by the firewall;
- the URL uses the port printed by `start.cmd` when a fallback port was selected.

## Persistent state

The default Compose deployment persists:

| State | Default location |
| --- | --- |
| Recorded/imported data, device state, onboarding state, recorder settings | `data/` |
| Workflow and analysis results | `results/` |
| Federation coordinator authority database | `relay_state` Docker volume |
| Local Ollama models | `ollama_models` Docker volume |
| Headless provider models | `model_provider_models` Docker volume |
| Optional environment configuration | `.env` |

`.env` is local and ignored by git.

Use configured `MSH_DATA_DIR`, `MSH_RESULTS_DIR`, and volume-name variables when the deployment must store state outside the repository defaults.

## Advanced deployment shapes

The following shapes remain useful for administration, development, and compatibility work. They are service selections, not permanent device identities, and they do not by themselves grant Federation authority.

### Default multi-capability device

```bash
docker compose up -d --build relay ollama recorder flask
```

### Flask without background orchestration

Set:

```text
MSH_SKIP_ORCHESTRATION=1
```

Then start the normal services. This is intended for inspection and debugging.

### Headless language-model provider

A Docker-capable laptop can contribute an Ollama endpoint without hosting another Flask workbench:

```bash
git clone https://github.com/Nettking/msh.git
cd msh
docker compose --profile provider run --rm model-provider-install
```

This starts `model-provider` and installs `MSH_PROVIDER_MODEL`, which defaults to `smollm2:360m`, in the persistent `model_provider_models` volume.

Subsequent starts do not need to reinstall the model:

```bash
docker compose --profile provider up -d model-provider
```

Verify:

```bash
docker compose --profile provider ps model-provider
```

Stop:

```bash
docker compose --profile provider down
```

Port `11434` is published for the provider profile. Restrict it to a trusted LAN or VPN.

See [Connected capabilities](connected_capabilities.md) for the authenticated MSH-side connection and contribution flow.

### One-shot preparation

```bash
docker compose --profile prep run --rm prep
```

### One-shot Observer Phoenix synchronization

```bash
docker compose --profile observer-sync run --rm observer-sync
```

### Compatibility setup helper

`setup_msh.py` can still write `.env` values and prepare specialized deployment selections. Use it only when the default capability-first deployment is not appropriate.

Running the helper does not replace capability-first browser onboarding, establish Federation membership, or grant contribution authority.

## Ollama configuration

The default local model is selected through:

```text
MSH_AI_MODEL=llama3.2:3b
OLLAMA_BASE_URL=http://ollama:11434
```

Install or retry the configured local model with:

```bash
docker compose up -d ollama
docker compose --profile model-install run --rm ollama-pull
```

The local Ollama service and the headless provider use separate persistent model volumes.

## Recorder configuration

The managed recorder polls configured MTConnect `/current` endpoints and writes normalized JSONL under:

```text
data/sources/mtconnect_recorder/jsonl/<machine>/<YYYY-MM-DD>.jsonl
```

Recorder source configuration uses entries such as:

```text
IG500=http://192.168.200.251:5000/current;VTC=http://192.168.200.252:5000/current
```

The equivalent environment value is:

```text
MSH_RECORDER_SOURCES=IG500=http://192.168.200.251:5000/current;VTC=http://192.168.200.252:5000/current
```

The recorder state is stored at:

```text
data/source_state/mtconnect_recorder_state.json
```

This preserves sequence tracking across restarts.

Use the bounded private-subnet scan in the recorder UI when discovering MTConnect Agents. The scan requests `/probe` only within the configured private range and proposes stable source keys from MTConnect identity data.

## Common commands

Start or update the default services:

```bash
docker compose up -d --build relay ollama recorder flask
```

Inspect service state:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f
```

Restart services:

```bash
docker compose restart
```

Stop containers while preserving mounted data and named volumes:

```bash
docker compose down
```

Update the checkout and rebuild:

```bash
git pull
docker compose up -d --build relay ollama recorder flask
```

## Related documentation

- [Quick start](quick_start.md)
- [Connected capabilities](connected_capabilities.md)
- [Troubleshooting](troubleshooting.md)
- [Federated session network](federated_session_network.md)
- [Capability-first Federation plan](implementation/capability_first_federation_plan.md)
