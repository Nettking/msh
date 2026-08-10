# Server setup and deployment

Status: **current administrator guide**
Reviewed: **2026-08-07**

FCP is designed to run as a persistent device that may host several independent capabilities. The normal product setup does not assign the device one permanent role.

The supported default deployment starts:

```text
FCP device
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
git clone <repository-url> fcp
cd fcp
start.cmd
```

`start.cmd`:

- verifies that Docker is available and running;
- resolves safe host paths, ports, and persistent volume names;
- builds the relay, Flask, and recorder images;
- starts the relay, Ollama, recorder, and Flask services;
- installs the configured Ollama model when it is missing;
- preserves existing device, capability-evidence, and data state;
- opens the workbench after readiness checks pass.

Windows web access is local-only by default through `127.0.0.1`. The launcher prints the resolved URL and may select a different host port if `5000` is occupied.

### Linux or macOS

From a fresh checkout:

```bash
git clone <repository-url> fcp
cd fcp
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

1. **Identity** creates or loads the stable identity of this FCP device.
2. **Federation** reconnects, joins, or creates a Federation through an authenticated path.
3. **Inspect** records and persists the device's supported local capabilities.
4. **Finish setup** opens the normal workbench after accepted inspection evidence exists.
5. **Federation** provides the read-only overview and entry points for optional follow-up work.

Benchmarks and contribution decisions are optional after setup. Completing onboarding does not automatically grant recorder, language-model, compute, or storage contribution authority.

Inspection and benchmark execution are explicit evidence-collection actions. The installed product reuses valid persisted evidence across ordinary starts and updates regardless of legacy time-only expiry metadata. Operators may explicitly run **Inspect again** or **Run again** after a relevant hardware, provider, model, service, or configuration change. Benchmark definition/version or declared dependency changes still require a new benchmark before that evidence can support contribution reconciliation.

The older role-first settings carrier remains in the repository for compatibility until its separately reviewed retirement work is accepted. It is not the canonical user-facing onboarding model.

## Starting an existing device

A normal start preserves all existing state:

```cmd
start.cmd
```

On Windows, use the explicit resume operation after an update when startup should reconnect the saved Federation and verify that persisted capability evidence is available before opening the workbench:

```cmd
start.cmd --resume
```

The resume operation is evidence-preserving:

- it retains the existing identity and Federation;
- it loads the saved inspection snapshot rather than running inspection;
- it loads saved benchmark results rather than executing benchmark probes;
- it does not change contribution authority in the one-shot resume process.

The long-running Flask app owns the single automatic contribution reconciliation. Persisted intent is reconciled only when the saved inspection is device-bound and the saved benchmark review still matches current benchmark identity, implementation version, and declared dependency inputs. If those structural inputs changed, enabled contribution intent is suspended through the existing fencing path rather than reactivated from stale evidence. Elapsed wall-clock time by itself does not trigger inspection, benchmarking, or contribution suspension.

`update.cmd` performs a fast-forward update and then invokes this resume path.

The underlying frozen inspection and benchmark contracts still contain finite `expires_at` metadata, and the strict low-level evaluator remains available for contract and safety tests. The installed FCP product uses run-once composition: age alone is not an automatic refresh trigger, including for evidence written by older releases with a short TTL. Old evidence is not rewritten to appear newer. Explicit refresh remains available from the product surface.

On Linux/macOS, ordinary `docker compose up -d` preserves the same mounted directories and named volumes. Inspection and benchmark reruns remain explicit actions from the product surface.

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
FCP_WEB_BIND=127.0.0.1
```

This is the safest normal setting when only the FCP computer needs the browser UI.

### Trusted LAN or VPN access

To allow another trusted computer to reach Flask, set the bind address before startup.

Windows Command Prompt:

```cmd
set FCP_WEB_BIND=0.0.0.0
start.cmd
```

PowerShell:

```powershell
$env:FCP_WEB_BIND = "0.0.0.0"
.\start.cmd
```

Linux/macOS `.env`:

```text
FCP_WEB_BIND=0.0.0.0
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

When pairing another physical device, open FCP using the reachable LAN or VPN address. Opening through `localhost` cannot advertise a relay address reachable from the other device.

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

Use configured `FCP_DATA_DIR`, `FCP_RESULTS_DIR`, and volume-name variables when the deployment must store state outside the repository defaults.

## Advanced deployment shapes

The following shapes remain useful for administration, development, and compatibility work. They are service selections, not permanent device identities, and they do not by themselves grant Federation authority.

### Default multi-capability device

```bash
docker compose up -d --build relay ollama recorder flask
```

### Flask without background orchestration

Set:

```text
FCP_SKIP_ORCHESTRATION=1
```

Then start the normal services. This is intended for inspection and debugging.

### Headless language-model provider

A Docker-capable laptop can contribute an Ollama endpoint without hosting another Flask workbench:

```bash
git clone <repository-url> fcp
cd fcp
docker compose --profile provider run --rm model-provider-install
```

This starts `model-provider` and installs `FCP_PROVIDER_MODEL`, which defaults to `smollm2:360m`, in the persistent `model_provider_models` volume.

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

See [Connected capabilities](connected_capabilities.md) for the authenticated FCP-side connection and contribution flow.

### One-shot preparation

```bash
docker compose --profile prep run --rm prep
```

### One-shot Observer Phoenix synchronization

```bash
docker compose --profile observer-sync run --rm observer-sync
```

### Compatibility setup helper

`setup_fcp.py` can still write `.env` values and prepare specialized deployment selections. Use it only when the default capability-first deployment is not appropriate.

Running the helper does not replace capability-first browser onboarding, establish Federation membership, or grant contribution authority.

## Ollama configuration

The default local model is selected through:

```text
FCP_AI_MODEL=llama3.2:3b
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
FCP_RECORDER_SOURCES=IG500=http://192.168.200.251:5000/current;VTC=http://192.168.200.252:5000/current
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
- [Capability-first Federation plan](implementation/federation/active/capability_first_federation_plan.md)
