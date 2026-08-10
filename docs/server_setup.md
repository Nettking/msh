# Server setup and deployment

Status: **current administrator guide**
Reviewed: **2026-08-11**

FCP is designed to run as a persistent device that may host several independent capabilities. The normal product setup does not assign the device one permanent role.

The supported default deployment starts:

```text
FCP device
  -> Federation relay
  -> Flask workbench and runtime
  -> managed recorder
  -> Ollama service
  -> bounded host update agent
  -> persistent data, results, model, and Federation state
```

A browser on the same computer, or on another trusted LAN/VPN device when explicitly enabled, connects to the Flask workbench.

Do not expose Flask, the Federation relay, Ollama, or recorder control directly to the public internet. Use a trusted private network, VPN, or an authenticated HTTPS reverse proxy.

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
- resolves the exact clean Git commit and bakes it into runtime images;
- builds the relay, Flask, and managed recorder images;
- starts relay, Ollama, recorder, and Flask;
- installs the configured Ollama model when it is missing;
- preserves existing device, capability-evidence, recorder, model, and data state;
- starts one bounded host-owned update agent; and
- opens the workbench after readiness checks pass.

Windows web access is local-only by default through `127.0.0.1`. The launcher prints the resolved URL and may select a different host port if `5000` is occupied.

### Linux or macOS

From a fresh checkout:

```bash
git clone <repository-url> fcp
cd fcp
bash start.sh
```

The POSIX launcher provides the same normal product composition, exact-build provenance, model verification, persistent state handling, and bounded host update agent.

Direct Compose remains available for development/troubleshooting:

```bash
export FCP_BUILD_COMMIT="$(git rev-parse --verify HEAD^{commit})"
docker compose up -d --build relay ollama recorder flask
```

If you bypass `start.sh`, Federation-wide updates will not have their normal host-owned activation boundary unless you start the update agent separately.

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
5. **Federation** provides the product overview and reviewed action surfaces for optional follow-up work.

Benchmarks and contribution decisions are optional after setup. Completing onboarding does not automatically grant recorder, language-model, compute, or storage contribution authority.

The installed product is capability-first. The former role-first runtime path is retired from normal operation. Retained legacy setup state, command aliases, and migration readers exist only where an upgraded installation still needs deterministic migration input; they do not define current product authority.

## Pairing another physical device

Generate a signed `FCP1-...` code from the current Federation. Browser-generated codes are one-use and valid for up to 10 minutes. A new code can be generated whenever another attempt is required.

Before generating the code, access the issuing FCP installation through a LAN/VPN address that the joining machine can reach. A `localhost` relay address is not reachable from another physical device.

After pairing, the joining installation persists a stable identity and public-safe reconnect binding. The pairing code itself is not a persistent credential.

See [Federation operations](federation_operations.md).

## Starting an existing device

A normal start preserves all existing state:

```cmd
start.cmd
```

On Windows, use the explicit resume operation after an update or migration when startup should reconnect the saved Federation and verify persisted capability evidence before opening the workbench:

```cmd
start.cmd --resume
```

The resume operation is evidence-preserving:

- it retains the existing identity and Federation;
- it loads the saved inspection snapshot rather than running inspection;
- it loads saved benchmark results rather than executing benchmark probes; and
- it leaves contribution authority to the long-running capability-first runtime reconciliation path.

`update.cmd` performs a safe approved-main fast-forward and then invokes this resume behavior.

Normal product composition treats inspection and benchmark evidence as run-once evidence until a relevant structural dependency changes or an operator explicitly reruns it. Old evidence is not rewritten to appear newer merely because time passed.

## Migrate an older Windows installation

Use the one-shot migration entry point when an older Windows checkout predates the current launcher/update-agent and retained-volume selection logic:

```cmd
migrate.cmd
```

The migration bootstrap:

- accepts only the approved `Nettking/msh` repository and `main`;
- requires a clean checkout and fast-forward relationship;
- preflights Docker/Compose before mutating Git;
- preserves the current data/results paths and device/Federation state;
- identifies the retained relay-state volume conservatively, using read-only inspection and the saved device identity when necessary;
- refuses to guess when multiple retained volumes remain ambiguous;
- uses only `git merge --ff-only` for source mutation;
- normalizes the supported Compose project and starts `start.cmd --resume`; and
- installs/starts the current host update agent for future Federation-wide rollouts.

It does not use `git reset`, `git clean`, stash, rebase, branch switching, `down -v`, or state deletion.

## Fresh-device reset

On Windows:

```cmd
start.cmd --fresh
```

The launcher shows the exact reset boundary and requires typing `RESET`.

It removes device identity, Federation membership and pairing state, onboarding progress, inspection and benchmark state, local relay authority state, and retained migration/setup state tied to the replaced device identity.

It intentionally preserves:

- recorded and imported data;
- source configuration;
- recorder checkpoints;
- workflow and analysis results;
- Docker images;
- downloaded Ollama and provider models.

Do not manually delete only part of the Federation state. Partial deletion can leave contradictory identity, membership, or authority records.

## Federation-wide software updates

The Federation coordinator/session creator can run **Check for updates** and then explicitly choose **Update all devices**.

The normal update path is manual. Each normal FCP installation's host update agent independently validates the exact approved commit, clean working tree, approved repository/branch, and fast-forward relationship before changing source or Docker runtime.

For an eligible FCP device, activation rebuilds `relay`, `flask`, and the Compose-managed `recorder`, resumes the saved installation, and reports success only after the running build commit matches the target and the required services are running.

The UI displays that successful `runtime_verified` state as **Updated** with a green indicator.

A standalone recorder launched directly with `python start_recorder.py` is not a host update agent and is not restarted by the normal Compose runtime activation. Administer that standalone checkout/process explicitly until a dedicated standalone-recorder update path exists.

See [Federation operations](federation_operations.md) for failure semantics and bootstrap requirements.

## Network access

### Local-only default

The supported launchers default Flask to:

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

Linux/macOS:

```bash
export FCP_WEB_BIND=0.0.0.0
bash start.sh
```

Then open:

```text
http://<server-ip>:5000
```

Find the address with `hostname -I` on Linux or `ipconfig` on Windows.

If access fails, verify:

- both devices are on the same trusted LAN or VPN;
- Docker is running;
- the `flask` and `relay` services are healthy;
- the selected host port is allowed by the firewall; and
- the URL uses the port printed by the launcher when a fallback port was selected.

## Persistent state

The default deployment persists:

| State | Default location |
| --- | --- |
| Recorded/imported data, capability/source config, recorder checkpoints, device and pairing state | `data/` |
| Workflow and analysis results | `results/` |
| Federation coordinator authority database | retained relay-state Docker volume |
| Local Ollama models | Ollama model Docker volume |
| Headless provider models | model-provider Docker volume |
| Optional environment configuration | `.env` |

`.env` is local and ignored by git.

Use configured `FCP_DATA_DIR`, `FCP_RESULTS_DIR`, and volume-name variables when the deployment must store state outside the repository defaults.

## Standalone MTConnect recorder

The standalone recorder is the simplest way to place loss-aware capture on a machine that does not need another Flask workbench.

### First join and automatic discovery

Generate a normal `FCP1-...` pairing code and run:

```bash
python start_recorder.py FCP1-...
```

On first configuration the launcher attempts the existing bounded private-network MTConnect scan, auto-selects discovered sources, creates/reuses a stable recorder identity, joins the Federation, starts recording, and starts independent publication/control workers.

After successful pairing, later starts can be:

```bash
python start_recorder.py
```

The source selection persists. A deliberately emptied source set is not automatically repopulated on restart.

Use an explicit scan network when needed:

```bash
python start_recorder.py FCP1-... --scan-cidr 192.168.200.0/24
```

or explicit sources for a controlled deployment:

```bash
python start_recorder.py Mazak=http://192.168.200.249:5000
```

### Remote recorder administration

From any trusted FCP device in the same Federation, open:

```text
/federation/recorders
```

Select a connected standalone recorder to request a bounded scan or add/remove sources.

The scan executes on the recorder host. Remote additions can select only opaque source IDs returned by the recorder's latest local scan; they cannot inject arbitrary URLs or credentials. Source removals affect future capture and do not delete existing telemetry/checkpoints.

### Local-first publication

Recorder capture commits locally before Federation delivery. Checkpoint-committed data is reconciled into a durable publication outbox and sent through the Federation owner's logical-storage authority. Relay/storage outages do not block MTConnect polling; the backlog retries later.

See [Standalone recorder](standalone_recorder.md) and [`catalog/standalone-recorder_v2/README.md`](../catalog/standalone-recorder_v2/README.md).

## Advanced deployment shapes

The following shapes remain useful for administration and development. They are local process selections, not permanent product identities and not contribution authority.

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

A Docker-capable laptop can provision a separate Ollama endpoint:

```bash
docker compose --profile provider run --rm model-provider-install
docker compose --profile provider up -d model-provider
```

Port `11434` is published for the provider profile. Restrict it to a trusted LAN or VPN. The process selection alone does not grant Federation contribution authority.

### One-shot preparation

```bash
docker compose --profile prep run --rm prep
```

### One-shot Observer Phoenix synchronization

```bash
docker compose --profile observer-sync run --rm observer-sync
```

### Command/bootstrap compatibility aliases

`setup_fcp.py` may still accept older deployment-mode spellings for compatibility. They normalize to role-free command profiles that select local processes only. They do not persist a permanent device role and cannot enable recorder, AI, compute, storage, job, or artifact authority.

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

## Managed recorder configuration

The Compose-managed recorder shares the current capability configuration and writes normalized JSONL under the recorder data tree. Recorder configuration and recorder contribution authority remain separate.

The recorder state is stored under:

```text
data/source_state/
```

and preserves sequence tracking across normal restarts.

The managed recorder is rebuilt/restarted when its containing normal FCP device completes a verified Federation software update.

## Common commands

Start the supported product:

```cmd
start.cmd
```

or:

```bash
bash start.sh
```

Inspect service state:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f
```

Stop containers while preserving mounted data and named volumes:

```bash
docker compose down
```

On Windows, perform a supported local update with:

```cmd
update.cmd
```

For a multi-device Federation, prefer the coordinator-owned **Check for updates** -> **Update all devices** flow once every participating normal FCP device has the current update agent.

## Related documentation

- [Quick start](quick_start.md)
- [Federation operations](federation_operations.md)
- [Standalone recorder](standalone_recorder.md)
- [Connected capabilities](connected_capabilities.md)
- [Troubleshooting](troubleshooting.md)
- [Federated session network](federated_session_network.md)
- [Capability-first Federation plan](implementation/federation/active/capability_first_federation_plan.md)
