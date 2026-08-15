# Quick start

Status: **current user guide**
Reviewed: **2026-08-12**

This guide describes the normal supported way to start FCP and complete capability-first onboarding.

An FCP device is not assigned one permanent product role during first setup. The required first-run flow is:

```text
Create first administrator in browser
  -> Human sign-in
  -> Identity
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
git clone <repository-url> fcp
cd fcp
start.cmd
```

The launcher builds and starts the current core services:

- Federation relay;
- Ollama;
- Flask workbench;
- managed recorder.

It checks that the configured Ollama model is installed and downloads it when necessary. Existing identity, Federation, inspection, benchmark, recorder, human-auth, model, and data state are preserved during normal starts. The launcher bakes the exact Git commit into the FCP runtime images and starts the bounded host-owned update agent used by **Federation -> Update all devices**.

The web interface is limited to the local FCP computer by default. Open:

```text
http://localhost:5000
```

On a fresh local authority, create the first human administrator in the browser before completing device onboarding. See **Create the first human administrator** below. After signing in, a device without completed onboarding uses:

```text
http://localhost:5000/onboarding
```

The actual web port may differ when port `5000` is already occupied. `start.cmd` prints the resolved address before opening the browser.

### Reconnect an existing setup after an update

Use the explicit resume path when you want startup to reconnect the saved Federation and verify that existing local capability evidence can be reused before opening the workbench:

```cmd
start.cmd --resume
```

Resume is evidence-preserving. It does **not** rerun device inspection or benchmarks, and it does not replace the device identity or create a new Federation.

`update.cmd` uses this resume path after a successful local fast-forward update. Federation-wide updates use the same saved-setup semantics through the host update agent, but additionally rebuild and verify the exact running image before reporting success.

### Migrate an older Windows installation

A Windows installation from before the current launcher/update-agent path can use:

```cmd
migrate.cmd
```

The migration is intentionally conservative. It validates the approved repository and `main`, requires a clean fast-forwardable checkout, preserves the existing data/results directories, human-auth state, and retained Federation relay state, then starts the current product through the supported resume path. It does not reset, clean, stash, rebase, delete volumes, or silently guess between ambiguous old relay-state volumes.

Use this migration only for an existing older checkout. New installations should use `start.cmd` directly.

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
- retained migration/setup state that belongs to the replaced device identity.

It preserves:

- human accounts and their authentication secrets;
- recorded telemetry and imported data;
- source configuration and recorder checkpoints;
- analysis and workflow results;
- Docker images;
- downloaded Ollama models.

After the reset, FCP verifies that authoritative device/Federation setup state is empty and opens capability-first onboarding after human sign-in.

## Linux or macOS: supported launcher

From a fresh checkout:

```bash
git clone <repository-url> fcp
cd fcp
bash start.sh
```

The POSIX launcher starts the same default core services, verifies the configured Ollama model, bakes the exact Git commit into the FCP images, and starts the single-instance host update agent. Normal starts preserve device, Federation, inspection, benchmark, recorder, human-auth, model, and data state.

To reconnect an existing saved Federation before opening the workbench:

```bash
bash start.sh --resume
```

After human sign-in, open:

```text
http://localhost:5000/onboarding
```

Inspection and benchmark execution remain explicit browser actions. Python 3 is required on the host only for the bounded update agent; FCP application dependencies still run in Docker.

### Manual Compose startup

Direct Compose remains useful for development and troubleshooting:

```bash
export FCP_BUILD_COMMIT="$(git rev-parse --verify HEAD^{commit})"
docker compose up -d --build relay ollama recorder flask
```

However, **Federation -> Update all devices** requires the host-owned update agent. If you deliberately bypass `start.sh`, run the agent separately from the same checkout and data directory:

```bash
python3 scripts/posix/fcp_update_agent.py \
  --repo-root "$PWD" \
  --data-directory "${FCP_DATA_DIR:-$PWD/data}"
```

The agent accepts only the bounded local handoff contract. It does not expose a network endpoint or accept arbitrary commands from Federation messages.

## Create the first human administrator

Human web accounts are separate from Federation node identities, pairing grants, and device credentials. Authentication is enabled by default.

On the first production start FCP atomically creates two independent random secrets under `data/auth/`: one for Flask sessions and one for password hashing. They are reused on later starts, so a normal restart does not invalidate sessions or password verification. Do not delete or rotate `data/auth/password-salt` unless you intentionally plan to reset existing human passwords.

A fresh local authority no longer needs a separate Flask CLI command for normal first-user setup:

1. start FCP;
2. open the FCP web interface;
3. while the local user database is empty, normal browser requests redirect to `/admin/users/bootstrap`;
4. enter a valid email address;
5. enter and confirm a password of at least **12 characters**; and
6. submit the form.

The first account is created as an active `admin`. The anonymous bootstrap closes as soon as that first account commits, and normal sign-in enforcement takes over.

A remotely paired Federation member with an empty local shadow-user database does **not** expose first-admin bootstrap. It uses Federation human sign-in instead.

Administrators manage additional human users at `/admin/users`. See [Human users, sign-in, and permissions](human-authentication.md) for roles, SSO, cookie/CSRF behavior, secret overrides, and deployment notes.

## Complete first-run onboarding

The mandatory setup is intentionally short:

1. **Human sign-in** — authenticate with a separate FCP human account.
2. **Identity** — create or load the stable identity for this FCP device.
3. **Federation** — reconnect, join, or create the user-facing Federation through an authenticated path.
4. **Inspect** — inspect the device's supported local capabilities and persist that evidence.
5. **Finish setup** — enable the normal workbench without granting optional contribution authority.
6. **Open Federation** — review connected devices and available capabilities.

Finishing setup does not automatically grant recorder, language-model, compute, or storage contribution authority. Optional benchmarks and contribution choices remain available from the Federation pages.

## Pair another FCP device

Generate a pairing code from the **current Federation leader**. Current browser-generated `FCP1-...` codes are signed, one-use, and valid for up to **10 minutes**. A fresh code can be generated whenever a previous attempt expired or another device needs to join.

When pairing across physical machines, open the issuing FCP installation using a LAN/VPN address reachable by the other device before generating the code.

See [Federation operations](federation_operations.md) for the trust and network boundaries.

### Optional Tailscale Federation discovery

If the joining host and an existing FCP host are already signed in to the same Tailscale tailnet, use:

Windows:

```cmd
start-tailscale.cmd
```

Linux/macOS:

```bash
bash start-tailscale.sh
```

The wrapper uses only the already authenticated local Tailscale client to find public-safe FCP Federation advertisements from online Tailscale IPv4 peers. FCP does not require a Tailscale API key or auth key.

A discovered Federation is **not** automatically joined. Use the discovered endpoint to locate the existing FCP installation, then obtain and redeem the normal signed one-use `FCP1-...` pairing code.

See [Tailscale Federation discovery](tailscale_federation_discovery.md).

## Current Federation leader and device names

Federation creator provenance and operational leadership are separate. The creator remains immutable history; current-leader product authority follows the coordinator-authored monotonic leadership term.

If the current leader remains offline beyond the bounded timeout and a valid connected successor exists, the coordinator can promote that successor and fence the former leader from current-leader controls. This does not provide replicated coordinator/quorum failover if the authoritative coordinator service itself is unavailable.

Use **Federation -> This device** to name the local device. The current leader can rename other members under **Federation -> Devices**. Display names never replace the stable cryptographic `node_id`.

Human password/credential authority remains creator-backed and does not automatically transfer with operational leadership.

## Standalone recorder: simplest first start

For a headless MTConnect recorder, generate the normal Federation pairing code on the current leader and run:

```bash
python start_recorder.py FCP1-...
```

On first configuration the recorder runs the existing bounded private-network scan by default, selects discovered MTConnect sources, joins the Federation, starts local loss-aware capture, and starts independent Federation publication/control workers.

After a successful first join, the code is no longer required:

```bash
python start_recorder.py
```

From any trusted Federation device, open:

```text
/federation/recorders
```

to request a scan on a connected standalone recorder and add/remove recorder sources. The scan executes on the recorder host, and remote additions can select only source IDs returned by that recorder's own latest bounded scan.

See [Standalone recorder](standalone_recorder.md) for details.

### Windows recorder over Tailscale

For a headless recorder on Windows, sign the host in
to the same Tailscale tailnet, generate the pairing code while the current
leader is open through its numeric Tailscale `100.x.y.z` address, and run:

```cmd
start-tailscale-recorder.cmd --storage-group fcp-local-storage
```

Paste the fresh `FCP1-...` code into the hidden prompt on first start. The
launcher requires Tailscale, Federation membership, and a usable recorder
publication route; it never silently falls back to local-only capture. Later
starts reuse the saved membership. See
[Standalone recorder](standalone_recorder.md#maskin-4-over-tailscale-windows)
for network, storage-group, JSONL-sharing, and verification details.

## Ask members to benchmark and contribute

The **current Federation leader** can ask all currently reachable remote members to refresh local capability inspection, run eligible locally registered benchmarks, and request contribution for candidates allowed by each member's own local policy.

The request does not grant remote shell/provider authority and cannot inject executable commands, paths, credentials, or arbitrary benchmark/provider definitions. Capabilities that require separate provider review may remain registering/pending until the current leader reviews them through the existing provider-enrollment surface.

Offline members are not queued. Issue another request after they reconnect.

See [Federation operations](federation_operations.md).

## Manual Federation-wide software updates

On the **current Federation leader**:

1. open **Federation**;
2. press **Check for updates**;
3. review every device result and the exact approved target commit; and
4. press **Update all devices** only when the rollout should proceed.

The operation is manual by design. A successful device eventually shows **Updated** with the green success indicator only after its running runtime proves the exact requested commit.

The supported `start.cmd` and `start.sh` launchers start the required host-owned update agent. Legacy devices must be manually bootstrapped onto an updater-capable version before Federation-wide updates can manage them.

The Compose-managed recorder on a normal FCP installation is rebuilt/restarted with that FCP device. A separate host process launched with `python start_recorder.py` is a standalone recorder node and is **not** itself a host-update agent; update its checkout/restart that process through its host administration path.

See [Federation operations](federation_operations.md).

## First pages to open

- <http://localhost:5000/login> — sign in with a human FCP account.
- <http://localhost:5000/admin/users> — administer human users when signed in as an admin.
- <http://localhost:5000/onboarding> — complete or repair capability-first onboarding.
- <http://localhost:5000/federation> — inspect the Federation, devices, capabilities, updates, benchmarks, and contribution state.
- <http://localhost:5000/federation/recorders> — manage connected standalone recorder scans and source selections.
- <http://localhost:5000/status> — verify discovery, bootstrap, catch-up, failures, and readiness.
- <http://localhost:5000/control> — select datasets, refresh data, and run workflows or scripts.
- <http://localhost:5000/operator-strategies> — capture structured operator decisions.
- <http://localhost:5000/sources/observer-phoenix> — configure and test Observer Phoenix access.
- <http://localhost:5000/ai> — ask read-only system-understanding questions.
- <http://localhost:5000/playback> — inspect playback-ready exports.
- <http://localhost:5000/analyses> — browse discovered CSV and JSON artifacts.
- <http://localhost:5000/docs> — browse repository documentation.

## Appearance

Use the light/dark switch in the top menu. With no explicit browser choice, FCP follows the operating-system preference. An explicit choice is stored in that browser and applied before first paint across the normal shell, onboarding, sign-in/account pages, and docs portal.

## Access from another trusted device

`start.cmd` and `start.sh` bind the Flask workbench to `127.0.0.1` by default. To allow access from another computer on a trusted LAN or VPN, set the bind address before starting.

From Windows Command Prompt:

```cmd
set FCP_WEB_BIND=0.0.0.0
start.cmd
```

From PowerShell:

```powershell
$env:FCP_WEB_BIND = "0.0.0.0"
.\start.cmd
```

From Linux/macOS:

```bash
export FCP_WEB_BIND=0.0.0.0
bash start.sh
```

Then open:

```text
http://<server-ip>:5000
```

Use the reachable LAN or VPN address when pairing another physical FCP device so the pairing material can advertise a reachable relay address. Do not expose Flask, the relay, or Ollama directly to the public internet.

See [Server setup](server_setup.md) for network, deployment, recorder, migration, and provider details.

## Advanced deployment profiles

`setup_fcp.py` and Compose profiles remain available for explicit local process composition such as a workbench, web-only process, recorder process, provider, preparation job, or Observer synchronization. Old mode names may remain accepted as compatibility aliases, but they do not define a permanent device role or grant Federation contribution authority.

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
- `FCP_FLASK_SECRET` — optional explicit session secret; production automatically persists one when omitted.
- `FCP_PASSWORD_SALT` — optional explicit password-hashing salt; production automatically persists a separate one when omitted.
- `FCP_AUTH_DATABASE` — human account database path; defaults to `data/auth/users.sqlite3`.
- `FCP_AUTH_SECRET_DIR` — persistent human-auth secret directory; defaults to `data/auth`.
- `FCP_HTTPS=1` — marks session and remember-me cookies Secure when HTTPS is actually enforced.
- `FCP_SKIP_ORCHESTRATION=1` — starts Flask without the background runtime.
- `FCP_SCAN_DIRS` — comma-separated artifact scan roots.
- `FCP_AI_MODEL` — selected Ollama model.
- `FCP_FEDERATED_JSONL_PUBLISH_UPLOADS=0` — withhold browser-uploaded JSONL from generic Federation publication on this installation.

Direct Flask startup does not provide the host-owned Docker activation boundary required by Federation-wide software updates.
