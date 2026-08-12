# Server setup and deployment

Status: **current administrator guide**
Reviewed: **2026-08-12**

FCP runs as a persistent device that may host several independent capabilities. The normal product does not assign a permanent device role.

The supported default deployment starts:

```text
FCP device
  -> Federation relay
  -> Flask workbench/runtime
  -> managed recorder
  -> Ollama
  -> bounded host update agent
  -> persistent data/results/model/Federation/auth state
```

Do not expose Flask, the Federation relay, Ollama, or recorder control directly to the public internet. Use a trusted LAN/VPN, Tailscale, or an authenticated HTTPS deployment boundary.

## Normal installation

### Windows

```cmd
git clone <repository-url> fcp
cd fcp
start.cmd
```

### Linux/macOS

```bash
git clone <repository-url> fcp
cd fcp
bash start.sh
```

The supported launchers:

- verify required host tooling;
- resolve a clean immutable Git commit and bake it into runtime images;
- start relay, Flask, recorder, and Ollama;
- ensure the configured Ollama model is available;
- preserve existing device/Federation/evidence/recorder/data/auth/model state; and
- start the bounded host update agent used by Federation-wide software updates.

The normal browser bind is `127.0.0.1` unless explicitly changed.

## First human administrator

A fresh local authority has no default human account.

Start FCP and open the web interface. While the local human-user database is empty, FCP redirects normal browser requests to:

```text
/admin/users/bootstrap
```

Create the first administrator there with a valid email address and a confirmed password of at least 12 characters. The one-time anonymous bootstrap closes after the first account commits.

Additional users are managed at `/admin/users`.

A remotely paired member with no local shadow users does not reopen local first-admin bootstrap; it uses Federation SSO.

See [Human users, sign-in, and permissions](human-authentication.md).

## Capability-first browser setup

After human sign-in:

```text
Identity
  -> Federation
  -> Inspect
  -> finish setup
```

Inspection evidence is sufficient for the required onboarding path. Benchmarks and contribution choices are optional follow-up work and do not automatically grant provider/storage authority.

## Pair another device

The current Federation leader creates a signed one-use `FCP1-...` pairing code, valid for up to 10 minutes. The joining device redeems it in onboarding.

Use a browser/LAN/VPN address that the joining device can actually reach before generating the code.

### Optional Tailscale discovery

If both hosts already use the same Tailscale tailnet, start the joining FCP host with:

```cmd
start-tailscale.cmd
```

or:

```bash
bash start-tailscale.sh
```

The wrapper uses only the already signed-in local Tailscale CLI, writes a bounded discovery snapshot under `data/federation/onboarding/`, and then delegates to the normal supported launcher.

It does not require a Tailscale API/auth key. Discovery does not grant Federation membership; the normal `FCP1-...` pairing code is still required.

See [Tailscale Federation discovery](tailscale_federation_discovery.md).

## Current leader and failover

The immutable Federation creator and current operational leader are separate.

The coordinator tracks a monotonic leadership term. When the active leader remains offline beyond the bounded timeout and a valid connected successor exists, the coordinator can promote a deterministic successor. Former leaders are fenced from current-leader controls after the transition.

This mechanism requires the authoritative coordinator/relay service to remain available. It does not provide replicated coordinator/quorum failover if that service's host disappears.

Human credential/password authority remains creator-backed and does not automatically move with operational leadership.

## Starting an existing device

Normal start preserves state:

```cmd
start.cmd
```

or:

```bash
bash start.sh
```

Use explicit resume after an update/migration when you want to reconnect saved Federation state before opening the workbench:

```cmd
start.cmd --resume
```

```bash
bash start.sh --resume
```

Resume preserves identity and accepted inspection/benchmark evidence rather than rerunning probes merely because time elapsed.

## Migrate an older Windows installation

```cmd
migrate.cmd
```

The migration path is conservative:

- approved repository and `main` only;
- clean fast-forwardable checkout;
- Docker/Compose preflight;
- retained data/results/device/Federation/auth state preserved;
- retained relay-state volume identified conservatively;
- no reset/clean/stash/rebase/volume deletion; and
- current launcher/update-agent installed for future managed updates.

If retained state is ambiguous, migration fails closed rather than guessing.

## Fresh-device reset

Windows:

```cmd
start.cmd --fresh
```

The launcher requires explicit `RESET` confirmation.

It removes device/Federation/onboarding/evidence state tied to the device identity. It intentionally preserves human accounts/auth secrets, recorded/imported data, source configuration, recorder checkpoints, analyses, Docker images, and downloaded models.

## Federation-wide updates

The **current operational leader** runs **Check for updates** and then **Update all devices**.

Each normal host independently validates the exact approved `main` target, clean checkout, approved remote/branch, and fast-forward relationship. Success is reported only after the running runtime proves the exact requested commit.

A standalone recorder launched directly with `python start_recorder.py` is not a normal host update agent and needs its own checkout/process administration path.

## Network access

### Local-only default

```text
FCP_WEB_BIND=127.0.0.1
```

### Trusted LAN/VPN

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

Then open the actual host address/port printed by the launcher.

### Tailscale-specific bind

`start-tailscale.cmd` / `start-tailscale.sh` bind FCP to the host's Tailscale IPv4 address and perform the optional pre-start Federation discovery scan.

## Persistent state

| State | Default location |
| --- | --- |
| Data, capability/source config, checkpoints, device/pairing/onboarding state | `data/` |
| Human accounts and auth secrets | `data/auth/` |
| Workflow/analysis results | `results/` |
| Federation coordinator authority database | retained relay-state Docker volume |
| Ollama/provider models | named Docker volumes |
| Optional environment settings | `.env` |

Do not delete individual state files/volumes as a shortcut for troubleshooting.

## Federation-visible JSONL deployment controls

Supported non-recorder `data/**/*.jsonl` can be shared through Federation logical storage.

Advanced controls include:

```text
FCP_FEDERATED_JSONL_PUBLISH_UPLOADS=0
```

Use that to withhold browser-uploaded JSONL from generic Federation publication on a specific installation.

The materialized generic mirror is bounded by:

```text
FCP_FEDERATED_JSONL_MAX_MIRROR_BYTES
```

Recorder telemetry uses its separate manifest/checkpoint mirror path.

## Standalone MTConnect recorder

First join:

```bash
python start_recorder.py FCP1-...
```

Later start:

```bash
python start_recorder.py
```

Use `/federation/recorders` from a trusted Federation device for bounded recorder-local scan/source administration.

## Headless language-model provider

A Docker-capable device can provision the provider profile:

```bash
docker compose --profile provider run --rm model-provider-install
docker compose --profile provider up -d model-provider
```

Keep port `11434` on a trusted LAN/VPN. Running the process does not itself grant Federation contribution authority; provider enrollment/contribution policy remains separate.

## Common commands

Inspect services:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f
```

Stop containers while preserving state:

```bash
docker compose down
```

## Related documentation

- [Quick start](quick_start.md)
- [Federation operations](federation_operations.md)
- [Tailscale Federation discovery](tailscale_federation_discovery.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Standalone recorder](standalone_recorder.md)
- [Connected capabilities](connected_capabilities.md)
- [Troubleshooting](troubleshooting.md)
