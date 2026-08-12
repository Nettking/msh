# Quick start

Status: **current user guide**
Reviewed: **2026-08-12**

This is the normal supported path for starting FCP and getting a new device into a Federation.

The required first-run flow is:

```text
Start FCP
  -> create first administrator in browser
  -> sign in
  -> Identity
  -> Federation
  -> Inspect
  -> finish setup
  -> open Federation
```

Benchmarks and contribution choices are optional follow-up work. They are evidence/intent, not prerequisites for normal workbench access.

## Prerequisites

- Git.
- Docker Desktop on Windows, or Docker Engine + Docker Compose on Linux/macOS.
- Python 3 on Linux/macOS for the bounded host update agent.
- Internet access the first time the configured Ollama model must be downloaded.

## Windows

```cmd
git clone <repository-url> fcp
cd fcp
start.cmd
```

The launcher builds and starts the Federation relay, Ollama, Flask workbench, managed recorder, and the bounded host-owned update agent used by Federation-wide updates. Existing device/Federation state, evidence, recorder state, human accounts, data, and models are preserved during normal starts.

Open the URL printed by the launcher, normally:

```text
http://localhost:5000
```

## Linux or macOS

```bash
git clone <repository-url> fcp
cd fcp
bash start.sh
```

Open:

```text
http://localhost:5000
```

## Create the first human administrator

A fresh local FCP authority has no default username or password. You no longer need a separate Flask CLI command for the normal first-user path.

1. Start FCP.
2. Open the FCP web interface.
3. While the local human-user database is empty, FCP redirects normal browser requests to the dedicated first-user setup page at `/admin/users/bootstrap`.
4. Enter a valid email address.
5. Enter and confirm a password of at least **12 characters**.
6. Submit the form. The first account is created as an active `admin`.
7. Sign in and continue onboarding.

The anonymous bootstrap closes as soon as the first account commits. Additional accounts are created by an administrator at `/admin/users`.

A remotely paired Federation member with an empty local shadow-user database does **not** expose first-admin bootstrap. It uses Federation human sign-in instead.

See [Human users, sign-in, and permissions](human-authentication.md).

## Complete onboarding

After sign-in:

1. **Identity** — create or load the stable cryptographic identity for this FCP device.
2. **Federation** — reconnect, join, or create a Federation.
3. **Inspect** — record the device's supported local capability evidence.
4. **Finish setup** — open the normal workbench.
5. **Federation** — review devices, services, evidence, contributions, and administration state.

Inspection describes what the device can do. It does not itself grant contribution/provider authority.

## Pair another FCP device

The current Federation leader can generate a signed `FCP1-...` pairing code. Browser-generated codes are one-use and valid for up to **10 minutes**.

On the joining device, paste the code into the Federation onboarding step. Pairing grants Federation membership only after the signed code and existing Federation authority are validated.

When pairing across physical machines, the issuing FCP device must be reachable through a trusted LAN/VPN address rather than only `localhost`.

## Optional: discover the Federation through Tailscale

If both devices already use the same Tailscale tailnet, start the joining FCP device with:

### Windows

```cmd
start-tailscale.cmd
```

### Linux/macOS

```bash
bash start-tailscale.sh
```

FCP uses the already signed-in local Tailscale client to find reachable FCP Federations and show them during onboarding. It does not require a Tailscale API/auth key.

**Discovery is not membership.** After finding the Federation, you still create and redeem the normal `FCP1-...` pairing code.

See [Tailscale Federation discovery](tailscale_federation_discovery.md).

## Current Federation leader and failover

Federation creation provenance and operational leadership are separate:

- the original creator remains immutable provenance;
- the current operational leader is tracked by a monotonic leadership term;
- if the active leader remains offline beyond the bounded timeout and a connected successor exists, the coordinator can promote a deterministic successor;
- former leaders are fenced from current-leader controls after the transition.

Current-leader controls include Federation-wide update operations, leader capability requests, member/invitation administration, and reviewed provider-management actions.

This does **not** mean the coordinator database/relay itself has replicated quorum failover. If the machine holding that authoritative coordinator service is unavailable, automatic leader promotion cannot substitute for replicated coordinator availability.

See [Federation operations](federation_operations.md).

## Rename devices

- Open **Federation -> This device** to set this device's own Federation display name.
- The current leader can rename other Federation members from **Federation -> Devices**.

Display names are public Federation metadata only. They do not replace the stable cryptographic `node_id`, and they do not change membership or authority.

## Ask members to benchmark and contribute

The current Federation leader can ask currently reachable remote members to refresh capability inspection, run eligible locally registered benchmarks, and request locally allowed contributions.

The request does not send arbitrary commands or grant provider authority. Each member re-evaluates its own policy and any separate approval requirements.

See [Federation operations](federation_operations.md).

## Manual Federation-wide software updates

On the **current Federation leader**:

1. open **Federation**;
2. choose **Check for updates**;
3. review the exact approved target commit and every device result;
4. choose **Update all devices** only when you intend to activate that target; and
5. wait for terminal per-device results.

A device is shown as **Updated** only after its running runtime proves the exact requested commit.

The normal Windows/POSIX launchers start the required host-owned update agent automatically. Older installations may need the documented migration/bootstrap first.

## Reconnect after an update

Windows:

```cmd
start.cmd --resume
```

Linux/macOS:

```bash
bash start.sh --resume
```

Resume preserves identity, Federation membership, and accepted capability evidence. It does not rerun inspection or benchmarks merely because time passed.

## Migrate an older Windows checkout

```cmd
migrate.cmd
```

The migration path is intentionally fail-closed: it accepts only the approved repository/`main`, requires a clean fast-forwardable checkout, preserves data and Federation state, and refuses to reset/clean/delete volumes to guess its way through ambiguous state.

## Start as a fresh device

Windows:

```cmd
start.cmd --fresh
```

The launcher shows the reset boundary and requires typing `RESET`.

It removes device/Federation/onboarding/evidence state tied to that device identity while preserving human accounts/auth secrets, recorded/imported data, recorder checkpoints, analysis results, Docker images, and downloaded models.

## Standalone recorder

First join:

```bash
python start_recorder.py FCP1-...
```

Later starts:

```bash
python start_recorder.py
```

From any trusted Federation device, use **Federation -> Recorders** to request a recorder-local bounded scan and manage sources selected from that recorder's own latest scan.

See [Standalone recorder](standalone_recorder.md).

## Appearance

The top menu contains the light/dark appearance control. With no explicit browser choice, FCP follows the operating-system preference. An explicit choice is stored in that browser and applies across the workbench, onboarding, and documentation portal.

## Useful pages

- `/login` — human sign-in.
- `/admin/users` — human-user administration for admins.
- `/onboarding` — device/Federation onboarding and repair.
- `/federation` — Federation overview and current-leader controls.
- `/federation/recorders` — standalone-recorder administration.
- `/status` — diagnostics.
- `/docs` — current documentation.

## Trusted remote browser access

The normal launchers bind Flask to `127.0.0.1` by default. For trusted LAN/VPN access, set `FCP_WEB_BIND` before startup.

Windows PowerShell:

```powershell
$env:FCP_WEB_BIND = "0.0.0.0"
.\start.cmd
```

Linux/macOS:

```bash
export FCP_WEB_BIND=0.0.0.0
bash start.sh
```

Do not expose Flask, the relay, Ollama, or recorder-control surfaces directly to the public internet.

## More detail

- [Getting started](getting_started.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Federation operations](federation_operations.md)
- [Tailscale Federation discovery](tailscale_federation_discovery.md)
- [Server setup](server_setup.md)
- [Troubleshooting](troubleshooting.md)
