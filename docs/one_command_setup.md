# One-command setup

Status: **current startup guide**
Reviewed: **2026-08-12**

This page describes the supported default startup. A device is capability-first and does not receive one permanent product role.

## Normal FCP device

### Windows

```cmd
start.cmd
```

### Linux or macOS

```bash
bash start.sh
```

The supported launcher starts the normal core services, preserves device/Federation/capability/data/auth state, verifies the configured Ollama model, bakes the exact Git commit into the runtime, and starts the bounded host update agent.

## First browser use

Open the URL printed by the launcher, normally:

```text
http://localhost:5000
```

A fresh local authority with zero human users redirects to `/admin/users/bootstrap`. Create the first active administrator there with a valid email address and a confirmed password of at least 12 characters, then sign in.

After sign-in, incomplete devices follow:

```text
Identity
  -> Federation
  -> Inspect
  -> finish setup
```

Benchmarks and contribution choices are optional follow-up work.

## Existing device

Windows:

```cmd
start.cmd --resume
```

Linux/macOS:

```bash
bash start.sh --resume
```

Resume reconnects the saved Federation and reuses persisted capability evidence without replacing identity or rerunning inspection/benchmarks merely because time passed.

## Fresh-device reset

Windows:

```cmd
start.cmd --fresh
```

After explicit confirmation, this removes local device/Federation onboarding/evidence state while preserving human accounts/auth secrets, recorded data, source configuration, recorder checkpoints, analyses, Docker images, and downloaded models.

## Older Windows installation

```cmd
migrate.cmd
```

The one-shot migration bootstrap preserves existing state and fails closed rather than resetting/guessing when the old installation cannot be identified safely.

## Optional Tailscale discovery

If an existing Federation is already reachable through the same Tailscale tailnet:

### Windows

```cmd
start-tailscale.cmd
```

### Linux/macOS

```bash
bash start-tailscale.sh
```

This uses the already signed-in local Tailscale client to find reachable FCP Federations before normal startup. No Tailscale API/auth key is required.

Discovery only helps locate the existing FCP endpoint. The joining device must still redeem a signed one-use `FCP1-...` pairing code.

See [Tailscale Federation discovery](tailscale_federation_discovery.md).

## Standalone recorder

First join:

```bash
python start_recorder.py FCP1-...
```

Later starts:

```bash
python start_recorder.py
```

Any trusted Federation device can use `/federation/recorders` for bounded recorder-local scan/source control.

## Federation-wide updates

The **current operational leader** can run **Check for updates** and then **Update all devices**. Normal FCP devices started with the supported launcher have the required host update agent.

A standalone `python start_recorder.py` process is not restarted by the normal Compose update agent and currently needs its own host process/update path.

## Related guides

- [Quick start](quick_start.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Federation operations](federation_operations.md)
- [Tailscale Federation discovery](tailscale_federation_discovery.md)
- [Server setup](server_setup.md)
