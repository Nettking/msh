# One-command setup

Status: **current startup guide**

Reviewed: **2026-08-11**

This page describes the supported default startup. It does not assign the device one permanent server role.

## Normal FCP device

### Windows

From the repository root:

```cmd
start.cmd
```

The launcher starts the current core services, preserves existing device/Federation/capability/data state, checks the configured Ollama model, bakes the exact Git commit into the runtime, starts the bounded host update agent, and opens the appropriate product page.

Use:

```cmd
start.cmd --resume
```

to reconnect the saved Federation and reuse persisted capability evidence before opening the workbench.

Use:

```cmd
start.cmd --fresh
```

to remove the local device identity and Federation onboarding state after an explicit confirmation. Recorded telemetry, source configuration, recorder checkpoints, analysis results, Docker images, and downloaded Ollama models are preserved.

For an older Windows checkout that predates the supported launcher/update-agent path, use the one-shot migration bootstrap:

```cmd
migrate.cmd
```

It preserves existing state and fails closed rather than resetting or guessing when the old installation cannot be identified safely.

### Linux or macOS

From the repository root:

```bash
bash start.sh
```

The launcher starts the same normal service set and bounded host update agent.

Direct Compose remains available for development/troubleshooting, but bypassing `start.sh` also bypasses the normal host-owned Federation update activation boundary unless the update agent is started separately.

## Open the product

Use:

```text
http://localhost:5000/onboarding
```

for a device without completed capability-first onboarding.

The required first-run path is:

```text
Identity
  -> Federation
  -> Inspect
  -> finish setup
  -> open Federation
```

A current inspection is enough to finish setup. Benchmarks and contribution choices are optional follow-up work.

Returning devices normally reconnect using persisted identity and trusted Federation state. Open:

```text
http://localhost:5000/federation
```

to inspect the current device, Federation members, services, recorders, software updates, benchmarks, storage, jobs, and activity.

## Standalone recorder: one pairing argument

For a headless MTConnect recorder, generate the normal signed Federation pairing code and run:

```bash
python start_recorder.py FCP1-...
```

On first configuration the recorder runs the existing bounded private-network scan by default, selects discovered sources, joins the Federation, starts loss-aware recording, and starts its independent Federation publication/control workers.

After the first successful join, the pairing code is not needed again:

```bash
python start_recorder.py
```

Any trusted Federation device can then open `/federation/recorders` to request a scan on that recorder and add/remove sources selected from its latest scan.

See [Standalone recorder](standalone_recorder.md).

## Federation-wide updates

The coordinator can explicitly run **Check for updates** and then **Update all devices**. Normal FCP devices started by `start.cmd`/`start.sh` have the host update agent required for this operation.

The Compose-managed recorder on a normal FCP device is updated together with that runtime. A standalone `python start_recorder.py` process is not restarted by the normal Compose update agent and must currently be administered through its own host process/update path.

See [Federation operations](federation_operations.md).

## Specialized deployment commands

`setup_fcp.py` and optional Compose profiles remain compatibility and administration tools for explicit local process composition. Old deployment-mode names may remain accepted as aliases, but selecting them does not grant Federation, provider, storage, compute, job, artifact, or recorder contribution authority.

See [Quick start](quick_start.md) and [Server setup](server_setup.md) for reset boundaries, network binding, recorder configuration, migration, model installation, and advanced deployment choices.
