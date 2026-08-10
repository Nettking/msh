# One-command setup

Status: **current startup guide**

Reviewed: **2026-08-07**

This page describes the supported default startup. It does not assign the device one permanent server role.

## Windows

From the repository root:

```cmd
start.cmd
```

The launcher starts the current core services, preserves existing device and Federation state, checks the configured Ollama model, and opens the appropriate product page.

Use:

```cmd
start.cmd --resume
```

to reconnect the saved Federation and refresh supported local capability state before opening the workbench.

Use:

```cmd
start.cmd --fresh
```

to remove the local device identity and Federation onboarding state after an explicit confirmation. Recorded telemetry, source configuration, recorder checkpoints, analysis results, Docker images, and downloaded Ollama models are preserved.

## Linux or macOS

Start the same default service set:

```bash
docker compose up -d --build relay ollama recorder flask
```

Install the configured model when necessary:

```bash
docker compose --profile model-install run --rm ollama-pull
```

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

to inspect the current device, Federation members, services, benchmarks, storage, jobs, and activity.

## Specialized deployment commands

`setup_fcp.py` and optional Compose profiles remain compatibility and administration tools for explicit deployments. They are not the normal first-run product flow, and selecting a deployment mode does not grant Federation, provider, storage, compute, job, or artifact authority.

See [Quick start](quick_start.md) and [Server setup](server_setup.md) for reset boundaries, network binding, recorder configuration, model installation, and advanced deployment choices.
