# Federated Capability Platform (FCP)

**Federated Capability Platform (FCP)** is a Flask-first CNC telemetry workbench with capability-first Federation support. One installation represents one persistent device. A device may use the workbench and independently contribute recording, language-model, registered-compute, or storage-candidate capabilities without being assigned one permanent product role.

## Start Federated Capability Platform

### Windows

```cmd
start.cmd
```

### Linux or macOS

```bash
docker compose up -d --build relay ollama recorder flask
```

Install the configured Ollama model when required:

```bash
docker compose --profile model-install run --rm ollama-pull
```

Open `http://localhost:5000/onboarding` on a device that has not completed setup. Returning devices normally open the workbench or Federation surface using their persisted identity and trusted Federation binding.

## Required first-run flow

```text
Identity
  -> Federation
  -> Inspect
  -> finish setup
  -> open Federation
```

A current device inspection is sufficient to finish setup. Benchmarks and contribution decisions are optional follow-up work. They never grant membership, provider authority, storage assignment, job ownership, or artifact access by themselves.

See:

- [Documentation index](docs/index.md)
- [Quick start](docs/quick_start.md)
- [Server setup](docs/server_setup.md)
- [Operator guide](docs/operator_guide.md)
- [Troubleshooting](docs/troubleshooting.md)

## Current release status

- Capability-first Federation implementation: merged baseline.
- Complete physical CF7 acceptance: not accepted.
- CF8 retirement of the retained role-first compatibility path: blocked until CF7 is accepted.
- Federation v1 release tag: not created.

The machine-readable acceptance source is `catalog/federation/tests/cf7_acceptance/scenarios.json`.

## Current development tracks

- [Federation implementation](docs/implementation/federation/) — active plans, acceptance, reference material, and historical delivery evidence.
- [Capability-first Federation plan](docs/implementation/federation/active/capability_first_federation_plan.md) — authoritative Federation product behavior and acceptance sequence.
- [OSL integration](docs/implementation/osl_integration/) — active planning package; production implementation has not started.
- [Current task handoff](docs/implementation/current_task_handoff.md) — current blockers and exact next deliveries.

## Repository map

- `catalog/flask_app/` — supported Flask application and operator surface.
- `catalog/federation/` — Federation identity, membership, authority, storage, transport, projections, onboarding, and recovery components.
- `catalog/node/` — persistent device identity and outbound Federation client behavior.
- `catalog/relay/` — authenticated relay service.
- `catalog/capabilities/` — inspection, benchmarking, contribution, provider, job, and handler contracts.
- `catalog/storage/` — logical storage contracts and providers.
- `catalog/ai/` — local and connected language-model support.
- `catalog/standalone-recorder_v2/` — supported MTConnect recorder.
- `data/` — local telemetry, source configuration, checkpoints, and device/Federation state.
- `results/` — generated workflow and analysis artifacts.
- `docs/` — product, operator, architecture, implementation, acceptance, release, and historical documentation.

## Authority boundaries

Discovery is not trust. Benchmark evidence is not authority. Contribution intent is not activation. Storage candidates cannot self-assign primary or replica authority. Compute is limited to explicitly registered local handlers. AI may explain or propose, but it does not approve, publish, assign authority, or execute unregistered code.
