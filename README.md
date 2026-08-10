# Federated Capability Platform (FCP)

**Federated Capability Platform (FCP)** is a Flask-first CNC telemetry workbench with capability-first Federation support. One installation represents one persistent device. A device may use the workbench and independently contribute recording, language-model, registered-compute, or storage-candidate capabilities without being assigned one permanent product role.

## Start Federated Capability Platform

### Windows

```cmd
start.cmd
```

### Linux or macOS

```bash
bash start.sh
```

The supported launchers preserve existing device/Federation state, start the core services, bake the exact Git commit into the runtime images, verify the configured Ollama model, and start the bounded host-owned update agent used by Federation-wide software updates.

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

## Current product highlights

- **Capability-first runtime** — the retired role-first product runtime is no longer a normal authority path. Retained legacy state is migration input only.
- **Manual Federation-wide updates** — the Federation coordinator can check an exact approved `main` commit and explicitly update eligible devices. Successful runtime verification is shown as **Updated**.
- **Headless standalone recorder** — `python start_recorder.py FCP1-...` can identify, pair, scan its local private network, start loss-aware recording, and publish checkpoint-committed data to Federation logical storage.
- **Federation-wide recorder control** — any trusted Federation device can request a bounded scan on a connected standalone recorder and add/remove sources selected from that recorder's latest scan.
- **Short-lived pairing** — browser-generated `FCP1-...` codes are signed, one-use, valid for up to 10 minutes, and can be generated again whenever another pairing attempt is needed.

See:

- [Documentation index](docs/index.md)
- [Quick start](docs/quick_start.md)
- [Federation operations](docs/federation_operations.md)
- [Standalone recorder](docs/standalone_recorder.md)
- [Server setup](docs/server_setup.md)
- [Operator guide](docs/operator_guide.md)
- [Troubleshooting](docs/troubleshooting.md)

## Current acceptance status

- Capability-first Federation baseline: merged.
- Role-first runtime compatibility retirement (CF8): merged for the installed product; migration seams remain where explicitly documented.
- Complete physical CF7 acceptance: **not accepted**.
- Complete Federation v1 end-to-end acceptance: **not accepted**.
- Federation v1 release tag: not created.

The machine-readable acceptance source is `catalog/federation/tests/cf7_acceptance/scenarios.json`. Automated gates demonstrate executable contracts and regression coverage, but they do not replace the required physical evidence.

## Current development tracks

- [Federation implementation](docs/implementation/federation/) — active plans, acceptance, reference material, and historical delivery evidence.
- [Capability-first Federation plan](docs/implementation/federation/active/capability_first_federation_plan.md) — authoritative Federation product behavior and acceptance sequence.
- [OSL integration](docs/implementation/osl_integration/) — active planning package; production implementation has not started.
- [Current task handoff](docs/implementation/current_task_handoff.md) — development blockers and exact next deliveries where still current.

## Repository map

- `catalog/flask_app/` — supported Flask application and operator surface.
- `catalog/federation/` — Federation identity, membership, authority, storage, transport, projections, onboarding, update and recorder-control contracts, and recovery components.
- `catalog/node/` — persistent device identity and outbound Federation client behavior.
- `catalog/relay/` — authenticated relay service.
- `catalog/capabilities/` — inspection, benchmarking, contribution, provider, job, and handler contracts.
- `catalog/storage/` — logical storage contracts and providers.
- `catalog/ai/` — local and connected language-model support.
- `catalog/standalone-recorder_v2/` — supported loss-aware MTConnect recorder.
- `start_recorder.py` — standalone recorder launcher, Federation bootstrap, startup discovery, and managed recorder-control composition.
- `data/` — local telemetry, capability/source configuration, checkpoints, and device/Federation state.
- `results/` — generated workflow and analysis artifacts.
- `docs/` — product, operator, architecture, implementation, acceptance, release, and historical documentation.

## Authority boundaries

Discovery is not trust. Benchmark evidence is not authority. Contribution intent is not activation. Storage candidates cannot self-assign primary or replica authority. Compute is limited to explicitly registered local handlers. AI may explain or propose, but it does not approve, publish, assign authority, or execute unregistered code.

Federation software updates and recorder controls follow the same principle: peers send bounded authenticated intent, while the target host independently validates and executes only locally fixed operations. FCP does not expose a general Federation shell.
