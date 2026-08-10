# FCP documentation

Use the guides below for the current FCP product. Start from the task or product area you are working with; installation and first-run onboarding are a separate path for new devices.

## Use FCP

- [Operator guide](operator_guide.md) — use Federation, Monitor, Knowledge, System, sources, recording, benchmarks, contributions, and diagnostics.
- [Federation operations](federation_operations.md) — pair devices, check for updates, run **Update all devices**, understand update states, and bootstrap legacy Windows hosts safely.
- [Standalone recorder](standalone_recorder.md) — start a headless recorder with a pairing code, run startup discovery, manage sources remotely, and understand local-first Federation publication.
- [Connected capabilities](connected_capabilities.md) — use capabilities contributed by another trusted device.
- [Operator knowledge capture](operator_strategy_capture.md) — capture, review, structure, compare, and export operator knowledge.
- [Source synchronization](source_synchronization.md) — configure sources, normalized landing paths, and synchronization state.
- [Workflow sessions](workflow_sessions.md) — work with workflow metadata, filtering, execution, cache reuse, bootstrap, and catch-up.
- [AI explainer](ai_explainer.md) — use read-only repository explanation with local or connected Ollama.
- [Troubleshooting](troubleshooting.md) — diagnose startup, data, playback, Docker, runtime, recorder, Federation update, and connectivity problems.

## New installation or device

- [Quick start](quick_start.md) — start FCP, complete capability-first onboarding, and open the workbench.
- [One-command setup](one_command_setup.md) — use the supported Windows or POSIX launcher.
- [Getting started](getting_started.md) — understand devices, Federations, inspection, contributions, authority, and the workbench mental model.
- [Server setup](server_setup.md) — administer network access, recorder configuration, model installation, migration, and advanced deployments.
- [Termux phone setup](termux_phone.md) — Android development and operation.

The required first-run product path remains `Identity -> Federation -> Inspect -> finish setup`. Benchmarks and contribution choices are optional follow-up work rather than prerequisites for normal workbench access.

The installed product is capability-first. Retained legacy setup state and old command spellings exist only where explicitly documented for migration/administration; they do not define product authority or permanent device roles.

## Understand how FCP works

- [Current architecture](architecture.md) — product model, components, authority boundaries, data flow, storage, jobs, recorder publication, distributed controls, persistence, and acceptance.
- [Federated network reference](federated_session_network.md) — identity, membership, control-plane, transport, storage, capability, and failure behavior.
- [Data contract](data_contract.md) — telemetry inputs, normalization, derived artifacts, and playback-ready requirements.
- [Federation v1 scope](releases/federation_v1_scope.md) — intended trusted release boundary and required evidence.

## Development and implementation

The material below is for development, acceptance, and repository maintenance. It is not the normal starting point for operating FCP.

Use this hierarchy when documents disagree:

1. Current user and administration guides describe supported operation.
2. [Implementation documentation](implementation/) classifies active tracks, acceptance, reference material, and history.
3. [Current task handoff](implementation/current_task_handoff.md) identifies development blockers and exact next deliveries where still current.
4. A document explicitly marked active or authoritative governs its named track.
5. Acceptance truth comes from the source named by the active plan.
6. Material under `history/` is non-authoritative evidence.

Current track entry points:

- [Federation implementation](implementation/federation/)
- [Capability-first Federation plan](implementation/federation/active/capability_first_federation_plan.md)
- [Detailed Federation update design](implementation/federation/active/manual_updates.md)
- [Federation acceptance](implementation/federation/acceptance/)
- [OSL integration](implementation/osl_integration/)
- [OSL implementation roadmap](implementation/osl_integration/10_phased_implementation_roadmap.md)
- [Post-v1 roadmap](roadmap/post_v1_product_roadmap.md)

## Current development status

- Capability-first Federation baseline: merged.
- Role-first runtime compatibility retirement (CF8): merged for the installed product.
- Verified manual Federation-wide runtime updates: merged.
- Standalone recorder Federation bootstrap, logical-storage publication, startup scan, and remote source control: merged.
- Complete physical CF7 acceptance: not accepted.
- Complete Federation v1 end-to-end acceptance: not accepted.
- OSL production implementation: not started.
- Federation v1 release tag: not created.

The machine-readable acceptance source remains `catalog/federation/tests/cf7_acceptance/scenarios.json`.

A file’s existence does not prove that its feature is current or accepted. Read its status and directory metadata before treating it as an instruction.
