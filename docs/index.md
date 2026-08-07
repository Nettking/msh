# MSH documentation

Start here for supported MSH operation, architecture, current development direction, acceptance, and retained history.

## Start and operate MSH

- [Quick start](quick_start.md) — supported startup and capability-first onboarding.
- [One-command setup](one_command_setup.md) — default Windows and Compose startup commands.
- [Server setup](server_setup.md) — network access, recorder configuration, model installation, and advanced deployment administration.
- [Operator guide](operator_guide.md) — Federation, Monitor, Knowledge, System, sources, recording, benchmarks, and contributions.
- [Troubleshooting](troubleshooting.md) — startup, data, playback, Docker, runtime, and diagnostic problems.
- [Termux phone setup](termux_phone.md) — Android development and operation.

## Use product capabilities

- [Connected capabilities](connected_capabilities.md) — use capabilities contributed by another trusted device.
- [AI explainer](ai_explainer.md) — read-only repository explanation with local or connected Ollama.
- [Operator knowledge capture](operator_strategy_capture.md) — capture, review, structure, compare, and export operator knowledge.
- [Source synchronization](source_synchronization.md) — source configuration, normalized landing paths, and synchronization state.
- [Workflow sessions](workflow_sessions.md) — workflow metadata, filtering, execution, cache reuse, bootstrap, and catch-up.

## Understand the system

- [Current architecture](architecture.md) — product model, components, authority boundaries, data flow, storage, jobs, persistence, and acceptance.
- [Federated network reference](federated_session_network.md) — current identity, membership, control-plane, transport, storage, capability, and failure model.
- [Data contract](data_contract.md) — telemetry inputs, normalization, derived artifacts, and playback-ready requirements.
- [Federation v1 scope](releases/federation_v1_scope.md) — intended trusted release boundary and required evidence.

## Current implementation direction

Use this hierarchy when documents disagree:

1. Current user and administration guides describe supported operation.
2. [Implementation documentation](implementation/) classifies active tracks, acceptance, reference material, and history.
3. [Current task handoff](implementation/current_task_handoff.md) identifies current blockers and exact next deliveries.
4. A document explicitly marked active or authoritative governs its named track.
5. Acceptance truth comes from the source named by the active plan.
6. Material under `history/` is non-authoritative evidence.

Current track entry points:

- [Federation implementation](implementation/federation/)
- [Capability-first Federation plan](implementation/federation/active/capability_first_federation_plan.md)
- [Federation acceptance](implementation/federation/acceptance/)
- [OSL integration](implementation/osl_integration/)
- [OSL implementation roadmap](implementation/osl_integration/10_phased_implementation_roadmap.md)
- [Post-v1 roadmap](roadmap/post_v1_product_roadmap.md)

## Current status

- Capability-first Federation baseline: merged.
- Complete physical CF7 acceptance: not accepted.
- CF8 compatibility retirement: blocked until CF7 is accepted.
- OSL planning: complete enough to start documentation-only D0-A.
- OSL production implementation: not started.
- Federation v1 release tag: not created.

A file’s existence does not prove that its feature is current or accepted. Read its status and directory metadata before treating it as an instruction.