# MSH Documentation

Start here for the supported MSH product, operator workflow, deployment, and current development direction.

## Run MSH

- [Quick start](quick_start.md) — supported first startup, capability-first onboarding, and the first pages to open.
- [Server setup](server_setup.md) — Docker deployment, network access, recorder configuration, and service profiles.
- [Troubleshooting](troubleshooting.md) — startup, data, playback, Docker, and runtime problems.
- [Termux phone setup](termux_phone.md) — running or developing MSH from Android.

## Use MSH

- [Operator guide](operator_guide.md) — Monitor, Knowledge, System, and the recommended operator workflow.
- [Operator knowledge capture](operator_strategy_capture.md) — capture, review, structure, compare, and export operator strategies.
- [Connected capabilities](connected_capabilities.md) — use capabilities contributed by another trusted MSH device.
- [AI explainer](ai_explainer.md) — read-only repository explanation with local or connected Ollama.
- [Source synchronization](source_synchronization.md) — source configuration, synchronization state, and normalized data landing paths.

## Understand the system

- [Architecture](architecture.md) — current components, data flow, runtime policy, and federation boundaries.
- [Data contract](data_contract.md) — telemetry input, normalization, derived artifacts, and playback-ready requirements.
- [Workflow sessions](workflow_sessions.md) — session layout, script execution, cache reuse, bootstrap, and catch-up.
- [Federated session network](federated_session_network.md) — federation protocol, authority, transport, and failure model.

## Current product and implementation direction

Use the following hierarchy when documents disagree:

1. Current user and administration guides describe supported product behavior.
2. [Implementation documentation](implementation/) classifies current tracks, acceptance material, and historical evidence.
3. [Current task handoff](implementation/current_task_handoff.md) identifies the active repository tracks and immediate next actions.
4. A plan explicitly marked **active authoritative plan** governs its named track.
5. Material under a `history/` directory is non-authoritative delivery evidence.

Active track entry points:

- [Federation implementation index](implementation/federation/) — active plans, acceptance material, and historical Federation evidence.
- [Capability-first Federation plan](implementation/federation/active/capability_first_federation_plan.md) — current Federation product behavior, remaining acceptance work, and authority boundaries.
- [OSL integration plan index](implementation/osl_integration/README.md) — entry point for OSL planning and supporting analyses.
- [OSL implementation execution plan](implementation/osl_integration/10_phased_implementation_roadmap.md) — authoritative OSL delivery order and acceptance gates.
- [Post-v1 product roadmap](roadmap/post_v1_product_roadmap.md) — longer-term product direction.
- [Federation v1 scope](releases/federation_v1_scope.md) — intended release boundary and required release evidence.

## Documentation status

The documentation tree still contains historical implementation material outside the new Federation history directories. Do not infer that a feature is current, accepted, or safe to implement merely because a document exists. Check its directory index, status statement, and the hierarchy above before using it as an instruction.
