# Implementation documentation

| Metadata | Value |
| --- | --- |
| Status | Active index |
| Audience | Repository owners, maintainers, reviewers, acceptance operators, and implementation agents |
| Scope | Current implementation tracks, acceptance work, maintained references, release closeout, and retained delivery history |
| Authority | This index classifies tracks; current status comes from this index/current handoff, while each track identifies its durable decisions and acceptance source |
| Entry point | [Current task handoff](current_task_handoff.md) |
| Parent | [FCP documentation](../index.md) |
| Reviewed | 2026-08-14 Europe/Oslo |
| Retention | Permanent while implementation planning remains in this repository |

## Current tracks

- [Current task handoff](current_task_handoff.md) — current merged baseline, blockers, resume safety, and next acceptance/documentation work.
- [Federation implementation](federation/) — durable plans, acceptance documents, technical references, current update/rollout notes, and historical delivery evidence.
- [OSL integration](osl_integration/) — active OSL planning package and authoritative execution order for that track.
- [MTConnect operational segmentation](mtconnect_operational_segmentation_plan.md) — **active authoritative execution order** from canonical MTConnect observations through semantic roles, state/context timelines, MachineRun, ProductionCycle, OperationalEpisode, durable projection, and reference-shape validation. This track explicitly stops before features, baselines, anomaly detection, prediction, recommendations, OSL/SysML integration, and dashboards.
- [Federation sharing evaluation](federation_sharing_evaluation.md) — what the Federation shares today, what remains device-local, and the open inconsistencies between the shared model, the code, and the canonical documentation.
- [Federation execution efficiency learning](federation_execution_efficiency_learning.md) — how completed executions become observations, how learned per-node/workload profiles are maintained, and how they rank already-eligible providers without changing capability or authority constraints.
- [Federated JSONL analysis jobs](federated_jsonl_analysis_jobs.md) — how discovered or uploaded JSONL becomes a durable federation-dispatched job: identity and deduplication, provider selection, ownership, artifact authorization, worker execution, and failure behaviour.

## Current status

- Capability-first Federation implementation is merged.
- CF8 retirement of the role-first installed-product runtime is merged. Retained legacy state/readers are migration/compatibility seams only.
- Verified manual Federation-wide software update support is merged, including host-owned Windows/POSIX activation and exact running-commit verification.
- Standalone recorder Federation bootstrap, checkpoint-gated logical-storage publication, startup discovery, and Federation-wide recorder source control are merged.
- Canonical MTConnect observations are merged via PR #284. The operational-segmentation track is active; its next permitted implementation phase is **S1 Semantic-role resolver**.
- Complete physical CF7 acceptance is **not accepted**.
- Complete Federation v1 end-to-end acceptance remains **false**.
- Federation v1 release tag is not created.
- OSL production implementation remains separate from Federation closeout and is not implied by the Federation changes above.

The machine-readable Federation acceptance source is `catalog/federation/tests/cf7_acceptance/scenarios.json`.

## Status reconciliation rule

Several long-lived implementation plans describe the sequencing that was correct before CF8 and the August 2026 runtime/update/recorder deliveries merged. Keep their durable product and authority decisions, but do not treat an old `current baseline`, `CF8 blocked`, or old commit hash inside those documents as newer than this index and the [Current task handoff](current_task_handoff.md).

Acceptance flags are different: only the named machine-readable/reviewed acceptance source can change an acceptance claim. A merged feature, green CI workflow, or successful live rollout does not by itself change the false CF7/Federation-v1 acceptance flags.

## Classification rule

A document under `docs/implementation/` may direct new work only when its directory index and the document itself mark it active or authoritative **and** its status/sequencing has not been superseded by this active index/current handoff.

Acceptance documents define procedures and evidence boundaries. Acceptance truth comes only from the machine-readable or reviewed source named by the active plan.

Maintained reference documents describe current contracts and architecture but do not independently change implementation order.

Material under `history/` is evidence only and must not be used as the current product behavior or next implementation sequence.
