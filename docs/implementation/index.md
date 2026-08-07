# Implementation documentation

| Metadata | Value |
| --- | --- |
| Status | Active index |
| Audience | Repository owners, maintainers, reviewers, acceptance operators, and implementation agents |
| Scope | Current implementation tracks, acceptance work, maintained references, release closeout, and retained delivery history |
| Authority | This index classifies tracks; each track index identifies its authoritative plan and acceptance source |
| Entry point | [Current task handoff](current_task_handoff.md) |
| Parent | [MSH documentation](../index.md) |
| Reviewed | 2026-08-07 Europe/Oslo |
| Retention | Permanent while implementation planning remains in this repository |

## Current tracks

- [Current task handoff](current_task_handoff.md) — current state, blockers, resume safety, and exact next deliveries.
- [Federation implementation](federation/) — active plans, acceptance documents, technical references, and historical delivery evidence.
- [OSL integration](osl_integration/) — active OSL planning package and authoritative execution order.

## Current status

- Capability-first Federation implementation is merged.
- Complete physical CF7 acceptance is not accepted.
- CF8 remains blocked until evidence-backed CF7 acceptance.
- OSL D0-A is ready as a documentation-only decision delivery.
- OSL production implementation remains blocked until D0-A is reviewed and merged.

## Classification rule

A document under `docs/implementation/` may direct new work only when its directory index and the document itself mark it active or authoritative.

Acceptance documents define procedures and evidence boundaries. Acceptance truth comes only from the machine-readable or reviewed source named by the active plan.

Maintained reference documents describe current contracts and architecture but do not independently change implementation order.

Material under `history/` is evidence only and must not be used as the current product behavior or next implementation sequence.