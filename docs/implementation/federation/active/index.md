# Active Federation plans

| Metadata | Value |
| --- | --- |
| Status | Active |
| Audience | Maintainers, reviewers, acceptance owners, and implementation agents |
| Scope | Durable Federation product behavior, current release closeout, update design, pending-contribution authority, remaining acceptance work, and stop conditions |
| Authority | Current status/sequencing is reconciled by the current handoff/track index; documents here retain their durable decisions where not superseded |
| Entry point | [Current task handoff](../../current_task_handoff.md) |
| Parent | [Federation implementation documentation](../) |
| Reviewed | 2026-08-11 Europe/Oslo |
| Retention | Retain until superseded; move fully superseded plans to `../history/` |

## Plans and active references

1. [Capability-first Federation implementation and acceptance plan](capability_first_federation_plan.md) — durable capability-first product/authority decisions and original CF0-CF8 acceptance sequencing. Its pre-CF8 status/commit/next-step text is superseded by the current handoff because CF8 has since merged.
2. [Federation v1 closeout plan](federation_v1_closeout_plan.md) — current documentation, cleanup, release-gate, acceptance-candidate, and publication work.
3. [Manual Federation-wide FCP updates](manual_updates.md) — implemented manual-only exact-commit update extension and host-authority boundary.
4. [Federation pending-contribution approval](pending_contribution_approval.md) — leader-only explicit decision contract for capability-first `REGISTERING` contributions, including the separation between enrollment approval and runtime/storage/compute authority.
5. Update rollout probe/acceptance notes in this directory — implementation/acceptance evidence for the update path, not a replacement for the complete CF7 acceptance manifest.

## Current rule

Use the [Current task handoff](../../current_task_handoff.md) first for merged status and next work. Use the capability-first plan for durable product/authority decisions, the pending-contribution contract for leader approval semantics, the closeout plan for release completion, and `catalog/federation/tests/cf7_acceptance/scenarios.json` for acceptance truth.

Do not interpret old statements that CF8 is blocked as current. Do not interpret merged CF8, update, recorder, or pending-contribution features as evidence that the false CF7/Federation-v1 acceptance flags have changed.
