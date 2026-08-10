# Federation implementation documentation

| Metadata | Value |
| --- | --- |
| Status | Active track index |
| Audience | Federation maintainers, acceptance operators, reviewers, release owners, and implementation agents |
| Scope | Current product direction, release closeout, acceptance, maintained technical references, and retained delivery history |
| Authority | Current status comes from this index/current handoff; active plans retain durable product/authority decisions where not superseded by later merged work |
| Entry point | [Current task handoff](../current_task_handoff.md) |
| Parent | [Implementation documentation](../) |
| Reviewed | 2026-08-11 Europe/Oslo |
| Retention | Permanent track index |

## Directories

- [Active](active/) — durable product/release plans plus the current update design.
- [Acceptance](acceptance/) — current acceptance harness, physical evidence contract, readiness guide, and machine handoff.
- [Reference](reference/) — maintained architecture, network, protocol, and authority references.
- [History](history/) — completed phase plans, superseded handoffs, audits, closeouts, and delivery evidence.

## Current status

- Capability-first Federation implementation baseline: merged.
- CF8 role-first installed-product runtime retirement: merged.
- Verified manual Federation-wide runtime updates: merged.
- Standalone recorder Federation bootstrap/publication and bounded remote recorder control: merged.
- Complete physical CF7 acceptance: not accepted.
- Complete Federation v1 end-to-end acceptance: false.
- Federation v1 release tag: not created.

## Authority/status rule

The [Current task handoff](../current_task_handoff.md) and [implementation index](../index.md) reconcile status after later merged deliveries. Older active plans still contain durable product/authority decisions, but pre-CF8 status text such as `CF8 blocked`, old commit hashes, or old next-step sequencing is superseded by the current indexes/handoff.

Acceptance procedures do not change acceptance flags. Acceptance truth comes from `catalog/federation/tests/cf7_acceptance/scenarios.json` and a separately reviewed evidence decision.

Reference documents describe maintained technical behavior but cannot grant authority or override acceptance truth.

The recorder-to-Federation delivery work described in the maintained reference material is now implemented through local-first checkpoint-gated publication and is complemented by the current standalone-recorder operator guide and bounded recorder-control protocol.

Historical documents explain prior delivery decisions. They are non-authoritative even when they retain imperative language from the original implementation.
