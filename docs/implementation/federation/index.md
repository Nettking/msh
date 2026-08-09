# Federation implementation documentation

| Metadata | Value |
| --- | --- |
| Status | Active track index |
| Audience | Federation maintainers, acceptance operators, reviewers, release owners, and implementation agents |
| Scope | Current product direction, release closeout, acceptance, maintained technical references, and retained delivery history |
| Authority | [Active plans](active/) define current product behavior and remaining work |
| Entry point | [Capability-first Federation plan](active/capability_first_federation_plan.md) |
| Parent | [Implementation documentation](../) |
| Reviewed | 2026-08-07 Europe/Oslo |
| Retention | Permanent track index |

## Directories

- [Active](active/) — authoritative product and release-closeout plans.
- [Acceptance](acceptance/) — current acceptance harness, physical evidence contract, readiness guide, and machine handoff.
- [Reference](reference/) — maintained architecture, network, protocol, and authority references.
- [History](history/) — completed phase plans, superseded handoffs, audits, closeouts, and delivery evidence.

## Current status

- Capability-first CF0-CF7 implementation baseline: merged.
- Complete physical CF7 acceptance: not accepted.
- CF8 role-first compatibility retirement: blocked.
- Federation v1 release tag: not created.

## Authority rule

Only active plans may define new Federation implementation order or current product behavior.

Acceptance procedures do not change acceptance flags. Acceptance truth comes from `catalog/federation/tests/cf7_acceptance/scenarios.json` and a separately reviewed evidence decision.

Reference documents describe maintained technical behavior but cannot grant authority or override the active plan.

The proposed [recorder-to-Federation delivery design](reference/recorder_federation_delivery.md)
describes how local loss-aware capture can feed logical Federation storage
without making network availability part of the recorder's commit boundary.

Historical documents explain prior delivery decisions. They are non-authoritative even when they retain imperative language from the original implementation.
