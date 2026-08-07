# Federation technical reference

| Metadata | Value |
| --- | --- |
| Status | Maintained reference |
| Audience | Maintainers, reviewers, operators, release owners, and implementation agents |
| Scope | Current architecture, network, protocol, authority, compatibility, and failure-model references |
| Authority | Reference material describes current behavior but does not define implementation order or change acceptance claims |
| Entry point | [Current MSH architecture](../../../architecture.md) |
| Parent | [Federation implementation documentation](../) |
| Reviewed | 2026-08-07 Europe/Oslo |
| Retention | Retain while the described contracts remain supported |

## Current references

- [MSH architecture](../../../architecture.md) — product model, components, authority boundaries, storage, jobs, data flow, persistence, and acceptance.
- [Federated network reference](../../../federated_session_network.md) — identity, membership, control plane, transport, capability lifecycle, storage, failure behavior, and trust boundary.
- [Federation v1 scope](../../../releases/federation_v1_scope.md) — intended release boundary and required evidence.
- [Data contract](../../../data_contract.md) — telemetry and derived-artifact contracts.

Technical references must remain consistent with the active capability-first plan. When they disagree, update the reference; do not choose a more permissive authority interpretation.