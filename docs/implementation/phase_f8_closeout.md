# Phase F8 closeout: trusted-provider federation implementation complete

Status: F8.1 through F8.6 are implemented on `main`. Final F8 closeout requires the F8.7 end-to-end acceptance test, this decision record, and the consolidated F8, F7, relay, Flask, and Phase 2 Linux/Windows gates to merge successfully. F8 implementation branches remain preserved until separate branch-cleanup approval is given.

Baseline: `main` at `10cd7dc38c33ebe1d855885b08aa7841c185649c` after F8.6 reconnect and restart reconciliation.

## Purpose

F8 productizes the safe F7 scheduling and execution boundaries for explicitly trusted session providers. It adds durable provider enrollment, expiring authenticated health, remote language-model binding, local compute-handler activation, a safe operator projection, and restart reconciliation without turning announcements, connectivity, or health into execution or data authority.

F8.7 closes that software milestone by exercising several trusted AI and compute providers together over the real authenticated relay and by consolidating all existing restart, cancellation, retry, artifact, storage, Flask, and Phase 2 evidence. It does not add a new authority source, provider protocol, scheduler, public deployment path, marketplace, or arbitrary-code mechanism.

## Delivered implementation sequence

| Implementation step | Pull request | Main commit | Delivered boundary |
| --- | ---: | --- | --- |
| F8.1 | #153 | `9f63ce3417e573762b8ba0be64682aa5e428aa8c` | actor-authorized discovery and durable revision-fenced pending, approved, suspended, and revoked provider enrollment |
| F8.2 | #154 | `5f239c8f7f4325d9ae3ace2f5fbe17152e9293d8` | authenticated expiring provider health, generation/report fencing, safe observations, and ordinary F7 resource reports |
| F8.3 | #155 | `991f54d785b68f5f052acdaad74cf2a1fd5afec4` | authenticated remote language-model adapters, duplicate suppression, route validation, timeout/cancellation seams, and F7.7 selection compatibility |
| F8.4 | #156 | `99bbe25d23bb160ad57c821ae275145dee9addb0` | exact binding of current compute providers to explicitly registered local handler objects without executable transfer or dynamic imports |
| F8.5 | #157 | `fe9e5fdc89cf28d55afeaf924f8a83bb1f92cf1d` | safe local operator discovery, health, activation, compatibility, and revision-fenced owner controls |
| F8.6 | #158 | `10cd7dc38c33ebe1d855885b08aa7841c185649c` | restart-safe event replay, runtime reconstruction, exact fencing evidence, idempotency, and atomic reconciler-owned replacement with rollback |
| F8.7 | #159 | closeout merge pending | integrated multi-provider relay acceptance and the final software implementation closeout decision |

## Consolidated F8 acceptance

The F8.7 relay scenario proves the following behavior in one session:

- two independently approved and healthy language-model providers coexist;
- two independently approved and healthy compute providers coexist;
- the unchanged F7 ranking policy rejects a provider with no available capacity and deterministically selects the eligible provider;
- the selected remote model is invoked over the authenticated F8.3 request/reply route;
- the selected compute worker is invoked over the authenticated F8.4/F7.4 dispatch route;
- identical duplicate compute delivery executes the local handler at most once;
- the owner sees all four providers through the safe F8.5 projection without announcement properties, health attributes, models, modalities, handler identities, fingerprints, enrollment IDs, endpoints, credentials, or backend paths;
- suspending the selected AI provider and publishing a newer report for the alternative provider causes subsequent binding and selection to use only the current alternative;
- revoking the selected compute provider fences its existing activation before another local handler call;
- a newer health report and refreshed activation make the alternative compute provider selectable and executable;
- natural health expiry removes live eligibility without deleting durable F8.1 approval.

The consolidated closeout workflow also runs the complete capability and AI suites. That evidence covers:

- F8.1 restart persistence, idempotent commands, stale revisions, immutable identity, same-type providers, announcement reconciliation, suspension, and revocation;
- F8.2 self-publication, enrollment/announcement matching, generation and report revision monotonicity, expiry, restart recovery, safe observations, and multiple current reports;
- F8.3 exact relay actor/session/target/provider/capability/generation/request validation, duplicate suppression, provider-side revalidation, error isolation, local/remote coexistence, and safe fallback;
- F8.4 local inventory compatibility, descriptor and binding fencing, handler removal/replacement, duplicate dispatch, and preservation of F7 ownership and lifecycle rules;
- F8.5 member-authorized read-only views, owner-only revision-fenced controls, CSRF and server-bound actor context, and controlled unavailable behavior;
- F8.6 persisted coordinator, enrollment, health, reconciliation, dispatch, and runtime reopen; bounded contiguous event replay; generation/report/enrollment/handler replacement; expiry, suspension, member removal, rollback, and preservation of unrelated runtime entries;
- F7 durable ownership, retry, cancellation, stale-worker fencing, one logical result, least-privilege artifact grants, verified placement, and multi-provider AI selection;
- Phase 2 identity, session membership, capability announcements, relay behavior, storage authority, replication, completeness-aware failover, and cross-platform persistence.

## Authority and security boundaries

F8 is fail-closed and preserves these separations:

- The Phase 2 coordinator remains authoritative for enrolled node identities, session membership, capability announcements, node revocation, connectivity, and ordered session events.
- A capability announcement is metadata only. It does not approve, schedule, invoke, activate, authorize, or grant data access.
- F8.1 is the only durable provider decision authority. Approval remains session-, capability-, node-, type-, protocol-, major-version-, revision-, and announcement-bound.
- F8.2 is the only F8 live provider-resource authority. Durable approval is not schedulable without a fresh authenticated report.
- F8.3 may invoke only the exact currently approved, healthy, generation-matching language-model provider over an authenticated logical route.
- F8.4 may execute only an explicitly supplied local handler object whose descriptor and current health binding still match. No remote node supplies code, module paths, packages, images, commands, or process instructions.
- F8.5 is a projection and command adapter. It never becomes a new source of enrollment, health, activation, job, artifact, or storage truth.
- F8.6 stores only safe replay cursors and immutable logical fencing evidence. It never revives expired health or infers authority from historical events.
- F7 remains authoritative for provider ranking, job creation, attempt generation, ownership leases, dispatch identity, duplicate suppression, cancellation, retry, stale-worker fencing, and one logical result commit.
- F7.6 artifact access remains short-lived and bound to the exact session, job, attempt, provider, worker, lease, generation, operation, and declared logical reference.
- Storage remains governed by its independent primary/replica terms, leases, fencing tokens, acknowledgement, completeness, replication, and failover rules.
- Provider approval, health, runtime binding, selection, dispatch, or successful execution never grants storage leadership or general storage access.
- Cross-session identities, unauthenticated actors, stale revisions, stale generations, stale reports, stale leases, changed announcements, removed membership, revoked nodes, unknown protocol majors, and mismatched routes fail closed.
- Contracts, checkpoints, events, status surfaces, audit evidence, and logs must not expose credentials, tokens, private endpoints, physical addresses, backend paths, handler implementations, prompts, model results, job payloads, artifact grants, or storage authority.
- F8 introduces no automatic public exposure of PostgreSQL, Ollama, Flask, relay, worker, or storage ports.

## Compatibility impact

F8 is additive to the existing local MSH workbench. Local Flask routes, recorder durability, JSONL compatibility outputs, the logical Storage API, private Ollama setup, and existing connected-model configuration remain supported.

The configured local model may coexist with trusted session-bound remote providers. Provider choice is made per request from explicit capability requirements and current safe resource state, not from a global active-provider flag.

Compute providers use the existing F7 job, ownership, dispatch, lifecycle, result, and artifact protocols. F8 does not redirect storage clients, reinterpret provider health as storage authority, or replace the local workflow orchestrator with the distributed coordinator.

## Architectural closeout decision

F8 is closed as a **software implementation milestone** when the exact F8.7 PR head passes all final closeout workflows and this decision is merged.

The repository then contains a complete explicitly trusted-provider path from discovery and durable approval through live health, deterministic AI/compute use, safe operator control, and restart reconciliation, while preserving the F7 and storage authority boundaries.

This decision does not claim production acceptance for every deployment, provider operator, network topology, workload, hostile participant, public relay, or untrusted execution environment.

## Explicitly deferred operational hardening

The following work remains outside F8 and requires separately scoped milestones:

- internet-facing relay and rendezvous operation;
- physical unrelated-network and restrictive-NAT deployment acceptance;
- anonymous or public provider participation;
- marketplace, payment, billing, reputation, dispute, and settlement behavior;
- arbitrary third-party code execution;
- production sandboxing, isolation, provenance, signing, and software supply-chain policy for untrusted handlers or models;
- production scheduling for cost, latency, locality, energy, affinity, fairness, quotas, priorities, preemption, and heterogeneous accelerators;
- durable distributed interactive AI queues, streaming responses, warm pools, and model lifecycle orchestration;
- production load, abuse, denial-of-service, soak, chaos, upgrade, observability, alerting, SLO, and incident-management acceptance;
- automatic public endpoint management or exposure of database, Ollama, Flask, relay, worker, or storage services;
- a full Kubernetes scheduler or replacement of established cluster schedulers.

Deferral does not weaken the current boundary: until a later gate explicitly proves otherwise, only explicitly trusted providers and preinstalled handlers may participate, private services must remain private, and every operation must remain session-, capability-, generation-, revision-, ownership-, lease-, and authorization-bound.

## Branch cleanup list

The following branches become obsolete implementation references after the F8.7 closeout PR is green on its final SHA, merged, and the resulting exact `main` commit is verified:

- `agent/phase-f81-provider-enrollment`
- `agent/phase-f82-provider-health-sync`
- `agent/phase-f83-remote-ai-binding`
- `agent/phase-f84-compute-worker-activation`
- `agent/phase-f85-operator-federation-surface`
- `agent/phase-f86-reconnect-reconciliation`
- `agent/phase-f87-f8-closeout`

Deleting these refs does not delete their commits, which remain reachable from `main`.

No branch may be deleted automatically as part of closeout. After merge and exact `main` verification, the list above must be presented to the repository owner and deletion may occur only after separate explicit approval.
