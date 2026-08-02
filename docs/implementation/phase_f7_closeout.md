# Phase F7 closeout: capability scheduling implementation complete

Status: F7 implementation complete. Final closeout requires this document and the consolidated F7, F6, and Phase 2 Linux/Windows gates to merge successfully. F7 implementation branches remain preserved until a separate branch-cleanup approval is given.

Baseline: `main` at `3145a750408e67a965589a95fbb7017876950183` after F7.7.

## Purpose

Implementation F7 maps to Phase 6 / PR G in the federated-session architecture. Its software deliverables are capability-specific scheduling for compute and language-model providers without applying storage primary/replica semantics to stateless or parallelizable work.

This closeout consolidates automated evidence, records the final authority and security boundaries, lists deferred operational hardening, and makes an explicit implementation-closeout decision. It does not add F8 work, public infrastructure, new runtime behavior, or branch deletion.

## Delivered implementation sequence

| Implementation step | Pull request | Main commit | Delivered boundary |
| --- | ---: | --- | --- |
| F7.1 | #145 | `6304f14e6d1642719403248610fb17e54b6fc9b6` | versioned job, capability requirement, artifact reference, retry, timeout, attempt, and state-transition contracts |
| F7.2 | #146 | `313dfad562f4892f1634e7eac118ebd72fe1d398` | deterministic capability-provider eligibility, resource reports, exclusions, capacity checks, and stable selection policy |
| F7.3 | #147 | `0605e2657eb7b2ad4c9560918814889133a606a6` | durable coordinator-owned jobs, attempts, exactly one active lease, revision fencing, recovery, and ownership audit history |
| F7.4 | #148 | `4f4285362919855527dd4d2ee16e9c6f8c2bfc6d` | authenticated worker dispatch, explicit allowlisted handlers, durable duplicate suppression, structured progress, and coordinator-applied worker events |
| F7.5 | #149 | `41f1662ae09cac8bb9fb6d03d9854e7794905872` | cancellation, heartbeat and lease-loss handling, bounded timeout/retry/backoff, atomic reassignment, stale-worker fencing, and one logical result commit |
| F7.6 | #150 | `b5bc4a29ba52532a3326d2053cb00e8a78204dba` | logical artifact placement, verified transfer, short-lived job-scoped grants, least-privilege input references, audit events, and idempotent publication |
| F7.7 | #151 | `3145a750408e67a965589a95fbb7017876950183` | logical multi-provider language-model runtime, model/modality scheduling, bounded queueing, safe fallback, private Ollama compatibility, and status reporting |

## Phase 6 acceptance mapping

The original scenario identifiers from `federated_session_test_matrix.md` remain the acceptance source of truth.

| Scenario | Automated evidence |
| --- | --- |
| `F6-001 Multiple AI providers` | F7.7 registers two language-model providers in the same session and selects deterministically by explicit model, modality, availability, exclusion, and capacity policy. No primary/replica or global active-provider authority is introduced. |
| `F6-002 Job ownership` | F7.2 selects among several eligible compute workers while F7.3 transactionally permits at most one active ownership lease for a job. Workers cannot self-assign or extend ownership. |
| `F6-003 Worker loss` | F7.5 detects heartbeat or lease loss, applies bounded backoff, reassigns through the existing deterministic selector, fences the old worker, recovers after coordinator restart, and commits at most one logical result reference. |
| `F6-004 Capability-specific authorization` | F7.6 gives a worker only short-lived grants for declared job inputs and output placement. Compute or AI registration alone grants no storage read, write, leadership, backend, or database authority. Cross-session and stale-lease access fail closed. |

## Consolidated closeout acceptance

The closeout gates exercise at least the following required behavior on Linux and Windows:

- two AI providers registered in one session;
- several eligible compute workers;
- exactly one active job owner;
- deterministic provider selection and explicit rejection reasons;
- worker heartbeat loss, lease expiry, bounded retry, and reassignment;
- cancellation before start and during execution;
- queue, start, run, overall, heartbeat, lease, and cancellation-grace timeout behavior;
- coordinator and worker restart recovery;
- stale-worker completion rejection after reassignment;
- idempotent, conflict-safe single result commit;
- unauthorized and cross-session storage access rejection;
- least-privilege input grants, expiry, revocation, and policy fencing;
- content hash, size, schema, verified transfer, and logical artifact placement;
- multiple AI-provider scheduling, bounded queueing, timeout/cancellation seams, and allowlisted fallback;
- rejection of silent downgrade after security, authorization, protocol, cancellation, invalid-request, or invalid-response failures;
- preservation of local and connected Ollama workflows without publishing private endpoints.

The dedicated F7 closeout workflow runs the complete `catalog/capabilities/tests` and `catalog/ai/tests` suites, affected Flask AI tests, F7.4/F7.5 relay acceptance tests, and F0/F1 compatibility regressions. It also compiles and lints the complete F7 Python boundary, validates Compose, and checks diff hygiene.

The unchanged complete F6 closeout workflow is triggered by this closeout change and revalidates the Go/libp2p sidecar, direct and relayed transport, circuit-v2 carriage, signed route rendezvous, verified chunk transfer, durable resume, and the full Phase 5 transport matrix on Linux and Windows.

The unchanged Phase 2 workflow is also triggered and revalidates identity, coordinator state, storage protocols, PostgreSQL on Linux, leadership, fencing, replication, completeness-aware failover, node/relay integration, Go sidecar behavior, Compose profiles, and relevant cross-platform regressions.

## Authority and security boundaries

F7 is fail-closed and preserves the following separations:

- The coordinator owns job creation, attempt generation, ownership leases, retry claims, reassignment, cancellation state, and the logical result commit.
- A worker may execute only an explicitly registered local handler. A job payload is data, not a shell command or arbitrary code-installation mechanism.
- A worker cannot select itself, extend its own lease, choose its successor, reopen a terminal job, or publish after its lease or generation becomes stale.
- Provider availability and resource reports influence eligibility only. They do not create ownership, authorization, storage leadership, or data access.
- Storage remains governed by its own primary/replica grants, terms, leases, fencing tokens, acknowledgement policy, and completeness rules.
- Compute and AI capabilities never receive storage authority merely because they are registered or selected.
- Artifact access requires a short-lived grant bound to the exact session, job, attempt, provider, worker, lease ID, lease generation, operation, and declared logical reference.
- Artifact publication requires declared placement policy, verified content identity, final size and hash, and the existing single-result fencing boundary.
- Cross-session dispatch, provider registration, artifact access, and result publication are rejected.
- Unknown protocol major versions are rejected. Additive same-major fields remain bounded by the relevant compatibility contract.
- Queues, retries, backoff, message sizes, provider capacity, grants, timeouts, pending maps, and artifact quotas are bounded.
- Security, identity, signature, protocol, authorization, cancellation, and invalid-response failures never trigger silent fallback or downgrade.
- Provider reports, runtime results, logs, audit events, and status surfaces must not expose credentials, backend paths, private database addresses, or private Ollama endpoints.
- F7 introduces no automatic public exposure of PostgreSQL, Ollama, Flask, worker, or storage ports.

## Compatibility impact

The local Flask workbench, recorder durability and JSONL compatibility outputs remain unchanged. `catalog/orchestrator/` remains the local workflow orchestrator and is not converted into the distributed coordinator.

Existing local and connected Ollama setup remains supported. The setup UI still identifies one configured Ollama connection, while the logical F7.7 runtime can retain that provider together with additional trusted session-bound language-model providers. Provider choice is made per request from explicit requirements and safe runtime state rather than from a single global active-provider truth.

F7 job and artifact protocols are additive subsystems. They do not redirect existing storage clients around the logical Storage API or reinterpret a transport route, provider report, transfer receipt, or successful model response as storage authorization.

## Architectural closeout decision

F7 is closed as a **software implementation milestone** when all final closeout workflows are green on the exact PR head and this decision is merged.

The repository then contains the required contracts and deterministic implementations for capability requirements, multi-provider selection, durable single ownership, authenticated dispatch, cancellation and bounded recovery, stale-worker fencing, one logical result commit, capability-specific artifact authorization, and multi-provider AI runtime integration.

This decision does not claim that every production deployment, external provider, network topology, workload, or untrusted execution environment has been operationally accepted.

## Explicitly deferred operational hardening

The following work is outside the F7 implementation closeout and must be handled through later, separately scoped milestones:

- internet-facing relay/rendezvous operation and physical unrelated-network acceptance already identified by F6 closeout;
- production enrollment, discovery, deployment, monitoring, and incident ownership for independently operated remote compute and AI providers;
- sandboxing, provenance verification, supply-chain policy, and resource isolation required before executing untrusted third-party handlers or models;
- production scheduling policy for cost, latency, locality, energy, affinity, fairness, quotas, priorities, preemption, and heterogeneous accelerators;
- a durable distributed queue for interactive AI requests beyond the bounded in-process F7.7 runtime;
- streaming model responses, model lifecycle orchestration, warm-pool management, and broader fallback-latency tuning;
- production load, abuse, denial-of-service, soak, chaos, upgrade, observability, alerting, and service-level acceptance;
- automatic public endpoint management or direct exposure of database, Ollama, Flask, worker, or storage services;
- marketplace, payment, billing, reputation, anonymous participation, and execution of unknown provider code;
- a full Kubernetes scheduler or replacement of established cluster schedulers;
- F8 and every later research or implementation phase.

Deferral does not weaken the current boundaries. Until a later gate explicitly proves otherwise, only trusted, explicitly registered handlers and providers should be used, private service endpoints must remain private, and every job and artifact operation must remain session- and capability-authorized.

## Branch cleanup list

The following branches are obsolete implementation references after the closeout PR is green on its final SHA, merged, and the resulting `main` commit is verified:

- `agent/phase-f71-job-contracts`
- `agent/phase-f72-provider-selection`
- `agent/phase-f73-durable-job-ownership`
- `agent/phase-f74-worker-dispatch`
- `agent/phase-f75-retry-cancellation`
- `agent/phase-f76-artifact-authorization`
- `agent/phase-f77-ai-runtime-integration`
- `agent/phase-f7-closeout`

Deleting these refs does not delete their commits, which remain reachable from `main`.

No branch may be deleted automatically as part of closeout. After merge and `main` verification, the exact list above must be presented to the repository owner and deleted only after explicit approval.

F8 must not begin as part of closeout or branch cleanup.
