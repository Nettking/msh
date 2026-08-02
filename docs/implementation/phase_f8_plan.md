# Phase F8 plan: productized trusted-provider federation

Status: F8 scope defined; only F8.1 is authorized for implementation in the current pull request.

Baseline: `main` at `a1adb8a603627397f64f533b03f61ff599024c0e` after the Phase F7 closeout.

## Purpose

F7 completed deterministic capability scheduling, durable job ownership, authenticated dispatch, bounded recovery, capability-specific artifact authorization, and the logical multi-provider AI runtime. F8 turns those safe internal boundaries into a productized federation path for explicitly trusted providers.

The first missing link is not another scheduler. The Phase 2 coordinator already owns durable session membership and capability announcements, while F7 already owns provider reports, selection, jobs, dispatch, lifecycle, artifacts, and AI execution. F8 connects those subsystems without treating an announcement as execution authority.

## Fixed F8 boundaries

- The coordinator remains authoritative for enrolled nodes, session membership, capability announcements, revocation, connectivity, and ordered session events.
- An announced capability is discoverable metadata only. It is not automatically approved, schedulable, callable, or authorized to access data.
- Provider approval is explicit, durable, revision-fenced, session-bound, and auditable.
- Approval grants no storage leadership, storage read/write access, artifact access, job ownership, shell execution, endpoint publication, or permission to install code.
- F7 provider selection still requires a fresh `ProviderResourceReport`; enrollment state is not a substitute for live resource state.
- Private endpoints, credentials, physical addresses, backend paths, and secret material must not enter announcements, enrollment records, status surfaces, events, or logs.
- Existing local Flask, recorder, JSONL, storage, relay, direct transport, Ollama setup, and F7 runtime behavior remain compatible.
- Unknown protocol major versions and cross-session identities fail closed.

## F8.1: authorized discovery and durable provider enrollment

Deliver:

- an actor-authorized coordinator API for listing capability announcements within one session;
- versioned provider-enrollment contracts and stable machine-readable states;
- a transactional SQLite enrollment store with expected-revision fencing, idempotent command replay, identity conflict detection, restart recovery, and an append-only audit trail;
- explicit request, approval, suspension, revocation, and announcement-reconciliation operations;
- exact binding to session, capability ID, contributing node, capability type, protocol, and protocol major;
- fail-closed reconciliation when an announcement is unavailable, disabled, revoked, removed, cross-session, identity-conflicting, or protocol-incompatible;
- deterministic listing of several approved providers of the same capability type;
- focused Linux and Windows CI plus affected Phase 2 and F7 regressions.

F8.1 does not:

- produce live resource reports from static announcement properties;
- reserve capacity, select a provider, assign a job, dispatch work, or invoke a model;
- discover or distribute private endpoints or credentials;
- create UI controls;
- execute remote or untrusted code;
- deploy an internet-facing relay or prove restrictive-NAT operation;
- change storage authority or artifact grants.

Exit criteria:

- only active session members can discover announcements for that session;
- an outsider or removed member cannot list session capabilities;
- two providers of the same type may be approved simultaneously;
- duplicate commands replay the original result, while conflicting command reuse fails;
- stale expected revisions fail without mutation;
- an approved record survives process restart;
- node removal, node revocation, or an unavailable/revoked announcement prevents eligibility and is durably reconciled;
- capability identity cannot move to another node, session, type, protocol, or incompatible protocol major;
- no enrollment object contains authority scopes, endpoints, credentials, or executable payloads;
- complete focused tests, compilation, Ruff, Compose validation, and diff hygiene pass on Linux and Windows.

## Proposed later F8 sequence

These steps are planning boundaries only. They are not authorized by the F8.1 pull request.

### F8.2: health and resource-report synchronization

Bind approved enrollment records to authenticated, short-lived provider heartbeats and F7 `ProviderResourceReport` updates. Preserve the distinction between durable approval and expiring live capacity.

### F8.3: trusted remote language-model adapter binding

Bind an approved language-model enrollment to an authenticated session transport and the existing F7.7 runtime without exposing private provider endpoints.

### F8.4: trusted compute-worker inventory and activation

Bind approved compute providers to explicit locally installed handler inventories. Do not transmit executable code or permit arbitrary shell commands.

### F8.5: operator status and control surface

Expose safe discovery, approval, suspension, revocation, health, and reason codes without exposing secrets or private locations.

### F8.6: reconnect and restart reconciliation

Rebuild approved runtime candidates after coordinator, provider, or client restart; replay ordered session changes; and fence stale provider generations.

### F8.7: end-to-end acceptance and closeout

Validate multiple trusted AI and compute providers across persisted nodes, interruption, revocation, restart, cancellation, artifact authorization, and F7 compatibility before closing F8.

## Explicitly outside F8

- public anonymous participation;
- marketplace, payment, billing, or reputation;
- arbitrary third-party code execution;
- full Kubernetes-style scheduling;
- production sandboxing and supply-chain acceptance for untrusted providers;
- public relay operations and physical restrictive-NAT deployment acceptance;
- advanced cost, energy, fairness, preemption, and heterogeneous-accelerator scheduling;
- production load, abuse, chaos, SLO, and incident-management acceptance.
