# Phase F8 plan: productized trusted-provider federation

Status: F8.1 is complete on `main`; only F8.2 is authorized for implementation in the current pull request.

Baseline: `main` at `9f63ce3417e573762b8ba0be64682aa5e428aa8c` after F8.1 trusted provider enrollment.

## Purpose

F7 completed deterministic capability scheduling, durable job ownership, authenticated dispatch, bounded recovery, capability-specific artifact authorization, and the logical multi-provider AI runtime. F8 turns those safe internal boundaries into a productized federation path for explicitly trusted providers.

The Phase 2 coordinator owns durable session membership and capability announcements. F8.1 added explicit durable provider approval. F7 already owns `ProviderResourceReport`, deterministic selection, jobs, dispatch, lifecycle, artifacts, and AI execution. F8.2 connects durable approval to expiring live capacity without making approval itself schedulable.

## Fixed F8 boundaries

- The coordinator remains authoritative for enrolled nodes, session membership, capability announcements, revocation, connectivity, and ordered session events.
- An announced capability is discoverable metadata only. It is not automatically approved, schedulable, callable, or authorized to access data.
- Provider approval is explicit, durable, revision-fenced, session-bound, and auditable.
- Approval grants no storage leadership, storage read/write access, artifact access, job ownership, shell execution, endpoint publication, or permission to install code.
- F7 provider selection still requires a fresh `ProviderResourceReport`; enrollment state is not a substitute for live resource state.
- Private endpoints, credentials, physical addresses, backend paths, and secret material must not enter announcements, enrollment records, health records, status surfaces, events, or logs.
- Existing local Flask, recorder, JSONL, storage, relay, direct transport, Ollama setup, and F7 runtime behavior remain compatible.
- Unknown protocol major versions, cross-session identities, stale generations, and stale revisions fail closed.

## F8.1: authorized discovery and durable provider enrollment — complete

Delivered on `main` by PR #153:

- actor-authorized coordinator discovery within one session;
- versioned pending, approved, suspended, and revoked enrollment records;
- transactional SQLite persistence, expected-revision fencing, idempotent command replay, restart recovery, immutable identity binding, and safe append-only audit evidence;
- exact binding to session, capability ID, contributing node, capability type, protocol, and protocol major;
- deterministic support for several approved providers of the same capability type;
- fail-closed eligibility when announcements are removed, changed, disabled, revoked, cross-session, identity-conflicting, or protocol-incompatible.

F8.1 does not create live resource state or runtime authority.

## F8.2: authenticated health and resource-report synchronization

Deliver:

- a versioned provider-health synchronization contract over the existing F7 `ProviderResourceReport`;
- authenticated publication in which the transport-authenticated actor must be the report's provider node and an active member of the report session;
- exact binding to one current F8.1 approved enrollment, including enrollment ID, enrollment revision, session, capability, node, type, protocol, and compatible protocol major;
- a transactional SQLite health store containing only the latest accepted short-lived report per enrolled capability, safe command replay evidence, and bounded append-only audit entries;
- monotonic provider generation and report revision fencing so stale processes cannot overwrite a restarted provider and stale reports cannot replace newer capacity;
- idempotent replay of an identical generation/revision report and rejection of conflicting reuse;
- acceptance only when `reported_at` is not in the future and `expires_at` is still live at receipt time;
- deterministic retrieval of fresh reports for F7 selection, filtered again through current F8.1 enrollment and current coordinator announcement state;
- explicit safe health observations for current, expired, superseded, enrollment-ineligible, announcement-ineligible, and absent live state without exposing private locations or credentials;
- invalidation of previous live capacity when a higher provider generation is established, while preserving the durable F8.1 approval;
- focused Linux and Windows CI plus complete affected F8.1, F7 provider-selection, and Phase 2 regressions.

F8.2 does not:

- infer capacity from capability announcements or enrollment metadata;
- approve, suspend, revoke, or mutate provider enrollment decisions;
- reserve capacity, select a provider, assign job ownership, dispatch work, or invoke a model;
- discover, store, or distribute private provider endpoints or credentials;
- create transport handshakes, UI controls, remote AI adapters, or compute handlers;
- execute remote or untrusted code;
- deploy an internet-facing relay or prove restrictive-NAT operation;
- change storage authority or artifact grants.

Exit criteria:

- an unapproved, suspended, revoked, removed, cross-session, or announcement-incompatible provider cannot publish schedulable live capacity;
- only the authenticated provider node can publish its report; a session owner cannot impersonate it;
- two approved providers of the same type can publish and remain simultaneously visible as fresh reports;
- a valid report survives coordinator process restart but naturally becomes unusable at its existing expiry;
- a report from the future, already expired report, excessive TTL, unsafe attributes, identity mismatch, or incompatible protocol is rejected without mutation;
- generation and report revisions are monotonic; duplicate identical publication replays, while conflicting reuse and stale publication fail;
- a higher generation fences all reports from an older provider process;
- expiration, enrollment suspension/revocation, membership removal, node revocation, announcement removal/change, or announcement disable/revoke prevents retrieval for F7 selection;
- durable approval remains intact when live capacity expires or is superseded;
- returned reports are ordinary F7 `ProviderResourceReport` objects and require no scheduler changes;
- no health object contains authority scopes, endpoints, credentials, physical locations, backend paths, or executable payloads;
- complete focused tests, compilation, Ruff, Compose validation, and diff hygiene pass on Linux and Windows.

## Proposed later F8 sequence

These steps are planning boundaries only. They are not authorized by the F8.2 pull request.

### F8.3: trusted remote language-model adapter binding

Bind an approved and currently healthy language-model enrollment to an authenticated session transport and the existing F7.7 runtime without exposing private provider endpoints.

### F8.4: trusted compute-worker inventory and activation

Bind approved and currently healthy compute providers to explicit locally installed handler inventories. Do not transmit executable code or permit arbitrary shell commands.

### F8.5: operator status and control surface

Expose safe discovery, approval, suspension, revocation, health, expiry, generation, and reason codes without exposing secrets or private locations.

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
