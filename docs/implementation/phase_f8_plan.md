# Phase F8 plan: productized trusted-provider federation

Status: F8.1 and F8.2 are complete on `main`; only F8.3 is authorized for implementation in the current pull request.

Baseline: `main` at `5f239c8f7f4325d9ae3ace2f5fbe17152e9293d8` after F8.2 provider health synchronization.

## Purpose

F7 completed deterministic capability scheduling, durable job ownership, authenticated dispatch, bounded recovery, capability-specific artifact authorization, and the logical multi-provider AI runtime. F8 turns those safe internal boundaries into a productized federation path for explicitly trusted providers.

The Phase 2 coordinator owns durable session membership and capability announcements. F8.1 added explicit durable provider approval. F8.2 connected that approval to expiring live resource state. F7 already owns `ProviderResourceReport`, deterministic selection, jobs, dispatch, lifecycle, artifacts, and AI execution. F8.3 binds one approved and currently healthy remote language-model provider to the existing F7.7 runtime over authenticated session transport.

## Fixed F8 boundaries

- The coordinator remains authoritative for enrolled nodes, session membership, capability announcements, revocation, connectivity, and ordered session events.
- An announced capability is discoverable metadata only. It is not automatically approved, schedulable, callable, or authorized to access data.
- Provider approval is explicit, durable, revision-fenced, session-bound, and auditable.
- Approval grants no storage leadership, storage read/write access, artifact access, job ownership, shell execution, endpoint publication, or permission to install code.
- F7 provider selection still requires a fresh `ProviderResourceReport`; enrollment state is not a substitute for live resource state.
- Private endpoints, credentials, physical addresses, backend paths, and secret material must not enter announcements, enrollment records, health records, invocation frames, status surfaces, events, or logs.
- Existing local Flask, recorder, JSONL, storage, relay, direct transport, Ollama setup, and F7 runtime behavior remain compatible.
- Unknown protocol major versions, cross-session identities, stale generations, stale revisions, unauthenticated actors, and mismatched relay routes fail closed.

## F8.1: authorized discovery and durable provider enrollment — complete

Delivered on `main` by PR #153:

- actor-authorized coordinator discovery within one session;
- versioned pending, approved, suspended, and revoked enrollment records;
- transactional SQLite persistence, expected-revision fencing, idempotent command replay, restart recovery, immutable identity binding, and safe append-only audit evidence;
- exact binding to session, capability ID, contributing node, capability type, protocol, and protocol major;
- deterministic support for several approved providers of the same capability type;
- fail-closed eligibility when announcements are removed, changed, disabled, revoked, cross-session, identity-conflicting, or protocol-incompatible.

F8.1 does not create live resource state or runtime authority.

## F8.2: authenticated health and resource-report synchronization — complete

Delivered on `main` by PR #154:

- a versioned provider-health synchronization contract over the existing F7 `ProviderResourceReport`;
- authenticated self-publication by the provider node inside an active session;
- exact enrollment, session, capability, node, type, protocol, generation, and revision binding;
- transactional latest-report persistence, idempotent command replay, safe audit evidence, restart recovery, and natural expiry;
- deterministic retrieval of fresh F7 reports after rechecking current enrollment and coordinator announcement state;
- safe health observations for current, expired, superseded, enrollment-ineligible, announcement-ineligible, and absent state;
- multiple simultaneously healthy providers of the same capability type.

F8.2 does not create transport handshakes, remote adapters, model invocation, or compute handlers.

## F8.3: trusted remote language-model adapter binding

Deliver:

- a versioned remote AI invocation request and response contract containing only logical session, capability, node, generation, request, outcome, and safe error fields;
- a bounded authenticated relay request/reply endpoint over the existing `relay.message` session route;
- exact route validation in both directions: authenticated actor, authenticated session, target node, provider node, capability ID, request ID, and invocation ID must agree;
- a provider-side host that binds one locally installed `LanguageModelProvider` implementation to its own approved and currently healthy enrollment;
- a client-side `RemoteLanguageModelProvider` adapter that satisfies the existing F7.7 `LanguageModelProvider` protocol without carrying a remote endpoint or credentials;
- a binder that creates remote adapters only from current F8.2 health records for the `language-model` capability and `msh-language-model` protocol;
- dynamic report sourcing so the existing F7.7 runtime evaluates the current F8.2 `ProviderResourceReport` at every selection instead of synthesizing remote health from enrollment metadata;
- provider-generation fencing so a restarted provider invalidates adapters and in-flight requests from an older process generation;
- revalidation immediately before send and again on the provider node before local model invocation, covering enrollment, announcement, membership, node revocation, health expiry, generation, identity, model, modality, and protocol;
- bounded in-flight invocation tracking, duplicate request replay protection, cancellation-before-send, request timeout, and safe translation of remote provider failures into existing `AIProviderInvocationError` classes;
- deterministic support for multiple simultaneously healthy remote language-model providers alongside existing local providers;
- focused Linux and Windows CI plus complete affected F8.1, F8.2, F7.7, relay, and Phase 2 regressions.

F8.3 does not:

- publish, discover, store, or distribute an Ollama URL, private IP address, port, credential, token, physical location, or backend path;
- approve, suspend, revoke, or mutate provider enrollment or health records;
- infer model availability or capacity from announcements or enrollment metadata;
- bypass F7.7 selection, fallback, timeout, cancellation, queue, or response-validation rules;
- grant storage authority, artifact authority, job ownership, shell access, executable code transfer, package installation, or compute-worker activation;
- add operator UI, public relay deployment, restrictive-NAT acceptance, marketplace behavior, payment, reputation, or untrusted-provider sandboxing;
- implement general reconnect inventory reconciliation, which remains F8.6.

Exit criteria:

- an unapproved, suspended, revoked, removed, expired, superseded, cross-session, protocol-incompatible, or announcement-incompatible provider cannot be bound or invoked;
- only a current session member may invoke, and only the authenticated provider node may answer for the target capability;
- the session owner cannot impersonate the provider, and another provider cannot answer for the selected capability;
- invocation frames contain no endpoint, credential, physical address, backend path, authority scope, arbitrary executable payload, or provider-local configuration;
- the provider-side host invokes only the explicitly registered local provider object for the exact capability ID;
- a higher provider generation fences an older remote adapter and rejects old in-flight requests before local model execution;
- health expiry, enrollment suspension/revocation, membership removal, node revocation, announcement removal/change, or health generation change prevents subsequent invocation without deleting durable approval;
- current health capacity, status, queue depth, utilization, models, and modalities participate in ordinary F7 provider selection without modifying the F7 selection algorithm;
- local and remote providers can coexist; two healthy remote providers can be selected and fallback remains limited to the existing allowlisted failure classes;
- duplicate identical invocation delivery does not execute the local provider twice; conflicting reuse fails closed;
- timeout, cancellation, malformed frames, oversized frames, unexpected responses, and unsafe error details fail with bounded safe errors;
- complete focused tests, compilation, Ruff, Compose validation, and diff hygiene pass on Linux and Windows.

## Proposed later F8 sequence

These steps are planning boundaries only. They are not authorized by the F8.3 pull request.

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
