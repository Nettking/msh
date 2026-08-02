# Phase F8 plan: productized trusted-provider federation

Status: F8.1 through F8.3 are complete on `main`; only F8.4 is authorized for implementation in the current pull request.

Baseline: `main` at `991f54d785b68f5f052acdaad74cf2a1fd5afec4` after F8.3 trusted remote AI binding.

## Purpose

F7 completed deterministic capability scheduling, durable job ownership, authenticated dispatch, bounded recovery, capability-specific artifact authorization, and the logical multi-provider AI runtime. F8 turns those safe internal boundaries into a productized federation path for explicitly trusted providers.

The Phase 2 coordinator owns durable session membership and capability announcements. F8.1 added explicit durable provider approval. F8.2 connected that approval to expiring live resource state. F8.3 bound approved and currently healthy remote language-model providers to the existing F7.7 runtime. F8.4 binds approved and currently healthy compute providers to explicit locally installed handler inventories while preserving the existing F7.4 dispatch, F7.3 ownership, and F7.5 lifecycle authority boundaries.

## Fixed F8 boundaries

- The coordinator remains authoritative for enrolled nodes, session membership, capability announcements, revocation, connectivity, and ordered session events.
- An announced capability is discoverable metadata only. It is not automatically approved, schedulable, callable, or authorized to access data.
- Provider approval is explicit, durable, revision-fenced, session-bound, and auditable.
- Approval grants no storage leadership, storage read/write access, artifact access, job ownership, shell execution, endpoint publication, or permission to install code.
- F7 provider selection still requires a fresh `ProviderResourceReport`; enrollment state is not a substitute for live resource state.
- Private endpoints, credentials, physical addresses, backend paths, and secret material must not enter announcements, enrollment records, health records, invocation frames, status surfaces, events, or logs.
- Existing local Flask, recorder, JSONL, storage, relay, direct transport, Ollama setup, and F7 runtime behavior remain compatible.
- Unknown protocol major versions, cross-session identities, stale generations, stale revisions, unauthenticated actors, and mismatched relay routes fail closed.
- F8 never transfers arbitrary executable code, module paths, package specifications, shell commands, environment variables, or process-launch instructions between nodes.

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

## F8.3: trusted remote language-model adapter binding — complete

Delivered on `main` by PR #155:

- versioned bounded remote AI request and response contracts containing only logical identities, request content, outcomes, and safe error fields;
- authenticated relay request/reply with exact bidirectional actor, session, target, provider, capability, generation, and request binding;
- provider-side and requester-side revalidation against current F8.1 enrollment and F8.2 health;
- provider-generation fencing, bounded pending requests, duplicate suppression, timeout and cancellation seams, and safe error translation;
- a remote `LanguageModelProvider` adapter without endpoint or credential fields;
- dynamic F8.2 report sourcing through the existing F7.7 selection and fallback rules;
- coexistence of local and multiple remote language-model providers.

F8.3 does not activate compute workers or transfer executable code.

## F8.4: trusted compute-worker inventory and activation

Deliver:

- a versioned local compute-handler descriptor containing a logical handler ID, capability type, protocol, protocol version, safe offered attributes, and an immutable descriptor fingerprint;
- a bounded in-process handler inventory that accepts only explicitly supplied local `CapabilityHandler` objects and rejects duplicate or conflicting descriptors;
- no reflection, dynamic imports, module paths, package names, source text, bytecode, binaries, shell commands, process launchers, environment variables, or executable payloads in inventory descriptors or federated records;
- a compute activation authority that revalidates current F8.1 enrollment, current F8.2 health, active node membership, announcement identity, provider generation, report revision, capability type, protocol, protocol major, and local node identity;
- exact binding between one current F8.2 compute-provider report and one locally installed descriptor selected by a logical `handler_id` advertised in safe report attributes;
- descriptor/report compatibility checks so the local descriptor's offered attributes satisfy every advertised health attribute except F8-reserved activation metadata;
- a generation-fenced activated worker wrapper around the existing F7.4 `CapabilityWorker` and `SQLiteDispatchInbox`;
- revalidation immediately before each dispatch reaches the local handler, so expiry, suspension, revocation, announcement change, membership removal, node revocation, generation change, report revision change, handler removal, or descriptor replacement prevents new execution;
- deterministic activation of multiple approved and healthy compute providers on one node when each maps to a distinct locally installed handler descriptor;
- explicit endpoint registration and removal helpers that add only activated workers to the existing `RelayDispatchEndpoint` without altering its authenticated request/reply protocol;
- preservation of F7.3 ownership leases, F7.4 dispatch identity and duplicate suppression, F7.5 retry/cancellation/result fencing, and F7.6 artifact authorization;
- focused Linux and Windows CI plus complete affected F8.1, F8.2, F7.3-F7.6, relay, and Phase 2 regressions.

F8.4 does not:

- download, install, compile, import, transmit, or execute code supplied by another node;
- accept a module path, executable path, shell command, container image, package coordinate, URL, private address, credential, token, environment variable, or backend path as a handler identity;
- infer handler availability from announcements or durable approval without a current F8.2 report and a matching local inventory entry;
- let a remote actor choose an arbitrary local handler or mutate the local inventory;
- create ownership, bypass lease fencing, broaden artifact grants, grant storage authority, or change F7 provider selection;
- add process isolation, container sandboxing, public untrusted execution, marketplace behavior, operator UI, or reconnect reconciliation;
- start F8.5 or later work.

Exit criteria:

- an unapproved, suspended, revoked, removed, expired, superseded, cross-session, protocol-incompatible, announcement-incompatible, or generation-mismatched compute provider cannot activate or execute;
- only the provider's authenticated local node can bind its health record to a local handler inventory;
- activation requires a current F8.2 report whose safe `handler_id` names exactly one preinstalled local descriptor;
- descriptor identity, capability type, protocol, protocol major, and offered attributes match the current health record;
- descriptors and health records contain no executable code, module or executable paths, package or image references, shell commands, endpoints, credentials, tokens, environment variables, backend paths, or authority scopes;
- replacing or removing a descriptor invalidates the prior activation before the next local handler call;
- a higher provider generation or report revision fences an older activation before local execution;
- health expiry, enrollment suspension/revocation, membership removal, node revocation, or announcement removal/change prevents subsequent dispatch without deleting durable approval;
- two separately approved and healthy compute providers can activate distinct local handlers and remain independently addressable by provider ID;
- an identical duplicate dispatch still executes at most once through the existing durable F7.4 inbox, while conflicting reuse fails closed;
- F7 ownership, dispatch, lifecycle, cancellation, retry, result, artifact, and storage authority boundaries remain unchanged;
- complete focused tests, compilation, Ruff, Compose validation, and diff hygiene pass on Linux and Windows.

## Proposed later F8 sequence

These steps are planning boundaries only. They are not authorized by the F8.4 pull request.

### F8.5: operator status and control surface

Expose safe discovery, approval, suspension, revocation, health, expiry, generation, activation, inventory compatibility, and reason codes without exposing secrets, private locations, or handler implementation details.

### F8.6: reconnect and restart reconciliation

Rebuild approved runtime candidates and compute activations after coordinator, provider, or client restart; replay ordered session changes; and fence stale provider generations and descriptor revisions.

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
