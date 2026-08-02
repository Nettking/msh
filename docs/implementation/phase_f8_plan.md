# Phase F8 plan: productized trusted-provider federation

Status: F8.1 through F8.6 are complete on `main`. F8.7 end-to-end acceptance and closeout is the only authorized work in the current pull request.

Baseline: `main` at `10cd7dc38c33ebe1d855885b08aa7841c185649c` after F8.6 reconnect and restart reconciliation.

## Purpose

F7 completed deterministic capability scheduling, durable job ownership, authenticated dispatch, bounded retry and cancellation, capability-specific artifact authorization, and the logical multi-provider AI runtime. F8 turns those internal boundaries into a productized path for explicitly trusted session providers.

The Phase 2 coordinator remains authoritative for node identity, membership, announcements, revocation, connectivity, and ordered events. F8 adds explicit durable approval, authenticated expiring resource state, safe AI and compute runtime binding, an operator projection, and restart reconciliation without granting storage authority or accepting arbitrary remote code.

## Fixed F8 boundaries

- A capability announcement is discoverable metadata only. It is never automatic approval, live health, scheduling, invocation, compute activation, artifact access, or storage authority.
- Provider approval is explicit, durable, revision-fenced, session-bound, capability-bound, node-bound, type-bound, protocol-bound, and auditable.
- Approval grants no storage leadership, storage read/write access, artifact access, job ownership, shell execution, endpoint publication, code installation, or process-launch authority.
- F7 provider selection still requires a fresh `ProviderResourceReport`; enrollment state is not a substitute for live resource state.
- F7 remains authoritative for provider ranking, job ownership, leases, dispatch, cancellation, retry, stale-worker fencing, and result commit.
- F7.6 remains authoritative for least-privilege job-scoped artifact grants and verified result publication.
- Storage remains governed by its separate primary/replica, term, lease, fencing, acknowledgement, completeness, replication, and failover rules.
- Private endpoints, credentials, physical addresses, backend paths, prompts, results, handler implementations, and secret material must not enter announcements, enrollment records, health records, reconciliation checkpoints, operator surfaces, audit events, or logs.
- Existing local Flask, recorder, JSONL, Storage API, relay, direct transport, private Ollama setup, and F7 runtime behavior remain compatible.
- Unknown protocol majors, cross-session identities, stale generations, stale revisions, stale leases, removed membership, revoked nodes, unauthenticated actors, and mismatched relay routes fail closed.
- F8 never transfers arbitrary executable code, module paths, package specifications, container images, shell commands, environment variables, or process-launch instructions between nodes.

## F8.1: authorized discovery and durable provider enrollment — complete

Delivered by PR #153 at `9f63ce3417e573762b8ba0be64682aa5e428aa8c`:

- actor-authorized coordinator discovery within one session;
- versioned pending, approved, suspended, and revoked enrollment records;
- transactional SQLite persistence, expected-revision fencing, idempotent command replay, restart recovery, immutable identity binding, and safe append-only audit evidence;
- exact binding to session, capability ID, contributing node, capability type, protocol, and protocol major;
- deterministic support for several approved providers of the same capability type;
- fail-closed eligibility when announcements are removed, changed, disabled, revoked, cross-session, identity-conflicting, or protocol-incompatible.

F8.1 creates no live resource or runtime authority.

## F8.2: authenticated health and resource synchronization — complete

Delivered by PR #154 at `5f239c8f7f4325d9ae3ace2f5fbe17152e9293d8`:

- authenticated provider self-publication of short-lived F7 resource reports;
- exact enrollment, session, capability, node, type, protocol, generation, and report-revision binding;
- transactional latest-report persistence, safe audit evidence, idempotency, restart recovery, and natural expiry;
- current-report retrieval only after rechecking enrollment and coordinator announcement state;
- safe current, expired, superseded, enrollment-ineligible, announcement-ineligible, and absent observations;
- simultaneous same-type providers without primary/replica semantics.

F8.2 creates no ownership, transport, invocation, handler, artifact, or storage authority.

## F8.3: trusted remote language-model binding — complete

Delivered by PR #155 at `991f54d785b68f5f052acdaad74cf2a1fd5afec4`:

- versioned bounded logical invocation request and response frames;
- authenticated relay request/reply with exact bidirectional route identity;
- provider-side and requester-side enrollment, health, generation, report, and membership revalidation;
- bounded pending calls, duplicate suppression, timeout/cancellation seams, and safe error translation;
- remote `LanguageModelProvider` adapters without endpoint or credential fields;
- current F8.2 report use through the unchanged F7.7 selection and fallback algorithm;
- coexistence of local and multiple remote language-model providers.

F8.3 activates no compute worker and transfers no executable code.

## F8.4: trusted compute inventory and activation — complete

Delivered by PR #156 at `99bbe25d23bb160ad57c821ae275145dee9addb0`:

- versioned logical handler descriptors with immutable fingerprints;
- bounded in-process inventories containing only explicitly supplied local handler objects;
- exact enrollment, health, generation, report, descriptor, binding, node, type, protocol, and local-identity validation;
- activated wrappers around the existing F7.4 worker and durable duplicate-suppression inbox;
- revalidation immediately before every local handler call;
- deterministic activation of multiple current providers and exact endpoint replacement/removal helpers;
- preservation of F7 ownership, dispatch, retry, cancellation, result, artifact, and storage boundaries.

F8.4 performs no reflection, dynamic import, download, install, compilation, process launch, shell execution, package/image resolution, or remote handler selection.

## F8.5: operator status and control surface — complete

Delivered by PR #157 at `fe9e5fdc89cf28d55afeaf924f8a83bb1f92cf1d`:

- versioned safe provider snapshots combining discovery, enrollment, health, generation, expiry, activation, compatibility, and reason codes;
- member-authorized read-only session views;
- owner-only request, approve, suspend, revoke, and reconcile operations delegated to F8.1 with expected-revision and idempotency fencing;
- server-bound actor/session context and CSRF-protected local HTML and JSON routes;
- controlled unavailable behavior without exposing announcement properties, health attributes, model lists, handler identities, prompts, results, endpoints, credentials, private locations, artifact grants, or storage authority.

F8.5 is a projection and adapter, not an authority source.

## F8.6: reconnect and restart reconciliation — complete

Delivered by PR #158 at `10cd7dc38c33ebe1d855885b08aa7841c185649c`:

- restart-safe per-session/per-actor checkpoints containing only ordered event cursors and immutable logical fencing evidence;
- bounded contiguous coordinator event replay and repeated active-membership validation;
- deterministic reconstruction of current F8.3 AI adapters and F8.4 compute workers;
- exact generation, report, enrollment, handler binding, binding revision, and descriptor fingerprint fencing;
- atomic replacement of only reconciler-owned runtime entries while unrelated local entries remain untouched;
- idempotent unchanged-state reconciliation and rollback on partial reconstruction or checkpoint failure;
- persisted coordinator, enrollment, health, reconciliation, dispatch, and runtime reopen tests.

F8.6 never revives expired health, reopens revoked approval, infers capacity from announcements, or creates invocation, dispatch, artifact, or storage authority.

## F8.7: end-to-end acceptance and closeout — current

F8.7 must:

- exercise two trusted AI providers and two trusted compute providers in one persisted session;
- prove deterministic selection from current F8.2 reports and successful authenticated F8.3/F8.4 execution;
- prove duplicate suppression, suspension, revocation, generation/report advancement, expiry, interruption, restart reconciliation, and preservation of durable approval;
- consolidate the existing F7 ownership, cancellation, retry, stale-worker, result, artifact, and storage compatibility gates;
- verify the safe F8.5 projection and secret/private-location exclusion;
- record the exact delivered sequence, authority boundaries, compatibility impact, deferred operational work, and branch-cleanup policy;
- pass the complete F8, affected F7, relay, Flask, Phase 2, compilation, Ruff, Compose, and diff-hygiene matrix on Linux and Windows.

F8 is closed as a software implementation milestone only after the exact final F8.7 PR head is green and the closeout decision is merged.

## Explicitly outside F8

- internet-facing relay and rendezvous operations;
- physical unrelated-network and restrictive-NAT deployment acceptance;
- public or anonymous provider participation;
- marketplace, payment, billing, reputation, or dispute handling;
- arbitrary third-party code execution;
- production sandboxing, provenance, signing, and supply-chain policy for untrusted providers;
- full Kubernetes-style scheduling or replacement of established schedulers;
- production cost, latency, locality, energy, fairness, quotas, priorities, preemption, and heterogeneous-accelerator scheduling;
- production load, abuse, denial-of-service, soak, chaos, upgrade, observability, alerting, SLO, and incident-management acceptance;
- automatic public endpoint management or exposure of PostgreSQL, Ollama, Flask, relay, worker, or storage services.

No implementation branch is deleted by F8.7. Cleanup requires separate explicit repository-owner approval after merge and exact `main` verification.
