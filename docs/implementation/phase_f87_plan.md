# Phase F8.7 plan: end-to-end acceptance and closeout

Status: F8.1 through F8.6 are complete on `main`. Only F8.7 acceptance and closeout are authorized by this branch.

Baseline: `main` at `10cd7dc38c33ebe1d855885b08aa7841c185649c` after F8.6 reconnect and restart reconciliation.

## Purpose

F8.7 validates the complete trusted-provider federation path as one software milestone and records the final closeout decision. It combines the existing Phase 2 coordinator, F8.1 enrollment, F8.2 health, F8.3 remote AI binding, F8.4 compute activation, F8.5 operator projection, F8.6 reconciliation, and the unchanged F7 scheduling, ownership, dispatch, lifecycle, artifact, and AI-runtime boundaries.

The closeout must prove that several explicitly trusted AI and compute providers can coexist across persisted nodes, survive interruption and restart, and fail closed on expiry, suspension, revocation, stale generation, stale report revision, membership loss, cancellation, or invalid artifact authority. It must not introduce a new runtime authority, provider type, transport, public deployment path, scheduler, marketplace, or arbitrary-code mechanism.

## Delivery

- one end-to-end acceptance scenario using persisted coordinator, enrollment, health, job, artifact, dispatch, and reconciliation state;
- at least two trusted language-model providers and two trusted compute providers in one session;
- authenticated session relay paths and exact provider, capability, generation, report-revision, job, attempt, lease, and artifact bindings;
- deterministic F7 provider selection and fallback with current F8 health;
- successful remote AI invocation and compute dispatch through the existing F8.3 and F8.4 adapters;
- cancellation and stale-worker fencing through the existing F7 lifecycle authority;
- least-privilege artifact authorization and rejection of cross-session or stale-lease access;
- coordinator, requester, provider, runtime, and reconciliation reopen from persisted state;
- interruption, health expiry, enrollment suspension/revocation, provider-generation advancement, report-revision advancement, member removal, and restart reconciliation coverage;
- safe operator status evidence without secrets, private locations, provider implementation details, prompts, results, handler paths, artifact grants, or storage authority;
- a final F8 closeout document mapping F8.1-F8.7 commits, acceptance evidence, authority boundaries, compatibility impact, deferred operational work, and preserved branch references;
- a consolidated Linux and Windows workflow running the complete F8, affected F7, relay, Flask, Phase 2, and repository-hygiene gates.

## Safety boundaries

F8.7 may compose and validate existing authorities, but it cannot approve providers outside F8.1, publish or revive health outside F8.2, bypass F8.3 or F8.4 binding checks, mutate F8.5 projection rules, advance F8.6 checkpoints without successful reconstruction, assign jobs outside F7.3, dispatch outside F7.4, alter cancellation or retry outside F7.5, broaden artifacts outside F7.6, or bypass F7.7 selection and fallback.

The closeout must not add arbitrary executable payloads, module paths, package or image references, shell commands, process launchers, environment variables, public endpoints, private addresses, credentials, tokens, backend paths, storage leadership, anonymous participation, billing, marketplace behavior, or untrusted-provider sandboxing.

## Exit criteria

- two trusted AI providers and two trusted compute providers can remain independently approved, healthy, discoverable, and addressable in one persisted session;
- remote AI and compute requests use current F8.2 resource state and the unchanged F7 selection policy;
- successful execution preserves exact route, provider, capability, generation, report-revision, ownership, attempt, lease, and artifact fencing;
- cancellation, worker loss, stale completion, duplicate delivery, and retry remain bounded by F7;
- artifact grants are least-privilege, short-lived, session-bound, attempt-bound, lease-bound, and rejected after revocation or stale ownership;
- restart reconciliation rebuilds only current bindings and preserves unrelated local providers and workers;
- health expiry, suspension, revocation, announcement or membership loss, generation advancement, report revision advancement, and handler replacement remove or replace stale bindings before the next invocation or dispatch;
- durable F8.1 approval is not deleted merely because live health expires;
- operator status remains a safe projection and cannot become an authority source;
- no serialized contract, checkpoint, event, status, log, or closeout evidence exposes secrets, private locations, executable implementation details, prompts, model results, job payloads, artifact grants, or storage authority;
- complete F8.1-F8.7, affected F7, relay, Flask, Phase 2, compilation, Ruff, Compose, and diff-hygiene gates pass on Linux and Windows;
- the closeout document explicitly distinguishes software implementation completion from deferred production, internet-facing, untrusted-execution, load, chaos, SLO, marketplace, and advanced-scheduling acceptance.

## Deferred beyond F8

- internet-facing relay and rendezvous operation;
- physical unrelated-network and restrictive-NAT deployment acceptance;
- anonymous or public provider participation;
- marketplace, payment, billing, reputation, and dispute handling;
- arbitrary third-party code execution and production sandboxing;
- provider supply-chain, provenance, package, image, and signing policy;
- production cost, latency, locality, energy, fairness, quotas, priorities, preemption, and heterogeneous-accelerator scheduling;
- production load, abuse, denial-of-service, soak, chaos, upgrade, observability, alerting, SLO, and incident-management acceptance;
- automatic public endpoint management or exposure of PostgreSQL, Ollama, Flask, worker, relay, or storage services.

No implementation branch may be deleted as part of F8.7. Branch cleanup requires separate explicit owner approval after the closeout merge and exact `main` verification.
