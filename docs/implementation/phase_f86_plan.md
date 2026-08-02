# Phase F8.6 plan: reconnect and restart reconciliation

Status: F8.1 through F8.5 are complete on `main`. Only F8.6 is authorized by this branch.

Baseline: `main` at `fe9e5fdc89cf28d55afeaf924f8a83bb1f92cf1d` after F8.5 operator federation surface.

## Purpose

F8.6 rebuilds trusted runtime bindings after coordinator, provider, requester, or local application restart without creating a new authority source. The coordinator event log remains authoritative for ordered session changes, F8.1 remains authoritative for durable approval, F8.2 remains authoritative for expiring live health, F8.3 remains authoritative for remote language-model binding, F8.4 remains authoritative for local compute activation, and F8.5 remains a read/control projection only.

Reconciliation may reconstruct adapters and workers from current authoritative state. It must never revive expired health, reopen revoked enrollment, infer live capacity from announcements, or preserve a stale provider generation, report revision, handler binding, or descriptor fingerprint.

## Delivery

- a versioned restart-safe reconciliation checkpoint per session and local actor, storing only the last applied authoritative session revision plus safe logical runtime binding evidence;
- paged replay of ordered coordinator session events from the durable checkpoint with contiguous revision validation and bounded replay limits;
- a reconciliation service that rechecks active membership before replay, before rebuilding, and before committing a new checkpoint;
- deterministic reconstruction of current F8.3 remote language-model adapters through `TrustedRemoteLanguageModelBinder` and registration through `ConfiguredLanguageModelRuntimeManager`;
- deterministic reconstruction of current F8.4 compute workers through `TrustedComputeWorkerBinder` and registration through the existing `RelayDispatchEndpoint` worker map;
- exact replacement/removal of only bindings previously owned by the reconciler, leaving unrelated local AI providers and compute workers unchanged;
- safe fencing evidence for AI provider generation and report revision, and for compute provider generation, report revision, handler binding revision, binding ID, and descriptor fingerprint;
- idempotent no-op reconciliation when the ordered event cursor and exact runtime evidence are unchanged;
- fail-closed rollback of newly installed runtime bindings if replay, rebuild, registration, validation, or checkpoint persistence fails;
- restart tests that reopen coordinator, enrollment, health, reconciliation, dispatch-inbox, and application-facing runtime state from persisted databases;
- reconnect tests for announcement change/removal, member removal, node revocation, enrollment suspension/revocation, health expiry, provider generation advancement, report revision advancement, local handler replacement/removal, and several simultaneous AI and compute providers;
- focused Linux and Windows CI plus complete affected F8.1-F8.5, F7, relay, Flask, and Phase 2 regressions.

## Safety boundaries

The checkpoint and reconciliation results must not contain announcement properties, health attributes, prompts, model results, handler objects, module or executable paths, package or image references, endpoints, IP addresses, relay URLs, credentials, tokens, environment variables, backend paths, storage authority, artifact grants, job payloads, or internal exception messages.

Reconciliation cannot approve, suspend, revoke, publish health, choose arbitrary handlers, install code, create jobs, assign ownership, dispatch work, invoke models, grant artifacts, mutate storage state, or bypass existing expected-revision, generation, report-revision, route, membership, duplicate-suppression, lifecycle, or authorization checks.

## Exit criteria

- a fresh process can reopen persisted coordinator, enrollment, health, and reconciliation stores and rebuild all currently valid remote AI adapters and local compute workers;
- expired or ineligible health is never rebuilt, while durable F8.1 approval remains unchanged;
- session events are replayed in exact contiguous revision order and the checkpoint advances only after successful runtime reconstruction;
- replay gaps, backward cursors, unsupported checkpoint schemas, cross-session records, actor mismatch, or excessive replay fail closed;
- a higher provider generation or report revision replaces and fences the older AI adapter or compute worker;
- handler removal, replacement, binding revision change, or descriptor fingerprint change removes the stale compute activation before subsequent dispatch;
- member removal, node revocation, announcement removal/change, enrollment suspension/revocation, and health expiry remove affected bindings without touching unrelated providers;
- several same-type providers can be rebuilt deterministically and remain independently addressable;
- repeated reconciliation with unchanged authoritative state is idempotent and does not duplicate providers or workers;
- checkpoint serialization and operator-visible results contain only safe logical IDs, revisions, generations, fingerprints, counts, and reason codes;
- F7 ownership, dispatch, retry, cancellation, result, artifact, AI selection, and storage boundaries remain unchanged;
- compilation, focused tests, Ruff, Compose validation, and diff hygiene pass on Linux and Windows.

## Deferred

F8.7 end-to-end acceptance and closeout remain unstarted. Public relay deployment, anonymous participation, marketplace/payment, arbitrary third-party code execution, untrusted-provider sandboxing, production load/chaos/SLO acceptance, and advanced scheduling remain outside F8.
