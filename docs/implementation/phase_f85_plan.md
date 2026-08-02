# Phase F8.5 plan: operator federation status and control

Status: F8.1 through F8.4 are complete on `main`. Only F8.5 is authorized by this branch.

Baseline: `main` at `99bbe25d23bb160ad57c821ae275145dee9addb0` after F8.4 trusted compute-worker activation.

## Purpose

F8.5 exposes the existing trusted-provider authority chain to a local operator without creating a second source of truth. Discovery remains owned by the Phase 2 coordinator, durable decisions remain owned by F8.1 enrollment, live state remains owned by F8.2 health, remote AI eligibility remains owned by F8.3, and compute compatibility remains owned by F8.4.

The operator surface is an authority-neutral projection and command adapter. It does not persist an independent provider, health, activation, job, artifact, or storage state.

## Delivery

- a versioned, immutable provider operator snapshot containing only safe logical identity, announcement status, enrollment state/revision, health state/reason, expiry, generation, report revision, bounded resource counters, activation eligibility, inventory compatibility, allowed actions, and safe reason codes;
- actor-authorized session listing that repeats coordinator membership checks before returning any state;
- owner-only approval, suspension, revocation, and reconciliation commands delegated to the existing F8.1 service with expected-revision fencing and idempotent command IDs;
- no direct mutation of coordinator announcements, health records, handler inventory, AI adapters, workers, job ownership, artifacts, or storage;
- language-model activation eligibility derived from the F8.3 `RemoteAIHealthAuthority` contract;
- compute activation and local inventory compatibility derived from the F8.4 `ComputeWorkerActivationAuthority` contract;
- safe handling of announced-but-not-requested, pending, approved, suspended, revoked, absent-health, expired, superseded, incompatible, and current states;
- a local Flask HTML surface and JSON API under `/provider-federation`;
- server-side binding of actor/session context through an injected operator surface; browser input cannot select or impersonate an actor node;
- POST-only state-changing routes with exact action allow-listing, expected revision, bounded safe reason codes, and generated idempotency IDs when none are supplied;
- graceful unavailable responses when no federation operator surface is configured;
- focused Linux and Windows CI plus complete affected F8.1-F8.4, F7, Flask, relay, and Phase 2 regressions.

## Safety boundaries

The surface must not emit announcement properties, health attributes, model prompts/results, local handler objects, module or executable paths, package or image references, endpoints, IP addresses, relay URLs, credentials, tokens, environment variables, backend paths, storage authority, artifact grants, audit command hashes, or internal exception messages.

The browser cannot supply `actor_node_id`, override `session_id`, write provider health, install/remove handlers, activate arbitrary code, create jobs, dispatch work, invoke models, change storage state, or bypass existing coordinator/enrollment revision checks.

## Exit criteria

- a non-member cannot read a session surface;
- a non-owner can read only member-authorized status and receives no management actions;
- an owner can request, approve, suspend, revoke, and reconcile only through the existing F8.1 service;
- stale expected revisions and reused command IDs with different content fail closed;
- status snapshots are deterministic and contain no private locations, secrets, backend paths, announcement properties, health attributes, or implementation details;
- health expiry and generation/report advancement update the displayed reason and activation eligibility without changing durable approval;
- compute inventory removal/replacement and descriptor incompatibility appear as safe incompatibility reason codes;
- language-model and compute providers can be displayed together and remain independently actionable;
- Flask HTML and JSON surfaces use the injected server-side actor/session context and expose no actor override;
- F8.1-F8.4 authority and F7 ownership, dispatch, lifecycle, artifact, AI, and storage boundaries remain unchanged;
- compilation, focused tests, Ruff, Compose validation, and diff hygiene pass on Linux and Windows.

## Deferred

F8.6 reconnect/restart reconciliation and F8.7 end-to-end closeout remain unstarted. Public relay deployment, anonymous participation, marketplace/payment, arbitrary code execution, untrusted-provider sandboxing, and production load/chaos/SLO acceptance remain outside F8.
