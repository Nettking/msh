# Current task handoff

Last updated: 2026-08-02 Europe/Oslo

## Repository state

- Repository: `Nettking/msh`
- Default branch: `main`
- Current planning branch: `agent/capability-first-onboarding-plan`
- Technical federation baseline: complete through F8.7
- Latest merged product change: `41934cd3b3907c4847fb788a3fd9f9647e165969` — integrated repository documentation browser
- Published release tag: not yet created

## Completed baseline

The validated federation implementation includes:

- authenticated persistent node identity, membership/session compatibility, ordered events, replay, and revocation;
- storage primary/replica authority, replication, fencing, completeness-aware failover, and recovery;
- direct encrypted transport, relay fallback, rendezvous, and resumable verified transfer;
- multi-provider AI and compute scheduling, durable job ownership, dispatch, retry, cancellation, stale-worker fencing, and artifact authorization;
- trusted-provider enrollment, expiring health, remote AI binding, compute activation, operator-safe projection, and restart reconciliation;
- Linux and Windows federation acceptance.

## Stabilization and documentation work completed

- V1-A audit, scope, closeout, roadmap, and handoff foundation;
- V1-B cleanup manifest;
- Graphify generated-output deletion and ignore rule;
- integrated `/docs` reader in the normal Flask application;
- manual `/docs` acceptance on the owner's laptop: passed;
- the standalone Markdown prototype is now superseded and remains only until a separate deletion change.

## Product-direction decision

The repository owner has explicitly replaced role-first setup as the planned product direction.

New product model:

- every installation is one persistent MSH device;
- a device may contribute several capabilities simultaneously;
- setup discovers or creates a federation before asking for contributions;
- MSH inspects the device and runs suitable bounded benchmarks;
- the user enables any combination of recommended contributions;
- returning trusted devices reconnect automatically;
- storage primary/replica, job owner, membership administration, leases, fencing, and artifact grants remain internal authority states rather than device identities.

The internal `session_id` boundary remains for compatibility. The UI uses Federation as the product concept.

## Authoritative active plan

- `docs/implementation/capability_first_federation_plan.md`

The plan defines:

- discovery, verification, join, reconnect, and local federation creation;
- device inspection;
- versioned AI, compute, storage, network, and data-source benchmarks;
- contribution candidates and contribution intents;
- legacy deployment-mode migration;
- Federation information architecture;
- CF0-CF8 implementation sequence;
- explicit parallel-agent file ownership and merge ordering;
- Linux/Windows, migration, security, restart, and end-to-end acceptance.

## Plans adjusted

The following documents are aligned with the new direction:

- `docs/roadmap/post_v1_product_roadmap.md`;
- `docs/implementation/federation_v1_closeout_plan.md`;
- `docs/releases/federation_v1_scope.md`;
- this handoff.

The previous roadmap assumptions about a user choosing one role, manually creating/resuming a technical session, and treating provider approval as the primary setup journey are superseded.

## Parallel-agent strategy

Parallel work begins only after CF1 contracts merge.

Wave 1 can use three agents concurrently on disjoint paths:

1. benchmark/inspection engine;
2. federation discovery and verified-join adapter;
3. new onboarding/Federation UI templates, CSS, and JavaScript only.

Wave 2 can use two agents concurrently:

1. contribution recommendation/activation service;
2. safe Federation projections.

One later integration agent exclusively owns shared files such as:

- `catalog/flask_app/app.py`;
- `catalog/flask_app/routes.py`;
- `catalog/flask_app/server_setup_routes.py`;
- `catalog/flask_app/services/server_setup_service.py`;
- `catalog/flask_app/templates/base.html`;
- `catalog/flask_app/templates/startup.html`;
- `setup_msh.py`;
- `.env.example`.

An independent acceptance agent owns final migration fixtures, CI additions, and end-to-end validation.

## Current exact action

Proceed with **CF1 only**:

1. add pure versioned onboarding contracts;
2. add `federation_id` to internal-session compatibility mapping;
3. add a read-only migration preview from every existing deployment mode;
4. prove that migration never silently enables a new contribution;
5. add malformed-state, serialization, compatibility, and migration tests;
6. do not change the current setup UI;
7. merge CF1 before creating the parallel Wave 1 branches.

## Do not do yet

- do not remove `session_id` from protocols or persistence;
- do not replace the current setup UI before CF1-CF4 services exist;
- do not let benchmark success grant authority;
- do not enable storage or compute contributions automatically during migration;
- do not let multiple agents edit shared Flask/setup files concurrently;
- do not delete the old role-first path until CF7 acceptance passes;
- do not mix unrelated cleanup into CF1.

## Resume safety

- Safe to resume: yes.
- Technical baseline: complete through F8.7.
- `/docs`: implemented, CI validated, and manually accepted.
- Capability-first direction: approved and planned.
- Runtime implementation of capability-first onboarding: not started.
- Next unit: CF1 contracts and migration preview only.
