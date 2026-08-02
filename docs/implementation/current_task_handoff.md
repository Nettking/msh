# Current task handoff

Last updated: 2026-08-02 Europe/Oslo

## Repository state

- Repository: `Nettking/msh`
- Default branch: `main`
- Current development mode: Federation v1 release stabilization
- Runtime feature expansion: frozen until v1 closeout or separate owner approval
- Latest planning commit at this checkpoint: `48d0f52aa1000a50e0ccc0f78e00708ace8bc64b`

## Completed technical baseline

The federated implementation is complete through F8.7 and merged to `main`.

The validated baseline includes:

- authenticated node identity, sessions, membership, ordered events, replay, and revocation;
- storage primary/replica authority, replication, fencing, completeness-aware failover, and recovery;
- direct encrypted transport, relay fallback, rendezvous, and resumable verified transfer;
- multi-provider AI and compute scheduling, durable job ownership, dispatch, retry, cancellation, stale-worker fencing, and artifact authorization;
- explicit trusted-provider enrollment, expiring health, remote AI binding, compute activation, operator-safe projection, and restart reconciliation;
- final F8 closeout acceptance on Linux and Windows.

The implementation is now named **MSH Federation v1.0** for release-stabilization purposes. The `v1.0.0` release has not yet been published.

## Current objective

Complete the repository, documentation, product, and release cleanup required for a stable Federation v1.0 release.

Do not start additional federation features during this work.

## V1-A completed

The following planning documents are now canonical:

- `docs/releases/federation_v1_scope.md`
- `docs/implementation/federation_v1_repository_audit.md`
- `docs/implementation/federation_v1_closeout_plan.md`
- `docs/roadmap/post_v1_product_roadmap.md`

The roadmap explicitly preserves future work for:

- integrated documentation at `/docs` inside the existing Flask application;
- canonical user documentation structure;
- guided Federation UI;
- device, provider, storage, activity, and troubleshooting pages;
- later observability, network-operation, scheduling, organization, and less-trusted-provider phases.

The prototype under `new-stuff/md_viewer/` is therefore no longer the only record of the intended documentation reader behavior.

## Audit findings requiring follow-up

High-confidence cleanup candidates after final dependency verification:

- generated `graphify-out/` reports, manifests, and cache;
- the standalone `new-stuff/md_viewer/` prototype after its requirements were preserved in the roadmap;
- other unclassified files under `new-stuff/` after individual review;
- the stale phase-era handoffs and superseded implementation plans after historical decisions are separated.

Verify-before-deletion candidates:

- `catalog/webapp/`, which appears to duplicate the supported `catalog/flask_app/` surface;
- `legacy/`, because AI grounding and documentation still mention it;
- deprecated, manual, deep, and legacy runner scripts;
- old phase workflows that may still provide unique regression coverage.

No runtime file, existing documentation file, branch, workflow, or generated directory has been deleted during V1-A.

## Important safety decisions

- Never remove a candidate based only on its name or age.
- Check imports, dynamic discovery, setup, Compose, launch commands, tests, documentation links, AI indexing, and workflow references.
- Preserve final closeout and durable authority/security decisions as historical evidence.
- Do not weaken storage, provider, scheduling, artifact, or identity authority separation.
- Do not delete implementation branches without separate explicit owner approval.
- Do not tag `v1.0.0` before exact release acceptance.
- Do not pull post-v1 UI or documentation implementation into the cleanup phase without explicit approval.

## Current changes

Documentation-only commits on `main`:

- `7751ccaf8330f37b9fb5920c145e20242b504c3b` — temporary audit placeholder created during connector publishing;
- `c2f70269804cef03a9271b266d40c9a4b9ed6dbf` — immediately replaced the placeholder with the complete audit;
- `6abe2be72c57c0bebd7d7ec222de14d69a4e8b44` — Federation v1 scope;
- `f6a29d70207b80dffbda5d4db248f7cbbf06f29e` — Federation v1 closeout plan;
- `48d0f52aa1000a50e0ccc0f78e00708ace8bc64b` — post-v1 product roadmap.

No executable source or test behavior changed.

## Validation

This checkpoint changes Markdown documentation only. Runtime tests were not rerun for V1-A because no executable, dependency, setup, Compose, workflow, or runtime configuration file changed.

The later cleanup changes must run the checks specified in `federation_v1_repository_audit.md` and `federation_v1_closeout_plan.md`.

## Next exact action

Proceed with **V1-B: exact path-level cleanup manifest** only.

1. Inventory every file under `graphify-out/`, `new-stuff/`, `catalog/webapp/`, and `legacy/`.
2. Inventory stale implementation plans, handoffs, and phase workflows.
3. For each proposed deletion or relocation, record dependency evidence, replacement/migration, required tests, and decision.
4. Present the first deletion batch for owner review.
5. Do not delete anything during the manifest step.

## Resume safety

- Safe to resume: yes.
- Current technical baseline: Federation implementation complete through F8.7.
- Current work boundary: release stabilization and cleanup only.
- Next authorized unit: V1-B manifest, no deletions.
