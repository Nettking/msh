# Current task handoff

Last updated: 2026-08-02 Europe/Oslo

## Repository state

- Repository: `Nettking/msh`
- Default branch: `main`
- Current development mode: Federation v1 release stabilization
- Runtime feature expansion: frozen until v1 closeout or separate owner approval
- Latest cleanup commit: `5cb2f780c4f6748f0232dd19a4a151010ec8d3f2`

## Completed technical baseline

The federated implementation is complete through F8.7 and merged to `main`.

The validated baseline includes:

- authenticated node identity, sessions, membership, ordered events, replay, and revocation;
- storage primary/replica authority, replication, fencing, completeness-aware failover, and recovery;
- direct encrypted transport, relay fallback, rendezvous, and resumable verified transfer;
- multi-provider AI and compute scheduling, durable job ownership, dispatch, retry, cancellation, stale-worker fencing, and artifact authorization;
- explicit trusted-provider enrollment, expiring health, remote AI binding, compute activation, operator-safe projection, and restart reconciliation;
- final F8 closeout acceptance on Linux and Windows.

The implementation is named **MSH Federation v1.0** for release-stabilization purposes. The `v1.0.0` release has not yet been published.

## Current objective

Complete the repository, documentation, product, and release cleanup required for a stable Federation v1.0 release.

Do not start additional federation features during this work.

## Canonical stabilization documents

- `docs/releases/federation_v1_scope.md`
- `docs/implementation/federation_v1_repository_audit.md`
- `docs/implementation/federation_v1_closeout_plan.md`
- `docs/implementation/federation_v1_cleanup_manifest.md`
- `docs/roadmap/post_v1_product_roadmap.md`

The roadmap preserves future work for integrated `/docs`, canonical user documentation, guided Federation UI, device/provider/storage/activity/troubleshooting pages, and later federation expansion.

## V1-A completed

V1-A established the v1 scope, repository audit, closeout plan, future roadmap, and current stabilization handoff.

No runtime behavior or existing path was removed.

## V1-B completed

Commit:

- `6fc2502ed0c39cdcdb5ead59a27c956c9831baf0` — exact cleanup manifest and deletion-batch ordering.

The manifest records path classification, dependency evidence, replacement or migration, required tests, and decision for generated output, experiments, archived UI, legacy markers, scripts, phase documentation, and workflows.

## V1-C batch 1 completed

Commit:

- `5cb2f780c4f6748f0232dd19a4a151010ec8d3f2` — remove all tracked `graphify-out/**` content and add `/graphify-out/` to `.gitignore`.

Validation performed:

- the complete commit comparison contains only removal of `graphify-out/**` plus one `.gitignore` line;
- a direct fetch of `graphify-out/GRAPH_REPORT.md` now returns not found;
- `/graphify-out/` is present in `.gitignore`;
- no runtime source, dependency, setup path, Compose configuration, workflow, test, or supported documentation file changed;
- no runtime tests were rerun because the removed files were generated analysis output outside the runtime and test paths.

Graphify remains usable locally, but newly generated `graphify-out/` content will remain untracked.

## Confirmed cleanup decisions

### Completed deletion

- `graphify-out/**` — deleted and ignored.

### Next deletion-list group for owner review

`new-stuff/md_viewer/**` is a standalone Flask prototype, not the production documentation architecture.

Its intended product direction is already preserved in `docs/roadmap/post_v1_product_roadmap.md`:

- integrated `/docs` inside the existing Flask application;
- canonical content from repository `docs/`;
- guided Federation UI and user-facing navigation;
- no separate Flask process.

No `new-stuff/md_viewer/**` path may be deleted until the owner explicitly approves this group after review.

### Strong later deletion candidate

`catalog/webapp/**` is explicitly archived, outside the default runtime/dependency/operator path, and replaced by `catalog/flask_app/`. It requires a dedicated review and validation batch.

### Experiments requiring owner preservation decision

The remaining `new-stuff/` image, Windows desktop, walkthrough, and goal-agent experiments are not Federation v1 product code. Before deletion, decide whether useful code should be exported to a separate experiments repository.

### Runner scripts

The supported manual and deep analysis scripts remain in v1. `corrolation_machine_pairs` requires a focused later usage review.

### Historical documentation and workflows

Completed plans require unique-decision comparison before deletion. Durable closeouts and technical decisions must be archived or consolidated. Phase workflows remain until a permanent v1 gate proves equivalent coverage.

## Safety decisions

- Never remove a candidate based only on its name or age.
- Every deletion group is reviewed with the owner before implementation.
- Preserve authority, fencing, recovery, scheduling, artifact, and identity boundaries.
- Do not delete implementation branches without separate explicit owner approval.
- Do not tag `v1.0.0` before exact release acceptance.
- Do not implement post-v1 UI or documentation features during cleanup unless separately approved.

## Next exact action

Review **`new-stuff/md_viewer/**` only** with the owner:

1. verify the exact prototype file list;
2. confirm that all useful product requirements are preserved in the post-v1 roadmap;
3. decide whether any visual or implementation detail needs to be extracted before deletion;
4. do not delete the prototype until explicit approval is given.

## Resume safety

- Safe to resume: yes.
- Current technical baseline: Federation implementation complete through F8.7.
- Current work boundary: release stabilization and cleanup only.
- V1-A: complete.
- V1-B: complete.
- V1-C batch 1: complete.
- Next proposed unit: review `new-stuff/md_viewer/**`, no deletion without approval.
