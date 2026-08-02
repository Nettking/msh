# Current task handoff

Last updated: 2026-08-02 Europe/Oslo

## Repository state

- Repository: `Nettking/msh`
- Default branch: `main`
- Current development mode: Federation v1 release stabilization
- Runtime feature expansion: frozen until v1 closeout or separate owner approval
- Latest planning commit at this checkpoint: `6fc2502ed0c39cdcdb5ead59a27c956c9831baf0`

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

The manifest records path classification, dependency evidence, replacement or migration, required tests, and decision for:

- generated `graphify-out/` files and cache;
- all identified `new-stuff/` experiment families;
- the archived `catalog/webapp/` Streamlit workspace;
- `legacy/` and ambiguous top-level `git/` markers;
- automatic, manual, deep, hidden, and legacy runner scripts;
- stale implementation plans and handoffs;
- durable closeouts, contracts, runbooks, and technical decisions;
- phase-specific GitHub Actions workflows.

No file, directory, workflow, branch, runtime source, dependency, setup path, or Compose configuration was deleted or changed during V1-B.

## Confirmed cleanup decisions

### Ready for the first deletion change

`graphify-out/**` is generated analysis output with no supported runtime role.

The first recommended deletion batch is limited to:

1. delete every tracked path under `graphify-out/**`;
2. add `/graphify-out/` to `.gitignore`;
3. remove only references that incorrectly treat generated reports as canonical documentation.

### Strong later deletion candidate

`catalog/webapp/**` is explicitly archived, outside the default runtime/dependency/operator path, and replaced by `catalog/flask_app/`.

It must still receive a dedicated deletion change with import, launcher, Compose, Flask-regression, documentation, and AI-index checks.

### Preserved future UI and docs work

`new-stuff/md_viewer/**` is a standalone prototype, not the production architecture. Its intended `/docs` and guided-UI behavior is preserved in `docs/roadmap/post_v1_product_roadmap.md` for V1.1.

The prototype can be removed later without losing the approved product direction.

### Experiments requiring owner preservation decision

The remaining `new-stuff/` image, Windows desktop, walkthrough, and goal-agent experiments are not Federation v1 product code. Before deletion, decide whether the useful code should be exported to a separate experiments repository.

`new-stuff/step2.py` also requires comparison with normal Flask tests so unique walkthrough expectations are not lost.

### Runner scripts

The following are intentionally supported and must not be deleted merely because they are manual or deep:

- `data_pr_day`;
- `find_stops`;
- `data_analysis`;
- `ml_analysis`.

`corrolation_machine_pairs` is the current runner-visible legacy deletion candidate and needs a focused usage check.

### Historical documentation

Completed plans and handoffs should be consolidated and removed only after unique durable decisions are mapped to closeouts, contracts, runbooks, or v1 reference documentation.

F6, F7, and F8 closeouts remain durable release evidence and should be archived rather than deleted.

### Workflows

Phase workflows remain in place. A permanent v1 validation workflow may replace them only after exact test, OS, service, Ruff, Compose, and diff-hygiene coverage is mapped and proven equivalent.

## Safety decisions

- Never remove a candidate based only on its name or age.
- Every deletion change must attach an exact tracked-file inventory and reference search.
- Preserve authority, fencing, recovery, scheduling, artifact, and identity boundaries.
- Do not delete implementation branches without separate explicit owner approval.
- Do not tag `v1.0.0` before exact release acceptance.
- Do not implement post-v1 UI or documentation features during cleanup unless separately approved.
- Workflow consolidation must not reduce validation coverage merely to reduce file count.

## Validation

V1-B changed Markdown documentation only.

Runtime tests were not rerun because no executable source, dependency, setup, Compose, workflow, or runtime configuration changed.

Repository evidence was checked through current files, commit history, code search, archived-module declarations, root requirements, AI indexing behavior, script catalog classification, and workflow inventory.

## Next exact action

Request owner approval for **V1-C cleanup batch 1 only**:

- remove tracked `graphify-out/**`;
- add `/graphify-out/` to `.gitignore`;
- run the low-risk reference and smoke validation documented in the cleanup manifest;
- do not touch `new-stuff/`, `catalog/webapp/`, old plans, workflows, branches, or runtime code in that batch.

## Resume safety

- Safe to resume: yes.
- Current technical baseline: Federation implementation complete through F8.7.
- Current work boundary: release stabilization and cleanup only.
- V1-A: complete.
- V1-B: complete.
- Next proposed unit: V1-C batch 1, pending explicit approval.
