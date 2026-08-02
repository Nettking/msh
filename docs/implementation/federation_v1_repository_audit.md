# Federation v1 repository audit

Status: approved V1-A audit; no runtime code or existing files are removed by this document.

Baseline: `main` after Phase F8 closeout and the addition of the experimental Markdown viewer under `new-stuff/`.

## Purpose

This audit establishes the cleanup boundary for **MSH Federation v1.0**. The goal is to turn the completed federation implementation into one stable, supportable release before adding more federation features.

The audit separates the repository into five classes:

1. current production implementation;
2. canonical v1 documentation;
3. historical evidence worth preserving;
4. generated or experimental material that should leave the production tree;
5. uncertain material that requires dependency proof before deletion.

This document authorizes classification and follow-up verification only. Deletion, movement, workflow consolidation, UI changes, dependency changes, protocol renaming, branch deletion, tagging, and release publication require separate changes and validation.

## V1 baseline finding

The federation implementation is technically complete through F8.7:

- Phase 2 provides identity, sessions, membership, ordered events, relay messaging, and revocation;
- storage federation provides primary/replica assignments, replication, fencing, completeness-aware failover, and recovery;
- F6 provides direct encrypted transport, relay fallback, rendezvous, and resumable verified transfer;
- F7 provides capability-specific scheduling, job ownership, dispatch, retry, cancellation, artifact authorization, and multi-provider AI runtime;
- F8 provides explicit trusted-provider enrollment, expiring health, remote AI binding, compute activation, operator projection, restart reconciliation, and consolidated acceptance.

The repository is therefore ready for release stabilization, but not yet ready to be called a clean product release. Documentation still describes several completed phases as current work, generated analysis output is committed, experimental programs share the production tree, multiple UI implementations coexist, and the permanent v1 operator path is not documented as one coherent workflow.

## Classification rules

### Keep

A file or directory remains in the v1 production tree when it is used by a supported runtime, installation path, test gate, operational procedure, or current user workflow.

### Consolidate

Material is consolidated when several files describe the same public behavior or when phase-specific evidence should become one stable v1 document.

### Archive

Material is archived only when it records a durable architecture or security decision that is still useful for maintenance, research traceability, or regression interpretation.

Archive is not a default alternative to deletion. Temporary agent instructions and implementation checkpoints should not be archived merely because they once existed.

### Delete

A file is deleted when it is generated output, an isolated experiment, obsolete duplicated implementation, stale handoff material, or a superseded plan with no remaining operational or architectural value.

### Verify before deletion

A candidate remains until all relevant checks show that it is not imported, started, packaged, referenced by Compose, indexed intentionally, linked from canonical docs, or required by tests.

## Keep: production implementation

The following package families are part of the v1 implementation boundary and must remain unless a later refactor proves a narrower replacement:

- `catalog/federation/` — federation contracts, coordinator, storage authority, replication, completeness, promotion, transport integration, and recovery;
- `catalog/node/` — persistent node identity and outbound federation client;
- `catalog/relay/` — authenticated relay service and relay-backed federation routes;
- `catalog/storage/` — logical storage contracts, clients, providers, replication, and artifacts where present;
- `catalog/capabilities/` — job, provider, selection, dispatch, lifecycle, enrollment, health, activation, operator, and reconciliation boundaries;
- `catalog/ai/` — local and federated AI runtime, provider adapters, grounding, and safe selection;
- `catalog/flask_app/` — current Flask-first application and supported operator surface;
- `catalog/orchestrator/`, `catalog/runner/`, `catalog/common/`, recorder packages, source connectors, setup code, Compose configuration, and supported launch scripts;
- current tests proving the above boundaries on Linux and Windows.

The v1 cleanup must not weaken authority separation. Provider approval, health, scheduling, execution, artifacts, and storage leadership remain independent authorities.

## Keep and rewrite: canonical documentation

The following subjects need one maintained v1 version:

- repository `README.md`;
- quick start;
- server setup and supported roles;
- operator guide;
- federation v1 overview;
- create/join/administer federation;
- storage provider guide;
- AI provider guide;
- compute provider guide;
- security and trust assumptions;
- backup, restart, recovery, and upgrade guide;
- troubleshooting;
- protocol and compatibility reference;
- developer architecture and test guide;
- release notes and changelog.

Existing documents may supply source material, but public v1 documentation must describe the final system rather than narrate implementation phases.

## Preserve as historical decision evidence

The following classes should be retained in a dedicated history/decision area after consolidation:

- final closeout decisions for the major implemented boundaries, especially F6, F7, and F8;
- authoritative federation contracts and acceptance matrix when they still explain stable invariants;
- design records for storage promotion, completeness-aware failover, fencing, and recovery;
- security decisions that explain why announcements, health, execution, artifacts, and storage authority are separated;
- the original high-level federated-session architecture, after marking it as implemented and mapping it to v1.

Historical documents must carry a banner stating that they are implementation history, not current setup instructions.

## Consolidate or replace

### `docs/implementation/current_task_handoff.md`

Replace immediately. It still presents Phase E work as current and is not a valid repository handoff after F8 closeout.

### Phase plans under `docs/implementation/`

Most `*_plan.md`, per-step implementation notes, temporary handoffs, and checkpoint documents should not remain in the default documentation navigation.

For every phase document:

- retain final closeout and enduring design decisions;
- merge stable invariants into the federation v1 architecture/reference docs;
- delete superseded step plans after link and citation verification;
- move retained history under an explicitly historical path.

### F6/F7/F8 workflows

The existing phase workflows are valuable evidence, but a permanent release should expose a stable `federation-v1` regression boundary rather than make completed development phases look ongoing.

Proposed later change:

- retain focused workflows only where they provide useful fault isolation;
- add one required v1 release workflow that invokes the complete permanent matrix;
- rename or document retained phase workflows as component regression suites;
- do not remove a workflow until the consolidated gate proves equivalent or stronger coverage.

### Root `README.md`

The README currently mixes user setup, architecture, route reference, AI details, repository map, cache internals, limitations, and developer guidance. Replace it with a concise product entry point and links to canonical documentation.

### `docs/operator_guide.md` and in-app guide

These contain useful user material but do not yet provide a complete Federation v1 journey. Preserve their operator-support content and later split role-specific and federation-specific guidance.

## Delete candidates: high confidence after final reference check

### `graphify-out/`

Classification: generated analysis output and cache.

Reason:

- contains reports, manifests, analysis JSON, and AST cache;
- should be reproducible rather than versioned as product source;
- currently adds repository noise and stale snapshots;
- no production dependency has been identified in the audit search.

Required deletion change:

- verify no workflow or documentation requires committed output;
- delete the directory;
- add `/graphify-out/` to `.gitignore`;
- document how to regenerate it only if the tool remains supported.

### `new-stuff/md_viewer/`

Classification: design prototype, not production implementation.

Reason:

- starts an independent Flask application on the same default port as MSH;
- reads its own sample `docs/` directory rather than the repository documentation;
- owns separate templates, static files, and dependency list;
- is not registered through the MSH application factory.

Required treatment:

- preserve the desired interaction and visual ideas in the future documentation/UI roadmap;
- implement the selected ideas later inside `catalog/flask_app/`;
- delete the prototype only after the roadmap records the required behavior.

### Other isolated files under `new-stuff/`

Classification: experiments requiring individual review.

Known examples include desktop-agent and step scripts. They must not be assumed to be supported MSH commands merely because they are committed.

Required treatment:

- inventory each file;
- identify an owner and supported purpose;
- move supported experiments to a clearly named `experiments/` area with their own README and exclusion from production packaging, or delete them;
- remove generated experiment output from Git and ignore it.

No `new-stuff/` file should remain in the v1 production root without an explicit classification.

## Verify-before-deletion candidates

### `catalog/webapp/`

The current product surface is `catalog/flask_app/`, while `catalog/webapp/` appears to contain an older parallel web implementation. Repository search found references concentrated inside that package rather than from the current Flask application.

Before deletion:

- search imports and module launch commands;
- inspect Compose, setup scripts, tests, and documentation;
- compare any unique behavior with `catalog/flask_app/`;
- migrate genuinely unique supported behavior;
- run the full Flask and setup regression suite.

Expected result: remove the duplicate package if it has no supported entry point.

### `legacy/`

The folder cannot be deleted as one unit yet. Current AI grounding and documentation intentionally mention legacy content.

Before deletion or reduction:

- inventory every file;
- identify AI-indexed paths;
- determine whether any file is current evidence, sample data, migration input, or merely obsolete notes;
- remove intentional indexing before deleting files;
- update documentation and tests together.

Expected result: retain only explicitly justified historical/reference material and remove generic legacy clutter.

### Deprecated runner and manual scripts

The README and operator guide identify deprecated, manual, deep, and legacy script paths. These need usage evidence before removal.

For each script:

- search runner metadata and dynamic discovery;
- search output consumers;
- inspect tests and documentation;
- decide whether it is supported, research-only, or obsolete;
- move research-only tools out of the default operator workflow.

### Old implementation documents

Many plans are safe deletion candidates, but links, AI grounding, issue references, and research traceability must be checked first. Closeout and decision documents are not automatically deletion candidates.

## Branch cleanup

F8 implementation branches remain on the remote. Branch deletion is outside V1-A.

After the v1 cleanup PRs merge and an exact `v1.0.0` tag is verified, present all obsolete implementation branches to the repository owner for separate approval. Never combine branch deletion with documentation or runtime cleanup.

## Required dependency checks for the cleanup PR

The later deletion PR must run at least:

```text
search imports and dynamic module names
search Compose services and command lines
search setup and launch scripts
search Flask blueprint registration and routes
search runner discovery and metadata
search AI grounding/indexed paths
search documentation links
search GitHub workflow paths
python compilation
complete applicable pytest suites
Ruff for changed Python boundaries
Docker Compose validation
link validation for canonical docs
git diff --check
```

## Proposed execution order

1. Define and publish Federation v1 scope and closeout criteria.
2. Replace the stale current handoff.
3. Create the future roadmap so prototype intent is not lost.
4. Build an exact path-level deletion manifest with dependency evidence.
5. Remove generated outputs and update `.gitignore`.
6. Remove or relocate experiments.
7. Consolidate historical implementation documentation.
8. Remove confirmed duplicate implementation packages.
9. Consolidate permanent v1 CI gates.
10. Rewrite public documentation.
11. Run clean-install and physical multi-node release acceptance.
12. Publish `v1.0.0` only after all exit criteria pass.

## Current decision

MSH Federation enters release stabilization. Feature expansion is frozen until v1 closeout. UI and documentation product improvements are intentionally preserved as future updates in the post-v1 roadmap rather than mixed into the initial repository audit or deletion change.
