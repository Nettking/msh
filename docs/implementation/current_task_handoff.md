# Current task handoff

Last updated: 2026-08-02 Europe/Oslo

## Repository state

- Repository: `Nettking/msh`
- Default branch: `main`
- Current development mode: Federation v1 release stabilization with one explicitly approved documentation feature
- Latest merge: `41934cd3b3907c4847fb788a3fd9f9647e165969` — integrated repository documentation browser
- Pull request: `#160` — merged
- Federation release name: **MSH Federation v1.0**
- Published `v1.0.0` release: not yet created

## Completed technical baseline

The federated implementation is complete through F8.7 and merged to `main`.

The validated baseline includes:

- authenticated node identity, sessions, membership, ordered events, replay, and revocation;
- storage primary/replica authority, replication, fencing, completeness-aware failover, and recovery;
- direct encrypted transport, relay fallback, rendezvous, and resumable verified transfer;
- multi-provider AI and compute scheduling, durable job ownership, dispatch, retry, cancellation, stale-worker fencing, and artifact authorization;
- trusted-provider enrollment, expiring health, remote AI binding, compute activation, operator-safe projection, and restart reconciliation;
- final federation acceptance on Linux and Windows.

## Stabilization work completed

### V1-A

Established the Federation v1 scope, repository audit, closeout plan, post-v1 roadmap, and stabilization handoff.

### V1-B

Commit `6fc2502ed0c39cdcdb5ead59a27c956c9831baf0` created the exact cleanup manifest and deletion-batch ordering.

### V1-C batch 1

Commit `5cb2f780c4f6748f0232dd19a4a151010ec8d3f2` removed all tracked `graphify-out/**` content and added `/graphify-out/` to `.gitignore`.

## Integrated documentation browser completed

The repository owner explicitly promoted the integrated documentation reader from the future roadmap into the current next step.

Merge commit:

- `41934cd3b3907c4847fb788a3fd9f9647e165969`

Implemented behavior:

- the existing MSH Flask application serves documentation at `/docs` on the normal port `5000`;
- the browser reads canonical Markdown from the repository root `docs/` directory;
- it does not run a second Flask server or use the prototype's sample documentation folder;
- navigation follows the real nested directory structure;
- the first H1 is used as the display title where available;
- active files and ancestor folders are marked and expanded;
- document pages include breadcrumbs and an on-page heading index;
- tables, fenced code, headings, lists, blockquotes, and admonitions render as HTML;
- relative links between Markdown files are rewritten to `/docs/...` routes;
- supported local images are served through a restricted documentation asset route;
- traversal outside the documentation root is rejected;
- symlink escapes and arbitrary file serving are not permitted;
- the interface is responsive and includes a mobile documentation drawer;
- no external font or CDN is required;
- documentation remains available while MSH is waiting for its runtime startup choice;
- `start.cmd` displays `http://localhost:5000/docs` after startup;
- the normal Docker image includes `/app/docs` because the repository is copied into the image.

Production files:

- `catalog/flask_app/docs_routes.py`
- `catalog/flask_app/templates/docs_viewer.html`
- `catalog/flask_app/static/css/docs.css`
- `catalog/flask_app/static/js/docs-navigation.js`
- `catalog/flask_app/tests/test_docs_viewer.py`

Supporting changes:

- `catalog/flask_app/app.py` registers the documentation blueprint before the runtime gate;
- `requirements.txt` includes `Markdown>=3.5,<4`;
- `start.cmd` reports the documentation URL;
- the operator-surface workflow permanently runs the documentation and Windows-start regression tests.

## Validation completed

Operator-surface matrix:

- Ubuntu: 321 tests passed;
- Windows: 321 tests passed;
- Flask and affected package compilation: passed on Ubuntu and Windows;
- Ruff: passed on Ubuntu and Windows;
- Docker Compose validation: passed on Ubuntu and Windows;
- diff hygiene: passed on Ubuntu and Windows.

Broader federation matrix:

- Phase 2/federation Linux job: passed;
- Phase 2/federation Windows job: passed;
- direct-peer sidecar checks: passed;
- federation identity, protocol, relay, storage, and Phase F regressions: passed.

Manual browser acceptance on the owner's laptop remains pending.

## Prototype decision

`new-stuff/md_viewer/**` remains in the repository temporarily as the original design reference.

Do not delete it until the owner has tested the integrated `/docs` implementation and confirmed that the useful visual and interaction behavior has been retained. After acceptance, compare the integrated implementation against the eight prototype files and delete the prototype only if no unique behavior still needs preservation.

## Current exact action

Perform only the laptop acceptance test:

1. update the local `main` checkout;
2. rebuild and start the Flask container using the normal Windows startup path;
3. open `http://localhost:5000/docs`;
4. verify the real repository documentation tree, document rendering, nested navigation, mobile layout if relevant, and relative links;
5. record any concrete defect before beginning another cleanup group.

Do not continue to another deletion group until this manual acceptance result is known.

## Resume safety

- Safe to resume: yes.
- Current technical baseline: Federation implementation complete through F8.7.
- Graphify cleanup: complete.
- Integrated `/docs`: implemented, merged, and CI validated.
- Manual laptop acceptance: pending.
- Standalone Markdown prototype: retained pending acceptance.
- Next proposed unit after acceptance: decide whether `new-stuff/md_viewer/**` can be deleted.
