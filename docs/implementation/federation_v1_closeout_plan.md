# Federation v1 closeout plan

Status: approved stabilization plan. No new federation feature work is authorized until this closeout is complete or the repository owner explicitly changes the priority.

## Objective

Convert the completed federation implementation into a stable, documented, supportable **MSH Federation v1.0** release.

The closeout is a sequence of small, independently reviewable changes. Cleanup must never remove a path merely because it looks old. Every deletion requires dependency evidence and regression validation.

## Fixed decisions

- The completed federation work is named **MSH Federation v1.0**.
- F8.7 is the technical implementation baseline.
- V1 is for explicitly trusted devices and providers.
- Runtime authority and security boundaries from the completed phases remain unchanged.
- Feature expansion is frozen during closeout.
- UI and documentation product improvements are saved in the post-v1 roadmap and are not lost when prototypes or stale files are removed.
- No implementation branch is deleted without separate explicit owner approval.
- No release tag is created before exact release acceptance.

## Work packages

### V1-A — audit and release boundary

Deliverables:

- repository audit;
- v1 scope definition;
- closeout plan;
- post-v1 product roadmap;
- current handoff replacement;
- first deletion-candidate classification.

Exit criteria:

- no runtime behavior changed;
- no existing path deleted;
- future UI/docs intent is recorded;
- the next cleanup action is unambiguous.

### V1-B — exact path-level cleanup manifest

Deliver one table covering every proposed deletion or relocation:

| Path | Classification | Evidence | Replacement/migration | Required tests | Decision |
| --- | --- | --- | --- | --- | --- |

Minimum candidate groups:

- `graphify-out/`;
- all content under `new-stuff/`;
- `catalog/webapp/`;
- `legacy/`;
- deprecated/manual/deep runner scripts;
- stale handoff and implementation-plan documents;
- redundant phase workflows;
- committed generated output or caches elsewhere in the tree.

Evidence must include searches for imports, commands, Compose references, setup references, tests, documentation links, AI indexing, runner discovery, and workflows.

Exit criteria:

- every path has a keep, consolidate, archive, delete, or defer decision;
- no directory-level deletion relies only on naming;
- uncertain candidates remain deferred.

### V1-C — generated output and experiment cleanup

Expected scope:

- remove confirmed generated `graphify-out/` content;
- update `.gitignore`;
- remove or relocate confirmed unsupported `new-stuff/` experiments;
- retain the Markdown viewer requirements as roadmap acceptance criteria rather than production code;
- remove generated experiment output;
- add an `experiments/README.md` only if supported research experiments remain.

Exit criteria:

- production tree contains no unclassified experiment directory;
- no generated cache or analysis snapshot is committed by default;
- compile, tests, Compose, and diff hygiene are green.

### V1-D — obsolete implementation cleanup

Expected scope after dependency proof:

- remove duplicate old web implementation if `catalog/webapp/` has no supported entry point;
- reduce `legacy/` to explicitly justified historical/reference material;
- remove obsolete scripts and their stale documentation;
- update AI grounding/indexing when indexed paths change;
- remove dead tests only when the corresponding unsupported implementation is removed and equivalent product behavior remains covered.

Exit criteria:

- one supported Flask application path;
- one documented runtime path per deployment role;
- no hidden duplicate behavior relied upon by setup or Compose;
- full affected regression suites green.

### V1-E — documentation consolidation

Deliver a canonical structure such as:

```text
docs/
  index.md
  getting-started/
  user-guides/
  federation-v1/
  administration/
  troubleshooting/
  developer/
  reference/
  history/
```

Required public documents:

- product overview;
- quick start;
- installation and roles;
- create and join a federation;
- device and provider administration;
- storage, AI, and compute guides;
- security/trust model;
- backup, restart, recovery, and upgrade;
- troubleshooting;
- compatibility/protocol reference;
- developer architecture and test guide.

Required cleanup:

- replace the oversized root README with a concise entry point;
- move durable design evidence to history/decisions;
- delete superseded phase plans and handoffs after link verification;
- mark retained history as non-current;
- validate all links and commands.

Exit criteria:

- a new user can identify the correct first document;
- current behavior is described without phase archaeology;
- no canonical page presents completed development as pending;
- docs are ready to be served by the later integrated `/docs` update.

### V1-F — permanent regression gate

Deliver:

- one named Federation v1 release workflow;
- Linux and Windows coverage;
- complete identity, session, relay, transport, storage, failover, capability, AI, compute, artifact, Flask, setup, and compatibility matrix;
- compile, Ruff, Compose, diff hygiene, and documentation-link checks;
- retained focused component workflows only where they add fault isolation.

Exit criteria:

- the v1 gate is equivalent to or stronger than the union of required closeout gates;
- completed phase names are no longer the only permanent quality signal;
- no workflow is deleted before replacement evidence exists.

### V1-G — release candidate acceptance

Required acceptance:

- fresh checkout installation on Linux and Windows;
- first-run setup without old state;
- documented start/stop/upgrade path;
- two independently persisted devices;
- session create, invite, join, disconnect, reconnect, restart, revoke;
- storage replication and controlled failover;
- AI provider approval, use, suspension/revocation, and recovery;
- compute provider approval, dispatch, duplicate suppression, suspension/revocation, and recovery;
- safe diagnostics without secret/private endpoint leakage;
- backup/recovery rehearsal;
- all user commands and docs links checked.

Record exact hardware/network constraints and do not overclaim unsupported public-internet acceptance.

### V1-H — release publication

Deliver:

- `CHANGELOG.md`;
- v1 release notes;
- exact version declaration;
- verified release commit;
- `v1.0.0` tag;
- updated original federation issue/status;
- post-release branch-cleanup proposal presented separately.

Exit criteria:

- tag points to the validated commit;
- release notes match actual scope and limitations;
- no pending mandatory closeout action is hidden in a historical plan.

## Stop conditions

Stop and report rather than broaden scope when:

- a deletion candidate has an unresolved runtime or documentation dependency;
- a test indicates lost compatibility, authority weakening, split brain, stale execution, or data risk;
- public documentation would require claiming behavior not demonstrated by acceptance;
- cleanup requires a new feature rather than removal/consolidation;
- a future roadmap item is being pulled into v1 without explicit approval.

## Next exact action

Proceed to **V1-B** only:

1. build the exact path-level cleanup manifest;
2. verify dependencies for `graphify-out/`, `new-stuff/`, `catalog/webapp/`, `legacy/`, stale implementation documents, and old workflows;
3. propose the first deletion batch;
4. do not delete anything until that manifest is reviewed.
