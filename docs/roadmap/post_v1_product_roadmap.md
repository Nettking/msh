# Post-v1 product roadmap

Status: **active product roadmap**

Reviewed: **2026-08-07 Europe/Oslo**

This roadmap describes product work after the merged capability-first Federation baseline. It does not override the active Federation implementation plan, acceptance manifest, or release-closeout plan.

## Authority hierarchy

1. Current user and administration guides describe supported operation.
2. `docs/implementation/federation/active/capability_first_federation_plan.md` defines current Federation product behavior and authority boundaries.
3. `catalog/federation/tests/cf7_acceptance/scenarios.json` defines recorded acceptance claims.
4. `docs/implementation/federation/active/federation_v1_closeout_plan.md` defines remaining release-closeout work.
5. This roadmap describes later product direction.

## Completed product foundations

The following foundations are merged:

- one persistent device identity per installation;
- capability-first Federation onboarding;
- authenticated Federation creation, discovery, pairing, join, reconnect, revocation, and controlled rejoin;
- device inspection;
- optional bounded benchmarks;
- independent contribution intent and lifecycle handling;
- read-only Federation overview and detail surfaces;
- integrated `/docs` browser over canonical repository documentation;
- compatibility with retained role-first settings during the transition;
- permanent Ubuntu and Windows component and product gates.

The supported mandatory first-run flow is:

```text
Identity
  -> Federation
  -> Inspect
  -> finish setup
  -> open Federation
```

A current inspection is sufficient to finish setup. Benchmarks and contribution decisions are optional follow-up work. They do not grant authority by themselves.

## Current pre-release work

Before publishing Federation v1:

1. resolve verified runtime, persistence, platform, browser, privacy, network, and compatibility defects;
2. maintain one coherent current documentation path;
3. freeze one exact release candidate;
4. execute complete physical CF7 acceptance on that commit;
5. update acceptance claims only through a separate evidence-backed review;
6. establish a permanent release gate at least as strong as the required existing matrices;
7. publish release notes, changelog, exact version, and tag.

Complete physical CF7 acceptance remains unaccepted; the implementation index and acceptance manifest govern current truth. CF8 compatibility retirement is merged for the installed product.

## CF8 compatibility retirement (implemented)

CF8 retired the role-first runtime for the installed product; retained setup data is migration input only. Subsequent changes must continue to:

- preserve stable device and Federation identity;
- migrate supported old settings deterministically;
- preserve explicit contribution intent;
- avoid treating old deployment roles as current authority;
- retain rollback or compatibility behavior required by the accepted migration plan;
- remain a separately reviewed change.

## Product documentation completion

After current truth is reconciled, organize public documentation into maintained user-facing levels:

```text
docs/
  getting-started/
  user-guides/
  administration/
  federation-v1/
  troubleshooting/
  developer/
  reference/
  integrations/
  implementation/
  roadmap/
  releases/
  history/
```

Each directory should contain an `index.md` defining status, audience, scope, authority, entry point, parent, review date, and retention policy.

## Federation product expansion

Later compatible work may improve:

- device and service administration;
- provider health and capacity presentation;
- benchmark comparison and expiry visibility;
- storage assignment, replication, and recovery observability;
- job and artifact inspection;
- network-path diagnostics;
- backup, upgrade, and recovery workflows;
- support for additional versioned capability types.

These improvements must use existing authenticated authority paths. A simpler UI must not create a second source of membership, provider, storage, compute, job, artifact, lease, term, or fencing authority.

## Broader trust and protocol work

Public or partially trusted participation, protocol-major renaming of the internal session boundary, decentralized coordination, multi-primary storage, untrusted remote code, or Byzantine-fault assumptions are outside the current trusted Federation v1 boundary. They require separate threat models, compatibility plans, and release decisions.

## Operator-knowledge modeling

FCP already supports operator-note capture, structuring, and a limited SysML text export. Future formal language semantics, validated model transformations, and operational-state reasoning require a separately reviewed MSH design and evaluation plan; they are not part of the v1 acceptance contract. See [Operator knowledge and modeling](../implementation/operator_knowledge_modeling.md).
