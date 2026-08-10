# OSL implementation execution plan

Status: **authoritative implementation sequence**.
Last reconciled: **2026-08-06 Europe/Oslo**.
Reconciled FCP baseline: `349da7cd0007bf3e0f97127ea6696fd325e3c583`.

This document is the single source of truth for what is implemented next, what
is blocked, and which acceptance gate must pass before work continues.

The other documents in this directory remain useful source analysis and design
references. They do not independently authorize implementation, change the
sequence below, or define a second backlog.

## Source and authority hierarchy

Use the planning material in this order:

1. **This file** decides implementation order, current status, and stop
   conditions.
2. `06_repository_file_plan.md` provides candidate file ownership and detailed
   change ideas. Revalidate every path against current `main` before use.
3. `00`-`05` and `07`-`09` provide source traceability, architecture, contracts,
   user journeys, validation, and migration analysis.
4. `docs/planned-work/method-osl-fcp-alignment.md` defines the W3 end-to-end
   acceptance scenario. It is not a separate implementation plan or a one-PR
   instruction.
5. `docs/implementation/federation/active/capability_first_federation_plan.md` governs the
   separate Federation product track. OSL work must not change Federation,
   provider, storage, benchmark, or capability authority unless a later,
   separately reviewed integration plan explicitly requires it.

When a supporting document conflicts with this file, stop and update the plan
rather than choosing the more permissive interpretation.

## Baseline status

### Completed

- Source-pinned analysis of `systems-paper`, `paper-repo`, and the analyzed FCP
  snapshot.
- Target architecture, proposed contracts, file plan, user journeys, test
  strategy, migration strategy, and phased delivery analysis.
- A bounded W3 Notebook-to-OSL acceptance scenario.

### Not implemented

- No canonical `catalog/osl/` production package.
- No accepted FCP OSL profile or compatibility policy.
- No canonical OSL persistence, lifecycle, API, UI, AI integration, migration,
  or current SysML v2 adapter.
- No evidence that the existing operator-note model or legacy SysML exporter is
  conformant with the planned OSL profile.

### Baseline drift rule

The detailed plan originally analyzed FCP at
`f580c71f7269643a077cc7e7db8ba9bf6050bb6a`. FCP has advanced since that
snapshot, including capability-first onboarding and benchmark runtime changes.
The paper source pins remain deliberate research inputs, but every FCP file,
service, route, test, security assumption, and integration point must be checked
against current `main` at the start of each delivery.

A planning path is not proof that the path still exists or remains the correct
integration seam.

## Non-negotiable boundaries

1. OSL is a non-executing, versioned bounded context. It grants no machine,
   compute, storage, provider, job, Federation, or capability authority.
2. Raw source, excerpts, candidates, immutable revisions, validation results,
   human review, approval, publication, and feedback remain distinct records.
3. A Decision is not an OperatorAction. A candidate action is not selected,
   recommended, approved, authorized, or executed merely because it is modeled.
4. Validation reports structural and profile findings for an exact revision. It
   does not establish truth, safety, evidence acceptance, or human approval.
5. Original source and canonical revisions are immutable. Correction,
   migration, and supersession create linked records rather than rewriting
   history.
6. The complete manual workflow must work before AI assistance is introduced.
7. AI may produce attributed candidates or explanations only. It may not create
   canonical revisions, validate meaning, sign reviews, approve, publish, or
   grant authority.
8. Authenticated human identity, authorization, secure sessions, and CSRF are a
   hard prerequisite for production mutation routes.
9. Import never transfers external lifecycle or authority. Export never changes
   lifecycle.
10. Canonical JSON is the first product representation. SysML v2 is a versioned,
    optional, one-way adapter until fidelity and reproducibility are proven.
11. No legacy/canonical dual write and no destructive normal rollback.
12. Every unresolved semantic, security, compatibility, or authority question
    fails closed or remains explicitly deferred.

## Delivery backlog

Only the delivery marked **NEXT** may begin. A later delivery remains blocked
until all earlier acceptance gates are merged and explicitly recorded.

| Delivery | Status | Reviewable outcome | Primary boundary |
| --- | --- | --- | --- |
| D0-A | **NEXT** | accepted language profile, authority boundary, and compatibility policy | documentation only |
| D1-A | BLOCKED | pure immutable identifiers, source, evidence, and strategy-fragment contracts | no persistence or frameworks |
| D1-B | BLOCKED | workflow, provenance, review, validation-result, and command contracts | no transition services |
| D2-A | BLOCKED | exact profile registry and deterministic canonical JSON codec | no semantic validity claim |
| D2-B | BLOCKED | deterministic selected WF-rule validator and rule catalogue | no repair or mutation |
| D3-A | BLOCKED | immutable source/blob repository foundation and schema | no workflow or HTTP |
| D3-B | BLOCKED | revision repository, provenance, events, and atomic audit | no approval or UI |
| D4-A | BLOCKED | lifecycle policy and authorization-safe read projections | no public mutation |
| D5-A | BLOCKED | manual capture, excerpts, annotation, clarification, and candidates | no AI |
| D5-B | BLOCKED | manual candidate-to-draft workflow and safe work queue | no review or publication |
| D6-A | BLOCKED | exact-revision human review and approval services | authenticated human authority only |
| D6-B | BLOCKED | publication, withdrawal, supersession, and feedback | no activation or execution |
| D7-P | BLOCKED | application identity, authorization, secure sessions, and CSRF prerequisite | separate security review |
| D7-A | BLOCKED | authenticated, redacted, read-only OSL API and UI | GET/HEAD only |
| D7-B | BLOCKED | capture, edit, validate, review, approve, and publish UI/API | delegates all policy to services |
| D8-A | BLOCKED | optional, switchable candidate-only AI assistance | no canonical writes |
| D9-A | BLOCKED | bounded canonical JSON bundle import/export | no external authority import |
| D9-B | BLOCKED | deterministic one-way SysML v2 export for a declared subset | no round-trip claim |
| D9-C | BLOCKED | conservative legacy-note dry-run and explicit migration | no in-place upgrade |
| D10-A | BLOCKED | cutover, recovery, compatibility hardening, and permanent Ubuntu/Windows gates | no operational OSL binding |

## Current delivery: D0-A

D0-A exists to prevent implementation convenience from silently deciding
research meaning, product lifecycle, compatibility, or authority.

### Files

Create:

- `docs/osl_language_profile.md`;
- `docs/osl_authority_boundary.md`;
- `docs/osl_compatibility_policy.md`.

Update:

- `docs/agent_notes/osl_sysml_alignment.md`.

Do not add or change production modules in D0-A.

### Decisions required

The delivery must decide, or explicitly defer fail-closed:

- the internal and public identifier for the first supported research profile;
- the exact paper commit and selected element, relation, maturity, evidence, and
  WF-rule subset;
- the distinction among provisional, model-ready, structurally valid,
  reviewed, approved, published, active, and executable;
- stable identity and revision semantics;
- applicability, gaps, `ValidationNeed`, evidence dimensions, and review scope;
- canonical JSON extension, Unicode, number, time, and hash policy;
- reader/writer compatibility and retention/deprecation ownership;
- authenticated human principal, role, scope, and separation requirements;
- explicit denial of AI, client, Federation, capability, compute, storage, job,
  and provider authority;
- the supported SysML v2 export claim and the conditions under which it remains
  deferred.

### Acceptance gate

D0-A is complete only when:

- every unresolved item required by the first profile is decided or explicitly
  deferred without a permissive default;
- research/domain review accepts the selected semantic subset;
- security/product review accepts the human authority and compatibility
  boundaries;
- the legacy exporter is clearly labelled as legacy behavior rather than
  canonical conformance evidence;
- internal links and source pins are valid;
- the diff is documentation-only;
- the draft PR records reviewers, decisions, deferred items, and the exact next
  delivery;
- the change is merged before D1-A begins.

## Later delivery rules

### D1-D2: pure language foundation

Build transport-neutral immutable contracts, then the exact profile registry,
canonical JSON codec, and deterministic validator. These deliveries may not
import Flask, SQLite, AI, Federation, capability, provider, storage, or runtime
modules.

### D3-D4: durable state and safe reads

Add immutable source/blob storage, revision transactions, provenance, audit,
lifecycle policy, and redacted projections. Recovery, replay, conflict, tamper,
and cross-platform behavior must be proven before workflow writes are exposed.

### D5-D6: complete manual workflow

Implement the manual Notebook-to-OSL backend from unchanged source through
candidate, draft, validation, human review, approval, publication, and feedback.
No AI and no public mutation routes are allowed in these deliveries.

### D7: application security and UI

Treat application identity and request security as a separate prerequisite. The
read-only surface lands before mutation controls. UI state is never canonical,
and routes call tested application/domain services rather than reimplementing
policy.

### D8: optional AI

Add candidate-only assistance only after the manual path is complete and the AI
data-locality/evaluation gate is accepted. The workflow must remain fully usable
with AI disabled.

### D9: interoperability and migration

Land the canonical bundle contract before import/export, then the optional
SysML adapter, then conservative legacy migration. Migration starts with a
bounded dry run and never maps `structured`, `reusable`, `validated`, or
`model-ready` directly to local approval or publication.

### D10: cutover and permanent gates

Enable permanent Ubuntu/Windows gates, backup/restore, recovery rehearsal,
security/leakage suites, compatibility fixtures, and a staged legacy read-only
cutover. Do not remove retained readers or historical evidence in the same PR
that changes the default path.

## W3 vertical-slice mapping

W3 is the final end-to-end acceptance scenario, not an implementation shortcut:

| W3 observation | Delivery that enables it |
| --- | --- |
| unchanged raw source and exact excerpt | D3, D5-A |
| explicit annotation, clarification, and provisional gaps | D1, D5-A |
| neutral strategy-path model | D1, D2, D5-B |
| semantic/profile validation | D2-B |
| immutable provenance and trace | D3 |
| human review/model-ready evidence | D6-A |
| intuitive human-facing workflow | D7-B |
| current declared OSL/SysML projection | D9-B |
| migration without claim inflation | D9-C |
| complete Linux/Windows and retained evidence | D10-A |

No agent should receive an instruction to "implement W3" as one PR. The W3
scenario becomes executable incrementally and is accepted only after all mapped
deliveries pass.

## Agent operating rules

For every implementation delivery:

1. branch from updated `main`;
2. state exactly one delivery ID in the branch and PR;
3. re-audit relevant current FCP paths before editing;
4. declare owned files and do not overlap shared files with another agent;
5. prefer new bounded modules and narrow adapters;
6. commit after each coherent contract, implementation, test, or documentation
   boundary so partial work remains recoverable;
7. open a draft PR and do not merge automatically;
8. include focused positive, negative, malformed, redaction, restart, and
   authority tests as applicable;
9. include Ubuntu and Windows coverage when persistence, paths, encoding, time,
   process, or browser behavior can differ;
10. report exact source/profile/contract versions and unsupported features;
11. distinguish automated evidence from physical, browser, toolchain, or human
   review evidence;
12. stop rather than broadening scope when a dependency or decision is missing.

## Stop conditions

Stop and report when:

- code would decide an unresolved profile or paper contradiction;
- the analyzed FCP path is stale and no current integration seam is confirmed;
- raw source or a canonical revision would be overwritten;
- validation, review, approval, publication, recommendation, authorization, or
  execution would collapse into one status;
- AI or client input would supply actor identity or lifecycle authority;
- OSL content would grant Federation, provider, compute, storage, job, artifact,
  lease, term, fencing, or machine authority;
- a mutation route would land before authenticated human identity,
  authorization, secure sessions, and CSRF;
- import would trust external approval/publication state;
- SysML parseability would be reported as semantic equivalence, domain
  correctness, safety, or approval;
- migration would upgrade legacy claims without evidence;
- a delivery requires unrelated Federation or capability redesign;
- tests cannot distinguish real evidence from fixtures or simulated paths;
- sensitive source, identities, paths, credentials, endpoints, or provenance
  cannot be redacted safely.

## Exact next action

Implement **D0-A only** as a documentation-only draft PR. Do not start
`catalog/osl/` production code until D0-A is reviewed, accepted, and merged.
