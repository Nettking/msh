# Current task handoff

Status: **current repository handoff**  
Reviewed: **2026-08-07 Europe/Oslo**

## Repository state

- Repository: `Nettking/msh`
- Default branch: `main`
- Documentation hierarchy established by PR #191
- Published Federation v1 release tag: not created
- Capability-first Federation implementation: merged baseline
- Complete physical CF7 acceptance: not accepted
- CF8 role-first compatibility retirement: blocked
- OSL integration: planning complete; production implementation not started

Always start new work from the current `main` head. Do not use a commit recorded in this document as a substitute for checking the repository directly.

Historical phase notes, old branch handoffs, and implementation checkpoints do not override the active documents named below.

## Track A: capability-first Federation

Authoritative plan:

- [Capability-first Federation plan](federation/active/capability_first_federation_plan.md)

Release-closeout plan:

- [Federation v1 closeout plan](federation/active/federation_v1_closeout_plan.md)

Acceptance workspace:

- [Federation acceptance documentation](federation/acceptance/)

Current product baseline includes:

- stable device identity;
- authenticated Federation discovery, verified join, pairing, reconnect, revocation, and local creation;
- bounded device inspection and optional benchmarks;
- independent contribution recommendation, intent, enable, disable, suspend, and reconcile behavior;
- the required first-run path `Identity -> Federation -> Inspect -> finish setup`;
- optional benchmark and contribution work after workbench access;
- read-only Federation product pages and public-safe projections;
- storage, transport, provider, compute, recorder, recovery, and compatibility authority boundaries;
- permanent Ubuntu and Windows component and product gates.

### Federation work still open

1. Resolve any verified runtime-parity, persisted-provider, native-host translation, privacy, browser, restart, or multi-host defects.
2. Freeze one exact candidate only after known blockers are closed.
3. Execute the complete physical CF7 campaign on that same commit.
4. Update acceptance flags only through a separate evidence-backed review.
5. Plan and review CF8 only after CF7 is accepted.
6. Continue documentation, release-gate, cleanup, and publication work as separate bounded PRs.

Do not restart CF1 or earlier capability-first implementation waves. Do not begin CF8 early.

## Track B: OSL integration

Plan index:

- [OSL integration index](osl_integration/)

Authoritative execution plan:

- [OSL implementation roadmap](osl_integration/10_phased_implementation_roadmap.md)

W3 end-to-end acceptance scenario:

- [Notebook-to-OSL method alignment](../planned-work/method-osl-msh-alignment.md)

Current OSL reality:

- source, architecture, contract, workflow, file, UI, validation, migration, and delivery analysis exists;
- the supporting MSH analysis was pinned to an older snapshot and every integration seam must be revalidated against current `main`;
- no accepted MSH OSL profile, human-authority policy, or compatibility policy has been merged;
- no canonical `catalog/osl/` production package, persistence, API, UI, AI integration, migration, or current SysML v2 adapter exists;
- W3 is a multi-delivery acceptance scenario, not permission to implement the entire workflow in one PR.

### Exact next OSL delivery

**D0-A is ready to start now.** It is documentation-only:

1. create `docs/osl_language_profile.md`;
2. create `docs/osl_authority_boundary.md`;
3. create `docs/osl_compatibility_policy.md`;
4. update `docs/agent_notes/osl_sysml_alignment.md`;
5. obtain research/domain and security/product review;
6. merge accepted decisions before D1-A begins.

Do not add production OSL modules in D0-A.

## Cross-track boundaries

- Do not combine Federation acceptance or runtime fixes with OSL implementation.
- OSL review, approval, or publication grants no Federation, provider, compute, storage, job, artifact, lease, fencing, or machine authority.
- Federation device identity is not a human OSL reviewer, approver, or publisher identity.
- OSL mutation remains blocked until server-verifiable human identity, authorization, secure sessions, and CSRF protections are implemented and reviewed.
- AI remains candidate-only in OSL and cannot sign, approve, publish, or create canonical authority.
- Existing operator records and legacy SysML exports are compatibility inputs, not proof of OSL conformance.

## Agent operating discipline

1. Start from updated `main`.
2. Scope each branch and PR to one named delivery, defect, acceptance unit, or documentation unit.
3. Declare owned paths before editing shared Flask, setup, navigation, security, persistence, or workflow files.
4. Commit after coherent boundaries so partial work remains recoverable.
5. Open a draft PR unless the repository owner explicitly requests another state.
6. Distinguish automated, simulated, browser, physical, multi-host, service, and human evidence.
7. Preserve authority, privacy, migration, restart, and cross-platform gates.
8. Stop when a missing decision would require scope expansion or a permissive assumption.

## Resume safety

- Safe to continue Federation closeout and CF7 preparation: **yes**.
- Safe to mark physical CF7 accepted: **no**.
- Safe to start CF8: **no**.
- Safe to start OSL D0-A: **yes**.
- Safe to start OSL production code: **no**, until D0-A is reviewed and merged.
- Safe to implement W3 as one PR: **no**.