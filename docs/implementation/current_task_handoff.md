# Current task handoff

Last updated: 2026-08-06 Europe/Oslo

## Repository state

- Repository: `Nettking/msh`
- Default branch: `main`
- Reconciled main commit: `349da7cd0007bf3e0f97127ea6696fd325e3c583`
- Published release tag: not yet created
- Capability-first Federation implementation: merged baseline
- Complete physical CF7 acceptance: not accepted
- CF8 role-first retirement: blocked
- OSL integration: planning complete; production implementation not started

This handoff identifies current plans and next actions. Historical phase notes and
branch delivery records do not override the authoritative documents listed
below.

## Track A: capability-first Federation

Authoritative plan:

- `docs/implementation/federation/active/capability_first_federation_plan.md`

Current product baseline includes:

- stable device identity;
- Federation discovery, verified join, pairing, reconnect, and local creation;
- bounded device inspection and benchmarks;
- independent contribution recommendation and intent handling;
- the three-step first-run path `Identity -> Federation -> Inspect`;
- optional benchmarks and contribution decisions after workbench access;
- read-only Federation product pages and supported Flask composition;
- permanent Ubuntu and Windows component/product gates;
- recovery and fresh-reset behavior.

PR #185 merged the capability-first product baseline. PR #186 subsequently
merged Docker/native Ollama benchmark runtime-parity changes.

### Federation work still open

- Review and resolve the post-merge PR #186 concerns about persisted connected
  provider settings being overridden by Compose defaults and native host
  translation preserving explicit ports.
- Reconcile any remaining documentation that treats PR #186 as an open draft or
  benchmarks/contribution review as mandatory before first completion.
- Freeze one exact acceptance candidate only after known code, authority,
  privacy, platform, browser, MTConnect, Ollama, restart, revocation, and
  multi-host issues are closed.
- Execute the complete physical CF7 campaign on that one commit.
- Change acceptance flags only through a separate evidence-backed review.
- Plan CF8 separately after CF7 acceptance. Do not begin role-first retirement
  earlier.

The old instructions to implement CF1, launch Wave 1, or replace the setup UI are
historical and must not be used.

## Track B: OSL and Notebook-to-OSL integration

Authoritative execution plan:

- `docs/implementation/osl_integration/10_phased_implementation_roadmap.md`

Plan index and source/design references:

- `docs/implementation/osl_integration/README.md`

W3 end-to-end acceptance scenario:

- `docs/planned-work/method-osl-msh-alignment.md`

Current OSL reality:

- source, architecture, contract, workflow, file, UI, validation, migration, and
  phased-delivery analysis exists;
- the detailed MSH analysis was pinned to an older snapshot and every current
  integration seam must be revalidated before implementation;
- no accepted MSH OSL profile, authority policy, or compatibility policy exists;
- no canonical `catalog/osl/` production package exists;
- no canonical OSL persistence, lifecycle, API, UI, AI integration, migration,
  or current SysML v2 adapter has been implemented;
- W3 is an acceptance scenario across multiple deliveries, not a one-branch
  implementation instruction.

### Exact next OSL delivery

Proceed with **D0-A only**, as a documentation-only draft PR:

1. create `docs/osl_language_profile.md`;
2. create `docs/osl_authority_boundary.md`;
3. create `docs/osl_compatibility_policy.md`;
4. update `docs/agent_notes/osl_sysml_alignment.md`;
5. obtain research/domain and security/product review;
6. merge the accepted decisions before D1-A begins.

Do not add production OSL modules in D0-A.

## Cross-track boundaries

- Do not combine Federation acceptance/fixes and OSL implementation in one PR.
- OSL content, validation, review, approval, or publication grants no Federation,
  provider, compute, storage, job, artifact, lease, fencing, or machine
  authority.
- Federation device/node identity is not automatically a human OSL reviewer,
  approver, or publisher identity.
- OSL mutation routes remain blocked until server-verifiable human identity,
  authorization, secure sessions, and CSRF are implemented and reviewed.
- AI remains candidate-only in the OSL track and cannot sign, approve, publish,
  or create canonical authority.
- Existing operator records and legacy SysML exports remain compatibility inputs,
  not proof of OSL conformance.
- Do not let multiple agents edit the same shared Flask, setup, navigation,
  security, persistence, or workflow file concurrently.

## Agent operating discipline

For both tracks:

1. branch from updated `main`;
2. scope the branch and PR to one named delivery, defect, acceptance unit, or
   documentation reconciliation;
3. declare owned paths;
4. commit after coherent file/test boundaries so partial work is recoverable;
5. open a draft PR and do not merge automatically;
6. distinguish automated, simulated, physical, browser/toolchain, and human
   evidence;
7. preserve authority, privacy, migration, restart, and cross-platform gates;
8. stop and report when a missing decision or dependency would require scope
   expansion or a permissive assumption.

## Resume safety

- Safe to resume Federation work: yes, through the authoritative capability-first
  plan and complete CF7 acceptance boundary.
- Safe to start CF8: no.
- Safe to start OSL D0-A: yes, after the plan-cleanup PR is merged.
- Safe to start OSL production code: no, until D0-A is reviewed and merged.
- Safe to implement W3 as one PR: no.
