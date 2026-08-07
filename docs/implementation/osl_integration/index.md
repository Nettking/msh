# OSL integration planning

| Metadata | Value |
| --- | --- |
| Status | Active planning package; production implementation not started |
| Audience | Research owners, domain reviewers, security/product reviewers, maintainers, and implementation agents |
| Scope | OSL profile, authority, compatibility, architecture, contracts, workflow, UI, validation, migration, and phased delivery planning |
| Authority | [Implementation roadmap](10_phased_implementation_roadmap.md) is the only authoritative source for status, order, gates, and stop conditions |
| Entry point | [Implementation roadmap](10_phased_implementation_roadmap.md) |
| Parent | [Implementation documentation](../) |
| Reviewed | 2026-08-07 Europe/Oslo |
| Retention | Retain until the planning package is replaced by maintained product and developer documentation |

## Current status

- Planning package: complete enough to start D0-A.
- Production OSL package: not implemented.
- Canonical OSL persistence, lifecycle, API, UI, AI integration, migration, and current SysML v2 adapter: not implemented.
- W3: multi-delivery acceptance scenario, not a one-PR instruction.

## Exact next delivery

**D0-A only:**

1. create `docs/osl_language_profile.md`;
2. create `docs/osl_authority_boundary.md`;
3. create `docs/osl_compatibility_policy.md`;
4. update `docs/agent_notes/osl_sysml_alignment.md`;
5. obtain research/domain and security/product review;
6. merge accepted decisions before D1-A.

Do not add production OSL modules in D0-A.

## Document map

| Document | Role |
| --- | --- |
| [10 — implementation roadmap](10_phased_implementation_roadmap.md) | **Authoritative:** delivery order, status, gates, acceptance, and stop conditions |
| [00 — scope and sources](00_scope_and_sources.md) | source pins and claim boundaries |
| [01 — language requirements](01_language_requirements.md) | source-derived requirements and open research questions |
| [02 — current MSH architecture](02_current_msh_architecture.md) | analyzed historical MSH snapshot; revalidate every seam against current `main` |
| [03 — target architecture](03_target_architecture.md) | proposed OSL component and authority boundaries |
| [04 — Notebook-to-OSL workflow](04_notebook_to_osl_workflow.md) | proposed research-to-product workflow |
| [05 — data model and contracts](05_data_model_and_contracts.md) | proposed contract shapes and examples |
| [06 — repository file plan](06_repository_file_plan.md) | candidate paths; not authoritative until revalidated |
| [07 — API, UI, and journeys](07_api_ui_and_user_journeys.md) | proposed surfaces and failure states |
| [08 — validation and CI](08_validation_testing_and_ci.md) | required evidence and permanent gates |
| [09 — migration and compatibility](09_migration_and_compatibility.md) | migration, rollback, and compatibility analysis |

## Fixed boundaries

- OSL is a versioned, non-executing bounded context.
- JSON and SysML v2 are representations or adapters, not the language itself.
- Source, excerpts, candidates, immutable revisions, validation, human review, approval, publication, and feedback remain separate.
- Validation is not truth, safety assurance, approval, or publication.
- Human review, approval, and publication must bind an authenticated decision to one unchanged revision.
- OSL publication grants no Federation, provider, compute, storage, job, artifact, lease, fencing, or machine authority.
- AI remains attributed candidate-only assistance and cannot sign, approve, publish, or create canonical state.
- Existing operator records and legacy SysML exports are compatibility inputs, not proof of OSL conformance.

When a supporting document conflicts with the roadmap, stop and update the roadmap. Do not select the more permissive interpretation.