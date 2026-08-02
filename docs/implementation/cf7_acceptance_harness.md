# CF7-A capability-first acceptance harness

Status: foundation implemented and extended through CFI-2; full CF7 product
acceptance is **not** claimed.

## Purpose

CF7-A provides deterministic, cross-platform test infrastructure for the
capability-first work that is executable in the current repository.

The permanent gate now includes:

- CF1 onboarding contracts and legacy migration preview;
- CF2-A benchmark/inspection kernel tests;
- CF2-B isolated concrete benchmark adapter tests;
- CF3 isolated discovery, join, reconnect, and authority tests;
- CF4 isolated contribution service tests;
- CF5 static onboarding and Federation shell tests;
- CFI-1 read-only `/federation` route integration;
- CFI-2 Flask identity, discovery, verified join, local creation, reconnect, and
  migration-preview integration;
- CF6 projection safety;
- focused Federation v1 authority regressions.

These remain bounded claims. Physical browser behavior, benchmark product
actions, contribution actions, setup transition, and complete end-to-end
acceptance remain outside CF7-A.

The machine-readable source of truth is
`catalog/federation/tests/cf7_acceptance/scenarios.json`.

## Harness boundaries

The harness provides:

- independent state roots for three MSH devices;
- stable `IdentityStore` creation and reopen helpers;
- durable `SessionCoordinator` restart and reconnect helpers;
- shell-free child-process lifecycle helpers;
- OS-assigned held TCP ports rather than fixed ports;
- configured discovery fixtures with two federation candidates;
- verified join, persisted reconnect, membership removal, fencing, and
  controlled rejoin fixtures;
- bounded malformed and partial-write injection against existing identity state;
- assertions for membership, route authority, fencing, identity stability, and
  private-data leakage;
- an explicit scenario manifest with `executable`, `blocked`, and `manual`
  status only.

The helpers and Flask integration never create a second authority model.
Membership changes go through `SessionCoordinator`; join and reconnect go through
`SessionOnboardingAuthority`; identity persistence goes through `IdentityStore`.

## Scenario-to-test mapping

| CF7 area | Current status | Automated evidence | Limitation |
| --- | --- | --- | --- |
| Independent device state | Executable | `test_device_state_directories_and_identities_are_isolated` | Test directories, not separate physical hosts |
| Cross-platform files, processes, ports | Executable | `test_cross_platform_port_and_process_restart_helpers` | Local loopback and child processes only |
| Several federation candidates | Executable | CF7 foundation and CFI-2 route tests | Configured discovery source, not general LAN discovery |
| Verified join | Executable | CF3 and CFI-2 route tests | Existing enrollment and invitation authority |
| No-candidate local creation | Executable | `test_no_candidate_local_creation_uses_existing_coordinator_authority` | One local coordinator authority |
| Returning trusted startup reconnect | Executable | `test_returning_startup_reconnect_mints_no_new_authority` | Revalidates saved membership; physical relay reconnect remains manual |
| Legacy mode browser preview | Executable | `test_legacy_migration_preview_never_renders_private_values` | Read-only preview; no migration write |
| Membership removal and fencing | Executable | CF7 foundation and CFI-2 route tests | Existing session authority only |
| Controlled rejoin | Executable | CF7 foundation | Existing invitation authority only |
| Corrupt and partial identity state | Executable | CF7 foundation | Existing identity format |
| Corrupt CFI-2 onboarding state | Executable | CFI-2 route test | Fail-closed render; no automatic recovery |
| CF2-A inspection and benchmark engine | Executable | focused capability tests | Isolated engine |
| CF2-B concrete adapters | Executable | `test_benchmarking_adapters.py` | Evidence only, not product execution |
| CF4 contribution service | Executable | contribution package tests | Isolated service and adapters |
| CF5 UI shell | Executable | `test_cf5_ui_shell.py` | Automated template/static checks |
| CFI-1 Federation overview | Executable | `test_federation_overview_route.py` | Read-only |
| CFI-2 onboarding composition | Executable | `test_capability_onboarding_route.py` | Identity and federation steps only |
| CF6 projections | Executable | `test_federation_projections.py` | Read-only safe projections |
| Federation v1 authority regression | Executable | `test_phase2_unit.py` | Focused gate, not physical deployment |
| Persisted legacy migration | Blocked | Manifest entry | Requires compatibility-controlled setup transition |
| Benchmark run/skip/rerun through onboarding | Blocked | Manifest entry | Requires separate benchmark product integration |
| Recorder plus AI contribution | Blocked | Manifest entry | Requires contribution authority binding |
| Separate AI, compute, and storage contributors | Blocked | Manifest entry | Requires supported contribution composition |
| Contribution disable/re-enable | Blocked | Manifest entry | Requires supported contribution actions |
| Federation overview mutations | Blocked | Manifest entry | Overview remains read-only |
| Fresh physical Windows/Linux checkout | Manual | Manifest entries | CI runners are not owner hardware |
| Desktop and mobile browser verification | Manual | Manifest entry | Route tests do not prove physical rendering |
| Multi-host relay, MTConnect, Ollama/GPU | Manual | Manifest entries | Requires physical services and devices |

## Permanent gate

`.github/workflows/cf7-acceptance-harness.yml` runs on `ubuntu-latest` and
`windows-latest`. It executes:

- the CF7-A foundation scenarios;
- CF1 contracts;
- CF2-A and CF2-B tests;
- CF3 discovery and authority tests;
- CF4 contribution service tests;
- CF5 UI-shell tests;
- CFI-1 read-only Federation route tests;
- CFI-2 onboarding route and persistence tests;
- CF6 projections;
- focused Federation authority regressions;
- compilation, manifest assertions, Ruff, and diff hygiene.

Passing this workflow means that the currently executable foundation and bounded
Flask integrations are green on both runner families. It must not be used as
evidence that benchmark or contribution mutations are supported, that physical
desktop/mobile behavior is accepted, or that complete capability-first or
Federation v1 end-to-end acceptance has passed.
