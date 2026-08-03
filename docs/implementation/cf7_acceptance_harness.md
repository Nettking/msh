# CF7-A capability-first acceptance harness

Status: foundation implemented and extended through CFI-4; full CF7 product acceptance is **not** claimed.

## Purpose

CF7-A provides deterministic, cross-platform test infrastructure for the capability-first work that is executable in the current repository.

The permanent gate now includes:

- CF1 onboarding contracts and legacy migration preview;
- CF2-A benchmark/inspection kernel tests;
- CF2-B isolated concrete benchmark adapter tests;
- CF3 isolated discovery, join, reconnect, and authority tests;
- CF4 isolated contribution service tests;
- CF5 static onboarding and Federation shell tests;
- CFI-1 read-only `/federation` route integration;
- CFI-2 Flask identity, discovery, verified join, local creation, reconnect, and migration-preview integration;
- CFI-3 Flask device-inspection composition, persistence, expiry, privacy, and no-authority tests;
- CFI-4 Flask benchmark run, skip, rerun, cancellation, validity, persistence, privacy, and no-authority tests;
- CF6 projection safety;
- focused Federation v1 authority regressions.

These remain bounded claims. Contribution actions, setup transition, physical browser and hardware behavior, and complete end-to-end acceptance remain outside CF7-A.

The machine-readable source of truth is `catalog/federation/tests/cf7_acceptance/scenarios.json`.

## Harness boundaries

The harness provides:

- independent state roots for three MSH devices;
- stable `IdentityStore` creation and reopen helpers;
- durable `SessionCoordinator` restart and reconnect helpers;
- shell-free child-process lifecycle helpers;
- OS-assigned held TCP ports rather than fixed ports;
- configured discovery fixtures with two federation candidates;
- verified join, persisted reconnect, membership removal, fencing, and controlled rejoin fixtures;
- bounded malformed and partial-write injection against existing identity state;
- assertions for membership, route authority, fencing, identity stability, and private-data leakage;
- an explicit scenario manifest with `executable`, `blocked`, and `manual` status only.

The helpers and Flask integrations never create a second authority model. Membership changes go through `SessionCoordinator`; join and reconnect go through `SessionOnboardingAuthority`; identity persistence goes through `IdentityStore`; CFI-3 inspection delegates to the existing CF2 `DeviceInspector`; CFI-4 benchmark execution delegates to the existing CF2 registry, runner, result store, validity evaluator, and exact adapter instances composed by CFI-3.

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
| CF2-B concrete adapters | Executable | `test_benchmarking_adapters.py` | Existing bounded trusted seams only |
| CF4 contribution service | Executable | contribution package tests | Isolated service and adapters |
| CF5 UI shell | Executable | `test_cf5_ui_shell.py` | Automated template/static checks |
| CFI-1 Federation overview | Executable | `test_federation_overview_route.py` | Read-only |
| CFI-2 onboarding composition | Executable | `test_capability_onboarding_route.py` | Identity and Federation steps only |
| CFI-3 device inspection | Executable | `test_capability_inspection_route.py` | Reads coarse local state and existing adapter seams |
| CFI-4 benchmark composition | Executable | `test_capability_benchmark_route.py` | Evidence only; no contribution activation or new authority |
| CF6 projections | Executable | `test_federation_projections.py` | Read-only safe projections |
| Federation v1 authority regression | Executable | `test_phase2_unit.py` | Focused gate, not physical deployment |
| Persisted legacy migration | Blocked | Manifest entry | Requires compatibility-controlled setup transition |
| Recorder plus AI contribution | Blocked | Manifest entry | Requires contribution authority binding |
| Separate AI, compute, and storage contributors | Blocked | Manifest entry | Requires supported contribution composition |
| Contribution disable/re-enable | Blocked | Manifest entry | CFI-4 stops before contribution actions |
| Federation overview mutations | Blocked | Manifest entry | Overview remains read-only |
| Fresh physical Windows/Linux checkout | Manual | Manifest entries | CI runners are not owner hardware |
| Desktop and mobile browser verification | Manual | Manifest entry | Route tests do not prove physical rendering |
| Multi-host relay, MTConnect, Ollama/accelerator | Manual | Manifest entries | Requires physical services and devices |

## CFI-3 bounded claim

The CFI-3 route suite proves that:

- inspection requires the server-bound device identity and revalidated membership;
- the existing CF2 inspector is called through the supported Flask onboarding flow;
- the snapshot survives restart with monotonic revisions;
- expired evidence is not treated as current;
- corrupt persisted evidence fails closed;
- request-supplied actor, device, session, endpoint, and credential context is rejected;
- private values do not reach the template;
- saved MTConnect discovery is read without starting a scan;
- configured Ollama inspection uses inventory only and does not invoke a model;
- no benchmark probe, contribution action, membership mutation, storage assignment, or job authority is produced by inspection.

## CFI-4 bounded claim

The CFI-4 route suite proves that:

- the exact CFI-3 registry and adapter instances are reused rather than rediscovered;
- trusted Federation context and a current device-bound inspection are required;
- run IDs, dependency fingerprints, prerequisites, timeouts, and identity/Federation context are server-owned;
- explicit run and rerun create immutable durable CF1 benchmark results;
- optional skip creates only local onboarding progress and invokes no probe;
- cooperative cancellation produces bounded cancelled evidence;
- TTL expiry, benchmark-version changes, and dependency changes require rerun;
- corrupt persisted evidence fails closed without rendering raw bytes;
- Ollama inference occurs only after an explicit benchmark POST;
- MTConnect does not start scanning or recording, compute handlers are not invoked, storage remains candidate-only, and network targets remain authenticated explicit seams;
- passing evidence grants no contribution, provider, membership, storage, job, lease, term, grant, or fencing authority.

Passing these tests does not prove physical hardware performance, production network reachability, representative benchmark suitability, contribution activation, or complete product acceptance.

## Permanent gate

`.github/workflows/cf7-acceptance-harness.yml` runs on `ubuntu-latest` and `windows-latest`. It executes:

- the CF7-A foundation scenarios;
- CF1 contracts;
- CF2-A and CF2-B tests;
- CF3 discovery and authority tests;
- CF4 contribution service tests;
- CF5 UI-shell tests;
- CFI-1 read-only Federation route tests;
- CFI-2 onboarding route and persistence tests;
- CFI-3 inspection route, persistence, expiry, safety, and no-authority tests;
- CFI-4 benchmark route, persistence, skip, rerun, cancel, validity, safety, and no-authority tests;
- CF6 projections;
- focused Federation authority regressions;
- compilation, manifest assertions, Ruff, and diff hygiene.

Passing this workflow means that the currently executable foundation and bounded Flask integrations are green on both runner families. It must not be used as evidence that contribution mutations are supported, that physical desktop/mobile or hardware behavior is accepted, or that complete capability-first or Federation v1 end-to-end acceptance has passed.
