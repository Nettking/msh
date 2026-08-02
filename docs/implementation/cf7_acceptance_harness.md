# CF7-A capability-first acceptance harness

Status: foundation implemented; full CF7 product acceptance is **not** claimed.

## Purpose

CF7-A provides deterministic, cross-platform test infrastructure for the
capability-first work that is executable in the current repository. It does not
change production startup, Flask composition, protocols, persistence, benchmark
adapters, authority semantics, or runtime behavior.

The permanent gate now includes the automated CFI-1 read-only Federation
overview route and the CF2-B concrete benchmark adapter tests. These are still
bounded claims:

- CFI-1 proves the supported read-only `/federation` route, authorization,
  template rendering, safe degraded states, and data-leakage boundaries;
- CF2-B proves isolated adapter behavior and evidence production only;
- neither item establishes the complete capability-first product flow;
- physical desktop and mobile browser review remains manual.

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
- bounded malformed and partial-write injection against the existing identity
  state format;
- assertions for membership, route authority, fencing, identity stability, and
  private-data leakage;
- an explicit scenario manifest with `executable`, `blocked`, and `manual`
  status only.

The helpers never create a second authority model. Membership changes go
through `SessionCoordinator`; reconnect goes through
`SessionOnboardingAuthority`; identity persistence goes through
`IdentityStore`. The CF7-A workflow invokes existing CFI-1 and CF2-B tests but
does not wrap them in a new product service.

## Scenario-to-test mapping

| CF7 area | Current status | Automated evidence | Limitation |
| --- | --- | --- | --- |
| Independent device state | Executable | `test_device_state_directories_and_identities_are_isolated` | Test directories, not separate physical hosts |
| Cross-platform files, processes, ports | Executable | `test_cross_platform_port_and_process_restart_helpers` | Local loopback and child processes only |
| Several federation candidates | Executable | `test_multiple_candidates_require_explicit_selection_and_hide_join_material` | Uses the existing configured discovery adapter, not a product route |
| Verified join | Executable | same test plus existing CF3 tests | Isolated authority service |
| Returning trusted reconnect | Executable | `test_joined_device_reopens_identity_and_reconnects_without_new_authority` | Explicit helper call, not startup integration |
| Membership removal and fencing | Executable | `test_removed_membership_is_fenced_and_saved_binding_cannot_reconnect` | Existing session authority only |
| Controlled rejoin | Executable | `test_controlled_rejoin_restores_membership_without_granting_observer_authority` | Existing invitation authority only |
| Corrupt and partial identity state | Executable | `test_partial_and_corrupted_identity_state_fail_closed` | Identity state supports deterministic fault injection; no new state format is added |
| CF1 contracts and legacy migration preview | Executable | `test_onboarding_contracts.py` | Contract-level only |
| CF2-A inspection and benchmark engine | Executable | capability tests selected by `benchmarking` while excluding adapters | Isolated engine and contract coverage |
| CF2-B concrete benchmark adapters | Executable | `test_benchmarking_adapters.py` | Isolated adapter tests only; not a supported benchmark product flow |
| CF4 contribution service | Executable | `catalog/capabilities/contributions/tests` | Isolated adapters and services only |
| CF5 UI shell | Executable | `test_cf5_ui_shell.py` | Static/template shell coverage |
| CFI-1 read-only Federation overview | Executable | `test_federation_overview_route.py` | Automated route, authorization, template, degraded-state, and leakage coverage; no write authority |
| CF6 projections | Executable | `test_federation_projections.py` | Framework-neutral projections |
| Federation v1 authority regression | Executable | `test_phase2_unit.py` | Focused authority/contract gate, not full physical deployment |
| Existing federation discovery and join through product | Blocked | Manifest entry | Requires CFI-2 supported onboarding composition |
| No-candidate local creation through product | Blocked | Manifest entry | Requires CFI-2 supported onboarding composition |
| Returning-device automatic startup reconnect | Blocked | Manifest entry | Requires CFI-2 startup integration |
| Benchmark run, skip, and rerun through Flask/onboarding | Blocked | Manifest entry | Concrete adapters exist, but no supported Flask/onboarding benchmark product flow exists |
| Recorder plus AI contribution | Blocked | Manifest entry | Requires contribution authority binding |
| Separate AI, compute, and storage contributors | Blocked | Manifest entry | Requires contribution authority binding and supported product composition |
| Federation overview mutations | Blocked | Manifest entry | CFI-1 is intentionally read-only and introduces no mutation authority |
| Contribution disable/re-enable product flow | Blocked | Manifest entry | Requires supported contribution actions |
| Fresh physical Windows/Linux checkout | Manual | Manifest entries | CI runners are clean virtual environments, not owner hardware |
| Desktop and mobile browser verification | Manual | `physical.mobile-and-desktop-browser-review` | Automated route tests do not prove physical responsive rendering or interaction |
| Multi-host relay, MTConnect, and Ollama/GPU checks | Manual | Manifest entries | Requires physical services, hosts, or devices |

## Permanent gate

`.github/workflows/cf7-acceptance-harness.yml` runs on both `ubuntu-latest` and
`windows-latest`. It executes:

- the CF7-A harness scenarios;
- CF1 onboarding contracts;
- CF2-A isolated benchmark-engine regressions;
- CF2-B isolated concrete benchmark adapter regressions;
- CF3 discovery and join regressions;
- CF4 contribution service regressions;
- CF5 static UI-shell regressions;
- CFI-1 `/federation` route, authorization, template, degraded-state, method,
  startup-gate, and data-leakage regressions;
- CF6 projection regressions;
- focused Federation v1 authority regressions;
- Python compilation, manifest validation, Ruff, and diff hygiene.

Passing this workflow means that the **CF7-A harness foundation** and currently
isolatable contracts are green on both runner families. It must not be used as
evidence that benchmark run/skip/rerun is available through Flask/onboarding,
that physical desktop/mobile browser behavior is accepted, or that Federation
v1 or capability-first onboarding has passed complete end-to-end CF7
acceptance.
