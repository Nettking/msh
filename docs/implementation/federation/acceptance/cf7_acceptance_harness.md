# CF7 capability-first acceptance harness

Status: automated foundation and product-composition coverage are implemented through CF7-B. **Physical product acceptance and complete Federation v1 end-to-end acceptance are not claimed.**

## Purpose

The CF7 acceptance system has two permanent layers:

- **CF7-A foundation gate** — verifies contracts, isolated services, Flask integrations, projections, authority regressions, persistence, restart behavior, redaction, and cross-platform compatibility through CFI-6.
- **CF7-B product and physical gate** — verifies integrated product flows with bounded deterministic external-service fixtures and validates the format of operator-produced physical evidence.

The machine-readable source of truth is:

```text
catalog/federation/tests/cf7_acceptance/scenarios.json
```

The detailed physical runbook is:

```text
docs/implementation/federation/acceptance/cf7b_product_physical_acceptance.md
```

## Authority and truth boundaries

The acceptance harness does not define replacement production behavior.

- Membership changes continue through `SessionCoordinator`.
- Join and reconnect continue through `SessionOnboardingAuthority`.
- Stable identity continues through `IdentityStore`.
- Inspection and benchmarks continue through the existing CF2 services and exact registered adapters.
- Contribution intent and activation continue through the CF4/CFI-5 service and existing recorder, AI, compute, and storage authority seams.
- Startup transition continues through CFI-6.
- Storage remains candidate-only until existing assignment authority acts.
- Passing benchmark evidence remains evidence rather than authority.

CI uses bounded local fixtures for physical services. A green workflow cannot prove real MTConnect, Ollama/GPU, multi-host transport, target storage, or real-browser behavior.

## CF7-A coverage

`.github/workflows/cf7-acceptance-harness.yml` runs on Ubuntu and Windows and covers:

- CF1 onboarding contracts and legacy migration mapping;
- CF2 inspection, benchmark engine, concrete adapters, expiry, invalidation, cancellation, and redaction;
- CF3 discovery, selection, verified join, reconnect, revocation, fencing, and controlled rejoin;
- CF4 contribution contracts;
- CF5 onboarding and Federation UI shell;
- CFI-1 read-only Federation overview;
- CFI-2 identity and Federation composition;
- CFI-3 device inspection composition;
- CFI-4 benchmark composition;
- CFI-5 contribution enable, disable, suspend, and reconcile composition;
- CFI-6 persisted migration, startup routing, runtime intent, navigation, and explicit compatibility fallback;
- CF6 safe projections;
- focused Federation v1 authority regressions;
- compilation, manifest validation, Ruff, and diff hygiene.

## CF7-B automated product coverage

`catalog/federation/tests/cf7_acceptance/test_product_acceptance.py` composes the supported Flask application and real CFI-2 through CFI-6 services.

The automated product scenarios prove, within deterministic local fixtures:

- fresh identity and local Federation creation from empty state;
- inspection, explicit benchmark, contribution review, and capability-first finish;
- simultaneous recorder and language-model contribution;
- contribution disable and re-enable without membership change;
- stable identity, trusted reconnect, startup routing, and contribution reconciliation after restart;
- expired inspection/benchmark evidence suspending an enabled contribution until explicit inspection and benchmark rerun;
- three independent device state roots joined to one Federation;
- separate AI, registered-compute, and storage-candidate paths;
- storage remaining pending until existing assignment authority reports an assignment;
- no private recorder endpoint reaching the Federation overview;
- no contribution action creating membership or unrelated authority.

These are automated product-composition claims, not physical deployment claims.

## Physical evidence contract

CF7-B adds:

```text
catalog/federation/tests/cf7_acceptance/physical_evidence.py
catalog/federation/tests/cf7_acceptance/physical_evidence.template.json
catalog/federation/tests/cf7_acceptance/test_physical_evidence.py
```

The validator requires a complete redacted document bound to one exact Git commit. It rejects incomplete environments or scenarios, commit mismatch, malformed or oversized data, unsafe paths, URLs, IP addresses, credentials, enrollment material, private keys, local setup filenames, and private database locations.

The checked-in template is intentionally pending and must fail full validation. CI verifies that this fail-closed behavior remains true.

Required physical evidence includes:

- fresh physical Windows and Linux checkouts;
- independent multi-host Federation transport;
- a real or approved MTConnect source;
- target Ollama model and accelerator;
- desktop and mobile browser review;
- recorder plus AI on one device;
- separate AI, compute, and storage devices;
- benchmark expiry and rerun;
- contribution disable and re-enable;
- restart and reconciliation;
- revocation, fencing, and controlled rejoin.

## Permanent CF7-B gate

`.github/workflows/cf7b-product-physical-acceptance.yml` runs on Ubuntu and Windows and includes:

- manifest truth-boundary assertions;
- integrated CF7-B product scenarios;
- physical evidence validator tests;
- CF7 foundation scenarios;
- CFI-2 through CFI-6 route suites;
- Federation overview tests;
- navigation, mobile, responsive, setup-handoff, provider, recorder, and AI runtime regressions;
- contribution, projection, onboarding, and discovery contracts;
- Phase 0, Phase 1, and focused Phase 2 authority regressions;
- Docker Compose validation;
- Ruff and diff hygiene.

## Acceptance decision

The manifest currently and intentionally keeps:

```json
{
  "acceptance_claim": "foundation-only",
  "federation_v1_end_to_end_accepted": false,
  "capability_first_onboarding_end_to_end_accepted": false,
  "physical_evidence_accepted": false
}
```

Those flags may change only in a separate review after:

1. CF7-A, CF7-B, and broad Federation matrices are green for the exact commit;
2. all physical scenarios pass on real equipment;
3. one complete redacted physical evidence document validates against that commit;
4. no unresolved authority, privacy, data-loss, cross-platform, or browser defect remains.

CF8 role-first retirement must not begin solely because automated CI is green. It begins only after the evidence-backed CF7 acceptance decision is reviewed and accepted.