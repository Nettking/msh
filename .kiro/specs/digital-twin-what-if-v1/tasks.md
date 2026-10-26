# Digital Twin What-If v1 — Tasks

Status: all implementation tasks complete; task evidence recorded
retrospectively after v1 was built.

Requirements are in [`requirements.md`](requirements.md); architecture is in
[`design.md`](design.md). Task references use the requirement IDs defined
there.

## 1. Specify the behavior

- [x] 1.1 Record user stories and EARS acceptance criteria for data discovery,
  classification, interval accounting, scenario math, scoping, page behavior,
  access boundaries, and corpus evaluation. (DT-01–DT-08)
- [x] 1.2 Define limits and non-goals so the scenario is not mistaken for a
  causal or production prediction. (DT-03, DT-04, DT-07)

## 2. Design the feature

- [x] 2.1 Define service, route, application registration, authorization,
  template, and shared-reader responsibilities. (DT-01–DT-07)
- [x] 2.2 Define interval classification, UTC daily aggregation, cache
  refresh, scenario equations, errors, and operational boundaries. (DT-01–DT-05,
  DT-07)
- [x] 2.3 Define service, route, and existing-corpus verification coverage.
  (DT-01–DT-08)

## 3. Build the historical time model

- [x] 3.1 Discover complete JSONL sources and parse machine IDs, UTC
  timestamps, execution states, and fallback signals. (DT-01–DT-02)
- [x] 3.2 Aggregate usable state intervals by machine and UTC day, including
  midnight splitting, continuity, and excluded long gaps. (DT-03)
- [x] 3.3 Validate machine/date filters and calculate the bounded scenario
  only when at least 30 classified intervals are available. (DT-04–DT-05)
- [x] 3.4 Cache the compact process-local snapshot and refresh it when the
  source fingerprint changes. (DT-01)

## 4. Deliver the workbench surface

- [x] 4.1 Register an authenticated GET-only `/digital-twin` blueprint,
  configure its data root, and grant `data.read`. (DT-06–DT-07)
- [x] 4.2 Add Monitor navigation and render the filters, observed metrics,
  scenario, state breakdown, assumptions, and empty/insufficient/error states.
  (DT-04–DT-06)
- [x] 4.3 Document the user-facing interpretation, source data, and model
  boundaries. (DT-04, DT-06–DT-07)

## 5. Verify v1 on existing data

- [x] 5.1 Add service tests for discovery, classification, intervals, date
  scope, scenario bounds, sample threshold, and invalid inputs. (DT-01–DT-05)
- [x] 5.2 Add route tests for empty and populated views, invalid/unavailable
  telemetry states, navigation, and read-only authorization. (DT-06–DT-07)
- [x] 5.3 Run the model read-only on the existing historical and example JSONL
  corpus, verify 0%, 10%, and 50% accounting identities, and record scope,
  results, and limitations. (DT-08)

Implementation evidence is in `catalog/flask_app/services/digital_twin_service.py`,
`catalog/flask_app/digital_twin_routes.py`, `catalog/flask_app/tests/`, and
[`docs/specs/digital_twin_what_if_v1_evaluation.md`](../../../docs/specs/digital_twin_what_if_v1_evaluation.md).
