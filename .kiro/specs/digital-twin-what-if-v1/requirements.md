# Digital Twin What-If v1 — Requirements

Status: implemented and evaluated

This is the requirements artifact for the product brief in
[`docs/specs/digital_twin_what_if_v1.md`](../../../docs/specs/digital_twin_what_if_v1.md).
The implementation is already present on branch
`codex/digital-twin-what-if-v1`; this spec set is being recorded
retrospectively so the delivered behavior, design, and work breakdown are
explicit and traceable.

## Product goal

An authenticated workbench user can inspect a machine's observed active-time
share and explore a bounded scenario that reallocates a chosen share of
classified non-active time to active time. Results must expose data quality and
the assumptions that constrain their interpretation.

## Users and boundaries

- **Primary user:** an authenticated MSH workbench user with `data.read` access.
- **Data:** local timestamped JSONL telemetry under the configured FCP data
  root; `data/` is the default.
- **V1 boundary:** descriptive historical accounting plus a transparent
  arithmetic scenario. It is not a causal prediction, production schedule,
  cycle-time estimate, output/revenue forecast, failure prediction, or machine
  control surface.

## Requirements

### DT-01 — Discover machine telemetry

**User story:** As a workbench user, I want telemetry found across the
configured data tree so that I can analyze the machine history already present
in MSH.

**Acceptance criteria (EARS):**

- WHEN the service loads telemetry, THE SYSTEM SHALL recursively discover JSONL
  files using the shared data-loading reader.
- WHEN a JSONL record has a non-empty `machine_id`, `machine`, or `resource`
  field, THE SYSTEM SHALL use the first available identifier in that order.
- WHEN a record has an invalid or missing timestamp or machine identifier,
  THE SYSTEM SHALL exclude that record from the model.
- WHEN a file belongs to an incomplete upload batch hidden by the shared
  reader, THE SYSTEM SHALL exclude that file from analysis.
- WHEN the configured telemetry root contains no complete JSONL telemetry
  records, THE SYSTEM SHALL return an empty model that the page can render.

### DT-02 — Classify observed machine states

**User story:** As a workbench user, I want recorded execution states to be
classified consistently so that the time totals have a clear interpretation.

**Acceptance criteria (EARS):**

- WHEN `execution` is `ACTIVE` or `EXECUTING`, THE SYSTEM SHALL classify the
  row as active.
- WHEN `execution` is one of `STOPPED`, `PROGRAM_STOPPED`, `READY`,
  `INTERRUPTED`, `FEED_HOLD`, `FEEDHOLD`, `IDLE`, `HOLD`, or `PAUSED`, THE
  SYSTEM SHALL classify the row as non-active.
- WHEN `execution` is missing or marked unavailable and at least one numeric
  `Srpm` or `Sload` signal is present, THE SYSTEM SHALL classify the row as
  active if `Srpm > 100` or `Sload > 1`, and otherwise as non-active.
- WHEN `execution` contains an unrecognized value, THE SYSTEM SHALL leave the
  row unclassified even if spindle or load signals are present.
- WHEN a row has neither a recognized execution state nor usable fallback
  signals, THE SYSTEM SHALL leave the row unclassified and exclude its
  following interval from classified-time totals.

### DT-03 — Account for time intervals and data quality

**User story:** As a workbench user, I want the model to handle timestamps and
gaps explicitly so that missing telemetry is not reported as machine
downtime.

**Acceptance criteria (EARS):**

- WHEN records are modeled, THE SYSTEM SHALL parse their timestamps as UTC and
  order them separately for each machine.
- WHEN two adjacent timestamps are duplicated or reversed, THE SYSTEM SHALL
  exclude that interval from time totals.
- WHEN a positive interval is at least 0.1 seconds and at most 30 seconds,
  THE SYSTEM SHALL attribute it to the starting row's classified state.
- WHEN a positive interval is shorter than 0.1 seconds or longer than 30
  seconds, THE SYSTEM SHALL exclude it from classified-time totals.
- WHEN a positive interval is longer than 30 seconds, THE SYSTEM SHALL count it
  as an excluded long gap and SHALL NOT describe it as downtime.
- WHEN a usable interval crosses UTC midnight, THE SYSTEM SHALL split its
  seconds across the affected UTC dates before date-range filtering.
- WHEN continuity is displayed, THE SYSTEM SHALL calculate it as usable
  positive intervals divided by all positive adjacent timestamp intervals.

### DT-04 — Calculate bounded what-if scenarios

**User story:** As a workbench user, I want to adjust an explicit recovery
percentage so that I can see how it changes the observed active-time share.

**Acceptance criteria (EARS):**

- WHEN a user selects a recovery percentage, THE SYSTEM SHALL accept only
  values from 0 through 50 inclusive in 5-point increments.
- WHEN the selected machine/date scope contains at least 30 classified
  intervals, THE SYSTEM SHALL calculate recovered time as classified
  non-active time multiplied by the selected percentage.
- WHEN the scenario is calculated, THE SYSTEM SHALL hold classified observed
  time constant and calculate simulated active time as observed active time
  plus recovered time.
- WHEN the scenario is calculated, THE SYSTEM SHALL calculate simulated
  active share from simulated active time divided by classified observed time,
  and report the change from observed share in percentage points.
- WHEN the recovery percentage is 0%, THE SYSTEM SHALL leave the observed
  active time and share unchanged.
- WHEN the scope contains fewer than 30 classified intervals or no classified
  time, THE SYSTEM SHALL omit scenario shares and identify the result as
  insufficient data.
- WHEN the scenario is shown, THE SYSTEM SHALL identify it as a hypothetical
  time reallocation and SHALL NOT claim causal effects, additional parts,
  revenue, avoided failures, or machine commands.

### DT-05 — Scope results to a machine and inclusive UTC date range

**User story:** As a workbench user, I want to select a machine and date range
so that the displayed result represents only the period I chose.

**Acceptance criteria (EARS):**

- WHEN a machine and inclusive start/end dates are supplied, THE SYSTEM SHALL
  aggregate only that machine's daily data whose UTC date falls within the
  selected range.
- WHEN dates are omitted for a selected machine, THE SYSTEM SHALL default to
  that machine's available history.
- WHEN a requested machine is absent, a date is invalid, or start date follows
  end date, THE SYSTEM SHALL reject the request with a clear input error.
- WHEN multiple machines are available and none is selected, THE SYSTEM SHALL
  select the machine with the greatest modeled history as the default.

### DT-06 — Provide a usable read-only workbench page

**User story:** As a workbench user, I want to explore the model in the Monitor
area so that I can understand observed time and its scenario in one place.

**Acceptance criteria (EARS):**

- WHEN a user opens `/digital-twin`, THE SYSTEM SHALL render machine/date
  filters, observed active and non-active time, active share, interval and
  continuity quality, execution-state breakdown, and the selected scenario
  when sufficient data is available.
- WHEN telemetry is absent, THE SYSTEM SHALL render an explanatory empty
  state.
- WHEN telemetry cannot be read within the supported analysis boundary, THE
  SYSTEM SHALL render a data-unavailable explanation.
- WHEN the selected scope has insufficient classified intervals, THE SYSTEM
  SHALL render observed data and explain why no scenario result is available.
- WHEN the route is registered in the workbench, THE SYSTEM SHALL expose it in
  Monitor navigation and support GET only.

### DT-07 — Enforce read-only access

**User story:** As an MSH operator, I want this analysis to respect existing
data permissions and never alter machine or telemetry state.

**Acceptance criteria (EARS):**

- WHEN the route policy is evaluated, THE SYSTEM SHALL require the existing
  human `data.read` permission.
- WHEN a user views or changes the page filters, THE SYSTEM SHALL only read
  local telemetry and SHALL NOT write telemetry, start jobs, change recorder
  state, contact a machine, or issue machine-control instructions.
- WHEN v1 is deployed, THE SYSTEM SHALL use existing project dependencies and
  the Flask application factory.

### DT-08 — Evaluate against existing local data

**User story:** As the feature owner, I want the completed model exercised
against the existing local corpus so that its behavior and limitations are
documented before deployment.

**Acceptance criteria (EARS):**

- WHEN the evaluation is performed, THE SYSTEM SHALL analyze the existing
  historical and example JSONL corpus without modifying those input files.
- WHEN results are reported, THE SYSTEM SHALL include corpus scope, machine
  coverage, scenario checks, excluded-gap handling, and known telemetry
  limitations.
- WHEN 0%, 10%, and 50% scenarios are evaluated, THE SYSTEM SHALL verify the
  accounting identities specified in DT-04 for every reported scope.

## Verification mapping

Detailed execution evidence is in
[`docs/specs/digital_twin_what_if_v1_evaluation.md`](../../../docs/specs/digital_twin_what_if_v1_evaluation.md).
The implementation tasks and their code/test evidence are in `tasks.md`.
