# MSH digital twin and what-if analysis v1

Status: implementation target

## Product intent

Give an authenticated workbench user a machine-specific, evidence-backed time
model and a bounded way to explore one operational scenario: what the recorded
active-time share would look like if a chosen share of observed non-active time
could be recovered.

V1 is a descriptive telemetry model with a transparent scenario calculation. It
is not a causal predictor, production scheduler, cycle-time estimator, or
machine-control surface.

## User experience

- Add a **Digital twin** page at `/digital-twin` in the workbench Monitor
  navigation.
- Let the user choose one machine and an inclusive UTC date range. The default
  range is the selected machine's full available history.
- Show observed active hours, observed non-active hours, observed active share,
  valid sample intervals, and telemetry continuity.
- Let the user simulate recovery of 0–50% of the observed non-active time in
  5-percentage-point steps. Reject values between those steps. Show the
  recovered hours, simulated active hours, simulated active share, and
  percentage-point change from baseline.
- Show how much time came from the reported execution states and explain the
  interval and recovery assumptions beside the results.
- Give clear empty and insufficient-data states. A machine/date selection with
  fewer than 30 usable intervals cannot produce a scenario result.

## Data contract

- Read recursively from the configured local telemetry directory, defaulting
  to `data/`. Use the shared JSONL discovery rules so incomplete upload batches
  stay hidden.
- A row must have a parseable timestamp and machine identifier (`machine_id`,
  `machine`, or `resource`). Parse times as UTC.
- Prefer `execution` when it has a recognized value. `ACTIVE` and `EXECUTING`
  are active. `STOPPED`, `PROGRAM_STOPPED`, `READY`, `INTERRUPTED`,
  `FEED_HOLD`, `FEEDHOLD`, `IDLE`, `HOLD`, and `PAUSED` are non-active.
- If execution is missing or unavailable, infer a row state only when `Srpm`
  or `Sload` is numeric: active when `Srpm > 100` or `Sload > 1`, otherwise
  non-active. Rows without a recognized state or usable fallback signals are
  unclassified and do not contribute to active share.
- Within each machine, sort rows by UTC timestamp. Attribute each row's state
  to the interval until the next row. Ignore duplicate or reversed timestamps.
  Use only intervals from 0.1 through 30 seconds; count longer gaps as
  excluded, never as machine downtime. Split included intervals at UTC
  midnight before applying a date-range filter.
- Exclude intervals with an unclassified starting row from the classified-time
  denominator. Expose the count of skipped long gaps and the continuity ratio:
  positive timestamp intervals from 0.1 through 30 seconds divided by all
  positive timestamp intervals.

## Scenario model

For the selected machine and inclusive UTC date range:

```text
observed_time = active_time + non_active_time
recovered_time = non_active_time * recovery_percent / 100
simulated_active_time = active_time + recovered_time
simulated_active_share = simulated_active_time / observed_time
active_share_change_pp = simulated_active_share - observed_active_share
```

Round only for display. Keep the scenario percentage between 0 and 50. The
scenario keeps total observed time fixed and reallocates the selected share of
non-active time to active time. It must not claim extra parts, revenue, avoided
failures, or a causal outcome.

## Access and operational boundaries

- The page is read-only and requires the existing human `data.read`
  permission.
- It must not write to telemetry, start analysis jobs, contact a machine,
  change recorder state, or issue machine-control instructions.
- Use only existing project dependencies and register the page through the
  existing Flask application factory.
- Missing telemetry must render an empty state rather than raising an error.

## Acceptance criteria

- **DT-01 Data discovery:** recursive discovery accepts `machine_id`, `machine`,
  and `resource`, and ignores incomplete upload batches through the shared
  reader.
- **DT-02 State classification:** known execution values take precedence;
  missing/unavailable execution uses the documented spindle/load thresholds;
  unknown rows are excluded.
- **DT-03 Interval accounting:** samples are sorted per machine; invalid and
  long gaps do not create active/non-active time; intervals crossing midnight
  split across UTC dates.
- **DT-04 Scenario math:** 0% leaves the baseline unchanged; 50% recovers
  exactly half of classified non-active time; out-of-range inputs are rejected;
  selections below 30 classified intervals have no scenario result.
- **DT-05 Scope:** machine and inclusive date-range selection affect only
  matching daily aggregates.
- **DT-06 Web surface:** an authenticated GET page is registered, appears in
  Monitor navigation, and renders loading-ready, empty, and insufficient-data
  states without changing source data.
- **DT-07 Permissions:** route policy grants read-only access through
  `data.read`; no write route is added.
- **DT-08 Existing-data evaluation:** run the complete model against the local
  historical/sample corpus without modifying it, record the dataset scope and
  aggregate outcome, and verify every reported scenario obeys the accounting
  identities above.
