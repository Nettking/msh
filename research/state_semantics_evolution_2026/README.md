# Operational-state semantics under digital-twin evolution (November 2026)

**Research worktree:** `Nettking/msh`, branch `research/state-semantics-evolution-2026`. This is **not** a new DT implementation or production feature. The default `main` branch and live recording are unchanged.

**Related PhD proposal:** RQ1 (formal assessment of operational state) and RQ3 (modular evolution and adaptation). It does **not** require federation. This is an exploratory controlled experiment, not a confirmed publication contribution.

## Concrete, falsifiable question

The completed experiment is documented in `EXPERIMENT_PROTOCOL.md`,
`DATASET_MANIFEST.json`, `MEASUREMENTS.json`, `EXPERIMENT_RESULTS.md`, and
`SCIENTIFIC_ASSESSMENT.md`. `execute.py` extends the existing replay rather than
replacing the production classifier. The legacy `replay.py` command below is a
bounded exploratory probe with file-local sampling; use the archive selector
for the balanced, time-disjoint historical experiment. Final measured status
and exact execution revisions are recorded in `EXPERIMENT_RESULTS.md`.

> Under which changes to a modular DT's incoming data representation and assessment logic are its operational-state assessments preserved, intentionally altered, or silently corrupted?

We reuse the actual `catalog.flask_app.services.digital_twin_service.DigitalTwinService`. As implemented, it assesses machine **active versus non-active**, not actual cutting stability, chatter or tool wear. Its precise rules come from `docs/digital_twin.md` and requirements `DT-02`/`DT-03`.

## Synthetic controlled test matrix

| ID | Experimental variation | A priori expectation |
|---|---|---|
| E1 | Rename `execution` upstream and restore through adapter | Classified rows and time totals invariant |
| E2 | Represent spindle rpm as rad/s and adapt to rpm before classification | Classified rows and time totals invariant (unit boundary cases included) |
| E3 | Replace provider schema, restoring canonical `machine_id`, `execution`, `Srpm` and `Sload` | Classified rows and totals invariant |
| E4 | Delete selected `execution` observations | Only altered rows can change row classification; data coverage and totals explicitly tracked |
| E5a | Reorder records and duplicate an identical record | Time accounting invariant, with caveat for pre-existing conflicting duplicate timestamps |
| E5b | Insert a 61-second gap by moving later timestamps | One extra long gap; its interval is excluded, not counted as downtime |
| E6 | Deliberately change fallback spindle threshold from >100 to >150 rpm | Expected differences in affected fallback rows are **semantic change**, not test failure |
| Negative control | Incorrectly consume rad/s numeric values as rpm | Regression comparison must detect a changed state |
| Safeguard | Explicit unknown `execution=OTHER` with apparent spindle activity | Remains unclassified, no silent fallback |

Run deterministic synthetic tests against the **actual service** (not a reimplementation):

```sh
python -m pip install -r requirements.txt
python -m pip install pytest
python -m pytest -q research/state_semantics_evolution_2026/test_evolution.py research/state_semantics_evolution_2026/test_replay.py
```

The 9 classifier-focused tests plus one replay CLI smoke test use isolated temporary JSONL files and never access a machine or original telemetry. E6 is a deliberately mutated alternate output for the selected condition, not a second production classifer. The semantics of 'unchanged' are operational-state labels, observed durations, classified-interval counts, excluded gaps and continuity. We should not confuse behavioral equivalence with physical correctness.

## Bounded replay of actual recorded MTConnect windows

Optional, on a host that already has **authorised, local** access to historical JSONL. This is not automatically run by the CI job and never connects to MSH devices:

```sh
python -m research.state_semantics_evolution_2026.replay \
  --source-root example-data \
  --max-windows 20 --window-records 200 \
  --max-scan-records 200000 \
  --output evolution-replay-results.json
```

- Reads via FCP's shared JSONL reader and selects the first complete non-overlapping 200-record blocks per source and machine, up to the bounded maximum. This sampling is **not random** and should not be characterised as representative.
- Keeps raw input unchanged. Creates isolated temporary JSONL subsets for existing-service evaluation.
- Includes anonymised machine hashes, trace SHA-256, code commit, record counts, baseline aggregate metrics, E1–E3 invariance, E4 degradation, E5 duplicate-order behavior and E6 potentially affected fallback rows.
- Exports **no raw telemetry and no literal machine IDs or source paths**.
- The replay deliberately does not infer stable/unstable machining or calculate fault-detection accuracy.
- If no full windows exist, it exits non-zero and writes an explicit empty report.
- The existing FCP evaluation mentions 29 historical JSONL files / 707,704 records (QuickTurn, VTC, IG500); that is **historical documentation**, not confirmation that these files are mounted or readable in this environment.

## Required methodological comparison

This should not be sold as a new technique merely for detecting regressions. Compare:

1. Existing FCP unit/regression tests (`catalog/flask_app/tests/test_digital_twin_service.py`).
2. A standard, **equal-information** metamorphic/regression suite with the same replay and stated invariants (this experiment currently provides such a suite).
3. Any **new** evolution-aware procedure only if it demonstrably detects additional meaningful semantic changes or reduces false alarms against baseline 2 without using extra privileged information.

Decide claim strength after reviewing actual results, failure labels, coverage and relevant literature on consistency management, metamorphic testing and data contracts. There is no independent physical state ground truth in this protocol. Once real operator labeling becomes available, the study could be extended to stable vs unstable machining, but not before.

## Scientific status

- Test implementation and exact executed results: see `EXPERIMENT_RESULTS.md` and `TEST_RESULTS.json`.
- Self-hosted CI status and historical replay evidence: see `EXPERIMENT_RESULTS.md`; writing a test is not execution evidence.
- Novel contribution: **not established**; conventional B1 remains the comparison standard.
- Main repository branch and original `view-paper` manuscript: unchanged.

This is a feasibility study for Kevin to review before 27 November; a negative finding against standard metamorphic testing remains valid evidence for narrowing the PhD work.
