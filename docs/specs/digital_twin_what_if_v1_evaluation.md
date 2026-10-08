# Digital twin v1 evaluation

Date: 2026-10-08

## Corpus

The existing local telemetry was read without modifying it:

| Source | JSONL files | Timestamped machine records | Machines |
| --- | ---: | ---: | --- |
| `data_old/` | 28 | 686,223 | QuickTurn, VTC |
| `example-data/` | 1 | 21,481 | QuickTurn, IG500 |
| **Total** | **29** | **707,704** | **QuickTurn, VTC, IG500** |

The shared date coverage is 2025-05-22 through 2026-03-23. `data/` itself has
no JSONL telemetry in this checkout, so the historical corpus was selected
explicitly for this evaluation; it was not copied into or published from
`data/`.

## Full-history model results

Scenario percentages are the share of each row's observed non-active time
reallocated to active time. Values are based on valid intervals of 0.1–30
seconds; longer gaps do not enter the reported hours.

| Source | Machine | Classified observed hours | Active share observed | At 10% recovery | At 50% recovery | Continuity | Long gaps excluded |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `data_old/` | QuickTurn | 173.36 h | 42.1% | 47.9% | 71.0% | 98.6% | 5,388 |
| `data_old/` | VTC | 139.10 h | 47.0% | 52.3% | 73.5% | 98.2% | 5,169 |
| `example-data/` | QuickTurn | 6.95 h | 58.4% | 62.6% | 79.2% | 99.7% | 49 |
| `example-data/` | IG500 | 1.78 h | 52.5% | 57.2% | 76.2% | 87.3% | 510 |

For every machine and each 0%, 10%, and 50% scenario, the run checked:

```text
simulated active time = observed active time + observed non-active time × recovery percentage
simulated active share = simulated active time ÷ classified observed time
```

Both identities held. The 30-second gap cutoff excluded 10,557 intervals from
`data_old/` and 559 intervals from the example day rather than inventing
operating or downtime. The example day's lower IG500 continuity is exposed in
the UI and should be considered when comparing its estimate.

## Acceptance runs

- `DT-01`–`DT-05`: service tests cover recursive inputs, incomplete upload
  hiding, state precedence/fallback, UTC midnight splitting, short/long gap
  handling, scope filtering, scenario bounds, and the 30-interval threshold.
- `DT-06`–`DT-07`: route tests render empty and populated pages, check Monitor
  navigation, verify the `data.read` route policy, and assert that no write
  method is registered.
- `DT-08`: the service was run against all 29 local JSONL files above. The
  source paths were read-only inputs; no results or changes were written to
  those data directories.
