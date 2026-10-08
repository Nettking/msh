# Digital twin and what-if analysis

Open **Monitor → Digital twin** to inspect a machine's recorded active-time
share and explore a bounded scenario.

## Choose a machine and period

Choose a machine and an inclusive UTC date range. The initial range covers that
machine's available telemetry history. The page reads timestamped JSONL under
the configured data root (`data/` by default, `/app/data` in the standard Docker
deployment). It reads telemetry locally and does not alter source files.

## Run a scenario

Choose how much non-active time to reallocate to active time, from 0% to 50%, in
5-point steps. The page reports the observed and simulated active hours and
shares, the recovered hours, and the change in percentage points.

The calculation keeps classified observed time constant:

```text
recovered time = observed non-active time × recovery percentage
simulated active time = observed active time + recovered time
simulated active share = simulated active time ÷ classified observed time
```

This is a hypothetical time-budget adjustment. It does not predict part counts,
cycle time, revenue, failure risk, or a causal result. It sends no machine
commands.

## How the machine time is classified

The model sorts each machine's records by UTC timestamp. A row's state applies
until the next row for that machine. Intervals from 0.1 to 30 seconds are
included; duplicates, reversed timestamps, longer gaps, and rows without a
recognized state are excluded from the time-share calculation. Intervals that
cross UTC midnight are split across the two dates.

Reported `execution` values take precedence. `ACTIVE` and `EXECUTING` count as
active. `STOPPED`, `PROGRAM_STOPPED`, `READY`, `INTERRUPTED`, `FEED_HOLD`,
`FEEDHOLD`, `IDLE`, `HOLD`, and `PAUSED` count as non-active. When execution is
missing or unavailable, spindle speed above 100 rpm or load above 1 infers an
active row; usable numeric signals below those thresholds infer a non-active
row.

The continuity percentage shows the share of positive adjacent timestamp
intervals that fall within the 0.1–30 second inclusion window. Longer gaps are
counted separately and are never treated as machine downtime. At least 30
classified intervals are required to show a scenario result.

## Data and access

The page requires the existing human `data.read` permission. It supports local
JSONL telemetry up to a combined source size of 2 GiB. The daily summary is
cached in the Flask process and refreshed when the source-file inventory
changes. If telemetry is being added while the page is open, reload after the
short inventory check interval to pick up new files.

The implementation and acceptance requirements are in
[`specs/digital_twin_what_if_v1.md`](specs/digital_twin_what_if_v1.md).
