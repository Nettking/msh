# Tool wear indicator

`tool_wear_indicator.py` generates early tool-wear candidate events from synchronized scalar telemetry, especially Observer Phoenix trend measurements.

It is a **screening indicator**, not a verified tool-wear detector. The script flags sustained positive deviations from a local rolling baseline for the same source, machine, point, and channel.

## Input

The script reads recursive MSH JSONL telemetry from `data/` by default. It works best with records produced by the Observer Phoenix connector, for example:

```json
{
  "timestamp": "2026-07-05T10:00:00Z",
  "machine_id": "87",
  "source": "observer_phoenix",
  "point_id": "92",
  "point_name": "Spindle vibration",
  "channel": "1",
  "value": 0.42,
  "unit": "g"
}
```

Required fields:

- `timestamp`
- `value`
- machine identifier: preferably `machine_id`

Strongly recommended fields:

- `source`
- `point_id`
- `point_name`
- `channel`
- `unit`

## Method

For each `source + machine_id + point_id + channel` signal, the script:

1. sorts records by timestamp.
2. builds a rolling baseline from previous samples only.
3. computes a positive deviation z-score.
4. estimates a simple slope over a previous-sample window.
5. aggregates sustained high-z samples into candidate events.

The default candidate threshold is `z_score >= 2.5` for at least three candidate samples.

## Output

Default outputs are written under the active workflow session when the script is run through MSH, or under the current directory fallback when run directly:

```text
results/workflows/<session-id>/analyses/tool_wear_indicator/tool_wear_candidates.csv
results/workflows/<session-id>/analyses/tool_wear_indicator/tool_wear_signal_summary.csv
```

`tool_wear_candidates.csv` includes:

- machine and point identifiers.
- event start/end.
- duration and sample count.
- max/mean z-score.
- baseline mean and last value.
- simple slope estimate.
- risk level: `watch`, `elevated`, or `high`.
- a short human-readable reason.

`tool_wear_signal_summary.csv` includes per-signal coverage and simple maxima so the user can see whether there was enough data to build a baseline.

## Run examples

Default Observer Phoenix source:

```bash
python -m catalog.tool_wear_indicator.tool_wear_indicator
```

Use all numeric telemetry sources:

```bash
python -m catalog.tool_wear_indicator.tool_wear_indicator --source all
```

Tune the sensitivity:

```bash
python -m catalog.tool_wear_indicator.tool_wear_indicator \
  --z-threshold 3.0 \
  --min-baseline-samples 40 \
  --baseline-window 200
```

## Interpretation

A candidate means: under the same source/machine/point/channel, the current scalar level is unusually high compared with its recent historical baseline.

That can indicate tool wear, but it can also indicate:

- different material or operation.
- feed/speed changes.
- fixture or clamping changes.
- coolant changes.
- sensor or mounting changes.
- machine condition changes unrelated to the tool.

For stronger claims, combine this output with tool-change timestamps, operation context, surface quality, operator notes, or measured flank wear.
