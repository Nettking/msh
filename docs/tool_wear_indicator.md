# Tool wear indication from synchronized condition-monitoring data

This document describes the first FCP approach for using synchronized condition-monitoring data as an early indicator of possible tool wear.

The method is deliberately framed as **indication**, not confirmed detection. It turns Observer Phoenix trend measurements and other normalized scalar signals into candidate events that should be reviewed against process context, tool-change logs, operator notes, and quality measurements.

## Rationale

Tool wear often appears indirectly before it is measured directly. Depending on the process and sensor placement, wear can coincide with changes in vibration, load, process levels, alarm frequency, or signal variance.

The Observer Phoenix integration adds synchronized scalar trend measurements to FCP. Once normalized to JSONL, those signals can be compared against recent baselines for the same machine, measurement point, and channel.

## Pipeline position

```text
Observer Phoenix trend measurements
  -> FCP-normalized JSONL
  -> tool_wear_indicator.py
  -> candidate wear events CSV
  -> operator/research review
```

The indicator runs after synchronization and before interpretation. It does not require changing the Observer Phoenix connector or the core cache layer.

## Signal assumptions

The first implementation assumes each record has:

- `timestamp`
- `machine_id`, or a fallback machine/resource field
- `source`
- `point_id`
- `channel`
- numeric `value`

Signals are grouped by:

```text
source + machine_id + point_id + channel
```

This avoids comparing unrelated sensors, channels, or machines.

## Scoring method

For each signal group, FCP computes:

- rolling baseline mean from previous samples.
- rolling baseline standard deviation from previous samples.
- positive deviation z-score.
- simple slope over a previous-sample window.
- sustained event intervals when z-score remains high.

The default threshold is conservative enough for first screening:

```text
z_score >= 2.5
minimum event samples = 3
```

The output is a candidate event, not a maintenance instruction.

## Output files

The script writes:

```text
results/workflows/<session-id>/analyses/tool_wear_indicator/tool_wear_candidates.csv
results/workflows/<session-id>/analyses/tool_wear_indicator/tool_wear_signal_summary.csv
```

The candidate file contains the event interval, point/channel, z-score, slope, baseline mean, latest value, risk level, and a short reason.

## Practical interpretation

A candidate means:

> This signal is unusually high compared with its recent baseline for the same source, machine, point, and channel.

It may be caused by tool wear, but it may also be caused by material, program, speed/feed, coolant, clamping, sensor drift, or a broader machine condition change.

For research-quality evidence, candidates should be compared with at least one of:

- tool-change timestamps.
- measured flank wear.
- surface roughness or dimensional quality.
- operator notes.
- alarm/event cases.
- CNC program or operation phase.

## Future improvements

The next useful improvements are:

- include speed/process regime bins so baselines compare only similar operating states.
- join candidate intervals with tool-change logs.
- add alarm and event-case annotations.
- add spectrum/TWF-specific features rather than flattening those signals into scalar trend JSONL.
- produce a reviewed label dataset for model training.
