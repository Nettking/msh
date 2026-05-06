# Intervention strategy candidates

The intervention strategy layer is a small, config-driven detector for operator intervention **candidates** during validation. It lets operators and developers iterate on thresholds, enabled rules, and suggested labels without editing detector code.

This layer does not define ground truth. It writes review-ready candidate rows that can later be validated with `review_status`, `human_label`, and `notes`. A small Flask editor is available at `/strategies` for quickly changing the strategy YAML without turning the detector into a full annotation platform.

## Labels vs. strategies

- **Labels** live in `catalog/common/intervention_labels.yaml` and define the controlled vocabulary available to strategy outputs. The initial vocabulary includes override changes, feed adjustments, spindle-load collapse, tool changes, process stops, setup/adjustment, and `unknown`.
- **Strategies** live in `catalog/common/intervention_strategies.yaml` and define enabled rules, signals, thresholds, suggested labels, and short descriptions.

Strategies only suggest a `suggested_label`. Human review can accept, replace, or reject that suggestion later.

## Flask strategy editor

The operator/developer Flask UI exposes `GET /strategies` and `POST /strategies/save` as a thin editor for `catalog/common/intervention_strategies.yaml`. The page shows the current strategy config, label vocabulary from `catalog/common/intervention_labels.yaml`, the active strategy signature, validation messages, and one editable card per strategy.

Use `/strategies` to enable or disable rules, tune thresholds, change suggested labels, update descriptions, add a new strategy, or remove an obsolete strategy. The UI validates form submissions against the same strategy-runner rules before writing YAML, including supported strategy types, known labels, numeric thresholds, non-negative windows, required descriptions, and duplicate enabled IDs.

Saving from the UI only updates the YAML config. It does not run the full workflow automatically. Strategy signatures continue to drive cache invalidation: when thresholds, enabled rules, suggested labels, or other active strategy fields change, cached candidate outputs are considered stale and candidate events regenerate the next time playback exports are prepared or rerun.

## Supported strategy types

| Type | Purpose | Score |
| --- | --- | --- |
| `delta_threshold` | Detects row-to-row numeric drops or jumps, such as `Sovr` decreasing by at least 10 points. Negative thresholds detect drops; positive thresholds detect jumps. | Absolute delta magnitude |
| `ratio_drop` | Detects sharp numeric drops relative to the previous sample, such as `Sload` falling below 50% of the prior value. Optional companion signals, such as `Srpm`, are included in evidence. | `1 - ratio` |
| `value_change` | Detects categorical or numeric value changes, such as `Tool_number` or `Tool_group`. | `1.0` |

## Candidate event schema

Strategy output uses a consistent CSV schema:

- `timestamp`
- `machine_id`
- `strategy_id`
- `strategy_type`
- `suggested_label`
- `event_score`
- `fired_rule`
- `evidence`
- `window_start`
- `window_end`
- `review_status`
- `human_label`
- `notes`

New candidates default to `review_status = unreviewed`; `human_label` and `notes` are empty until a later validation workflow fills them in.

## Workflow outputs

Session playback export preparation now writes strategy artifacts alongside the timeline export under the session export directory, typically `results/workflows/<session-id>/exports/timeline/`:

- `candidate_events.csv` — strategy-generated candidate event rows with review fields.
- `strategy_summary.csv` — one row per enabled strategy with candidate counts and mean score.
- `strategies_used.yaml` — the enabled strategy configuration and strategy signature used for the run.

The current active strategy signature is also stored in `manifest.json`; playback export reuse compares it with the current config so edits to thresholds, enabled rules, or suggested labels regenerate candidate outputs. The timeline export remains playback-compatible; candidate events are additional validation artifacts and are treated as overlays rather than playback rows.

## Validation iteration

To iterate on validation strategy behavior, use `/strategies` or edit `catalog/common/intervention_strategies.yaml` directly to enable or disable strategies, tune thresholds, or change suggested labels. Keep labels in `catalog/common/intervention_labels.yaml` so candidate output remains consistent across runs.

Because candidates are suggestions, a detected row should be read as “review this event” rather than “this event is true.” Ground-truth assignment belongs to later human validation using `review_status`, `human_label`, and `notes`.
