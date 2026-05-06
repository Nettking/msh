# Operator guide

This guide focuses on operating MSH through Flask after the app is running.

## Core concepts

- **Session** — a date or date/hour-scoped workflow directory under `results/workflows/<session-id>`. A session owns its filtered data, script run status, script outputs, and playback exports.
- **Artifact** — a discovered output file, usually CSV/JSON/HTML/PNG, under scanned roots such as `results/`, `data/`, or paths from `MSH_SCAN_DIRS`.
- **Playback export** — a session artifact under `exports/timeline/` that converts telemetry into row-level machine states consumable by `/playback`.
- **Orchestration** — the non-interactive runtime that discovers dates, creates/reuses sessions, filters data, runs automatic scripts, writes runtime state, and prepares playback exports.
- **Bootstrap** — startup processing for the latest discovered source day.
- **Catch-up** — background processing that walks older unprocessed days one day at a time after bootstrap.

## Recommended daily workflow

1. Start the system with Docker or the local Flask command from the quick start.
2. Open `/status` and confirm that discovery has started or completed.
3. Open `/control` to see the selected/latest workflow session and recent control actions.
4. Wait for latest-day bootstrap to finish if you need playback or health-check artifacts immediately.
5. Use `/playback` for machine/day timeline review.
6. Use `/analyses` for generated analysis tables and quick chart previews.
7. Trigger manual/deep scripts only when needed; they are not part of automatic startup because they can be heavier or exploratory.

## `/status`

Use `/status` to answer: "Is the runtime alive, what is it doing, and what data is ready?"

Important fields include:

- app/runtime startup timestamps.
- discovery and bootstrap completion flags.
- current processing phase and currently processing date.
- latest and earliest source dates.
- processed, pending, and total available day counts.
- last successful refresh and last failure.
- playback/catch-up readiness indicators.

A partial failure does not necessarily mean Flask is unusable. The runtime is best-effort and may hand off partial outputs so operators can inspect available artifacts.

## `/control`

Use `/control` for manual operation:

- refresh runtime discovery/update state.
- select an existing workflow session.
- create or reuse a session for latest day, selected day, custom range, or full historical range.
- run startup-safe health checks, the configured workflow, or a single script.
- review recent action status and stdout/stderr snippets.

Only one control action runs at a time in the current single-process threaded implementation. Recent action history is in memory and is not restart-persistent.

## `/playback`

The playback view uses playback-compatible exports, primarily `timeline_rows.csv`. A practical playback-ready session has non-empty filtered data and a reusable or newly generated export manifest for the current session filter signature.

Normal timeline states such as `active`, `dense_idle`, `idle`, and `stopped` can appear when inference supports them. Candidate intervention rows are retained as `intervention_candidate` flags/states and should be treated as overlays rather than the only playback data.

## Automatic vs manual/deep scripts

Automatic scripts are startup-safe and support the playback-ready contract:

- `machines_active_per_day`
- `analyze_missing_sequence_number`
- `missing_per_day_by_machine`
- `sampling_rate_analysis`
- `data_visualizer`

Manual/deep scripts are available from `/control` but excluded from startup automation:

- `data_analysis`
- `ml_analysis`
- `corrolation_machine_pairs`
- `data_pr_day`
- `find_stops`

## Hidden and legacy tools

Recorder, simulator, automation, and environment-specific folders are intentionally hidden from runner discovery. They may still be documented in `catalog/README.md`, but they are not part of the default session workflow.
