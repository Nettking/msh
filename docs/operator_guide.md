# Operator guide

This guide focuses on operating MSH through Flask after the app is running.

## Main app areas

MSH is organised around three top-level areas:

```text
Monitor   = what is happening now
Knowledge = capture, interpret, compare, and export operator knowledge
System    = setup, sources, guide, and troubleshooting
```

### Monitor

Use Monitor when you want to inspect current or replayed machine/data state.

Primary pages:

- **Overview** — start here to see whether MSH is usable now.
- **Live** — recent telemetry snapshot from discovered JSONL/cache sources.
- **Playback** — machine/day replay from playback-ready timeline exports.
- **Assist** — support cards with possible causes, suggested next steps, risks, alternatives, and operator confirmation.

### Knowledge

Use Knowledge when you want to capture and structure operator experience.

Primary pages:

- **Capture** — save one raw statement during a site visit.
- **Review Notes** — review captured statements later.
- **Strategies** — compare structured notes by situation, action, evidence, confidence, outcome, and trade-off.
- **Intervention Logic** — technical YAML rule layer for detecting candidate situations from telemetry.
- **SysML Export** — export reusable structured strategies using the paper method: coded CNC statement -> OSL keywords -> SysML artefact.

### System

Use System when you need configuration or troubleshooting.

Primary pages:

- **Guide** — in-app onboarding and navigation help.
- **Setup** — server/startup mode choices.
- **Sources** — machines, MTConnect endpoints, vibration sensors, Observer Phoenix, and connection tests.
- **Diagnostics** — runtime/data readiness details and failure reasons.

## Recommended operator workflow

1. Start the system with Docker or the local Flask command from the quick start.
2. Open **System -> Guide** if you are unsure where to go.
3. Open **Monitor -> Overview** and confirm that the app/runtime state is usable.
4. Use **System -> Sources** to configure machines, MTConnect URLs, vibration sensors, and source connectors.
5. Use **Test MTConnect** and **Test VPN/network** from Sources when setting up machine connectivity.
6. Use **Monitor -> Live** or **Monitor -> Playback** to inspect current/replayed telemetry.
7. Use **Monitor -> Assist** when support cards are relevant.
8. During a site visit, use **Knowledge -> Capture** to save raw operator statements quickly.
9. Later, use **Knowledge -> Review Notes** to structure notes into OSL/paper fields.
10. Use **Knowledge -> Strategies** to compare structured strategies.
11. Add **Knowledge -> Intervention Logic** only when telemetry can detect a candidate situation.
12. Export reusable strategies with **Knowledge -> SysML Export**.

## Sources and connection tests

System -> Sources is the source-configuration page.

For each machine, you can store:

- machine name.
- machine type.
- controller/adapter.
- MTConnect URL.
- VPN/network test host.
- VPN/network test port.
- notes.

### MTConnect test

The **Test MTConnect** button tests the configured MTConnect endpoint from the Flask server/container. If the stored URL is a base adapter URL, MSH tests the `/current` endpoint automatically.

Example:

```text
10.0.0.20:5000
-> http://10.0.0.20:5000/current
```

A successful test means the Flask server/container reached the MTConnect endpoint and received a response.

### VPN/network test

The **Test VPN/network** button opens a TCP connection from the Flask server/container to the configured host and port.

This does not prove that the VPN client is connected at the operating-system level. It proves the useful thing for MSH: whether the app can reach the machine-side network target from where MSH is running.

If no VPN/network test host is configured, the test falls back to the MTConnect host/port when possible.

## Knowledge capture workflow

The current knowledge flow is statement-first:

```text
raw statement
  -> review later
  -> structured strategy
  -> reusable strategy
  -> intervention logic if detectable
  -> SysML export
```

Capture should stay fast. The user should be able to save one useful raw statement without deciding the structure immediately.

A raw statement can be incomplete, for example:

```text
When this machine is cold, the first part can drift. I usually wait before changing offsets.
```

Later, the note can be mapped into OSL/paper fields such as context, observation, hypothesis, action, rationale, risk, evidence, and outcome.

## Intervention logic

Intervention logic is not the same as an operator strategy.

```text
Structured strategy = interpreted operator knowledge.
Intervention logic  = technical YAML rule for detecting candidate situations from telemetry.
```

Use intervention logic only when a situation can reasonably be detected from signals such as state, load, vibration, alarms, measurements, or event timing.

## SysML export

Reusable structured strategies can be exported through Knowledge -> SysML Export.

The export follows the Systems-paper method:

```text
coded CNC strategy statement
  -> OSL keywords
  -> SysML artefact
```

## Core runtime concepts

- **Workflow session** — a date or date/hour-scoped directory under `results/workflows/<session-id>`. A workflow session owns its filtered data, script run status, script outputs, and playback exports.
- **Artifact** — a discovered output file, usually CSV/JSON/HTML/PNG, under scanned roots such as `results/`, `data/`, or paths from `MSH_SCAN_DIRS`.
- **Playback-ready contract** — the minimum session state needed by `/playback`: non-empty filtered data plus a current `exports/timeline/` manifest/export. See [Data contract](data_contract.md#playback-ready-contract).
- **Bootstrap** — startup processing for the latest discovered source day.
- **Catch-up** — background processing that walks older unprocessed days one day at a time after bootstrap.
- **Automatic script** — startup-safe script included in bootstrap/catch-up and the playback-ready contract.
- **Manual script** — operator-triggered script available from `/control`, excluded from bootstrap/catch-up.
- **Deep/exploratory script** — a manual script that may be slower, research-oriented, or less operationally bounded.
- **Legacy script** — retained for historical or compatibility value, but not recommended as the main workflow path.

## Diagnostics

Use System -> Diagnostics to answer: "Is the runtime alive, what is it doing, and what data is ready?"

Important fields include:

- app/runtime startup timestamps.
- discovery and bootstrap completion flags.
- current processing phase and currently processing date.
- latest and earliest source dates.
- processed, pending, and total available day counts.
- last successful refresh and last failure.
- playback/catch-up readiness indicators.
- telemetry analytics cache state, source file count, cached row count, and last rebuild time.

A partial failure does not necessarily mean Flask is unusable. The runtime is best-effort and may hand off partial outputs so operators can inspect available artifacts.

## Playback

The playback view uses playback-compatible exports, primarily `timeline_rows.csv`. A practical playback-ready workflow session follows the [playback-ready contract](data_contract.md#playback-ready-contract).

Normal timeline states such as `active`, `dense_idle`, `idle`, and `stopped` can appear when inference supports them. Candidate intervention rows are retained as `intervention_candidate` flags/states and should be treated as overlays rather than the only playback data.

## Script categories

Automatic scripts are startup-safe and support the playback-ready contract:

- `machines_active_per_day`
- `analyze_missing_sequence_number`
- `missing_per_day_by_machine`
- `sampling_rate_analysis`
- `data_visualizer`

Manual scripts are available from legacy/control surfaces but excluded from bootstrap/catch-up:

- `data_pr_day` — manual raw inspection; writes the machine/day summary CSV used by machine/day views.
- `find_stops` — manual stop-focused inspection; writes hourly stop-interval summaries.

Deep/exploratory scripts are also manual and may be slower or research-oriented:

- `data_analysis`
- `ml_analysis`

Legacy scripts are retained for historical compatibility rather than the main path:

- `corrolation_machine_pairs`

See [catalog/README.md](../catalog/README.md) for runner-visible script metadata and workflow stages.

## Hidden and legacy tools

Recorder, simulator, automation, and environment-specific folders are intentionally hidden from runner discovery. They may still be documented in [catalog/README.md](../catalog/README.md#hidden-or-non-workflow-folders), but they are not part of the default session workflow.
