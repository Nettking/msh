# MSH CNC Telemetry Server

MSH is a server-oriented CNC telemetry workbench and operator-support prototype. A single checkout can run as an always-on Flask server, an MTConnect data recorder, a full recorder-plus-workbench server, a language-model provider for another MSH device, or a one-shot preparation/synchronization job.

The main deployment pattern is: keep raw telemetry under `data/`, keep generated workflow/session artifacts under `results/workflows/`, run selected services with Docker Compose profiles, and access the Flask workbench from another computer through a browser.

## Quick start

Windows recorder station:

```cmd
start.cmd
```

This starts the MSH web app and managed recorder service in the background,
opens Setup, and reuses the selected role, recording switch, checkpoints, and
data already stored under `data\`. The launcher binds the web page to
`127.0.0.1` by default, so setup and network-scan controls are available only
from the MSH machine unless `MSH_WEB_BIND` is explicitly changed.

Recommended server setup:

```bash
python setup_msh.py
docker compose up -d --build
```

Then open the web UI, if the selected mode includes it:

```text
http://localhost:5000
```

From another computer on the same network:

```text
http://<server-ip>:5000
```

The setup helper lets you choose what this checkout should activate:

- full server: Flask workbench plus MTConnect recorder.
- web workbench: Flask, orchestration, playback, source settings, and analysis UI.
- web UI only: Flask without background orchestration.
- recorder station: MTConnect data recorder with setup, controls, and diagnostics UI.
- language-model provider: headless Ollama capability contributed to another MSH device.
- one-shot prep or Observer Phoenix sync.

For web-capable modes, setup also asks whether to enable the AI explainer, whether Ollama runs on the MSH computer or a connected computer, and which standard model profile to use. The three model choices are `edge-small`, `laptop-standard`, and `workstation-strong`.

For detailed setup, ports, profiles, model choices, and firewall notes, see [docs/server_setup.md](docs/server_setup.md) and [docs/quick_start.md](docs/quick_start.md).

Local developer fallback:

```bash
python -m catalog.flask_app.app
```

## Main Flask structure

The current UI is organised around three top-level areas:

```text
Monitor   = current machine/data state and operator support
Knowledge = capture, interpret, compare, and export operator knowledge
System    = setup, connected capabilities, sources, guide, and troubleshooting
```

Main pages:

- `/` — Monitor overview and current readiness summary.
- `/live` — recent telemetry snapshot from scan-discovered JSONL/cache sources.
- `/playback` — playback-compatible timeline exports and machine/day replay views.
- `/assist` — support cards with possible causes, next steps, risks, alternatives, and operator confirmation.
- `/guide` — in-app user guide.
- `/startup` — server role, connected language-model provider, model choice, and runtime startup decisions.
- `/get-started` — focused first-task handoff shown once after initial setup and any required session-start choice.
- `/sources/` — machine/source inventory, MTConnect URL setup, vibration sensors, Observer Phoenix link, and connection tests.
- `/status` — recorder health, private-network MTConnect discovery, runtime milestones, cache state, and readiness signals.
- `/operator-strategies/capture` — capture one raw operator statement quickly during field work.
- `/operator-strategies/review` — review captured statements and open notes for structuring.
- `/operator-strategies/structure/<id>` — map one note into OSL/paper fields.
- `/strategy-comparison` — compare structured strategies by situation, action, evidence, confidence, outcome, and trade-off.
- `/strategies` — intervention logic: YAML rules for detecting candidate situations from telemetry.
- `/osl-export` — export reusable structured strategies to SysML using the paper method.
- `/ai` — read-only AI explainer backed by local Ollama or a connected model-providing computer.

## Machine source checks

System -> Sources supports two machine-level connection checks:

```text
Test MTConnect    = HTTP test from Flask server/container to the configured MTConnect endpoint.
Test VPN/network  = TCP reachability test from Flask server/container to the configured machine-network target.
```

The VPN/network test does not prove that the VPN client is connected at the operating-system level. It proves whether MSH can reach the configured machine-network host from where the app is running.

The Recorder step in main Setup can scan an explicitly entered private IPv4
subnet (at most a `/24`) for MTConnect Agents on port `5000`. Discovery reads
`/probe` and uses the MTConnect UUID, serial number, and Device name. A generic
vendor name such as `Mazak` is never used as the only machine identity. Checked
Agents and the selected role are saved together.

## Knowledge capture flow

The current knowledge flow is statement-first:

```text
raw statement
  -> review later
  -> structured strategy
  -> reusable strategy
  -> intervention logic if detectable
  -> SysML export
```

Use Knowledge -> Capture during a site visit. Structure the note later under Knowledge -> Review Notes. Only reusable structured strategies should be exported to SysML.

## AI explainer

MSH includes a first read-only AI explainer for asking system-understanding questions about the repository. It indexes selected documentation and code, retrieves relevant context, and sends that context to either local Ollama or an Ollama provider contributed by a connected computer.

For the phone-to-laptop flow, see [Connected capabilities](docs/connected_capabilities.md).

`setup_msh.py` can install the model through Docker Compose. The standard choices are:

| Setup choice | Model | Intended device |
| --- | --- | --- |
| `edge-small` | `smollm2:360m` | Small CPU, Raspberry Pi class, or very low memory testing. |
| `laptop-standard` | `llama3.2:3b` | Normal laptop or small server. Default balance. |
| `workstation-strong` | `qwen2.5:7b` | Gaming laptop, workstation, or GPU server. Stronger answers. |

To turn a Docker-capable laptop into a small MSH language-model provider directly from this repository:

```bash
git clone https://github.com/Nettking/msh.git
cd msh
docker compose --profile provider run --rm model-provider-install
```

That command starts the provider on port `11434`, installs `smollm2:360m`, and keeps the model in a persistent Docker volume. The equivalent command-driven setup is:

```bash
python setup_msh.py --mode language-model-provider --ai-profile edge-small --start --pull-model
```

Retry model installation manually with:

```bash
docker compose up -d ollama
docker compose run --rm ollama-pull
```

Preview retrieved context without calling Ollama:

```bash
python -m catalog.ai.ask --dry-run "How does data flow through MSH?"
```

Ask through Ollama:

```bash
python -m catalog.ai.ask "What does Sources do?"
```

Use `MSH_AI_MODEL` or `--model` to select the Ollama model. See [docs/ai_explainer.md](docs/ai_explainer.md) for scope, exclusions, and safety boundaries.

## Repository map

- `setup_msh.py` — interactive local deployment setup that writes ignored Docker Compose settings.
- `docker-compose.yml` — profile-driven server components: web, recorder, full, prep, observer-sync, local AI, and connected model provider.
- `catalog/flask_app/` — primary Flask application, routes, templates, and UI-facing services.
- `catalog/orchestrator/` — non-interactive bootstrap/catch-up orchestration.
- `catalog/runner/` — workflow session metadata, date filtering, script discovery/execution, and playback export helpers.
- `catalog/common/` — shared telemetry loading, normalization, source synchronization state, state inference, metrics, and timeline export utilities.
- `catalog/observer_phoenix/` — source connector that synchronizes SKF Observer Phoenix trend measurements into MSH-normalized JSONL.
- `catalog/standalone-recorder_v2/` — configurable MTConnect recorder for recorder-only or full-server deployments.
- `catalog/ai/` — read-only repository explanation helpers backed by local or connected Ollama.
- `catalog/*/` — runner-visible automatic, manual, deep/exploratory, and legacy scripts plus script-specific README files.
- `data/` — local raw JSONL telemetry input and source-specific landing location; not intended for committed production data.
- `data/sources/` — source-specific normalized JSONL landing area for synchronized/recorded external sources.
- `data/source_config/` — local machine/sensor inventory and source UI configuration.
- `data/source_state/` — source synchronization and recorder watermarks/state; not telemetry input.
- `data/operator_strategy_records/` — local operator knowledge capture notes; JSON, not telemetry JSONL.
- `results/` — generated analysis outputs, workflow sessions, runtime state, and discovered artifacts.
- `example-data/` — small sample JSONL input for development and documentation.
- `ops/` — host-side operational helpers.
- `legacy/` — retained historical notes or deprecated material, not the current path.

See [catalog/README.md](catalog/README.md) for the script catalog and analysis workflow.

## Detailed documentation

- [Server setup](docs/server_setup.md) — deployment modes, LAN access, Docker Compose profiles, AI model setup, and recorder setup.
- [Quick start](docs/quick_start.md) — setup commands and first-run expectations.
- [Operator guide](docs/operator_guide.md) — app areas, user workflow, source tests, knowledge capture, playback, and diagnostics.
- [Operator knowledge capture](docs/operator_strategy_capture.md) — raw-statement capture, review, structuring, intervention logic, and SysML export.
- [Source synchronization](docs/source_synchronization.md) — multi-source landing layout, machine/source inventory, connection tests, watermarks, and synchronization policy.
- [SKF Observer Phoenix integration](docs/integrations/skf_observer_phoenix.md) — Observer Phoenix setup, export command, and mapping to MSH JSONL.
- [Intervention strategies](docs/intervention_strategies.md) — config-driven candidate event labels, strategies, and review-ready output schema.
- [Workflow sessions](docs/workflow_sessions.md) — session layout, cache reuse, script status, bootstrap, and catch-up behavior.
- [Data contract](docs/data_contract.md) — raw JSONL assumptions, normalized fields, derived artifacts, and playback-ready contract.
- [Architecture](docs/architecture.md) — system components, dataflow diagram, policies, and design intent.
- [AI explainer](docs/ai_explainer.md) — read-only Ollama/RAG explainer scope, indexed sources, exclusions, and usage.
- [Troubleshooting](docs/troubleshooting.md) — common startup, data, playback, Docker, and script-run issues.

## Data layout note

Current runtime and cache paths generally support recursive `data/**/*.jsonl` discovery. Some older manual or legacy scripts may still assume a flat `data/*.jsonl` layout. Prefer the shared helpers in `catalog/common/` and `catalog/runner/` for new work so recursive discovery, timestamp parsing, and machine normalization stay consistent.

For synchronized external sources, write only MSH-normalized telemetry JSONL under `data/sources/<source>/jsonl/`. Keep connector state, API metadata, machine/source inventory, and raw non-telemetry payloads out of `.jsonl` files under `data/`.

## Deprecated interactive menu

`catalog/runner/menu.py` is retained for backward compatibility, but Flask is the primary operational surface. New operation and documentation should assume the Flask-first app unless explicitly maintaining legacy behavior.

## Telemetry analytics cache (Parquet + DuckDB)

Raw JSONL telemetry files in `data/` remain the source of truth. The project also includes an analytical read cache that converts those JSONL records into partitioned Parquet files and queries them with DuckDB. Fresh caches are now used by selected production Flask paths, including live/latest telemetry, playback machine/day loading, exploration filtering, and machine/day summaries, while JSONL/session files remain the fallback when the cache is missing, stale, or lacks required fields.

Cache layout:

```text
data/cache/parquet/machine_id=<machine>/date=<YYYY-MM-DD>/part.parquet
```

The cache is safe to delete and rebuild because it is derived entirely from raw JSONL. Existing scripts can continue reading JSONL directly; cache-aware Flask routes check freshness first and fall back conservatively. Rebuilds also write `data/cache/parquet/_manifest.json` with source JSONL paths, mtimes, sizes, imported row count, and rebuild time so freshness/status checks can detect changed, renamed, or deleted source files.

### Rebuild the cache

From the control/runtime surfaces, use **Rebuild telemetry analytics cache** to run a rebuild and show the result in recent control activity. From the host or inside the Flask container, you can also run:

```bash
python -m catalog.cache.rebuild_telemetry_cache
```

The command recursively scans `data/**/*.jsonl`, loads the raw rows, writes partitioned Parquet under `data/cache/parquet/`, prints the imported row count, and prints the output cache path. It rewrites the cache from source JSONL on each run, so it is safe to run multiple times without appending duplicate cache rows. This is still a full rebuild, not incremental ingestion.

Custom paths are available for development and tests:

```bash
python -m catalog.cache.rebuild_telemetry_cache --data-dir data --cache-dir data/cache/parquet
```

### Docker relationship

The standard server startup is:

```bash
python setup_msh.py
docker compose up -d --build
```

The Flask image installs the same Python dependencies as local development, including `duckdb` and `pyarrow`. The Docker services mount `./data:/app/data`, so raw JSONL, source configuration, recorder state, and the derived cache remain persistent on the host across container rebuilds.

Cache rebuild is manual-only for now: starting the server does not automatically refresh `data/cache/parquet/`. Use the app or run `python -m catalog.cache.rebuild_telemetry_cache` whenever new raw telemetry should be reflected in DuckDB/Parquet queries. Diagnostics shows whether the cache is missing, fresh, or stale, plus source file and cached-row counts.

### Querying the cache

Use `catalog.common.telemetry_cache.TelemetryCache` for DuckDB-backed helper queries:

- latest sample per machine
- recent samples per machine for live/latest UI state
- playback timeline derivation for cache-covered machine/day slices
- exploration samples filtered by date window
- samples by machine and timestamp range
- samples by date range
- machine/day row counts
- machine activity summary
- optional pandas DataFrame output via `as_dataframe=True`

If the Parquet cache is absent, helper queries return empty results rather than failing. Cache-aware Flask paths additionally require a fresh cached status check before using the cache and log when they use DuckDB/Parquet versus JSONL or session-export fallback paths. Cache-status scans are protected by a short TTL in request paths so frequently refreshed pages do not recursively inspect every JSONL/Parquet file on every request.

### Limitations and future path

- The cache is rebuilt from JSONL and is not an operational write-ahead store.
- Rebuilds currently load JSONL into pandas before writing Parquet; streaming/incremental rebuilds are future work.
