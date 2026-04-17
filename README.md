# MSH MTConnect Data Tools

Script-first repository for recording and analyzing MTConnect machine telemetry.

## Current layout

```text
.
├── README.md
├── Dockerfile
├── requirements.txt
├── catalog/
│   ├── README.md
│   ├── runner/
│   │   ├── menu.py
│   │   └── menu_utils.py
│   └── <script-folder>/
│       ├── <script>.py
│       └── README.md
├── data/
├── results/
├── legacy/
└── record data/
```

## Docker quick start

Build the interactive runner image:

```bash
docker compose build msh
```

Run the interactive one-shot analysis container:

```bash
docker compose run --rm msh
```

The `msh` service is an **interactive runner** (`catalog/runner/menu.py`), not a long-running background service.
Use `docker compose run --rm msh` for normal usage so keyboard input is attached directly to the CLI prompt.
`docker compose up` / `docker compose up --build` is **not** the intended entrypoint for this service.

Because this is a one-shot CLI workflow, the container exits when the run completes (`--rm` removes it automatically).

Docker Compose automatically mounts local folders:
- `./data` → `/app/data`
- `./results` → `/app/results`

No manual `-v` flags are needed.

Container details:
- Working directory: `/app`
- Entrypoint: `python catalog/runner/menu.py`
- Python runs unbuffered for readable logs.

## Interactive numeric menu flow

When the container starts, `catalog/runner/menu.py` runs and prompts:

1. Choose one discovered script (numbered `1..N`)
2. Choose start date by number from discovered available dates
3. Choose end date by number from the same list
4. Confirm and execute

Script discovery is dynamic at runtime and sorted alphabetically for stable numbering.

Date list discovery (`data/`):
- Primary strategy: parse JSONL `timestamp` fields
- Fallback: parse `YYYY-MM-DD` or `YYYYMMDD` from filename
- Output dates are unique and sorted ISO dates (`YYYY-MM-DD`)
- Filtering uses the same logic to avoid offering date choices that cannot match data.

Execution strategy:
- A filtered dataset is built under `results/menu_runs/menu_run_*/data`
- Only records inside the selected date range are copied
- The selected script runs inside that run directory, so outputs for that run stay together

The launcher prints:
- selected script
- selected script path
- selected start and end date
- filtered dataset path
- matched record/file counts
- run output directory

## Script discovery rules and exclusions

Discovery includes scripts under `catalog/` matching:
- `catalog/<folder>/<folder>.py`
- `catalog/<folder>/main.py` (fallback if present)

Discovery excludes:
- `catalog/runner/*`
- helper-style files (`menu.py`, `menu_utils.py`, `__init__.py`)
- known environment-specific/incompatible folders:
  - `auto_connect`
  - `data_simulator`
  - `interventions`
  - `standalone_recorder`
  - `standalone-recorder_v2`

## Run scripts directly from repository root (without Docker)

```bash
python catalog/sampling_rate_analysis/sampling_rate_analysis.py
python catalog/analyze_missing_sequence_number/analyze_missing_sequence_number.py
python catalog/missing_per_day_by_machine/missing_per_day_by_machine.py
python catalog/machines_active_per_day/machines_active_per_day.py
python catalog/find_stops/find_stops.py
python catalog/data_pr_day/data_pr_day.py
python catalog/corrolation_machine_pairs/corrolation_machine_pairs.py
python catalog/data_analysis/data_analysis.py
python catalog/data_visualizer/data_visualizer.py
python catalog/ml_analysis/ml_analysis.py
```

## Limitations

- Scripts still have mixed output paths internally, but menu runs isolate outputs per run directory.
- Date filtering keeps records with parseable `timestamp`; malformed JSONL lines and records without valid timestamps are skipped.
- For files without parseable timestamps, filename date fallback is used only when the entire file follows a date-in-name pattern.
