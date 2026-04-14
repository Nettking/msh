# MSH MTConnect Data Tools

A script-oriented repository for recording and analyzing MTConnect telemetry from CNC machines at Mekanisk Service Halden (MSH).

This is **not** a packaged application; it is a practical working repo of standalone scripts, generated analysis outputs, and recorder utilities.

---

## Who this repository is for

- Engineers/analysts working with MSH machine telemetry.
- Collaborators who need to run existing recorders and analysis scripts.
- Maintainers who need to understand script relationships without redesigning the project.

---

## Inferred repository purpose

Based on current files, the repository supports this workflow:

1. Poll MTConnect endpoints and store telemetry as JSONL.
2. Run data integrity checks (sampling frequency, sequence gaps, active machine counts).
3. Generate exploratory plots for machine/day behavior and stop intervals.
4. Run targeted analyses (intervention episodes, override changes, stop-pattern correlation).
5. Optionally run an ML experiment to predict future stops.
6. Optionally inspect historical telemetry via Streamlit playback.

Where intent is uncertain, this README explicitly notes uncertainty instead of assuming.

---

## Repository inventory and classification

| Path / item | Likely role | Classification |
|---|---|---|
| `record data/standalone_recorder.py` | Original MTConnect recorder | scripts/utilities |
| `record data/standalone-recorder_v2.py` | Newer recorder with buffering/state/backoff | scripts/utilities |
| `record data/auto_connect.py` + `button.png` | Desktop auto-click helper using image matching | scripts/utilities (separate from MTConnect pipeline) |
| `sampling_rate_analysis.py` | Daily sampling-rate QA | source code (analysis script) |
| `analyze_missing_sequence_number.py` | Missing MTConnect sequence counts by day | source code (analysis script) |
| `missing_per_day_by_machine.py` | Missing sequence analysis by machine/day | source code (analysis script) |
| `machines_active_per_day.py` | Active machine count per day | source code (analysis script) |
| `find_stops.py` | Detect stop intervals and hourly stop timeline plots | source code (analysis script) |
| `corrolation_machine_pairs.py` | Correlation matrix of machine stop patterns | experiment/analysis script |
| `data_pr_day.py` | Per-machine/day numeric timeseries plots | source code (analysis script) |
| `interventions.py` | Intervention episode + override-change extraction | source code (analysis script) |
| `ml_analysis.py` | RandomForest stop prediction experiment | experiments |
| `data_simulator.py` | Streamlit playback of recorded data | source code (interactive tool) |
| `data/` | Intended local telemetry input area | raw data location (not versioned) |
| `graphs/`, `plots/`, `plots_per_machine/`, `ml_results/` (created at runtime) | Generated figures/artifacts | generated outputs/results |
| `git/` | Unclear historical folder with ignore-only references | uncertain / likely legacy |
| `README.md`, `record data/README.md`, `data/README.md`, `results/README.md`, `legacy/README.md` | Project documentation | documentation |

### Not present currently

- Notebooks (`*.ipynb`) were not found.
- No committed raw/processed datasets were found.
- No formal package/build configuration (`pyproject.toml`, `setup.py`) was found.

---

## Current structure

```text
.
├── README.md
├── data/                     # documented input location (contents ignored)
├── legacy/                   # reserved archive area
├── results/                  # optional shared output anchor (contents ignored)
├── analyze_missing_sequence_number.py
├── corrolation_machine_pairs.py
├── data_pr_day.py
├── data_simulator.py
├── find_stops.py
├── interventions.py
├── machines_active_per_day.py
├── missing_per_day_by_machine.py
├── ml_analysis.py
├── sampling_rate_analysis.py
├── git/                      # unclear historical folder (left untouched)
└── record data/
    ├── README.md
    ├── auto_connect.py
    ├── standalone_recorder.py
    └── standalone-recorder_v2.py
```

---

## High-level workflow

### 1) Record data

- Legacy/simple recorder:
  ```bash
  python "record data/standalone_recorder.py"
  ```

- More robust recorder:
  ```bash
  python "record data/standalone-recorder_v2.py"
  ```

### 2) Run quality checks

```bash
python sampling_rate_analysis.py
python analyze_missing_sequence_number.py
python missing_per_day_by_machine.py
python machines_active_per_day.py
```

### 3) Run exploratory analysis

```bash
python find_stops.py
python corrolation_machine_pairs.py
python data_pr_day.py
python interventions.py
```

### 4) Interactive playback / experiment

```bash
streamlit run data_simulator.py
python ml_analysis.py
```

---

## Script-by-script inputs and outputs

### Recorders

- `record data/standalone_recorder.py`
  - **Input:** MTConnect HTTP endpoints defined in `SOURCES`.
  - **Output:** `data/YYYY-MM-DD.jsonl`.

- `record data/standalone-recorder_v2.py`
  - **Input:** MTConnect HTTP endpoints defined in `SOURCES`.
  - **Output:** `data/<machine>/YYYY-MM-DD.jsonl` + `recorder_state.json`.

### Data quality

- `sampling_rate_analysis.py`
  - **Input:** `data/*.jsonl`
  - **Output:** `sampling_rate_summary.csv`, `daily_sampling_rate.png`

- `analyze_missing_sequence_number.py`
  - **Input:** `data/*.jsonl`
  - **Output:** `missing_per_day.csv`, `missing_per_day.png`

- `missing_per_day_by_machine.py`
  - **Input:** `data/*.jsonl`
  - **Output:** `missing_per_day_by_machine.csv`, `plots_per_machine/*.png`

- `machines_active_per_day.py`
  - **Input:** `data/*.jsonl`
  - **Output:** `machines_active_per_day.csv`, `machines_active_per_day.png`

### Analysis / experiments

- `find_stops.py`
  - **Input:** `data/*.jsonl`
  - **Output:** `plots/<day>/<machine>/<hour>.png`

- `corrolation_machine_pairs.py`
  - **Input:** `data/*.jsonl`
  - **Output:** `correlation_heatmap.png`

- `data_pr_day.py`
  - **Input:** `data/*.jsonl`
  - **Output:** `graphs/<machine>/<day>/<variable>.png`

- `interventions.py`
  - **Input:** JSONL files from `DATA_DIR` (currently hardcoded absolute path)
  - **Output:** `intervention_states.csv`, `override_changes.csv`

- `ml_analysis.py`
  - **Input:** `data/*.jsonl`
  - **Output:** `ml_results/<machine>/...`, `ml_results/summary.csv`

### Interactive tool

- `data_simulator.py`
  - **Input:** `data/*.jsonl`
  - **Output:** local Streamlit UI (no fixed file output)

---

## Getting started

1. Use Python 3.10+ (3.11 recommended).
2. Create and activate a virtual environment.
3. Install required libraries used by scripts.

Example:

```bash
python -m venv .venv
source .venv/bin/activate
pip install pandas matplotlib streamlit requests scikit-learn joblib numpy opencv-python pillow pyautogui pynput
```

> There is no committed, pinned dependency file in this repository yet.

---

## Path/layout assumptions and current limitations

1. **Two data layouts are in active use**:
   - flat: `data/*.jsonl`
   - per-machine: `data/<machine>/*.jsonl`

2. Most analysis scripts currently read only the flat layout (`data/*.jsonl`).

3. `interventions.py` uses an environment-specific Windows absolute path by default.

4. Output locations are spread across multiple folders and root-level files, which is historically normal for this repo but can feel inconsistent.

5. The top-level `git/` folder appears to contain legacy references only; intent is uncertain.

---

## Safe maintenance guidance

- Prefer incremental, reversible changes over broad refactors.
- Preserve existing entry points and script behavior.
- If layout is standardized later, add backward-compatible path handling rather than rewriting analysis logic.
- When uncertainty exists, document it explicitly before moving files.
