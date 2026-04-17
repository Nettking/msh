# Python Script Catalog

This catalog documents behavior from **static code inspection** for each script after relocation to `catalog/`.

## Inspection method (static)
- Reviewed each script's constants, file IO paths, and required columns directly in source.
- Checked runtime path assumptions caused by relocation (especially relative `data/` and output paths).
- Marked each script as **Active**, **Experimental**, or **Legacy**.

## Script index

| Script | Status | Inspected inputs/outputs summary |
|---|---|---|
| `catalog/analyze_missing_sequence_number/analyze_missing_sequence_number.py` | **Active** | Input: Reads `data/*.jsonl` from `DATA_DIR = "data"`. Output: Writes `missing_per_day.csv` and `missing_per_day.png` in repo root. |
| `catalog/auto_connect/auto_connect.py` | **Legacy (utility)** | Input: Requires desktop session and template image (e.g., `record data/button.png`) for OpenCV matching. Output: No structured files; performs mouse movement/click automation. |
| `catalog/corrolation_machine_pairs/corrolation_machine_pairs.py` | **Experimental** | Input: Reads `data/*.jsonl` from `DATA_DIR = Path("data")`. Output: Generates `correlation_heatmap.png` in repo root. |
| `catalog/data_analysis/data_analysis.py` | **Experimental** | Input: Reads `./data/*.jsonl` from `FOLDER = "./data"`. Output: Primarily console analysis output (no fixed export path in constants). |
| `catalog/data_pr_day/data_pr_day.py` | **Active** | Input: Reads `data/*.jsonl` and requires `timestamp` + `machine` fields. Output: Writes plots under `graphs/` (`GRAPH_BASE_DIR = Path("graphs")`). |
| `catalog/data_simulator/data_simulator.py` | **Experimental** | Input: Reads `data/*.jsonl` from `DATA_DIR = "data"`. Output: Streamlit UI only (no guaranteed file output). |
| `catalog/data_visualizer/data_visualizer.py` | **Experimental** | Input: Reads `./data/*.jsonl` (`FOLDER = "./data"`). Output: Writes timeline images to `./timeline_images` and CSV to `./candidate_events.csv`. |
| `catalog/find_stops/find_stops.py` | **Active** | Input: Reads `data/*.jsonl` from `DATA_DIR = Path("data")`. Output: Writes plots under `plots/` (`OUTPUT_DIR = Path("plots")`). |
| `catalog/interventions/interventions.py` | **Legacy (env-specific)** | Input: Reads from `DATA_DIR = Path(r"C:\wsl\msh\data")` (hardcoded absolute path). Output: Writes `intervention_states.csv` and `override_changes.csv` in current working directory. |
| `catalog/machines_active_per_day/machines_active_per_day.py` | **Active** | Input: Reads `data/*.jsonl`; requires `timestamp` and `machine`. Output: Writes `machines_active_per_day.csv` and `machines_active_per_day.png`. |
| `catalog/missing_per_day_by_machine/missing_per_day_by_machine.py` | **Active** | Input: Reads `data/*.jsonl`; requires `timestamp`, `sequence`, and `machine`. Output: Writes `missing_per_day_by_machine.csv` and plots under `plots_per_machine/`. |
| `catalog/ml_analysis/ml_analysis.py` | **Experimental** | Input: Reads `data/*.jsonl` from `DATA_DIR = Path("data")`. Output: Writes model artifacts and reports under `ml_results/`. |
| `catalog/sampling_rate_analysis/sampling_rate_analysis.py` | **Active** | Input: Reads `data/*.jsonl` from `DATA_DIR = "data"`. Output: Writes `sampling_rate_summary.csv` and `daily_sampling_rate.png`. |
| `catalog/standalone-recorder_v2/standalone-recorder_v2.py` | **Active** | Input: Polls MTConnect sources defined in `SOURCES` over HTTP. Output: Writes machine-partitioned JSONL under `data/` and `recorder_state.json`. |
| `catalog/standalone_recorder/standalone_recorder.py` | **Legacy** | Input: Polls MTConnect sources defined in `SOURCES` over HTTP. Output: Writes flat daily JSONL files under `data/`. |

## Compatibility note (path migration)
All entrypoints changed to `catalog/...`. Existing commands must be updated to the new paths.

## Shared utility helpers
- Shared JSONL and timestamp parsing helpers now live in `catalog/common/`:
  - `catalog/common/data_loading.py`
  - `catalog/common/time_utils.py`
- Runner date discovery now keeps a small JSON cache at `results/runner/data_index.json` to avoid reparsing unchanged JSONL files on repeated runs.
