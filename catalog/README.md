# Python Script Catalog

This folder contains analysis and ingestion scripts for MTConnect telemetry.  
The goal of this index is to make it obvious which tools are the normal path, which are exploratory, and which are legacy.

## Recommended analysis path

For most investigations, run tools in this order:

1. **Health checks (Simple)**
   - `machines_active_per_day`
   - `analyze_missing_sequence_number`
   - `missing_per_day_by_machine`
   - `sampling_rate_analysis`
2. **Raw inspection (Simple)**
   - `data_pr_day`
3. **Stop inspection (Simple)**
   - `find_stops`
4. **Deeper exploratory analysis (Advanced)**
   - `data_visualizer`
   - `data_analysis`
   - `ml_analysis`
   - `data_simulator` (interactive exploration)

## Tool categories

### Simple (recommended first)
Quick, practical scripts for routine checks and day-to-day analysis.
**Runner visibility:** shown in the interactive runner.

- `machines_active_per_day`: count distinct active machines per day.
- `analyze_missing_sequence_number`: daily missing-sequence summary.
- `missing_per_day_by_machine`: missing-sequence summary per machine/day.
- `sampling_rate_analysis`: average telemetry sampling rate per day.
- `data_pr_day`: per-machine/day raw telemetry plots.
- `find_stops`: stop-focused timeline plots.

### Advanced (exploratory)
Deeper or broader analysis tools that are useful after baseline checks.

- `data_visualizer`: reconstruct machine states and export candidate events. *(runner-visible)*
- `data_analysis`: exploratory batch analysis in the terminal. *(runner-visible)*
- `data_simulator`: Streamlit playback/simulation view for telemetry. *(Advanced catalog tool, but not a runner tool because it is an interactive Streamlit app rather than a one-shot script)*
- `ml_analysis`: train and evaluate per-machine stop-prediction models. *(runner-visible)*

### Legacy (special-case or not recommended as primary workflow)
Kept for reference and niche use; not the default workflow.

- `corrolation_machine_pairs`: legacy pairwise stop-correlation heatmap exploration. *(runner-visible)*
- `interventions`: environment-specific script (hardcoded Windows/WSL path assumptions). *(documented, not runner-visible)*
- `standalone_recorder`: legacy recorder retained for compatibility. *(documented, not runner-visible)*
- `auto_connect`: desktop automation helper, not an analysis workflow tool. *(documented, not runner-visible)*

## Recorder / ingestion tools

These are ingestion tools, not one-shot analysis tools, so they are documented
but not part of the interactive runner for normal analysis runs:

- **Preferred recorder:** `standalone-recorder_v2`
- **Legacy recorder:** `standalone_recorder`

## Runner behavior

The interactive runner (`catalog/runner/menu.py`) is for **normal one-shot
analysis tools** and presents only **runner-visible** scripts in grouped
categories:

- **Simple**
- **Advanced**
- **Legacy**

This keeps recommended one-shot analysis scripts prominent, keeps runnable
legacy analysis scripts visible for compatibility, and documents Streamlit,
recorder, and environment-specific tools separately by design.
