# MSH MTConnect Data Tools

Script-first repository for recording and analyzing MTConnect machine telemetry.

## Current layout

```text
.
├── README.md
├── catalog/
│   ├── README.md
│   └── <script-folder>/
│       ├── <script>.py
│       └── README.md
├── data/
├── results/
├── legacy/
└── record data/
    ├── README.md
    └── button.png
```

## Where Python scripts live

All Python entrypoints are under `catalog/`.

- Catalog index and script status: `catalog/README.md`
- Script docs: `catalog/<script-folder>/README.md`

Status meanings:

- **Active** — operational scripts in regular use
- **Experimental** — exploratory/prototype scripts
- **Legacy** — retained for compatibility/history

## Run from repository root

### Recorders

```bash
python catalog/standalone-recorder_v2/standalone-recorder_v2.py
python catalog/standalone_recorder/standalone_recorder.py
```

### Quality and analysis

```bash
python catalog/sampling_rate_analysis/sampling_rate_analysis.py
python catalog/analyze_missing_sequence_number/analyze_missing_sequence_number.py
python catalog/missing_per_day_by_machine/missing_per_day_by_machine.py
python catalog/machines_active_per_day/machines_active_per_day.py
python catalog/find_stops/find_stops.py
python catalog/data_pr_day/data_pr_day.py
python catalog/interventions/interventions.py
```

### Experimental and interactive

```bash
python catalog/corrolation_machine_pairs/corrolation_machine_pairs.py
python catalog/data_analysis/data_analysis.py
python catalog/data_visualizer/data_visualizer.py
python catalog/ml_analysis/ml_analysis.py
streamlit run catalog/data_simulator/data_simulator.py
```

## Documentation scope

Catalog documentation is based on static code inspection (constants, paths, and declared required fields), not full end-to-end runtime validation.

## Breaking change

Python entrypoints were moved from repository root and `record data/` into `catalog/`.
Update local scripts/automation that still use old paths.
