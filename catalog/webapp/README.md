# MSH Digital Twin Web App

The web app is now the **primary workspace** for this repository.
It continuously scans configured folders, indexes analysis outputs, and exposes playback + analysis inspection directly in the UI.

## Run

```bash
streamlit run catalog/webapp/app.py
```

## Always-on behavior

- The app starts as a long-running Streamlit service.
- On startup it scans data/result folders (defaults: `results,data`).
- It rescans periodically via auto-refresh.
- It tracks playback-compatible exports separately from generic tabular outputs.

Configure scan roots:

```bash
MSH_SCAN_DIRS=results,data streamlit run catalog/webapp/app.py
```

## UI sections

- **System status**: scan roots, indexed artifacts, playback-capable count, read errors.
- **Overview**: compact catalog of discovered analysis outputs.
- **Analyses**: analysis browser with metadata + direct dataset inspection.
- **Machine view**: machine/day trends when machine columns are available.
- **Playback**: timeline replay for valid playback exports.
- **Exploration**: generic tabular exploration (including manual upload/path as secondary mode).

## Playback bootstrap compatibility

The app still supports existing playback bootstrap inputs:

- `--session-export-dir`
- `--source-path`
- `MSH_PLAYBACK_SOURCE_PATH`
- `MSH_PLAYBACK_EXPORT_DIR`

Session export auto-discovery filenames:

- `timeline_rows.csv`
- `timeline_rows.parquet`
- `timeline_rows.jsonl`
- `timeline_rows.json`
