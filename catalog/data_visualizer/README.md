# data_visualizer

## Status
**Experimental**

## Script path
`catalog/data_visualizer/data_visualizer.py`

## Behavior observed via static code inspection
- Reads `./data/*.jsonl` (`FOLDER = "./data"`).
- Writes timeline images to `./timeline_images` and CSV to `./candidate_events.csv`.

## Runtime/path assumptions (not runtime-tested)
- Output/input paths are relative to repository root.
- Recommended invocation: `python catalog/data_visualizer/data_visualizer.py`

## Inspection scope
This documentation is based on direct source inspection on 2026-04-15 (constants, file IO paths, and required columns/signals). It is not a claim of successful end-to-end runtime execution after relocation.
