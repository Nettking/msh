# Results directory

Optional shared location for generated outputs.

The current scripts in this repository primarily write structured outputs (CSV/JSON-style artifacts) to repo-root analysis folders and workflow session paths (for example `results/workflows/...` and `ml_results/`). This folder is provided as a low-risk organizational anchor for future manual result collection without changing script behavior.

## Version-control policy

- Generated result files are ignored by default.
- This README is tracked as structural documentation.
