"""Script discovery and runner-visible script metadata."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

# Catalog folders intentionally hidden from the interactive runner.
#
# These remain documented in catalog/README.md, but they are not part of the
# default "pick script + date range" analysis flow.
RUNNER_HIDDEN_FOLDERS = {
    "runner",  # runner implementation internals
    "auto_connect",  # desktop automation helper
    "data_simulator",  # streamlit app, not a one-shot CLI analysis run
    "interventions",  # environment-specific script
    "standalone_recorder",  # ingestion tool (legacy)
    "standalone-recorder_v2",  # ingestion tool (preferred recorder)
}

ToolCategory = Literal["Simple", "Advanced", "Legacy"]

# Runner-facing metadata. Keep this conservative and focused on navigation.
SCRIPT_METADATA: dict[str, dict[str, str | bool]] = {
    # Stage 1: data health checks (first-pass).
    "machines_active_per_day": {
        "category": "Simple",
        "description": "Stage 1 (Health): count distinct active machines per day.",
    },
    "analyze_missing_sequence_number": {
        "category": "Simple",
        "description": "Stage 1 (Health): summarize missing sequence numbers per day.",
    },
    "missing_per_day_by_machine": {
        "category": "Simple",
        "description": "Stage 1 (Health): per-machine missing sequence summary by day.",
    },
    "sampling_rate_analysis": {
        "category": "Simple",
        "description": "Stage 1 (Health): average telemetry sampling rate per day.",
    },
    # Stage 3: manual raw inspection.
    "data_pr_day": {
        "category": "Simple",
        "description": "Stage 3 (Manual raw): per-machine/day raw signal plots.",
    },
    # Stage 2: playback timeline prerequisites.
    "data_visualizer": {
        "category": "Simple",
        "description": "Stage 2 (Playback): state timelines and candidate-event export.",
    },
    # Stage 4: manual stop-focused inspection.
    "find_stops": {
        "category": "Simple",
        "description": "Stage 4 (Manual stops): stop timeline plots for day/hour windows.",
    },
    # Stage 5: deeper exploratory analysis.
    "data_analysis": {
        "category": "Advanced",
        "description": "Stage 5 (Explore): deeper terminal diagnostics and exploratory summaries.",
    },
    "ml_analysis": {
        "category": "Advanced",
        "description": "Stage 5 (Explore): per-machine ML baseline for future-stop prediction.",
    },
    # Legacy / no longer a recommended main workflow.
    "corrolation_machine_pairs": {
        "category": "Legacy",
        "description": "Legacy: pairwise machine stop-correlation heatmap exploration.",
    },
}

CATEGORY_ORDER: dict[str, int] = {"Simple": 0, "Advanced": 1, "Legacy": 2}


@dataclass(frozen=True)
class ScriptOption:
    """Description of one runnable catalog script."""

    number: int
    key: str
    script_path: Path
    description: str
    category: ToolCategory


def repo_root() -> Path:
    """Return the repository root directory."""
    return Path(__file__).resolve().parents[2]


def _script_description(script_path: Path, fallback: str) -> str:
    """Extract a short script description from the first line of a module docstring."""
    try:
        source = script_path.read_text(encoding="utf-8")
        module = ast.parse(source)
        docstring = ast.get_docstring(module)
    except (OSError, SyntaxError, UnicodeDecodeError):
        docstring = None

    if not docstring:
        return fallback

    first_line = docstring.strip().splitlines()[0].strip()
    return first_line or fallback


def discover_runnable_scripts(catalog_dir: Path) -> list[ScriptOption]:
    """Discover runnable catalog scripts from catalog subdirectories."""
    script_items: list[tuple[str, Path, str, ToolCategory]] = []

    for folder in sorted(catalog_dir.iterdir()):
        if not folder.is_dir():
            continue
        if folder.name in RUNNER_HIDDEN_FOLDERS:
            continue

        convention_script = folder / f"{folder.name}.py"
        main_script = folder / "main.py"

        selected_script: Path | None = None
        if convention_script.exists():
            selected_script = convention_script
            key = folder.name
        elif main_script.exists():
            selected_script = main_script
            key = folder.name
        else:
            continue

        metadata = SCRIPT_METADATA.get(key, {})
        fallback_description = key.replace("_", " ").replace("-", " ")
        description = str(metadata.get("description")) if metadata.get("description") else _script_description(
            selected_script, fallback_description
        )
        category = str(metadata.get("category", "Advanced"))
        if category not in CATEGORY_ORDER:
            category = "Advanced"
        script_items.append((key, selected_script.relative_to(repo_root()), description, category))

    script_items.sort(key=lambda item: (CATEGORY_ORDER[item[3]], item[0].lower()))
    return [
        ScriptOption(number=index, key=key, script_path=script_path, description=description, category=category)
        for index, (key, script_path, description, category) in enumerate(script_items, start=1)
    ]
