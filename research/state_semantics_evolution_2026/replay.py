"""Read-only, bounded historical replay for the DT evolution study.

From the root of Nettking/msh:
python -m research.state_semantics_evolution_2026.replay \
  --source-root example-data --max-windows 20 \
  --output research-state-semantics-results.json

The output contains hashes, aggregate results and anonymous machine labels,
not raw telemetry or original machine identifiers.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

from catalog.common.data_loading import iter_jsonl_files, iter_jsonl_records
from catalog.flask_app.services.digital_twin_service import (
    DigitalTwinService,
    _classify_rows,
    _machine_identifier,
)

COMPARISON_KEYS = (
    "active_seconds",
    "non_active_seconds",
    "observed_seconds",
    "positive_intervals",
    "usable_intervals",
    "classified_intervals",
    "long_gaps",
    "continuity",
)


def _digest(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=True, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _head_commit() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, timeout=3, stderr=subprocess.DEVNULL
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return None


def _select_windows(
    source_root: Path, machine_filter: str | None,
    max_windows: int, window_records: int, max_scan_records: int,
) -> tuple[list[list[dict]], int]:
    windows: list[list[dict]] = []
    scanned = 0
    for source in iter_jsonl_files(source_root, recursive=True):
        pending: dict[str, list[dict]] = {}
        for item in iter_jsonl_records(source):
            scanned += 1
            if scanned > max_scan_records or len(windows) >= max_windows:
                return windows, scanned - 1
            machine = _machine_identifier(item)
            if not machine or (machine_filter and machine != machine_filter):
                continue
            # Keep original record order; the service applies timestamp sorting.
            # Preserve missing fields and invalid timestamps so they remain testable.
            normalized = {
                "timestamp": item.get("timestamp"),
                "machine_id": machine,
                "execution": item.get("execution"),
                "Srpm": item.get("Srpm"),
                "Sload": item.get("Sload"),
            }
            pool = pending.setdefault(machine, [])
            pool.append(normalized)
            if len(pool) == window_records:
                windows.append(pool)
                pending[machine] = []
                if len(windows) >= max_windows:
                    return windows, scanned
    return windows, scanned


def _analyze(records: list[dict]) -> dict:
    with tempfile.TemporaryDirectory(prefix="dt-evolution-") as tmp:
        filepath = Path(tmp) / "window.jsonl"
        with filepath.open("w", encoding="utf-8") as stream:
            for record in records:
                stream.write(json.dumps(record, default=str) + "\n")
        machine_id = records[0]["machine_id"]
        return DigitalTwinService(Path(tmp)).analyze(
            machine_id=machine_id, recovery_percent=0,
        )


def _metric_comparison(a: dict, b: dict) -> dict:
    diff: dict[str, dict[str, float | int | None]] = {}
    for key in COMPARISON_KEYS:
        av, bv = a.get(key), b.get(key)
        if av is None or bv is None:
            if av != bv:
                diff[key] = {"baseline": av, "changed": bv}
        elif not math.isclose(float(av), float(bv), rel_tol=1e-9, abs_tol=1e-8):
            diff[key] = {"baseline": av, "changed": bv}
    return diff


def _state_class(records: list[dict]) -> list[str]:
    return (
        _classify_rows(pd.DataFrame.from_records(records))["state_class"]
        .fillna("unknown")
        .tolist()
    )


def _execution_token(value: Any) -> str:
    """Use production's missing-only coercion, preserving explicit 0/False."""
    return str(pd.Series([value]).astype("string").fillna("").str.strip().str.upper().iloc[0])


def _transform(source: list[dict], variant: str) -> list[dict]:
    rows = [dict(row) for row in source]
    if variant == "rename_execution":
        # Simulate ingestion from a schema with 'machine_execution'.
        for row in rows:
            row["machine_execution"] = row.pop("execution")
        for row in rows:
            row["execution"] = row.pop("machine_execution")
    elif variant == "rpm_unit":
        for row in rows:
            v = row["Srpm"]
            if v is not None:
                try:
                    rpm = float(v)
                    # Provider supplies rad/s; canonical adapter restores rpm.
                    row["Srpm"] = (rpm * (2.0 * math.pi / 60.0)) * (60.0 / (2.0 * math.pi))
                except (TypeError, ValueError, OverflowError):
                    # Preserve invalid signals as missing, never as zero.
                    row["Srpm"] = None
    elif variant == "provider":
        for row in rows:
            provider = {
                "timestamp": row["timestamp"],
                "resource": row["machine_id"],
                "execution_state": row["execution"],
                "spindle_rpm": row["Srpm"],
                "spindle_load": row["Sload"],
            }
            row.clear()
            row.update({
                "timestamp": provider["timestamp"],
                "machine_id": provider["resource"],
                "execution": provider["execution_state"],
                "Srpm": provider["spindle_rpm"],
                "Sload": provider["spindle_load"],
            })
    elif variant == "missing_execution":
        for idx in range(0, len(rows), 10):
            rows[idx]["execution"] = None
    elif variant == "shuffle_identical_duplicate":
        if rows:
            rows.append(dict(rows[0]))
        random.Random(2026).shuffle(rows)
    else:
        raise ValueError("Unknown variant: " + variant)
    return rows


def _analyze_window(records: list[dict]) -> dict:
    baseline = _analyze(records)
    original_states = _state_class(records)
    equivalence = {}
    for variant in ("rename_execution", "rpm_unit", "provider"):
        transformed = _transform(records, variant)
        updated = _analyze(transformed)
        changes = _metric_comparison(baseline, updated)
        state_diffs = sum(a != b for a, b in zip(original_states, _state_class(transformed)))
        equivalence[variant] = {
            "preserved": not changes and state_diffs == 0,
            "metric_diffs": changes,
            "row_state_differences": state_diffs,
        }

    degraded = _transform(records, "missing_execution")
    degraded_states = _state_class(degraded)
    mask_indices = set(range(0, len(records), 10))
    unexpected = [i for i, (a, b) in enumerate(zip(original_states, degraded_states))
                  if a != b and i not in mask_indices]
    degradation = _analyze(degraded)

    duplicated = _transform(records, "shuffle_identical_duplicate")
    duplicate_report = _metric_comparison(baseline, _analyze(duplicated))
    # E6 is an explicitly intended alternative rule; we count affected fallback
    # rows rather than mislabeling a changed rule as an unintended regression.
    boundary_count = sum(
        _execution_token(r.get("execution")) in
        {"", "UNAVAILABLE", "NAN", "NONE", "N/A"}
        and r.get("Srpm") is not None
        and _numeric_between(r.get("Srpm"), 100, 150)
        and _numeric_missing_or_at_most(r.get("Sload"), 1)
        for r in records
    )
    return {
        "machine_hash": _digest(records[0]["machine_id"])[:12],
        "window_sha256": _digest(records),
        "records": len(records),
        "baseline": {k: baseline.get(k) for k in COMPARISON_KEYS},
        "equivalent_adapters": equivalence,
        "missing_execution": {
            "changed_rows": sum(a != b for a, b in zip(original_states, degraded_states)),
            "unexpected_affected_rows": unexpected,
            "metric_diffs": _metric_comparison(baseline, degradation),
        },
        "duplicate_shuffle": {
            "preserved": not duplicate_report,
            "metric_diffs": duplicate_report,
            "note": "Pre-existing conflicting duplicate timestamps may make order meaningful.",
        },
        "threshold_100_to_150": {
            "potentially_affected_fallback_rows": int(boundary_count),
            "interpretation": "Intentional semantic change, not a regression failure.",
        },
    }


def _numeric_between(value: Any, low: float, high: float) -> bool:
    try:
        v = float(value)
        return math.isfinite(v) and low < v <= high
    except (TypeError, ValueError, OverflowError):
        return False


def _numeric_at_most(value: Any, high: float) -> bool:
    try:
        v = float(value)
        return math.isfinite(v) and v <= high
    except (TypeError, ValueError, OverflowError):
        return False


def _numeric_missing_or_at_most(value: Any, high: float) -> bool:
    """Match the classifier's numeric coercion when load cannot prevent E6."""
    load = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return bool(pd.isna(load) or load <= high)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--machine", help="Optional machine identifier, used only for filtering")
    parser.add_argument("--max-windows", type=int, default=20)
    parser.add_argument("--window-records", type=int, default=200)
    parser.add_argument("--max-scan-records", type=int, default=200_000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if args.max_windows <= 0 or args.max_windows > 100 or args.window_records < 31:
        parser.error("max-windows must be 1..100 and window-records at least 31")
    if args.max_scan_records < args.window_records:
        parser.error("max-scan-records must be >= window-records")
    if not args.source_root.is_dir():
        parser.error("source-root must be an existing directory")

    windows, scanned = _select_windows(
        args.source_root, args.machine, args.max_windows,
        args.window_records, args.max_scan_records,
    )
    results = [_analyze_window(w) for w in windows]
    total_equivalence_failures = sum(
        not check["preserved"]
        for w in results for check in w["equivalent_adapters"].values()
    )
    report = {
        "scope": "read-only, deterministic historical-replay probe; not physical state ground truth",
        "git_commit": _head_commit(),
        "selection": {
            "strategy": "first consecutive full windows per source file/machine; non-random",
            "records_scanned": scanned,
            "windows_selected": len(windows),
            "window_records": args.window_records,
            "source_root_not_recorded": True,
        },
        "equivalence_failures": total_equivalence_failures,
        "windows": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(
        f"windows={len(windows)} scanned={scanned} "
        f"adapter_equivalence_failures={total_equivalence_failures} "
        f"report={args.output}"
    )
    return 1 if total_equivalence_failures or not windows else 0


if __name__ == "__main__":
    raise SystemExit(main())
