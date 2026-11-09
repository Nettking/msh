"""Execute the reviewed conventional validation on a local confidential tar corpus."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from .archive_dataset import select_archive
from .metrics import evaluate_window
from .replay import _head_commit


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def software_metadata() -> dict:
    root = Path(__file__).resolve().parents[2]
    files = sorted(Path(__file__).parent.glob("*.py"))
    files += [root / "catalog/flask_app/services/digital_twin_service.py",
              root / "catalog/common/data_loading.py",
              Path(__file__).with_name("EXPERIMENT_PROTOCOL.md")]
    return {
        "commit": _head_commit(),
        "tracked_dirty": bool(subprocess.check_output(
            ["git", "diff", "--name-only", "HEAD"], text=True).strip()),
        "python": sys.version,
        "platform": platform.platform(),
        "dependencies": {name: importlib.metadata.version(name)
                         for name in ("numpy", "pandas", "pytest", "python-dateutil")},
        "source_sha256": {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in files},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--windows-per-machine", type=int, default=6)
    parser.add_argument("--target-records", type=int, default=200)
    args = parser.parse_args()
    if args.windows_per_machine < 1 or args.target_records < 31:
        parser.error("positive machine quota and target-records >=31 required")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    wall, cpu = time.perf_counter(), time.process_time()
    software = software_metadata()
    windows, manifest = select_archive(args.archive, args.windows_per_machine, args.target_records)
    manifest["software"] = software
    manifest["created_at_utc"] = datetime.now(timezone.utc).isoformat()
    write_json(args.output_dir / "DATASET_MANIFEST.json", manifest)
    # The selection is frozen on disk before any evolved implementation runs.
    results = []
    for index, records in enumerate(windows):
        result = evaluate_window(records)
        result["window_index"] = index
        results.append(result)
        print(f"evaluated_window={index + 1}/{len(windows)}", flush=True)
    resources = {"wall_seconds": time.perf_counter() - wall,
                 "cpu_seconds": time.process_time() - cpu}
    try:
        import resource
        resources["peak_rss_kib_linux"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except ImportError:
        resources["peak_rss_kib_linux"] = None
    report = {"scope": "conventional B1; software consistency only; no physical ground truth",
              "software": software, "created_at_utc": datetime.now(timezone.utc).isoformat(),
              "resources": resources,
              "manifest_sha256": hashlib.sha256((args.output_dir / "DATASET_MANIFEST.json").read_bytes()).hexdigest(),
              "windows": results}
    report["unexpected_experiment_outcomes"] = sum(r["unexpected_experiment_outcomes"] for r in results)
    write_json(args.output_dir / "MEASUREMENTS.json", report)
    print(json.dumps({"windows": len(windows), "resources": resources}))
    return int(not windows or report["unexpected_experiment_outcomes"] > 0)


if __name__ == "__main__":
    raise SystemExit(main())
