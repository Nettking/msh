"""Capture commands, test output, dependencies and revisions without telemetry."""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import platform
import subprocess
import sys
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    groups = {
        "B0": ["catalog/flask_app/tests/test_digital_twin_service.py"],
        "B1": ["research/state_semantics_evolution_2026"],
    }
    evidence = {"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                "python": sys.version, "platform": platform.platform(),
                "dependencies": {d: importlib.metadata.version(d) for d in ("numpy", "pandas", "pytest")},
                "suites": {}}
    for name, files in groups.items():
        command = [sys.executable, "-m", "pytest", "-o", "addopts=", "-q", *files,
                   "--basetemp", str(args.output.parent / f"test-tmp-{name}")]
        started = time.perf_counter()
        result = subprocess.run(command, capture_output=True, text=True)
        evidence["suites"][name] = {"command": command, "exit_code": result.returncode,
                                  "wall_seconds": time.perf_counter()-started,
                                  "stdout": result.stdout, "stderr": result.stderr}
        print(f"{name}: {result.stdout}", flush=True)
    args.output.write_text(json.dumps(evidence, indent=2)+"\n", encoding="utf-8")
    return int(any(s["exit_code"] != 0 for s in evidence["suites"].values()))


if __name__ == "__main__":
    raise SystemExit(main())
