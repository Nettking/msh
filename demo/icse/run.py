"""Reviewer entrypoint for the FCP ICSE tool demonstration."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

_SCENARIOS = (
    "E1-selective-contribution",
    "E2-authority-boundary",
    "E3-runtime-eligibility",
    "E4-ownership-boundary",
)


def _run(name: str) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix=f"fcp-icse-{name.lower()}-") as raw:
        completed = subprocess.run(
            [sys.executable, "-m", "demo.icse.worker", name, raw],
            check=False,
            capture_output=True,
            text=True,
        )
        lines = [line for line in completed.stdout.splitlines() if line.strip()]
        if not lines:
            return {
                "scenario": name,
                "result": "fail",
                "error_type": "WorkerProtocolError",
                "error": completed.stderr.strip() or "scenario worker produced no JSON",
            }
        try:
            result = json.loads(lines[-1])
        except json.JSONDecodeError as exc:
            return {
                "scenario": name,
                "result": "fail",
                "error_type": type(exc).__name__,
                "error": f"invalid worker output: {lines[-1]}",
            }
        if completed.returncode != 0 and result.get("result") != "fail":
            return {
                "scenario": name,
                "result": "fail",
                "error_type": "WorkerExitError",
                "error": completed.stderr.strip() or f"worker exited {completed.returncode}",
            }
        return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run deterministic FCP ICSE scenarios")
    parser.add_argument(
        "--output",
        type=Path,
        help="optional path for the same JSON summary printed to stdout",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    results = [_run(name) for name in _SCENARIOS]
    summary = {
        "schema": "fcp.icse-demo-summary.v1",
        "implementation_commit": os.environ.get("FCP_BUILD_COMMIT", "unknown"),
        "scenarios": results,
        "passed": sum(item["result"] == "pass" for item in results),
        "total": len(results),
    }
    rendered = json.dumps(summary, indent=2, sort_keys=True)
    print(rendered)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 0 if summary["passed"] == summary["total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
