"""One-process execution boundary for an ICSE demonstration scenario."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from demo.icse.ownership_boundary import ownership_boundary
from demo.icse.runtime_eligibility import runtime_eligibility
from demo.icse.scenarios import authority_boundary, selective_contribution

_SCENARIOS = {
    "E1-selective-contribution": selective_contribution,
    "E2-authority-boundary": authority_boundary,
    "E3-runtime-eligibility": runtime_eligibility,
    "E4-ownership-boundary": ownership_boundary,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", choices=tuple(_SCENARIOS))
    parser.add_argument("root", type=Path)
    args = parser.parse_args()

    try:
        evidence = _SCENARIOS[args.scenario](args.root)
        result = {
            "scenario": args.scenario,
            "result": "pass",
            "evidence": evidence,
        }
        status = 0
    except Exception as exc:  # noqa: BLE001 - process boundary serializes failures
        result = {
            "scenario": args.scenario,
            "result": "fail",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        status = 1

    print(json.dumps(result, sort_keys=True))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
