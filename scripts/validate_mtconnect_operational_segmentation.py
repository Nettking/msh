#!/usr/bin/env python3
"""Inspect MTConnect operational segmentation from canonical SQLite or a capture ZIP."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from catalog.mtconnect_recorder.canonical.store import CanonicalObservationStore
from catalog.mtconnect_recorder.operational.store import OperationalSegmentationStore
from catalog.mtconnect_recorder.operational.validation import (
    OperationalValidationError,
    rebuild_validation_report,
    render_validation_report,
    validate_reference_capture_zip,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild and inspect the v1 MTConnect operational segmentation stack. "
            "The input evidence is never modified."
        )
    )
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument(
        "--canonical-db",
        type=Path,
        help="existing canonical_observations SQLite projection",
    )
    inputs.add_argument(
        "--capture-zip",
        type=Path,
        help="private recorder raw-batch ZIP; staged read-only through canonical projection",
    )
    parser.add_argument(
        "--segmentation-db",
        type=Path,
        help=(
            "optional derived operational SQLite path; when omitted a temporary "
            "database is used"
        ),
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        help=(
            "optional workspace for --capture-zip canonical/segmentation projections; "
            "useful when retaining local review evidence"
        ),
    )
    parser.add_argument("--source-key", help="optional canonical source_key filter")
    parser.add_argument(
        "--agent-instance-id",
        type=int,
        help="optional Agent instance filter",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit deterministic JSON instead of inspection text",
    )
    return parser


def _render(report: dict[str, object], *, as_json: bool) -> str:
    if as_json:
        return json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    return render_validation_report(report)


def _from_canonical(args: argparse.Namespace) -> dict[str, object]:
    canonical_path: Path = args.canonical_db
    if not canonical_path.is_file():
        raise OperationalValidationError(
            f"canonical database does not exist: {canonical_path}"
        )
    canonical = CanonicalObservationStore(canonical_path)

    if args.segmentation_db is not None:
        segmentation = OperationalSegmentationStore(args.segmentation_db)
        return rebuild_validation_report(
            canonical,
            segmentation,
            source_key=args.source_key,
            agent_instance_id=args.agent_instance_id,
        )

    with tempfile.TemporaryDirectory(prefix="fcp-mtconnect-s7-") as temporary:
        segmentation = OperationalSegmentationStore(
            Path(temporary) / "operational_segmentation.sqlite3"
        )
        return rebuild_validation_report(
            canonical,
            segmentation,
            source_key=args.source_key,
            agent_instance_id=args.agent_instance_id,
        )


def _from_capture(args: argparse.Namespace) -> dict[str, object]:
    capture_path: Path = args.capture_zip
    if not capture_path.is_file():
        raise OperationalValidationError(f"capture ZIP does not exist: {capture_path}")
    if args.segmentation_db is not None:
        raise OperationalValidationError(
            "--segmentation-db is only used with --canonical-db; "
            "use --workspace to retain capture validation projections"
        )
    report, _canonical_reports = validate_reference_capture_zip(
        capture_path,
        workspace=args.workspace,
        source_key=args.source_key,
        agent_instance_id=args.agent_instance_id,
    )
    return report


def main() -> int:
    parser = _parser()
    args = parser.parse_args()
    try:
        report = _from_canonical(args) if args.canonical_db else _from_capture(args)
    except (OperationalValidationError, OSError, ValueError) as exc:
        parser.error(str(exc))
    print(_render(report, as_json=args.json), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
