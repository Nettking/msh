"""Assert that a CI runner meets the product's host-resource precondition.

The Federation storage providers admit work through
``catalog.federation.host_resources``: below a bounded free-space floor they
refuse to open at all, which is correct product behaviour on a host that is
genuinely short of disk. A hosted CI runner that starts below that floor is an
environment defect, not a product defect, and it surfaces deep inside unrelated
test fixtures as ``HostResourceRefused``.

This preflight measures the filesystem the suite actually writes into, using the
product's own measurement and its own unchanged thresholds, and fails before the
tests run with an explicit infrastructure message. It never relaxes, overrides,
or bypasses admission.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# Load the product's host-resource module straight from its source file rather
# than importing catalog.federation. The package __init__ pulls in optional
# backends such as psycopg, which focused CI jobs deliberately do not install,
# and this preflight has to run in those jobs too. The module itself only uses
# the standard library, so loading it by path is exact: these are the product's
# own thresholds and its own measurement, not a copy that could drift.
_HOST_RESOURCES = REPO / "catalog" / "federation" / "host_resources.py"
_spec = importlib.util.spec_from_file_location(
    "_fcp_host_resources_preflight", _HOST_RESOURCES
)
if _spec is None or _spec.loader is None:  # pragma: no cover - packaging guard
    raise SystemExit(f"cannot load the product resource policy from {_HOST_RESOURCES}")
_host_resources = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _host_resources
_spec.loader.exec_module(_host_resources)

PressureLevel = _host_resources.PressureLevel
PressureThresholds = _host_resources.PressureThresholds
assess_measurement = _host_resources.assess_measurement
measure_filesystem = _host_resources.measure_filesystem

GIB = 1024**3


def _describe(path: Path, thresholds: PressureThresholds) -> tuple[bool, bool]:
    measurement = measure_filesystem(path)
    assessment = assess_measurement(measurement, thresholds=thresholds)

    free_bytes = measurement.free_bytes
    total_bytes = measurement.total_bytes
    free_inodes = measurement.free_inodes

    print(f"  path            : {path}")
    print(f"  resource        : {measurement.resource_id}")
    if total_bytes is None or free_bytes is None:
        print("  bytes           : unavailable")
    else:
        print(
            f"  bytes           : {free_bytes / GIB:.2f} GiB free "
            f"of {total_bytes / GIB:.2f} GiB"
        )
    print(
        "  inodes          : "
        + ("unavailable" if free_inodes is None else f"{free_inodes} free")
    )
    print(f"  pressure level  : {assessment.level.name}")
    if assessment.reasons:
        print(f"  reasons         : {', '.join(assessment.reasons)}")

    return (
        assessment.level >= PressureLevel.PRESSURE,
        assessment.level == PressureLevel.WARNING,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="*",
        help="paths to check; defaults to the workspace and the temp root",
    )
    arguments = parser.parse_args()

    thresholds = PressureThresholds()
    targets = (
        [Path(item) for item in arguments.paths]
        if arguments.paths
        else [REPO, Path(tempfile.gettempdir())]
    )

    print("Product host-resource precondition (thresholds are the product's own):")
    print(f"  refuse at or below : {thresholds.pressure_free_bytes / GIB:.0f} GiB free")
    print(f"  emergency floor    : {thresholds.critical_free_bytes / GIB:.0f} GiB free")
    print(f"  warn at or below   : {thresholds.warning_free_bytes / GIB:.0f} GiB free")
    print(f"  refuse at or below : {thresholds.pressure_free_inodes} free inodes")

    refused: list[Path] = []
    warned: list[Path] = []
    for target in targets:
        print(f"\n{target}:")
        is_refused, is_warned = _describe(target, thresholds)
        if is_refused:
            refused.append(target)
        elif is_warned:
            warned.append(target)

    if refused:
        print(
            "\nINFRASTRUCTURE PRECONDITION FAILED: this runner does not meet the "
            "product's host-resource requirement.",
            file=sys.stderr,
        )
        for target in refused:
            print(f"  - {target} is at or below the admission floor", file=sys.stderr)
        print(
            "\nThe storage providers will refuse to open with "
            "HostResourceRefused('resource_pressure'), so the suite would fail "
            "inside unrelated fixtures. This is an environment defect: reclaim "
            "disk on the runner. Do not lower the product floor and do not add a "
            "CI-only admission bypass.",
            file=sys.stderr,
        )
        return 1

    if warned:
        print(
            "\nNOTE: free space is above the admission floor but inside the "
            "product's warning band. The reclaim step is close to insufficient "
            "and should be revisited before it starts failing."
        )

    print("\nRunner meets the product host-resource precondition.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
