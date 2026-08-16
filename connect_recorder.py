"""Join an existing FCP Federation and start the standalone MTConnect recorder.

This command is deliberately join-only.  A recorder is never allowed to create a
Federation or become the first Federation installation::

    python connect_recorder.py FCP1-...

The signed one-use code is consumed by the existing ``start_recorder`` bootstrap.
On successful pairing, the recorder performs the existing bounded MTConnect
source discovery/selection and starts recording.  Failed Federation bootstrap is
fatal for this command rather than silently falling back to local-only capture.
"""

from __future__ import annotations

import argparse

import start_recorder


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Connect the standalone recorder to an existing FCP Federation.",
        allow_abbrev=False,
    )
    parser.add_argument("pairing_code", metavar="FCP1-...")
    parser.add_argument(
        "--device-name",
        default="FCP MTConnect recorder",
        help="Stable display name for the recorder identity.",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Recorder data/state directory (default: data).",
    )
    parser.add_argument(
        "--storage-group",
        help="Optional logical Federation storage group.",
    )
    parser.add_argument(
        "--require-data-sharing",
        action="store_true",
        help="Require a ready authenticated logical-storage publication path before capture starts.",
    )
    parser.add_argument(
        "--scan-cidr",
        default="",
        help="Optional bounded private IPv4 CIDR for first MTConnect source discovery.",
    )
    parser.add_argument(
        "--no-auto-scan",
        action="store_true",
        help="Skip automatic bounded startup source discovery.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one recorder catch-up cycle and exit.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    code = str(args.pairing_code or "").strip()
    if not code.startswith("FCP1-"):
        raise SystemExit("connect_recorder requires one signed FCP1 pairing code")

    forwarded = [
        "--federation-key",
        code,
        "--require-federation",
        "--device-name",
        args.device_name,
        "--data-dir",
        args.data_dir,
    ]
    if args.storage_group:
        forwarded.extend(["--storage-group", args.storage_group])
    if args.require_data_sharing:
        forwarded.append("--require-data-sharing")
    if args.scan_cidr:
        forwarded.extend(["--scan-cidr", args.scan_cidr])
    if args.no_auto_scan:
        forwarded.append("--no-auto-scan")
    if args.once:
        forwarded.append("--once")
    return start_recorder.main(forwarded)


if __name__ == "__main__":
    raise SystemExit(main())
