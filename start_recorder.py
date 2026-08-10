"""Start the FCP MTConnect recorder with one command.

Examples:
    python start_recorder.py Mazak=http://192.168.200.249:5000
    python start_recorder.py Mazak=http://192.168.200.249:5000 --once
    python start_recorder.py A=http://10.0.0.1:5000 B=http://10.0.0.2:5000
"""

from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path
import os
import runpy
import sys
from urllib.parse import urlsplit


def _source(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise ValueError("Use NAME=http://host:port, for example Mazak=http://192.168.200.249:5000")
    name, url = value.split("=", 1)
    name = name.strip()
    url = url.strip()
    parsed = urlsplit(url)
    if not name or parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"Invalid MTConnect source: {value!r}")
    return name, url


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Start the loss-aware FCP MTConnect recorder.",
    )
    parser.add_argument(
        "sources",
        nargs="+",
        metavar="NAME=URL",
        help="MTConnect Agent base, /current, /sample, or /probe URL.",
    )
    parser.add_argument("--data-dir", default="data", help="Output directory (default: data).")
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.2,
        help="Seconds between catch-up cycles (default: 0.2).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Maximum observations requested per /sample call (default: 1000).",
    )
    parser.add_argument(
        "--max-batches-per-cycle",
        type=int,
        default=20,
        help="Maximum catch-up batches per source and cycle (default: 20).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="HTTP timeout in seconds (default: 10).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one catch-up cycle and exit.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        parsed_sources = [_source(value) for value in args.sources]
    except ValueError as exc:
        parser.error(str(exc))

    if args.poll_interval <= 0:
        parser.error("--poll-interval must be greater than zero.")
    if args.batch_size <= 0:
        parser.error("--batch-size must be greater than zero.")
    if args.max_batches_per_cycle <= 0:
        parser.error("--max-batches-per-cycle must be greater than zero.")
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero.")

    data_dir = Path(args.data_dir).resolve()
    state_dir = data_dir / "source_state"
    os.environ.update(
        {
            "FCP_RECORDER_MANAGED": "false",
            "FCP_RECORDER_SOURCES": ";".join(
                f"{name}={url}" for name, url in parsed_sources
            ),
            "FCP_RECORDER_DATA_DIR": str(data_dir),
            "FCP_RECORDER_STATE_FILE": str(state_dir / "mtconnect_recorder_state.json"),
            "FCP_RECORDER_STATUS_FILE": str(state_dir / "mtconnect_recorder_status.json"),
            "FCP_RECORDER_LOG_FILE": str(state_dir / "mtconnect_recorder.log"),
            "FCP_RECORDER_POLL_INTERVAL": str(args.poll_interval),
            "FCP_RECORDER_BATCH_SIZE": str(args.batch_size),
            "FCP_RECORDER_MAX_BATCHES_PER_CYCLE": str(args.max_batches_per_cycle),
            "FCP_RECORDER_REQUEST_TIMEOUT": str(args.timeout),
            "FCP_RECORDER_ONCE": "true" if args.once else "false",
        }
    )

    recorder_path = (
        Path(__file__).resolve().parent
        / "catalog"
        / "standalone-recorder_v2"
        / "standalone-recorder_v2.py"
    )
    if not recorder_path.exists():
        print(f"Recorder script not found: {recorder_path}", file=sys.stderr)
        return 2

    print("Starting loss-aware MTConnect recorder")
    print("Sources:")
    for name, url in parsed_sources:
        print(f"  {name}: {url}")
    print(f"Data: {data_dir}")
    print("Press Ctrl+C to stop." if not args.once else "Running one catch-up cycle.")

    runpy.run_path(str(recorder_path), run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
