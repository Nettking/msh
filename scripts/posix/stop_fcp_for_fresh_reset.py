from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def _run_docker(
    executable: str,
    arguments: list[str],
    timeout_seconds: int,
    description: str,
) -> int:
    print(description)
    try:
        completed = subprocess.run(
            [executable, *arguments],
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        print(
            f"{description} timed out after {timeout_seconds} seconds. "
            "The Docker CLI process was terminated; attempting bounded project recovery.",
            file=sys.stderr,
        )
        return 124
    except OSError as exc:
        print(f"{description} could not start Docker: {exc}", file=sys.stderr)
        return 125
    return int(completed.returncode)


def stop_fcp(
    *,
    docker_executable: str = "docker",
    primary_timeout: int = 45,
    recovery_timeout: int = 20,
) -> int:
    resolved = (
        shutil.which(docker_executable)
        if Path(docker_executable).name == docker_executable
        else docker_executable
    )
    if not resolved or not Path(resolved).exists():
        print(
            f"Configured Docker executable does not exist: {docker_executable}",
            file=sys.stderr,
        )
        return 1

    primary = _run_docker(
        resolved,
        ["compose", "down", "--remove-orphans", "--timeout", "10"],
        primary_timeout,
        "Stopping the current FCP Compose project...",
    )
    if primary == 0:
        return 0

    print(
        f"Normal Compose shutdown did not complete cleanly (exit {primary}). "
        "Attempting bounded project-scoped recovery.",
        file=sys.stderr,
    )
    kill = _run_docker(
        resolved,
        ["compose", "kill"],
        recovery_timeout,
        "Force-stopping containers in this FCP Compose project...",
    )
    if kill != 0:
        print(
            f"Project-scoped docker compose kill exited with code {kill}; "
            "cleanup will still be attempted.",
            file=sys.stderr,
        )

    cleanup = _run_docker(
        resolved,
        ["compose", "down", "--remove-orphans", "--timeout", "5"],
        recovery_timeout,
        "Removing stopped FCP containers and project network...",
    )
    if cleanup == 0:
        print("FCP shutdown recovered successfully.")
        return 0

    print(
        f"FCP could not be stopped safely after bounded recovery (cleanup exit {cleanup}). "
        "Device state was not reset.",
        file=sys.stderr,
    )
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--docker-executable", default="docker")
    parser.add_argument("--primary-timeout", type=int, default=45)
    parser.add_argument("--recovery-timeout", type=int, default=20)
    args = parser.parse_args(argv)
    if not 5 <= args.primary_timeout <= 300 or not 5 <= args.recovery_timeout <= 120:
        parser.error("timeouts are outside the supported bounded range")
    return stop_fcp(
        docker_executable=args.docker_executable,
        primary_timeout=args.primary_timeout,
        recovery_timeout=args.recovery_timeout,
    )


if __name__ == "__main__":
    raise SystemExit(main())
