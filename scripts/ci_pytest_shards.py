"""Run disjoint, file-preserving pytest shards and verify their complete coverage.

Every worker collects the same suite. Files are greedily balanced by collected
test count; keeping files intact avoids multiplying module-scoped fixtures.
The unsharded order-independence jobs remain responsible for cross-file leaks.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from collections import defaultdict
from pathlib import Path


def source_identity() -> dict[str, str]:
    """Read the actual clean checkout, independently of workflow labels."""

    def git(*args: str) -> str:
        result = subprocess.run(
            ["git", "--no-optional-locks", *args],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        if result.returncode:
            raise ValueError("Cannot establish the actual Git checkout")
        return result.stdout.strip()

    if Path(git("rev-parse", "--show-toplevel")).resolve() != Path.cwd().resolve():
        raise ValueError("Run shards from the Git checkout root")
    identity = {
        "sha": git("rev-parse", "HEAD"),
        "tree": git("rev-parse", "HEAD^{tree}"),
    }
    if not all(re.fullmatch(r"[0-9a-f]{40}", value) for value in identity.values()):
        raise ValueError("Actual Git identity is malformed")
    if git("status", "--porcelain", "--untracked-files=all"):
        raise ValueError("Shard checkout is not clean")
    return identity


def partition(nodeids: list[str], count: int) -> list[list[str]]:
    if count < 1:
        raise ValueError("Shard count must be positive")
    if len(nodeids) != len(set(nodeids)):
        raise ValueError("Duplicate collected test IDs")
    files: dict[str, list[str]] = defaultdict(list)
    for nodeid in nodeids:
        files[nodeid.partition("::")[0]].append(nodeid)
    if len(files) < count:
        raise ValueError("Not enough test files to populate every shard")
    shards: list[list[str]] = [[] for _ in range(count)]
    for path in sorted(files, key=lambda path: (-len(files[path]), path)):
        index = min(range(count), key=lambda index: (len(shards[index]), index))
        shards[index].extend(sorted(files[path]))
    return shards


def verify(directory: Path, count: int, source_sha: str) -> int:
    if re.fullmatch(r"[0-9a-f]{40}", source_sha) is None:
        raise ValueError("Expected source SHA must be a complete Git commit")
    manifests = [
        json.loads(path.read_text()) for path in sorted(directory.glob("shard-*.json"))
    ]
    if count < 1 or len(manifests) != count:
        raise ValueError(f"Expected {count} shard manifests, found {len(manifests)}")
    by_index = {manifest["shard_index"]: manifest for manifest in manifests}
    if set(by_index) != set(range(count)):
        raise ValueError("Missing or duplicate shard indexes")
    identity = by_index[0]["source_identity_before"]
    if (
        set(identity) != {"sha", "tree"}
        or identity["sha"] != source_sha
        or re.fullmatch(r"[0-9a-f]{40}", identity["tree"]) is None
    ):
        raise ValueError("Missing or invalid actual checkout identity")
    universe = by_index[0]["collected"]
    expected = partition(universe, count)
    covered: set[str] = set()
    for index in range(count):
        manifest = by_index[index]
        if (
            manifest["schema"] != 2
            or manifest["shard_count"] != count
            or manifest["source_sha"] != source_sha
            or manifest["source_identity_before"] != identity
            or manifest["source_identity_after"] != identity
            or manifest.get("source_error") is not None
            or manifest["exit_code"] != 0
            or manifest["collection_complete"] is not True
            or manifest["collected"] != universe
        ):
            raise ValueError(
                f"Shard {index} has failed, incomplete or inconsistent evidence"
            )
        if sorted(manifest["selected"]) != sorted(expected[index]):
            raise ValueError(f"Shard {index} does not contain its exact assigned tests")
        if manifest["executed"] != manifest["selected"]:
            raise ValueError(f"Shard {index} did not execute every selected test")
        if covered.intersection(manifest["selected"]):
            raise ValueError("Tests executed by multiple shards")
        covered.update(manifest["selected"])
    if covered != set(universe):
        raise ValueError("Shards do not cover the complete collected suite")
    return len(covered)


def run(index: int, count: int, manifest_path: Path, pytest_args: list[str]) -> int:
    import pytest

    if not 0 <= index < count:
        raise ValueError("Shard index must be between zero and shard count minus one")
    before = source_identity()
    expected_sha = os.environ.get("GITHUB_SHA")
    if expected_sha is not None and expected_sha != before["sha"]:
        raise ValueError("Actual checkout does not match GITHUB_SHA")
    manifest = {
        "schema": 2,
        "shard_index": index,
        "shard_count": count,
        "source_sha": before["sha"],
        "source_identity_before": before,
        "source_identity_after": None,
        "source_error": None,
        "collected": [],
        "selected": [],
        "executed": [],
        "collection_complete": False,
        "exit_code": 2,
    }

    executed: set[str] = set()

    class ShardPlugin:
        def pytest_configure(self, config):
            if config.option.collectonly:
                raise pytest.UsageError(
                    "Shard evidence requires executing tests, not collect-only"
                )

        @pytest.hookimpl(trylast=True)
        def pytest_collection_modifyitems(self, config, items):
            nodeids = [item.nodeid for item in items]
            try:
                assigned = set(partition(nodeids, count)[index])
            except ValueError as exc:
                raise pytest.UsageError(str(exc)) from exc
            manifest["collected"] = sorted(nodeids)
            manifest["selected"] = sorted(assigned)
            deselected = [item for item in items if item.nodeid not in assigned]
            items[:] = [item for item in items if item.nodeid in assigned]
            config.hook.pytest_deselected(items=deselected)

        def pytest_collection_finish(self, session):
            if sorted(item.nodeid for item in session.items) != manifest["selected"]:
                raise pytest.UsageError("Another plugin changed the assigned shard")
            manifest["collection_complete"] = True

        def pytest_runtest_logreport(self, report):
            # A test skipped during setup never produces a call-phase report.
            if report.when == "call" or (report.when == "setup" and report.skipped):
                executed.add(report.nodeid)

        def pytest_report_header(self):
            return f"CI shard {index + 1}/{count} (whole test files)"

    try:
        manifest["exit_code"] = int(pytest.main(pytest_args, plugins=[ShardPlugin()]))
        manifest["executed"] = sorted(executed)
        if manifest["exit_code"] == 0 and manifest["executed"] != manifest["selected"]:
            print("Incomplete shard execution: some selected tests produced no result")
            manifest["exit_code"] = 2
    finally:
        try:
            manifest["source_identity_after"] = source_identity()
            if manifest["source_identity_after"] != before:
                raise ValueError("Shard checkout identity changed during pytest")
        except (ValueError, OSError) as exc:
            manifest["source_error"] = str(exc)
            manifest["exit_code"] = 2
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
    return manifest["exit_code"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    runner = commands.add_parser("run")
    runner.add_argument("--index", type=int, required=True)
    runner.add_argument("--count", type=int, required=True)
    runner.add_argument("--manifest", type=Path, required=True)
    runner.add_argument("pytest_args", nargs=argparse.REMAINDER)
    verifier = commands.add_parser("verify")
    verifier.add_argument("--directory", type=Path, required=True)
    verifier.add_argument("--count", type=int, required=True)
    verifier.add_argument("--source-sha", required=True)
    args = parser.parse_args()
    try:
        if args.command == "verify":
            covered = verify(args.directory, args.count, args.source_sha)
            print(
                f"Verified {covered} tests across {args.count} disjoint successful shards"
            )
            return 0
        pytest_args = args.pytest_args
        if pytest_args[:1] == ["--"]:
            pytest_args = pytest_args[1:]
        return run(args.index, args.count, args.manifest, pytest_args)
    except (ValueError, KeyError, TypeError, OSError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
