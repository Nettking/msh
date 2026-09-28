"""One native CI observation; keep private logs, never qualify or alter product."""
from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

SOURCE = "9f6ffb5b959608879a7853b838a1823deac7efb1"
HEAD = "650434b2828e7a5b65bbaf077e6eeb90253addb0"
ORIGINAL_RUN = "36399577324"
ORIGINAL_JOB = "108853986825"
BRANCH = "refs/heads/codex/federation-icse-native-diagnostic"
LIMIT = 2 * 1024 * 1024
LOGS = ("driver-failure.log", *(f"{name}/worker.stderr.log" for name in (
    "voter-a", "voter-b", "voter-c", "reviewer",
)))
ERROR_TYPES = frozenset({
    "RuntimeError", "OSError", "FileNotFoundError", "PermissionError",
    "ValueError", "TypeError", "KeyError", "TimeoutError", "AssertionError",
    "CalledProcessError", "FederationValidationError", "ControlPlaneError",
    "QuorumUnavailable", "ConnectionError", "ModuleNotFoundError", "ImportError",
})


def utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, value: dict) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), *args], text=True, stderr=subprocess.PIPE, timeout=20,
    ).strip()


def retain_logs(source: Path, destination: Path) -> list[dict]:
    retained = []
    for name in LOGS:
        path = source / name
        if not path.exists():
            continue
        before = path.stat()
        if not stat.S_ISREG(before.st_mode) or before.st_size > LIMIT:
            raise ValueError("Unbounded diagnostic log")
        for parent in (path, *path.parents):
            if parent.is_symlink() or getattr(parent.lstat(), "st_file_attributes", 0) & 0x400:
                raise ValueError("Reparse diagnostic log rejected")
        raw = path.read_bytes()
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError("Diagnostic changed during retention")
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as stream:
            stream.write(raw)
        text = raw.decode("utf-8", errors="replace")
        error_names = re.findall(r"(?m)^(?:[A-Za-z_][\w.]*\.)?([A-Za-z_]\w*):", text)
        retained.append({
            "path": name, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
            "worker": name.split("/")[0] if "/" in name else "driver",
            "stage": "worker" if "/" in name else "driver",
            "error_types": sorted(set(error_names) & ERROR_TYPES),
            "winerror": sorted({int(x) for x in re.findall(r"\[WinError (\d+)\]", text)}),
            "errno": sorted({int(x) for x in re.findall(r"\[Errno (\d+)\]", text)}),
        })
    return retained


def main() -> int:
    expected = {
        "GITHUB_REPOSITORY": "Nettking/msh", "GITHUB_REF": BRANCH,
        "GITHUB_EVENT_NAME": "push", "GITHUB_RUN_ATTEMPT": "1",
        "GITHUB_ACTOR": "Nettking", "GITHUB_TRIGGERING_ACTOR": "Nettking",
        "RUNNER_NAME": "Nettking", "RUNNER_OS": "Windows",
    }
    if any(os.environ.get(key) != value for key, value in expected.items()):
        raise ValueError("Unexpected observer identity")
    if platform.node().casefold() != "nettking" or sys.version_info[:3] != (3, 12, 10):
        raise ValueError("Unexpected native runtime")
    source = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
    if git(source, "rev-parse", "HEAD") != SOURCE or git(source, "status", "--porcelain"):
        raise ValueError("Original product source must be exact and clean")
    if git(source, "rev-parse", SOURCE + "^{tree}") != git(source, "rev-parse", HEAD + "^{tree}"):
        raise ValueError("Failed merge and current candidate trees differ")
    root = Path(os.environ["FCP_ICSE_DIAGNOSTIC_ROOT"]).resolve()
    work = Path(os.environ["RUNNER_TEMP"]).resolve().parent
    if root.parent != work or root.is_relative_to(source):
        raise ValueError("Diagnostic output is not isolated")
    if shutil.disk_usage(root).free <= 12 * 1024**3 + 32 * 1024**2:
        raise OSError("Insufficient diagnostic capacity")
    identity = {
        "started_utc": utc(), "diagnostic_source": os.environ["GITHUB_SHA"],
        "diagnostic_run_id": os.environ["GITHUB_RUN_ID"], "diagnostic_attempt": 1,
        "original_tested_sha": SOURCE, "candidate_head": HEAD,
        "original_run_id": ORIGINAL_RUN, "original_job_id": ORIGINAL_JOB,
        "original_attempt": 1, "runner": os.environ["RUNNER_NAME"],
        "python_version": platform.python_version(), "qualification_status": "NOT_EVALUATED",
        "product_mutation": False, "private_logs_uploaded": False,
    }
    write_json(root / "IDENTITY.json", identity)
    previous = Path(os.environ["RUNNER_TEMP"]) / f"fcp-icse-network-{ORIGINAL_RUN}-1-Windows" / "private-state"
    logs = retain_logs(previous, root / "original-private-logs") if previous.exists() else []
    if logs:
        result = {**identity, "original_logs_retained": True, "test_execution": False, "logs": logs}
    else:
        marker = work / f"fcp-icse-native-{ORIGINAL_JOB}-observed-once.json"
        write_json(marker, identity)
        output = Path(os.environ["RUNNER_TEMP"]) / (
            f"fcp-icse-network-{os.environ['GITHUB_RUN_ID']}-1-Windows"
        )
        if output.exists():
            raise ValueError("Observation output already exists")
        command = [sys.executable, "-B", "-m", "demo.icse.network.run",
                   "--source-sha", SOURCE, "--output", str(output)]
        write_json(root / "EXECUTION.json", {"command": command, "started_utc": utc()})
        with (root / "driver.stdout.log").open("xb") as out, (root / "driver.stderr.log").open("xb") as err:
            completed = subprocess.run(command, cwd=source, stdout=out, stderr=err, check=False)
        logs = retain_logs(output / "private-state", root / "observed-private-logs")
        public = output / "public" / "summary.json"
        if public.exists():
            shutil.copyfile(public, root / "observed-public-summary.json")
        if completed.returncode and not logs:
            raise ValueError("Failed execution has no retained worker diagnostics")
        result = {**identity, "original_logs_retained": False, "test_execution": True,
                  "execution_count": 1, "returncode": completed.returncode, "logs": logs}
    result["finished_utc"] = utc()
    result["diagnostic_complete"] = True
    write_json(root / "RESULT.json", result)
    # Only fixed error vocabulary and numeric errno/WinError values enter CI logs.
    print(json.dumps(result, indent=2))
    print("Private diagnostic directory: " + str(root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
