"""One-shot original Beast evidence inspection; never opens source SQLite in SQLite.

Only exact named files are read. Raw bytes remain in a private runner directory.
WAL replay/backup operates on disposable private copies; all SELECTs operate on
the resulting immutable read-only snapshot and exclude every command payload.
This is an observation, never a replacement test or qualification result.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import subprocess
import sys
import time
from datetime import datetime, timezone
import xml.etree.ElementTree as ET


CANDIDATE = "11238b6bb6cacaf17c816ded0baece75c59ec5fe"
RUN = 35758028225
ATTEMPT = 2
JOB = 106874413640
RUNNER = 28
BRANCH = "codex/bootstrap-original-evidence-readonly"
ROOT = Path("C:/actions-runner/_work")
FIXTURE = Path("C:/fcp-qtmp/pytest-of-BEAST$/pytest-482/test_pairing_material_rolls_ba0")
PENDING = ROOT / "fcp-archive-pending/7aafddc04f6e46a5b7cbc78c4d19603e"
METADATA = (
    "cluster_id", "current_term", "voted_for", "commit_index", "last_applied",
    "fencing_epoch", "last_snapshot_index", "last_snapshot_term",
)
LIMIT = 16 * 1024 * 1024
MAX_ROWS = 4096


def utc():
    return datetime.now(timezone.utc).isoformat()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def write_json(path, value):
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def private_directory(path):
    path.mkdir(mode=0o700)
    rows = subprocess.check_output(
        ["whoami", "/user", "/fo", "csv", "/nh"], text=True, timeout=5,
    )
    sid = next(csv.reader(rows.splitlines()))[1]
    subprocess.run(
        ["icacls", str(path), "/inheritance:r", "/grant:r", "*" + sid + ":(OI)(CI)F"],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=5,
    )


def fingerprint(value):
    # Compare stat with stat, and fstat with fstat, never Windows ctime across APIs.
    return {
        "device": value.st_dev, "inode": value.st_ino, "size": value.st_size,
        "mtime_ns": value.st_mtime_ns,
    }


def guarded_stat(path):
    for part in reversed((path, *path.parents)):
        try:
            info = part.lstat()
        except FileNotFoundError:
            return None
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError("Reparse/symlink path rejected")
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("Non-regular file rejected")
    if info.st_size > LIMIT:
        raise ValueError("Exact file exceeds 16 MiB read bound")
    return fingerprint(info)


def read_exact(path):
    before = guarded_stat(path)
    if before is None:
        return {"present": False, "path": path.as_posix()}, None
    with path.open("rb") as stream:
        opened = fingerprint(os.fstat(stream.fileno()))
        raw = stream.read(LIMIT + 1)
        closed = fingerprint(os.fstat(stream.fileno()))
    after = guarded_stat(path)
    stable = before == after and opened == closed and len(raw) == before["size"]
    if len(raw) > LIMIT:
        raise ValueError("Exact file exceeded byte bound during read")
    return {
        "path": path.as_posix(), "present": True, "stat_before": before,
        "stat_after": after, "fstat_stable": opened == closed,
        "sha256": sha(raw), "size": len(raw), "stable_read": stable,
    }, raw


def copy_group(paths, destination):
    destination.mkdir()
    copied = []
    started = utc()
    for path in paths:
        evidence, raw = read_exact(path)
        if raw is not None:
            with (destination / path.name).open("xb") as stream:
                stream.write(raw)
            evidence["private_copy_sha256"] = sha((destination / path.name).read_bytes())
        copied.append(evidence)
    # A second read of every exact file detects changing bytes or sidecar membership.
    stable = True
    for path, evidence in zip(paths, copied, strict=True):
        again, _ = read_exact(path)
        evidence["final_observation"] = again
        same = evidence["present"] == again["present"]
        if evidence["present"] and again["present"]:
            same = same and all((
                evidence["stable_read"], again["stable_read"],
                evidence["stat_before"] == again["stat_after"],
                evidence["sha256"] == again["sha256"],
                evidence["sha256"] == evidence["private_copy_sha256"],
            ))
        evidence["stable_group_member"] = same
        stable = stable and same
    return {"started_at": started, "finished_at": utc(), "stable": stable, "files": copied}


def safe_rows(connection, sql, parameters=()):
    rows = connection.execute(sql, parameters).fetchmany(MAX_ROWS + 1)
    if len(rows) > MAX_ROWS:
        raise ValueError("Consensus table exceeds bounded row count")
    for row in rows:
        for value in row:
            if value is not None and not isinstance(value, (str, int)):
                raise ValueError("Unexpected consensus scalar type")
            if isinstance(value, str) and len(value) > 256:
                raise ValueError("Unexpected consensus scalar length")
    return rows


def snapshot_and_project(raw_dir, work_dir):
    work_dir.mkdir()
    for suffix in ("", "-wal", "-shm"):
        source = raw_dir / ("replica.sqlite3" + suffix)
        if source.is_file():
            shutil.copyfile(source, work_dir / source.name)
    source_db = work_dir / "replica.sqlite3"
    snapshot = work_dir / "consistent.sqlite3"
    deadline = time.monotonic() + 10

    def progress(_status, _remaining, _total):
        if time.monotonic() >= deadline:
            raise TimeoutError("Private-copy SQLite backup exceeded ten seconds")
        if snapshot.exists() and snapshot.stat().st_size > 2 * LIMIT:
            raise ValueError("Private snapshot exceeds 32 MiB bound")

    # mode=ro includes committed WAL; SQLite may create SHM only in this owned copy.
    source = sqlite3.connect(source_db.as_uri() + "?mode=ro", uri=True, timeout=0.2)
    target = sqlite3.connect(snapshot)
    try:
        source.backup(target, pages=64, progress=progress, sleep=0.01)
    finally:
        target.close()
        source.close()
    connection = sqlite3.connect(snapshot.as_uri() + "?mode=ro&immutable=1", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA trusted_schema=OFF")
        allowed = {
            "replica_metadata": {"key", "value"}, "replica_voters": {"voter_id"},
            "replica_log": {"log_index", "log_term", "command_id", "content_hash"},
            "replica_receipts": {"command_id", "content_hash", "log_index", "log_term"},
        }

        def authorize(action, table, column, _database, _trigger):
            if action == sqlite3.SQLITE_SELECT:
                return sqlite3.SQLITE_OK
            if action == sqlite3.SQLITE_READ and column in allowed.get(table, set()):
                return sqlite3.SQLITE_OK
            return sqlite3.SQLITE_DENY

        connection.set_authorizer(authorize)
        metadata = safe_rows(
            connection,
            "SELECT key,value FROM replica_metadata WHERE key IN (?,?,?,?,?,?,?,?) ORDER BY key",
            METADATA,
        )
        voters = safe_rows(connection, "SELECT voter_id FROM replica_voters ORDER BY voter_id")
        logs = safe_rows(
            connection,
            "SELECT log_index,log_term,command_id,content_hash FROM replica_log ORDER BY log_index",
        )
        receipts = safe_rows(
            connection,
            "SELECT command_id,content_hash,log_index,log_term FROM replica_receipts ORDER BY log_index",
        )
    finally:
        connection.close()
    return {
        "snapshot_sha256": sha(snapshot.read_bytes()), "metadata": dict(metadata),
        "voter_ids": [row[0] for row in voters],
        "log_columns": ["log_index", "log_term", "command_id", "content_hash"], "log": logs,
        "receipt_columns": ["command_id", "content_hash", "log_index", "log_term"],
        "receipts": receipts,
        "excluded": ["command_json", "state_json", "snapshot_json", "credentials", "pairing_material"],
    }


def validate_job(path):
    job = json.loads(path.read_text(encoding="utf-8-sig"))
    expected = {
        "id": JOB, "run_id": RUN, "run_attempt": ATTEMPT, "head_sha": CANDIDATE,
        "head_branch": "main", "runner_id": RUNNER, "runner_name": "Beast",
        "status": "completed", "conclusion": "failure",
        "name": "Windows regressions (journal-artifacts)",
        "workflow_name": "Federation v1 release gate",
    }
    if any(job.get(key) != value for key, value in expected.items()):
        raise ValueError("Original native job identity does not match")
    if set(job.get("labels", [])) != {"self-hosted", "Windows", "X64", "fcp-test-windows"}:
        raise ValueError("Original job requested labels differ")
    for key in ("started_at", "completed_at"):
        expected[key] = job[key]
    expected["labels"] = job["labels"]
    expected["native_response_sha256"] = sha(path.read_bytes())
    return expected


def print_safe_native_projection(result):
    """Retain useful allowlisted observations even if the later archive fails."""
    def ends(rows, limit):
        if len(rows) <= limit:
            return rows
        if not limit:
            return []
        return rows[:limit // 2] + rows[-limit // 2:]

    for limit in (16, 8, 4, 2, 0):
        native = {
            "original": result["original"], "observed_at": result["finished_at"],
            "qualification": "NOT_EVALUATED", "diagnostic_only": True,
            "pending": result.get("pending"), "databases": [],
            "limitations": result["limitation"],
            "row_retention": "Complete private/public report retained; native log rows bounded with explicit counts",
        }
        for row in result["databases"]:
            observed = {key: row[key] for key in ("fixture_identity", "status", "observed_at")}
            observed["files"] = [{
                "path": item["path"], "present": item["present"],
                "sha256": item.get("sha256"), "size": item.get("size"),
                "final_sha256": item.get("final_observation", {}).get("sha256"),
                "mtime_ns": item.get("stat_before", {}).get("mtime_ns"),
                "stable": item.get("stable_group_member"),
            } for item in row.get("raw_observation", {}).get("files", [])]
            if "projection" in row:
                projection = row["projection"]
                observed["metadata"] = projection["metadata"]
                observed["snapshot_sha256"] = projection["snapshot_sha256"]
                for name in ("voter_ids", "log", "receipts"):
                    rows = projection[name]
                    observed[name + "_count"] = len(rows)
                    observed[name] = ends(rows, limit)
                    observed[name + "_native_log_truncated"] = len(rows) > limit
                observed["log_columns"] = projection["log_columns"]
                observed["receipt_columns"] = projection["receipt_columns"]
            native["databases"].append(observed)
        encoded = json.dumps(native, sort_keys=True, separators=(",", ":"))
        if len(encoded.encode("utf-8")) <= 64000:
            print("SAFE_ORIGINAL_CONSENSUS_OBSERVATION=" + encoded)
            return
    raise ValueError("Safe native projection exceeded fixed 64 KiB bound")


def retain_pending(private, public):
    receipt = PENDING / "INCOMPLETE.json"
    junit = PENDING / "files/windows-journal-artifacts.xml"
    group = copy_group([receipt, junit], private / "pending-raw")
    if not group["stable"] or not all(row["present"] for row in group["files"]):
        return {"status": "ABSENT_OR_UNSTABLE", "observation": group}
    raw_receipt = (private / "pending-raw/INCOMPLETE.json").read_bytes()
    raw_junit = (private / "pending-raw/windows-journal-artifacts.xml").read_bytes()
    value = json.loads(raw_receipt)
    required = {
        "schema": "fcp.local-incomplete-evidence.v1", "archive_complete": False,
        "repo": "Nettking/msh", "tested_sha": CANDIDATE,
        "run_id": str(RUN), "run_attempt": str(ATTEMPT),
        "artifact": "windows-regression-journal-artifacts",
    }
    if any(value.get(key) != expected for key, expected in required.items()):
        raise ValueError("Original pending receipt identity mismatch")
    files = value.get("files", [])
    if len(files) != 1 or files[0].get("path") != "windows-journal-artifacts.xml":
        raise ValueError("Unexpected original retained input inventory")
    if files[0].get("sha256") != sha(raw_junit) or files[0].get("size") != len(raw_junit):
        raise ValueError("Original JUnit is not byte-bound to original receipt")
    if b"<!DOCTYPE" in raw_junit.upper() or b"<!ENTITY" in raw_junit.upper():
        raise ValueError("JUnit entity declarations rejected")
    tree = ET.fromstring(raw_junit)
    cases = list(tree.iter("testcase"))
    failures = [case for case in cases if case.find("failure") is not None or case.find("error") is not None]
    target = [case for case in failures if case.get("name", "").startswith("test_pairing_material_rolls_back")]
    # Publish the exact user-requested original files; no database payload joins.
    with (public / "INCOMPLETE.json").open("xb") as stream:
        stream.write(raw_receipt)
    with (public / "windows-journal-artifacts.xml").open("xb") as stream:
        stream.write(raw_junit)
    return {
        "status": "VERIFIED_ORIGINAL_PENDING_INPUT", "observation": group,
        "testcases": len(cases), "failures_or_errors": len(failures),
        "skips": sum(case.find("skipped") is not None for case in cases),
        "target_failure_cases": [{"name": case.get("name"), "classname": case.get("classname")} for case in target],
        "native_job_binding": "Separate fresh native job proof; original null binding preserved unchanged",
        "complete_original_archive_reconstructed": False,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--native-job", type=Path, required=True)
    parser.add_argument("--base", type=Path, required=True)
    args = parser.parse_args()
    if os.name != "nt" or os.environ.get("RUNNER_NAME") != "Beast":
        raise ValueError("Collector is restricted to the original Beast runner")
    if sys.version_info[:3] != (3, 12, 10):
        raise ValueError("Existing qualified Windows Python 3.12.10 required")
    if os.environ.get("GITHUB_REPOSITORY") != "Nettking/msh" or os.environ.get("GITHUB_RUN_ATTEMPT") != "1":
        raise ValueError("Trusted repository and single diagnostic attempt required")
    if os.environ.get("GITHUB_EVENT_NAME") != "push" or os.environ.get("GITHUB_REF_NAME") != BRANCH:
        raise ValueError("Isolated diagnostic workflow identity required")
    if not re.fullmatch(r"[0-9a-f]{40}", os.environ.get("GITHUB_SHA", "")):
        raise ValueError("Diagnostic source SHA required")
    current_run = os.environ["GITHUB_RUN_ID"]
    if not current_run.isdecimal():
        raise ValueError("Invalid diagnostic run ID")
    expected_base = ROOT / ("fcp-bootstrap-original-" + current_run)
    if args.base.absolute() != expected_base or args.base.exists():
        raise ValueError("New exact one-shot private output root required")
    native = validate_job(args.native_job)
    for part in (ROOT, *ROOT.parents):
        info = part.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError("Output ancestry contains reparse/symlink path")
    # At most 20 exact files (16 MiB each), duplicated only in owned private
    # staging. Keep the established 12 GiB Beast floor even at these caps.
    if shutil.disk_usage(ROOT).free < 13 * 1024**3:
        raise ValueError("Insufficient room above protected 12 GiB free-space floor")
    private_directory(args.base)
    private = args.base / "private"
    public = args.base / "public"
    private.mkdir()
    public.mkdir()
    result = {
        "schema": "fcp.bootstrap-original-readonly.v1", "started_at": utc(),
        "original": native, "instrumentation_sha": os.environ["GITHUB_SHA"],
        "instrumentation_run_id": current_run, "instrumentation_attempt": 1,
        "diagnostic_only": True, "qualification": "NOT_EVALUATED", "databases": [],
        "private_retention": (private / "raw").as_posix(),
        "limitation": "Later stable on-disk state cannot recover unpersisted runtime leader roles, expected term at failure, or a causal interleaving. No historical DB hash exists for independent creation-time attribution.",
    }
    raw_root = private / "raw"
    raw_root.mkdir()
    complete = True
    try:
        result["pending"] = retain_pending(private, public)
    except Exception as error:
        complete = False
        result["pending"] = {"status": "REFUSED", "error_type": type(error).__name__}
    completed_ns = int(datetime.fromisoformat(native["completed_at"].replace("Z", "+00:00")).timestamp() * 1e9)
    for attempt in (1, 2):
        for voter in (0, 1, 2):
            identity = f"listener-attempt-{attempt}/voter-{voter}"
            db = FIXTURE / identity / "replica.sqlite3"
            target = raw_root / f"attempt-{attempt}-voter-{voter}"
            row = {"fixture_identity": identity, "observed_at": utc()}
            try:
                group = copy_group([Path(str(db) + suffix) for suffix in ("", "-wal", "-shm")], target)
                row["raw_observation"] = group
                present = group["files"][0]["present"]
                late = any(item["present"] and item["stat_before"]["mtime_ns"] > completed_ns for item in group["files"])
                if not present:
                    row["status"] = "ORIGINAL_DB_ABSENT"
                elif not group["stable"] or late:
                    row["status"] = "RETAINED_PRIVATELY_NOT_PARSED_UNSTABLE_OR_MODIFIED_AFTER_JOB"
                    complete = False
                else:
                    row["projection"] = snapshot_and_project(target, private / f"working-{attempt}-{voter}")
                    row["status"] = "STABLE_ORIGINAL_PATH_COPY_SAFE_CONSENSUS_PROJECTION"
                    # Verify private raw bytes were not changed by working-copy SQLite operations.
                    for item in group["files"]:
                        if item["present"] and sha((target / Path(item["path"]).name).read_bytes()) != item["sha256"]:
                            raise ValueError("Private raw preservation verification failed")
            except Exception as error:
                complete = False
                row.pop("projection", None)
                row["status"] = "REFUSED"
                row["error_type"] = type(error).__name__
            result["databases"].append(row)
    result["finished_at"] = utc()
    result["bounded_inspection_complete"] = complete
    result["database_count_projected"] = sum("projection" in row and row["status"] != "REFUSED" for row in result["databases"])
    result["database_count_absent"] = sum(row["status"] == "ORIGINAL_DB_ABSENT" for row in result["databases"])
    write_json(public / "ORIGINAL_BOOTSTRAP_OBSERVATION.json", result)
    print_safe_native_projection(result)
    print(json.dumps({key: result[key] for key in ("finished_at", "bounded_inspection_complete", "database_count_projected", "database_count_absent", "qualification")}))
    return 0 if complete else 3


if __name__ == "__main__":
    raise SystemExit(main())
