"""One bounded read-only observation of the new Flask-population test failure.

Original SQLite files are byte-read only. WAL reconciliation happens only on
owned private copies. Public output contains hardcoded SQL scalar projections.
No test/product imports, old bootstrap-fixture reads, or qualification claims.
"""
import csv
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

C = "11238b6bb6cacaf17c816ded0baece75c59ec5fe"
ORIGINAL_SOURCE = "a7b4f8b548f5ab48831bcb17800b4e39a5c44c19"
ORIGINAL_RUN = 35779391870
ORIGINAL_JOB = 106920694721
BRANCH = "codex/bootstrap-second-failure-state"
WORK = Path("C:/actions-runner/_work")
FIXTURE = Path("C:/fcp-qtmp/pytest-of-BEAST$/pytest-483/test_configured_flask_process_0")
TARGET = "catalog/flask_app/tests/test_c03_pairing_onboarding.py::test_configured_flask_process_publishes_through_quorum_and_cannot_fall_back"
WORKFLOW = ".github/workflows/nitro-archive-capacity-readonly.yml"
HELPER = "scripts/inspect_second_failure_state.py"
LIMIT = 16 * 1024 * 1024
MAX_ROWS = 4096
METADATA = ("cluster_id", "current_term", "voted_for", "commit_index", "last_applied",
            "fencing_epoch", "last_snapshot_index", "last_snapshot_term")


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True,
                                   stderr=subprocess.PIPE, timeout=20).strip()


def api(path):
    request = urllib.request.Request("https://api.github.com/repos/Nettking/msh/" + path,
        headers={"Authorization": "Bearer " + os.environ["GH_TOKEN"], "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def prepare():
    expected = {"GITHUB_REPOSITORY": "Nettking/msh", "GITHUB_ACTOR": "Nettking",
                "GITHUB_TRIGGERING_ACTOR": "Nettking", "GITHUB_EVENT_NAME": "push",
                "GITHUB_REF": "refs/heads/" + BRANCH, "GITHUB_RUN_ATTEMPT": "1",
                "RUNNER_NAME": "Beast", "RUNNER_OS": "Windows"}
    if any(os.environ.get(key) != value for key, value in expected.items()) or sys.version_info[:3] != (3, 12, 10):
        raise ValueError("Trusted original Beast identity and existing Python required")
    root = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
    head = os.environ["GITHUB_SHA"]
    if git(root, "rev-parse", "HEAD") != head or git(root, "rev-parse", "HEAD^") != C:
        raise ValueError("Reader must be direct diagnostic child of exact C")
    if git(root, "status", "--porcelain") or set(git(root, "diff", "--name-only", C, "HEAD").splitlines()) != {HELPER, WORKFLOW}:
        raise ValueError("Reader source identity or two-file scope mismatch")
    run_id = int(os.environ["GITHUB_RUN_ID"])
    rows = api(f"actions/runs/{run_id}/attempts/1/jobs?per_page=100")
    if rows["total_count"] != 1 or len(rows["jobs"]) != 1:
        raise ValueError("Exactly one reader job required")
    current = rows["jobs"][0]
    if (current["runner_id"] != 28 or current["runner_name"] != "Beast" or current["head_sha"] != head
            or current["run_id"] != run_id or current["name"] != "Observe new failed fixture state once"):
        raise ValueError("Current native reader job/runner binding failed")
    original = api(f"actions/jobs/{ORIGINAL_JOB}")
    identity = {"id": ORIGINAL_JOB, "run_id": ORIGINAL_RUN, "run_attempt": 1,
                "head_sha": ORIGINAL_SOURCE, "runner_name": "Beast", "runner_id": 28,
                "name": "Observe original Beast journal stage once", "status": "completed", "conclusion": "failure"}
    if any(original.get(key) != value for key, value in identity.items()):
        raise ValueError("Original failed job identity mismatch")
    identity.update(started_at=original["started_at"], completed_at=original["completed_at"])
    for part in (WORK, *WORK.parents):
        value = part.lstat()
        if stat.S_ISLNK(value.st_mode) or getattr(value, "st_file_attributes", 0) & 0x400:
            raise ValueError("Private output ancestry must not use reparse points")
    free = shutil.disk_usage(WORK).free
    # Six groups: 3*16MiB raw + 3*16MiB working +32MiB snapshot each.
    if free <= 12 * 1024**3 + 6 * 8 * LIMIT:
        raise OSError("Insufficient reserve above existing 12 GiB floor")
    write_json(WORK / "fcp-bootstrap-second-failure-state-35779391870-once.json",
               {"at": utc(), "run_id": run_id, "source": head})
    base = WORK / ("fcp-bootstrap-second-failure-state-" + str(run_id))
    private_directory(base)
    (base / "private-do-not-upload").mkdir()
    public = base / "public"
    public.mkdir()
    with Path(os.environ["GITHUB_ENV"]).open("a", encoding="utf-8") as stream:
        stream.write("FCP_SECOND_FAILURE_PUBLIC=" + str(public) + "\n")
    write_json(public / "INSTRUMENTATION_IDENTITY.json", {
        "at": utc(), "reader_source": head, "reader_run_id": run_id, "reader_attempt": 1,
        "reader_native_job_id": current["id"], "original": identity, "product_test_source": C,
        "failed_test": TARGET, "original_test_result": "TEST_FAIL", "failure_code": "federation-quorum-leader-required",
        "failure_stage": "_populate announce_capability before Flask process start",
        "test_execution": False, "private_database_upload": False, "qualification": "NOT_EVALUATED"})
    return base, identity


def extra_projection(snapshot):
    before = sha(snapshot.read_bytes())
    connection = sqlite3.connect(snapshot.as_uri() + "?mode=ro&immutable=1", uri=True)
    deadline = time.monotonic() + 10
    connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA trusted_schema=OFF")
        # All SQL is hardcoded. JSON payloads/state are never returned to Python;
        # only these safe scalar fields leave SQLite, including nested commands.
        commands = safe_rows(connection, """WITH envelopes AS (
            SELECT log_index,log_term,0 AS inner_index,command_json AS envelope FROM replica_log
            UNION ALL SELECT l.log_index,l.log_term,CAST(e.key AS INTEGER)+1,e.value
            FROM replica_log l,json_each(l.command_json,'$.payload.authority_commands') e
            WHERE json_extract(l.command_json,'$.command_type')='PRODUCT_TRANSACTION')
            SELECT log_index,log_term,inner_index,json_extract(envelope,'$.command_type'),
              json_extract(envelope,'$.issued_by'),
              CASE WHEN json_extract(envelope,'$.command_type') IN ('FEDERATION_GENESIS','SESSION_CREATE')
                THEN json_extract(envelope,'$.payload.creator_node_id') END,
              json_extract(envelope,'$.payload.session_id'),
              CASE WHEN json_extract(envelope,'$.command_type')='LEADER_TRANSITION'
                THEN json_extract(envelope,'$.payload.previous_leader_node_id') END,
              CASE WHEN json_extract(envelope,'$.command_type')='LEADER_TRANSITION'
                THEN json_extract(envelope,'$.payload.leader_node_id') END,
              CASE WHEN json_extract(envelope,'$.command_type')='LEADER_TRANSITION'
                THEN json_extract(envelope,'$.payload.term') END,
              CASE WHEN json_extract(envelope,'$.command_type')='LEADER_TRANSITION'
                THEN json_extract(envelope,'$.payload.reason') END,
              CASE WHEN json_extract(envelope,'$.command_type') IN ('FEDERATION_GENESIS','LEADER_TRANSITION')
                THEN json_extract(envelope,'$.payload.occurred_at') END
            FROM envelopes ORDER BY log_index,inner_index""")
        leaders = safe_rows(connection, """SELECT s.key,json_extract(s.value,'$.creator_node_id'),
            json_extract(s.value,'$.leader_node_id'),json_extract(s.value,'$.term')
            FROM replica_projection p,json_each(p.state_json,'$.leaders') s ORDER BY s.key""")
        nodes = safe_rows(connection, """SELECT json_extract(n.value,'$.node_id')
            FROM replica_projection p,json_each(p.state_json,'$.nodes') n ORDER BY n.key""")
        events = safe_rows(connection, """SELECT s.key,CAST(r.key AS INTEGER),json_extract(r.value,'$.event_type')
            FROM replica_projection p,json_each(p.state_json,'$.product_journal.sessions') s,
            json_each(s.value,'$.rows') r ORDER BY s.key,CAST(r.key AS INTEGER)""")
    finally:
        connection.close()
    after = sha(snapshot.read_bytes())
    if before != after:
        raise ValueError("Owned immutable snapshot changed during projection")
    return {"snapshot_sha256_before": before, "snapshot_sha256_after": after,
            "command_columns": ["log_index", "log_term", "inner_index", "type", "issued_by", "creator_node_id",
                                "session_id", "previous_leader_node_id", "leader_node_id", "session_term", "reason", "occurred_at"],
            "commands": commands, "leader_columns": ["session_id", "creator_node_id", "leader_node_id", "session_term"],
            "leaders": leaders, "node_columns": ["node_id"], "nodes": nodes,
            "public_event_columns": ["session_id", "row_index", "event_type"], "public_events": events,
            "excluded": ["raw_command_json", "raw_state_json", "raw_snapshot_json", "payload_json", "private_rows", "public_keys", "credentials"]}


def inspect(base, original):
    public, private = base / "public", base / "private-do-not-upload"
    report = {"started_at": utc(), "original": original, "failed_test": TARGET,
              "original_test_result": "TEST_FAIL", "product_test_source": C,
              "qualification": "NOT_EVALUATED", "test_execution": False, "databases": [],
              "limitation": "Later persisted state does not capture in-memory role/leader at rejection or prove the cause. No independent creation-time hash exists for these retained fixture paths."}
    completed_ns = int(datetime.fromisoformat(original["completed_at"].replace("Z", "+00:00")).timestamp() * 1e9)
    complete = True
    for attempt in (1, 2):
        for voter in (0, 1, 2):
            identity = f"listener-attempt-{attempt}/voter-{voter}"
            db = FIXTURE / identity / "replica.sqlite3"
            row = {"fixture_identity": identity, "observed_at": utc()}
            raw = private / f"raw-{attempt}-{voter}"
            working = private / f"working-{attempt}-{voter}"
            try:
                group = copy_group([Path(str(db) + suffix) for suffix in ("", "-wal", "-shm")], raw)
                row["raw_observation"] = group
                changed_late = any(item["present"] and item["stat_before"]["mtime_ns"] > completed_ns for item in group["files"])
                if not group["files"][0]["present"]:
                    row["status"] = "ORIGINAL_DB_ABSENT"
                elif not group["stable"] or changed_late:
                    row["status"] = "PRIVATE_ONLY_UNSTABLE_OR_MODIFIED_AFTER_JOB"
                    complete = False
                else:
                    row["consensus"] = snapshot_and_project(raw, working)
                    row["safe_details"] = extra_projection(working / "consistent.sqlite3")
                    for item in group["files"]:
                        if item["present"] and sha((raw / Path(item["path"]).name).read_bytes()) != item["sha256"]:
                            raise ValueError("Private original copy changed")
                    row["status"] = "STABLE_ORIGINAL_PATH_COPY_SAFE_PROJECTION"
            except Exception as error:  # noqa: BLE001 -- preserve explicit bounded read failure without retry
                row.pop("consensus", None)
                row.pop("safe_details", None)
                row.update(status="REFUSED", error_type=type(error).__name__)
                complete = False
            report["databases"].append(row)
    report.update(finished_at=utc(), bounded_inspection_complete=complete,
                  projected_count=sum(row["status"] == "STABLE_ORIGINAL_PATH_COPY_SAFE_PROJECTION" for row in report["databases"]),
                  absent_count=sum(row["status"] == "ORIGINAL_DB_ABSENT" for row in report["databases"]))
    write_json(public / "SECOND_FAILURE_STATE_OBSERVATION.json", report)
    compact = {key: report[key] for key in ("original", "failed_test", "original_test_result", "qualification", "limitation",
                                           "projected_count", "absent_count", "bounded_inspection_complete")}
    compact["databases"] = [{"fixture_identity": row["fixture_identity"], "status": row["status"],
        "metadata": row.get("consensus", {}).get("metadata"), "safe_details": row.get("safe_details")}
        for row in report["databases"]]
    encoded = json.dumps(compact, separators=(",", ":"), sort_keys=True)
    if len(encoded.encode("utf-8")) > 25000:
        for row in compact["databases"]:
            if row["safe_details"]:
                details = row["safe_details"]
                row["row_counts"] = {key: len(details[key]) for key in ("commands", "leaders", "nodes", "public_events")}
                row["safe_details"] = {key: details[key] for key in ("snapshot_sha256_after", "leaders", "nodes")}
        compact["native_projection_scope"] = "Metadata and current leaders only; full bounded command/event projections retained on disk"
        encoded = json.dumps(compact, separators=(",", ":"), sort_keys=True)
    if len(encoded.encode("utf-8")) <= 25000:
        print("SAFE_SECOND_FAILURE_STATE=" + encoded)
    else:
        print("SAFE_SECOND_FAILURE_STATE=" + json.dumps({"projected_count": report["projected_count"], "qualification": "NOT_EVALUATED",
              "native_projection_scope": "Full safe projection exceeds 25KB and remains retained on disk"}))
    return 0 if complete else 3


def main():
    base, original = prepare()
    return inspect(base, original)


# Bounded original-copy and safe-consensus routines below are reused from the
# previously reviewed original-fixture inspector; no SQLite access to sources.

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
    stable = before == opened and before == after and opened == closed and len(raw) == before["size"]
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
        source.backup(target, pages=64, progress=progress, sleep=0)
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



if __name__ == "__main__":
    raise SystemExit(main())
