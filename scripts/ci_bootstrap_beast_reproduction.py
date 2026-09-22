"""One-shot research of an exact original Beast test stage, never qualification.

Only the named target gets method observation. Real calls, source, assertions,
election clocks and deadlines stay unchanged. Observation can affect timing.
Private SQLite copies never enter the retained/public upload directory.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import functools
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import subprocess
import sys
import threading
import time
import urllib.request
import xml.etree.ElementTree as ET

C = "11238b6bb6cacaf17c816ded0baece75c59ec5fe"
BRANCH = "codex/bootstrap-beast-reproduction"
WORK = Path("C:/actions-runner/_work")
TARGET = "catalog/federation/tests/test_control_plane_product_transactions.py::test_pairing_material_rolls_back_first_grant_when_invitation_half_rejects"
WORKFLOW = ".github/workflows/nitro-artifact-smoke.yml"
HELPER = "scripts/ci_bootstrap_beast_reproduction.py"
OLD_SNAPSHOT = WORK / "fcp-bootstrap-original-35776222378/private/working-1-0/consistent.sqlite3"
OLD_HASH = "c1c8b87c79d9c88bc45b16867f49f4a6082f523b9953fe5b28b2222bbc9cbebb"
ORIGINAL = {"run_id": 35758028225, "attempt": 2, "job_id": 106874413640,
            "product_test_sha": C, "runner_id": 28, "runner_name": "Beast"}
METADATA = ("cluster_id", "current_term", "voted_for", "commit_index", "last_applied",
            "fencing_epoch", "last_snapshot_index", "last_snapshot_term")
LIMIT = 16 * 1024 * 1024
MAX_ROWS = 4096
MAX_EVENTS = 10000
FREE_SPACE_FLOOR = 12 * 1024 * 1024 * 1024
# Six possible fixture groups: 3 x 16 MiB raw + 3 x 16 MiB working
# files + a 32 MiB standalone snapshot per group. No recursive size scan.
SNAPSHOT_RESERVATION = 6 * (6 * LIMIT + 2 * LIMIT)
EVENTS, PATCHES, NODES = [], [], {}
EVENT_LOCK = threading.Lock()
TARGET_ROOT = None
DROPPED = 0


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True,
                                   stderr=subprocess.PIPE, timeout=30).strip()


def output_root():
    return Path(os.environ["FCP_BOOTSTRAP_OUTPUT"])


def retained():
    return output_root() / "retained"


def assert_product(root):
    if git(root, "rev-parse", "HEAD") != C or git(root, "status", "--porcelain"):
        raise ValueError("Product checkout must be clean exact C")


def admit_disk_space(path):
    usage = shutil.disk_usage(path)
    result = {"at": utc(), "path": str(path), "free_bytes": usage.free,
              "floor_bytes": FREE_SPACE_FLOOR, "snapshot_reservation_bytes": SNAPSHOT_RESERVATION}
    if usage.free <= FREE_SPACE_FLOOR + SNAPSHOT_RESERVATION:
        raise OSError("Insufficient free space to preserve 12 GiB floor and snapshot reservation")
    return result


def original_test_files(product):
    source = (product / ".github/workflows/federation-v1-release.yml").read_text(encoding="utf-8")
    part = source.split("- name: Windows replicated journal and reviewer artifact regressions", 1)[1]
    command = part.split("run: >-", 1)[1].split("\n", 2)[1].strip()
    prefix, files = command.split('" catalog/', 1)
    if not prefix.startswith('python -m pytest -o addopts= -q --durations=20 --junitxml="'):
        raise ValueError("Original stage command changed")
    files = ("catalog/" + files).split()
    if len(files) != 21 or TARGET.split("::")[0] not in files:
        raise ValueError("Unexpected original file selection")
    return files


def prepare():
    expected = {"GITHUB_REPOSITORY": "Nettking/msh", "GITHUB_ACTOR": "Nettking",
                "GITHUB_TRIGGERING_ACTOR": "Nettking", "GITHUB_EVENT_NAME": "push",
                "GITHUB_REF": "refs/heads/" + BRANCH, "GITHUB_RUN_ATTEMPT": "1",
                "RUNNER_NAME": "Beast", "RUNNER_OS": "Windows"}
    if any(os.environ.get(k) != v for k, v in expected.items()):
        raise ValueError("Untrusted invocation or second attempt")
    diagnostic = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
    diagnostic_sha = os.environ["GITHUB_SHA"]
    if git(diagnostic, "rev-parse", "HEAD") != diagnostic_sha:
        raise ValueError("Diagnostic checkout SHA mismatch")
    if git(diagnostic, "rev-parse", "HEAD^") != C:
        raise ValueError("Diagnostic must be a direct child of exact C")
    if set(git(diagnostic, "diff", "--name-only", C, "HEAD").splitlines()) != {WORKFLOW, HELPER}:
        raise ValueError("Only diagnostic workflow and helper may differ")
    if git(diagnostic, "status", "--porcelain"):
        raise ValueError("Diagnostic checkout is dirty")
    run = os.environ["GITHUB_RUN_ID"]
    request = urllib.request.Request(
        f"https://api.github.com/repos/Nettking/msh/actions/runs/{run}/attempts/1/jobs?per_page=100",
        headers={"Authorization": "Bearer " + os.environ["GH_TOKEN"],
                 "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(request, timeout=20) as response:
        native = json.load(response)
    jobs = native["jobs"]
    if native["total_count"] != 1 or len(jobs) != 1:
        raise ValueError("Expected exactly one diagnostic job")
    job = jobs[0]
    if (job["runner_id"] != 28 or job["runner_name"] != "Beast"
            or job["head_sha"] != diagnostic_sha or job["run_id"] != int(run)
            or job["name"] != "Observe original Beast journal stage once"):
        raise ValueError("Actual native runner/job identity mismatch")
    disk_admission = admit_disk_space(WORK)
    # Permanent exclusive marker: a second push/run may not execute this research.
    with (WORK / "fcp-bootstrap-beast-reproduction-11238-once.json").open("x", encoding="utf-8") as stream:
        json.dump({"run_id": run, "diagnostic_sha": diagnostic_sha, "at": utc()}, stream)
    product = WORK / ("fcp-bootstrap-repro-product-" + run)
    out = WORK / ("fcp-bootstrap-repro-evidence-" + run)
    if product.exists() or out.exists():
        raise ValueError("Refusing to reuse evidence or product path")
    private_directory(out)
    (out / "retained").mkdir()
    (out / "private-target-databases-do-not-upload").mkdir()
    subprocess.run(["git", "clone", "--no-hardlinks", "--no-checkout", str(diagnostic), str(product)],
                   check=True, timeout=60)
    subprocess.run(["git", "-C", str(product), "checkout", "--detach", C], check=True, timeout=60)
    assert_product(product)
    for name, value in {"FCP_BOOTSTRAP_PRODUCT": product, "FCP_BOOTSTRAP_OUTPUT": out,
                        "FCP_BOOTSTRAP_RETAINED": out / "retained"}.items():
        os.environ[name] = str(value)
        with Path(os.environ["GITHUB_ENV"]).open("a", encoding="utf-8") as stream:
            stream.write(f"{name}={value}\n")
    write_json(retained() / "PREPARATION.json", {
        "original": ORIGINAL, "diagnostic_sha": diagnostic_sha, "run_id": int(run),
        "attempt": 1, "job_id": job["id"], "runner_id": job["runner_id"],
        "runner_name": job["runner_name"], "prepared_at": utc(),
        "disk_admission_before_clone": disk_admission,
        "product_root": str(product), "diagnostic_root": str(diagnostic),
        "original_file_order": original_test_files(product),
        "qualification": "NOT_EVALUATED", "diagnostic_only": True,
        "differences": ["External exact-C local clone; source outside diagnostic checkout",
                        "Target-only external pytest observer; timing perturbation possible",
                        "JUnit and process output retained in isolated evidence directory",
                        "Persistent private database preservation at target teardown",
                        "Dependencies are original commands; resolved versions recorded"]})
    historical_projection()


def historical_projection():
    result = {"path": str(OLD_SNAPSHOT), "expected_sha256": OLD_HASH,
              "scope": "Historical committed commands; not guard-time state", "at": utc()}
    try:
        before, raw = read_exact(OLD_SNAPSHOT)
        if raw is None:
            result["status"] = "UNAVAILABLE"
        elif not before["stable_read"] or sha(raw) != OLD_HASH:
            result["status"] = "HASH_MISMATCH_NOT_OPENED"
        else:
            connection = sqlite3.connect(OLD_SNAPSHOT.as_uri() + "?mode=ro&immutable=1", uri=True)
            try:
                connection.execute("PRAGMA query_only=ON")
                connection.execute("PRAGMA trusted_schema=OFF")
                columns = ["type", "issued_by", "creator_node_id", "occurred_at",
                           "previous_leader_node_id", "leader_node_id", "term", "reason"]
                rows = safe_rows(connection, """SELECT json_extract(command_json,'$.command_type'),
                    json_extract(command_json,'$.issued_by'),
                    CASE WHEN json_extract(command_json,'$.command_type')='FEDERATION_GENESIS'
                      THEN json_extract(command_json,'$.payload.creator_node_id') END,
                    CASE WHEN json_extract(command_json,'$.command_type') IN ('FEDERATION_GENESIS','LEADER_TRANSITION')
                      THEN json_extract(command_json,'$.payload.occurred_at') END,
                    CASE WHEN json_extract(command_json,'$.command_type')='LEADER_TRANSITION'
                      THEN json_extract(command_json,'$.payload.previous_leader_node_id') END,
                    CASE WHEN json_extract(command_json,'$.command_type')='LEADER_TRANSITION'
                      THEN json_extract(command_json,'$.payload.leader_node_id') END,
                    CASE WHEN json_extract(command_json,'$.command_type')='LEADER_TRANSITION'
                      THEN json_extract(command_json,'$.payload.term') END,
                    CASE WHEN json_extract(command_json,'$.command_type')='LEADER_TRANSITION'
                      THEN json_extract(command_json,'$.payload.reason') END
                    FROM replica_log ORDER BY log_index LIMIT 5""")
                nodes = safe_rows(connection, """SELECT json_extract(n.value,'$.display_name'),
                    json_extract(n.value,'$.node_id') FROM replica_log,
                    json_each(command_json,'$.payload.nodes') AS n
                    WHERE json_extract(command_json,'$.command_type')='FEDERATION_GENESIS' LIMIT 4""")
                if len(rows) != 4 or len(nodes) != 3:
                    raise ValueError("Unexpected historical command/node count")
            finally:
                connection.close()
            after, after_raw = read_exact(OLD_SNAPSHOT)
            if not after["stable_read"] or sha(after_raw) != OLD_HASH:
                raise ValueError("Historical snapshot changed during read")
            result.update(status="VERIFIED_READ_ONLY", sha256_before=sha(raw),
                          sha256_after=sha(after_raw), columns=columns, commands=rows,
                          node_columns=["display_name", "node_id"], nodes=nodes)
    except (OSError, sqlite3.Error, ValueError) as error:
        result.update(status="OBSERVATION_UNAVAILABLE", exception_type=type(error).__name__)
    write_json(retained() / "HISTORICAL_COMMANDS.json", result)


def eligible(node):
    if TARGET_ROOT is None or not hasattr(node, "store"):
        return False
    try:
        relative = Path(node.store.database).resolve().relative_to(TARGET_ROOT).as_posix()
    except (ValueError, AttributeError):
        return False
    return bool(re.fullmatch(r"listener-attempt-[12]/voter-[012]/replica.sqlite3", relative))


def state(node):
    with node._state_lock:
        return {"voter_id": node.voter_id, "term": node.store.current_term,
                "role": node.role, "leader_id": node.leader_id,
                "commit_index": node.store.commit_index, "last_log_index": node.store.last_log_index()}


def emit(event):
    global DROPPED
    event.update(utc=utc(), monotonic_ns=time.monotonic_ns(),
                 thread=threading.current_thread().name, thread_ident=threading.get_ident(),
                 native_thread_id=threading.get_native_id())
    with EVENT_LOCK:
        if len(EVENTS) < MAX_EVENTS:
            EVENTS.append(event)
        else:
            DROPPED += 1


def frames(error):
    rows = []
    cursor = error.__traceback__
    while cursor:
        frame = cursor.tb_frame
        row = {"function": frame.f_code.co_name, "file": Path(frame.f_code.co_filename).name,
               "line": cursor.tb_lineno}
        if frame.f_code.co_name == "propose_bootstrap_command":
            for name in ("term", "matched"):
                value = frame.f_locals.get(name)
                if isinstance(value, int):
                    row[name] = value
            row["term_semantics"] = "Exact expected local guard term from exception traceback"
        rows.append(row)
        cursor = cursor.tb_next
    return rows


def wrap(cls, name, transitions_only=False):
    original = getattr(cls, name)
    owned = name in cls.__dict__

    @functools.wraps(original)
    def observed(self, *args, **kwargs):
        node = getattr(self, "node", self)
        if not eligible(node):
            return original(self, *args, **kwargs)
        NODES[str(node.store.database)] = node
        try:
            before = state(node)
            entry = {"method": cls.__name__ + "." + name, "before": before}
            for key in ("candidate_id", "term", "leader_id", "leader_term"):
                value = kwargs.get(key)
                if isinstance(value, (str, int)):
                    entry[key] = value
            if name == "_propose_bootstrap_command":
                entry.update(command_id=args[0].command_id, command_type=args[0].command_type)
            if name == "_resume_fresh_bootstrap":
                entry.update(last_leader_contact=node.last_leader_contact,
                             next_election_at=self._next_election_at,
                             election_timeout_seconds=self.election_timeout_seconds,
                             observed_monotonic=time.monotonic())
            if name in {"force_follower", "start_election", "receive_vote_request"}:
                callers, frame = [], sys._getframe(1)
                for _ in range(16):
                    if frame is None:
                        break
                    callers.append({"function": frame.f_code.co_name,
                                    "file": Path(frame.f_code.co_filename).name, "line": frame.f_lineno})
                    frame = frame.f_back
                entry["callers"] = callers
                entry["authenticated_dispatch_on_stack"] = any(
                    row["function"] == "_dispatch" and row["file"] == "control_plane_transport.py"
                    for row in callers)
            if not transitions_only:
                emit(dict(entry, event="enter"))
        except Exception as error:
            emit({"event": "observer_error", "exception_type": type(error).__name__})
            return original(self, *args, **kwargs)
        started = time.monotonic_ns()
        try:
            result = original(self, *args, **kwargs)
        except BaseException as error:
            try:
                emit(dict(entry, event="error", after=state(node),
                          elapsed_ns=time.monotonic_ns()-started,
                          exception_type=type(error).__name__, frames=frames(error),
                          after_state_semantics="Sampled after unwinding; not atomic guard-time state"))
            except Exception as observation_error:
                emit({"event": "observer_error", "exception_type": type(observation_error).__name__})
            raise
        try:
            after = state(node)
            if not transitions_only or after != before:
                row = dict(entry, event="exit", after=after, elapsed_ns=time.monotonic_ns()-started)
                if isinstance(result, (int, bool)):
                    row["result"] = result
                for key in ("term", "granted", "success"):
                    value = getattr(result, key, None)
                    if isinstance(value, (int, bool)):
                        row["response_" + key] = value
                emit(row)
        except Exception as error:
            emit({"event": "observer_error", "exception_type": type(error).__name__})
        return result

    PATCHES.append((cls, name, original, owned))
    setattr(cls, name, observed)


def pytest_collection_finish(session):
    ids = [item.nodeid for item in session.items]
    write_json(retained() / "COLLECTION.json", {"count": len(ids), "target_count": ids.count(TARGET),
                                               "ordered_nodeids_sha256": sha("\n".join(ids).encode())})
    if len(ids) != 298 or ids.count(TARGET) != 1:
        raise ValueError("Original stage must collect exactly 298 cases and target once")


def pytest_runtest_call(item):
    global TARGET_ROOT
    if item.nodeid != TARGET:
        yield
        return
    TARGET_ROOT = Path(item.funcargs["tmp_path"]).resolve()
    from catalog.federation.control_plane_replication import ReplicaNode
    from catalog.federation.control_plane_runtime import ObservedReplicaNode, PhysicalReadyReplicatedFederationRuntime
    from catalog.federation.federation_v1_release_runtime import FederationV1ReleaseRuntime
    for name in ("start_election", "synchronize", "_step_down", "receive_vote_request"):
        wrap(ReplicaNode, name)
    for name in ("receive_append_entries", "receive_install_snapshot"):
        wrap(ReplicaNode, name, transitions_only=True)
    wrap(ObservedReplicaNode, "force_follower")
    wrap(PhysicalReadyReplicatedFederationRuntime, "_lifecycle_round")
    for name in ("_propose_bootstrap_command", "_bootstrap_new_federation", "_resume_fresh_bootstrap"):
        wrap(FederationV1ReleaseRuntime, name)
    emit({"event": "target_call_start", "nodeid": TARGET})
    yield
    emit({"event": "target_call_finished", "nodeid": TARGET})


pytest_runtest_call.pytest_impl = {"hookwrapper": True, "tryfirst": True}


def pytest_runtest_teardown(item, nextitem):
    yield
    if item.nodeid != TARGET or TARGET_ROOT is None:
        return
    for cls, name, original, owned in reversed(PATCHES):
        if owned:
            setattr(cls, name, original)
        else:
            delattr(cls, name)
    PATCHES.clear()
    report = {"target": TARGET, "at": utc(), "diagnostic_only": True,
              "qualification": "NOT_EVALUATED", "databases": [],
              "preservation_before_next_test": True,
              "limitation": "Read-only state locks and observer calls can perturb timing",
              "exception_state_semantics": "Traceback term is the exact expected local guard term; wrapper after-state is sampled after unwinding, not atomic guard-time state"}
    private = output_root() / "private-target-databases-do-not-upload"
    try:
        report["disk_admission_before_copies"] = admit_disk_space(private)
    except OSError as error:
        report["preservation_error"] = {"reason": "DISK_ADMISSION_FAILED", "exception_type": type(error).__name__}
        with EVENT_LOCK:
            report.update(events=list(EVENTS), dropped_events=DROPPED)
        report["target_nodes_observed"] = len(NODES)
        write_json(retained() / "TARGET_OBSERVATION.json", report)
        return
    # Exactly six known possible fixture files, no recursive directory scan.
    for attempt in (1, 2):
        for voter in (0, 1, 2):
            path = TARGET_ROOT / f"listener-attempt-{attempt}/voter-{voter}/replica.sqlite3"
            if not path.exists():
                continue
            row = {"identity": f"listener-attempt-{attempt}/voter-{voter}"}
            try:
                paths = [Path(str(path) + suffix) for suffix in ("", "-wal", "-shm")]
                raw = private / f"raw-{attempt}-{voter}"
                row["raw_observation"] = copy_group(paths, raw)
                if not row["raw_observation"]["stable"]:
                    row["status"] = "UNSTABLE_COPY_NOT_PROJECTED"
                else:
                    row["projection"] = snapshot_and_project(raw, private / f"working-{attempt}-{voter}")
                    row["status"] = "CONSISTENT_PRIVATE_SNAPSHOT"
            except Exception as error:
                row.update(status="PRESERVATION_ERROR", exception_type=type(error).__name__)
            report["databases"].append(row)
    with EVENT_LOCK:
        report.update(events=list(EVENTS), dropped_events=DROPPED)
    report["target_nodes_observed"] = len(NODES)
    write_json(retained() / "TARGET_OBSERVATION.json", report)


pytest_runtest_teardown.pytest_impl = {"hookwrapper": True, "tryfirst": True}


def run_stage():
    product = Path(os.environ["FCP_BOOTSTRAP_PRODUCT"])
    assert_product(product)
    disk_admission = admit_disk_space(output_root())
    with (output_root() / "execution-once.json").open("x", encoding="utf-8") as stream:
        json.dump({"started": utc()}, stream)
    command = [sys.executable, "-m", "pytest", "-o", "addopts=", "-q", "--durations=20",
               "--junitxml=" + str(retained() / "windows-journal-artifacts.xml"), *original_test_files(product)]
    dependencies = sorted({(item.metadata["Name"], item.version) for item in importlib.metadata.distributions()})
    write_json(retained() / "EXECUTION_START.json", {
        "at": utc(), "product_test_sha": C, "diagnostic_sha": os.environ["GITHUB_SHA"],
        "disk_admission_before_run": disk_admission,
        "command": command, "cwd": str(product), "python": sys.version,
        "dependencies": dependencies, "go": subprocess.check_output(["go", "version"], text=True, timeout=10).strip(),
        "TEMP": "C:/fcp-qtmp", "TMP": "C:/fcp-qtmp", "qualification": "NOT_EVALUATED"})
    env = os.environ.copy()
    env.update(PYTHONPATH=str(Path(__file__).resolve().parent),
               PYTEST_PLUGINS="ci_bootstrap_beast_reproduction", TEMP="C:/fcp-qtmp", TMP="C:/fcp-qtmp")
    for key in ("GH_TOKEN", "GITHUB_TOKEN"):
        env.pop(key, None)
    with (retained() / "pytest.stdout.log").open("xb") as stdout, (retained() / "pytest.stderr.log").open("xb") as stderr:
        result = subprocess.run(command, cwd=product, env=env, stdout=stdout, stderr=stderr, check=False)
    write_json(retained() / "EXECUTION_END.json", {"at": utc(), "exit_code": result.returncode,
                                                  "product_clean": git(product, "status", "--porcelain") == "",
                                                  "head": git(product, "rev-parse", "HEAD")})
    assert_product(product)
    return result.returncode


def collect():
    product = Path(os.environ["FCP_BOOTSTRAP_PRODUCT"])
    assert_product(product)
    observation = retained() / "TARGET_OBSERVATION.json"
    junit = retained() / "windows-journal-artifacts.xml"
    summary = {"at": utc(), "qualification": "NOT_EVALUATED", "diagnostic_only": True,
               "product_test_sha": C, "diagnostic_sha": os.environ["GITHUB_SHA"],
               "target_observation_present": observation.is_file(), "junit_present": junit.is_file(),
               "private_database_copies_uploaded": False,
               "conclusion_limit": "Success is non-reproduction under observation, never proof of a repair or environment cause"}
    reasons = []
    if not (retained() / "EXECUTION_END.json").is_file():
        reasons.append("EXECUTION_END_MISSING")
    if not junit.is_file():
        reasons.append("JUNIT_MISSING")
    if junit.is_file():
        tree = ET.parse(junit).getroot()
        summary["junit"] = {key: sum(int(s.get(key, "0")) for s in tree.iter("testsuite"))
                            for key in ("tests", "failures", "errors", "skipped")}
        if summary["junit"]["tests"] != 298:
            reasons.append("JUNIT_CASE_COUNT_NOT_298")
    if observation.is_file():
        value = json.loads(observation.read_text(encoding="utf-8"))
        summary["dropped_events"] = value["dropped_events"]
        summary["database_statuses"] = [row["status"] for row in value["databases"]]
        events = value.get("events", [])
        if value.get("target") != TARGET or not value.get("preservation_before_next_test"):
            reasons.append("TARGET_OR_PRESERVATION_BOUNDARY_INVALID")
        if not any(row.get("event") == "target_call_start" for row in events):
            reasons.append("TARGET_CALL_START_MISSING")
        if not any(row.get("event") == "target_call_finished" for row in events):
            reasons.append("TARGET_CALL_FINISH_MISSING")
        if value["dropped_events"]:
            reasons.append("TRACE_EVENTS_DROPPED")
        if any(row.get("event") == "observer_error" for row in events):
            reasons.append("OBSERVER_ERRORS")
        if value.get("target_nodes_observed", 0) < 3:
            reasons.append("THREE_TARGET_NODES_NOT_OBSERVED")
        if value.get("preservation_error"):
            reasons.append("PRESERVATION_ERROR")
        if any(row.get("status") != "CONSISTENT_PRIVATE_SNAPSHOT" for row in value["databases"]):
            reasons.append("DATABASE_PRESERVATION_INCOMPLETE")
        identities = {row["identity"] for row in value["databases"]
                      if row.get("status") == "CONSISTENT_PRIVATE_SNAPSHOT"}
        # A first listener-acquisition attempt may leave extra retained files;
        # the latest fixture attempt must still supply all three voter snapshots.
        latest = max((int(identity.split("/")[0].rsplit("-", 1)[1]) for identity in identities), default=0)
        expected = {f"listener-attempt-{latest}/voter-{voter}" for voter in (0, 1, 2)}
        if not latest or not expected.issubset(identities):
            reasons.append("LATEST_ATTEMPT_THREE_CONSISTENT_SNAPSHOTS_MISSING")
    else:
        reasons.append("TARGET_TRACE_MISSING")
    summary["observation_complete"] = not reasons
    summary["observation_incomplete_reasons"] = reasons
    summary["files"] = [{"name": p.name, "bytes": p.stat().st_size, "sha256": sha(p.read_bytes())}
                        for p in sorted(retained().iterdir()) if p.is_file()]
    write_json(retained() / "SUMMARY.json", summary)
    print(json.dumps(summary, sort_keys=True))
    return 1 if reasons else 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=("prepare", "run", "collect"))
    args = parser.parse_args()
    return {"prepare": prepare, "run": run_stage, "collect": collect}[args.operation]() or 0


# The bounded copy/projection functions below are copied from the reviewed original
# evidence collector. Source SQLite is never opened by SQLite; replay/backup is on
# owned private copies only. The only adjustment is backup sleep=0 (no sleeps).

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
