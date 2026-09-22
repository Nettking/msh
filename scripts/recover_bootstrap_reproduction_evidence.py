"""Read exactly ten retained public files, preserving original bytes and identity.

No tests, product imports, private database reads, recursive scans, reconstruction
of an original archive, or source mutations. This is a separate recovery packet.
"""
import csv
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

PRODUCT = "11238b6bb6cacaf17c816ded0baece75c59ec5fe"
ORIGINAL_SOURCE = "a7b4f8b548f5ab48831bcb17800b4e39a5c44c19"
ORIGINAL_RUN = 35779391870
ORIGINAL_JOB = 106920694721
BRANCH = "codex/bootstrap-repro-evidence-recovery"
WORK = Path("C:/actions-runner/_work")
RETAINED = WORK / "fcp-bootstrap-repro-evidence-35779391870/retained"
PENDING = WORK / "fcp-archive-pending/f8aa3fb28c5e43faa1ff29101bf1744a/INCOMPLETE.json"
WORKFLOW = ".github/workflows/nitro-archive-capacity-readonly.yml"
HELPER = "scripts/recover_bootstrap_reproduction_evidence.py"
FILE_LIMIT = 1024 * 1024
TOTAL_LIMIT = 4 * 1024 * 1024
PRINT_LIMIT = 25000


def utc():
    return datetime.now(timezone.utc).isoformat()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def write_json(path, value):
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write("\n")


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True,
                                   stderr=subprocess.PIPE, timeout=20).strip()


def fingerprint(value):
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)


def guarded_stat(path):
    for part in reversed((path, *path.parents)):
        value = part.lstat()
        if stat.S_ISLNK(value.st_mode) or getattr(value, "st_file_attributes", 0) & 0x400:
            raise ValueError("Reparse/symlink path rejected")
    if not stat.S_ISREG(value.st_mode) or value.st_size > FILE_LIMIT:
        raise ValueError("Named input is not a bounded regular file")
    return fingerprint(value)


def read_exact(path):
    before = guarded_stat(path)
    with path.open("rb") as stream:
        opened = fingerprint(os.fstat(stream.fileno()))
        raw = stream.read(FILE_LIMIT + 1)
        closed = fingerprint(os.fstat(stream.fileno()))
    after = guarded_stat(path)
    # Windows stat/fstat ctime values are not compared across APIs.
    if before != opened or before != after or opened != closed or len(raw) != before[2] or len(raw) > FILE_LIMIT:
        raise ValueError("Named input changed during read")
    return raw, {"path": str(path), "size": len(raw), "sha256": sha(raw),
                 "stat_before": before, "stat_after": after, "fstat_stable": True}


def native_guard():
    expected = {"GITHUB_REPOSITORY": "Nettking/msh", "GITHUB_ACTOR": "Nettking",
                "GITHUB_TRIGGERING_ACTOR": "Nettking", "GITHUB_EVENT_NAME": "push",
                "GITHUB_REF": "refs/heads/" + BRANCH, "GITHUB_RUN_ATTEMPT": "1",
                "RUNNER_NAME": "Beast", "RUNNER_OS": "Windows"}
    if any(os.environ.get(key) != value for key, value in expected.items()):
        raise ValueError("Untrusted recovery invocation or second attempt")
    if sys.version_info[:3] != (3, 12, 10):
        raise ValueError("Expected existing Beast Python 3.12.10")
    root = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
    head = os.environ["GITHUB_SHA"]
    if git(root, "rev-parse", "HEAD") != head or git(root, "rev-parse", "HEAD^") != PRODUCT:
        raise ValueError("Recovery source must be direct diagnostic child of exact C")
    if git(root, "status", "--porcelain"):
        raise ValueError("Recovery source checkout is dirty")
    if set(git(root, "diff", "--name-only", PRODUCT, "HEAD").splitlines()) != {HELPER, WORKFLOW}:
        raise ValueError("Only recovery workflow/helper may differ")
    run = int(os.environ["GITHUB_RUN_ID"])
    request = urllib.request.Request(
        f"https://api.github.com/repos/Nettking/msh/actions/runs/{run}/attempts/1/jobs?per_page=100",
        headers={"Authorization": "Bearer " + os.environ["GH_TOKEN"], "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(request, timeout=20) as response:
        native = json.load(response)
    if native["total_count"] != 1 or len(native["jobs"]) != 1:
        raise ValueError("Expected exactly one native recovery job")
    job = native["jobs"][0]
    if (job["runner_id"] != 28 or job["runner_name"] != "Beast" or job["run_id"] != run
            or job["head_sha"] != head or job["name"] != "Recover existing bootstrap observation once"):
        raise ValueError("Recovery native job/runner binding failed")
    free = shutil.disk_usage(WORK).free
    if free <= 12 * 1024**3 + 2 * TOTAL_LIMIT:
        raise OSError("Recovery would cross the existing 12 GiB free-space floor")
    marker = WORK / "fcp-bootstrap-repro-evidence-recovery-35779391870-once.json"
    write_json(marker, {"at": utc(), "run_id": run, "diagnostic_source": head})
    out = WORK / ("fcp-bootstrap-public-recovery-" + str(run))
    out.mkdir()
    rows = subprocess.check_output(["whoami", "/user", "/fo", "csv", "/nh"], text=True, timeout=5)
    sid = next(csv.reader(rows.splitlines()))[1]
    subprocess.run(["icacls", str(out), "/inheritance:r", "/grant:r", "*" + sid + ":(OI)(CI)F"],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=5)
    with Path(os.environ["GITHUB_ENV"]).open("a", encoding="utf-8") as stream:
        stream.write("FCP_BOOTSTRAP_RECOVERY_OUT=" + str(out) + "\n")
    write_json(out / "RECOVERY_IDENTITY.json", {
        "recovery_source": head, "recovery_run_id": run, "recovery_attempt": 1,
        "recovery_native_job_id": job["id"], "runner_id": 28, "runner_name": "Beast",
        "original_diagnostic_source": ORIGINAL_SOURCE, "original_run_id": ORIGINAL_RUN,
        "original_attempt": 1, "original_native_job_id": ORIGINAL_JOB, "product_test_source": PRODUCT,
        "original_receipt_reconstructed": False, "test_execution": False, "private_database_reads": False,
        "original_receipt_observation": ORIGINAL_RECEIPT_OBSERVATION,
        "qualification": "NOT_EVALUATED", "new_archive_namespace": True,
        "free_bytes_before_read": free, "created_at": utc()})
    return out


def causal_summary(files):
    trace = json.loads(files["TARGET_OBSERVATION.json"])
    history = json.loads(files["HISTORICAL_COMMANDS.json"])
    cases = list(ET.fromstring(files["windows-journal-artifacts.xml"]).iter("testcase"))
    failures = []
    for case in cases:
        for tag in ("failure", "error"):
            failure = case.find(tag)
            if failure is not None:
                message = failure.get("message") or ""
                safe_messages = {"bootstrap proposal lost its leader term",
                    "StaleTerm: bootstrap proposal lost its leader term",
                    "catalog.federation.control_plane_replication.StaleTerm: bootstrap proposal lost its leader term"}
                failures.append({"classname": case.get("classname"), "name": case.get("name"),
                                 "kind": tag, "type": failure.get("type"),
                                 "recognized_message": message if message in safe_messages else None,
                                 "original_message_sha256": sha(message.encode("utf-8")),
                                 "original_message_bytes": len(message.encode("utf-8"))})
    selected = ("event", "method", "utc", "monotonic_ns", "native_thread_id", "thread_ident",
                "before", "after", "exception_type", "frames", "command_type", "command_id",
                "term", "candidate_id", "leader_term", "leader_id", "response_term", "response_granted",
                "authenticated_dispatch_on_stack", "last_leader_contact", "next_election_at",
                "election_timeout_seconds", "observed_monotonic", "after_state_semantics")
    errors, elections = [], []
    for event in trace["events"]:
        row = {key: event[key] for key in selected if key in event}
        if event.get("event") == "error":
            errors.append(row)
        before, after = event.get("before", {}), event.get("after", {})
        changed = bool(before and after and any(before.get(key) != after.get(key) for key in ("term", "role", "leader_id")))
        if changed or any(event.get("method", "").endswith("." + name) for name in (
                "start_election", "_resume_fresh_bootstrap")):
            elections.append(row)
    history_safe = {key: history[key] for key in ("status", "scope", "sha256_before", "sha256_after",
                    "columns", "commands", "node_columns", "nodes") if key in history}
    result = {"original_run_id": ORIGINAL_RUN, "original_job_id": ORIGINAL_JOB,
              "product_test_source": PRODUCT, "original_diagnostic_source": ORIGINAL_SOURCE,
              "qualification": "NOT_EVALUATED", "test_execution": False,
              "original_test_result": "TEST_FAIL" if failures else "NO_JUNIT_FAILURE_FOUND",
              "original_receipt_observation": ORIGINAL_RECEIPT_OBSERVATION,
              "junit_cases": len(cases), "failed_cases": failures, "historical_commands": history_safe,
              "target": trace["target"], "target_error_count": len(errors),
              "election_event_count": len(elections), "target_errors": errors,
              "election_events": elections,
              "causal_limit": "Expected traceback term is exact; wrapper after-state is post-unwind and not atomic guard-time state. Historical committed commands are not original guard-time proof."}
    return result


def print_bounded(summary):
    # Never truncate the decisive chain. Full stacks remain in the recovery packet;
    # use a minimal ordered timeline when the first safe projection exceeds budget.
    encoded = json.dumps(summary, sort_keys=True, separators=(",", ":"))
    if len(encoded.encode("utf-8")) <= PRINT_LIMIT:
        print("SAFE_BOOTSTRAP_RECOVERY_CAUSAL_SUMMARY=" + encoded)
        return
    unique = {}
    for event in summary["target_errors"] + summary["election_events"]:
        key = (event["monotonic_ns"], event["method"], event["event"], event.get("native_thread_id"))
        unique[key] = event
    timeline = []
    for event in sorted(unique.values(), key=lambda row: row["monotonic_ns"]):
        row = {key: event[key] for key in ("method", "event", "utc", "native_thread_id", "exception_type",
               "last_leader_contact", "next_election_at", "election_timeout_seconds", "observed_monotonic")
               if key in event}
        for name in ("before", "after"):
            if name in event:
                row[name] = {key: event[name][key] for key in ("voter_id", "term", "role", "leader_id")
                             if key in event[name]}
        guards = [{key: frame[key] for key in ("function", "term", "matched") if key in frame}
                  for frame in event.get("frames", []) if frame.get("function") == "propose_bootstrap_command"]
        if guards:
            row["expected_guard_locals"] = guards
        timeline.append(row)
    reduced = {key: summary[key] for key in ("original_run_id", "original_job_id", "original_test_result",
               "failed_cases", "target_error_count", "election_event_count", "qualification", "causal_limit",
               "historical_commands", "product_test_source", "original_diagnostic_source")}
    reduced.update(critical_timeline=timeline, projection="All critical events in original monotonic order; compact fields; complete stacks retained in packet",
                   critical_event_count=len(timeline), critical_events_omitted=0)
    encoded = json.dumps(reduced, sort_keys=True, separators=(",", ":"))
    if len(encoded.encode("utf-8")) <= PRINT_LIMIT:
        print("SAFE_BOOTSTRAP_RECOVERY_CAUSAL_SUMMARY=" + encoded)
        return
    compact = {key: summary[key] for key in ("original_run_id", "original_job_id", "original_test_result",
               "failed_cases", "target_error_count", "election_event_count", "qualification", "causal_limit")}
    compact.update(native_full_projection_omitted="25KB budget exceeded; no event chain truncated; complete CAUSAL_SUMMARY.json retained",
                   causal_summary_compact_json_sha256=sha(encoded.encode("utf-8")))
    encoded = json.dumps(compact, sort_keys=True, separators=(",", ":"))
    if len(encoded.encode("utf-8")) > PRINT_LIMIT:
        raise ValueError("Full failure messages exceed bounded native summary; originals retained")
    print("SAFE_BOOTSTRAP_RECOVERY_CAUSAL_SUMMARY=" + encoded)


def recover(out):
    pending_raw, pending_read = read_exact(PENDING)
    pending = json.loads(pending_raw)
    identity = {"schema": "fcp.local-incomplete-evidence.v1", "archive_complete": False,
                "qualification_status": "NOT_EVALUATED", "repo": "Nettking/msh",
                "tested_sha": ORIGINAL_SOURCE, "run_id": str(ORIGINAL_RUN), "run_attempt": "1",
                "job": "observe", "artifact": "diagnostic-bootstrap-beast-journal-artifacts",
                "workflow_ref": "Nettking/msh/.github/workflows/nitro-artifact-smoke.yml@refs/heads/codex/bootstrap-beast-reproduction",
                "workflow_sha": ORIGINAL_SOURCE, "native_job_id": None,
                "native_job_binding": "NOT_VERIFIED_BY_LOCAL_FAILURE_RETENTION", "missing_patterns": []}
    if any(pending.get(key) != value for key, value in identity.items()):
        raise ValueError("Original pending inventory identity mismatch")
    native_rows = {row["name"]: row for row in NATIVE_SUMMARY["files"]}
    names = set(native_rows) | {"SUMMARY.json"}
    rows = pending["files"]
    if len(rows) != 10 or {row["path"] for row in rows} != names:
        raise ValueError("Expected exactly ten named public pending inputs")
    inventory = {row["path"]: row for row in rows}
    for name, row in inventory.items():
        if (type(row["size"]) is not int or not 0 <= row["size"] <= FILE_LIMIT
                or not re.fullmatch(r"[0-9a-f]{64}", row["sha256"])
                or row["method"] not in {"hardlink", "copy"}):
            raise ValueError("Invalid pending input bound/hash/method")
        if name in native_rows and (row["size"] != native_rows[name]["bytes"]
                                    or row["sha256"] != native_rows[name]["sha256"]):
            raise ValueError("Pending inventory differs from original native announcement")
    if sum(row["size"] for row in rows) > TOTAL_LIMIT:
        raise ValueError("Exact public set exceeds fixed total byte bound")
    files, observations = {}, []
    for name in sorted(names):
        raw, read = read_exact(RETAINED / name)
        if len(raw) != inventory[name]["size"] or sha(raw) != inventory[name]["sha256"]:
            raise ValueError("Original retained file size/hash mismatch: " + name)
        files[name] = raw
        observations.append(read)
    if json.loads(files["SUMMARY.json"]) != NATIVE_SUMMARY:
        raise ValueError("SUMMARY differs from native collection object")
    # Re-read only the same eleven exact files; no wildcard or recursive inventory.
    for name in sorted(names):
        again, observation = read_exact(RETAINED / name)
        if again != files[name] or observation["stat_before"] != next(
                row["stat_after"] for row in observations if Path(row["path"]).name == name):
            raise ValueError("Original input identity/bytes changed across observation")
    again, final_pending = read_exact(PENDING)
    if again != pending_raw or final_pending["stat_before"] != pending_read["stat_after"]:
        raise ValueError("Pending inventory changed across observation")
    preserved = out / "original-files"
    preserved.mkdir()
    for name, raw in files.items():
        with (preserved / name).open("xb") as stream:
            stream.write(raw)
    with (out / "ORIGINAL_PENDING_INCOMPLETE.json").open("xb") as stream:
        stream.write(pending_raw)
    summary = causal_summary(files)
    write_json(out / "CAUSAL_SUMMARY.json", summary)
    write_json(out / "RECOVERY_VERIFICATION.json", {
        "at": utc(), "complete": True, "qualification": "NOT_EVALUATED",
        "all_nine_native_hashes_verified": True, "all_ten_pending_hashes_verified": True,
        "native_summary_equal": True, "pending_inventory": pending_read, "retained_files": observations,
        "new_packet_not_original_receipt": True, "original_files_modified": False,
        "original_observation_complete": NATIVE_SUMMARY["observation_complete"]})
    print_bounded(summary)


def main():
    out = native_guard()
    try:
        recover(out)
    except Exception as error:  # noqa: BLE001 - retain bounded failure evidence, never retry
        write_json(out / "RECOVERY_INCOMPLETE.json", {"at": utc(), "complete": False,
                   "qualification": "NOT_EVALUATED", "error_type": type(error).__name__,
                   "error_detail_sha256": sha(str(error).encode("utf-8"))})
        print("READ_ONLY_RECOVERY_INCOMPLETE: " + type(error).__name__)
        return 1
    return 0


# The original, retained native collection SUMMARY is embedded below. It is a
# literal evidence anchor, not a product import or reconstructed original packet.

NATIVE_SUMMARY = json.loads(r'''{
  "at": "2026-09-22T20:26:35.149554+00:00",
  "conclusion_limit": "Success is non-reproduction under observation, never proof of a repair or environment cause",
  "database_statuses": [
    "CONSISTENT_PRIVATE_SNAPSHOT",
    "CONSISTENT_PRIVATE_SNAPSHOT",
    "CONSISTENT_PRIVATE_SNAPSHOT"
  ],
  "diagnostic_only": true,
  "diagnostic_sha": "a7b4f8b548f5ab48831bcb17800b4e39a5c44c19",
  "dropped_events": 0,
  "files": [
    {
      "bytes": 136,
      "name": "COLLECTION.json",
      "sha256": "fb065daf09d278990d58aa4a15c4096c6aa76d40f0c64979cbbb2b1ace79a45d"
    },
    {
      "bytes": 144,
      "name": "EXECUTION_END.json",
      "sha256": "ee7df400d20afc83fb293eebc3674a4c8412fe26ec812e63f3bff187842348ee"
    },
    {
      "bytes": 5380,
      "name": "EXECUTION_START.json",
      "sha256": "3e455f91ff1273120a0b0c2a4ee4cdacebb37b0c10e8b0fb28e6b36119648834"
    },
    {
      "bytes": 1950,
      "name": "HISTORICAL_COMMANDS.json",
      "sha256": "fd020504a8c9739eeae784f03ea0fcec2df926cfe62c0d21735033b7d67737d4"
    },
    {
      "bytes": 2776,
      "name": "PREPARATION.json",
      "sha256": "894e912ba45568af79094d822308346f5864230206d523446b7a12d6ba49c0b0"
    },
    {
      "bytes": 0,
      "name": "pytest.stderr.log",
      "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    },
    {
      "bytes": 12875,
      "name": "pytest.stdout.log",
      "sha256": "e4c8015712b8193cfea8bcba90e9e827924bd9e8b44aebd9f7315a75c54c1b55"
    },
    {
      "bytes": 162404,
      "name": "TARGET_OBSERVATION.json",
      "sha256": "91819b8bc25b01e00cbb9812711d82068b8ce0ee75af786976a34b4ec5669a7f"
    },
    {
      "bytes": 83183,
      "name": "windows-journal-artifacts.xml",
      "sha256": "af2c48ad52328959f7ef1ecf0174f05c845b36bfcef80ae96891171fa4c7e004"
    }
  ],
  "junit": {
    "errors": 0,
    "failures": 1,
    "skipped": 1,
    "tests": 298
  },
  "junit_present": true,
  "observation_complete": true,
  "observation_incomplete_reasons": [],
  "private_database_copies_uploaded": false,
  "product_test_sha": "11238b6bb6cacaf17c816ded0baece75c59ec5fe",
  "qualification": "NOT_EVALUATED",
  "target_observation_present": true
}''')
ORIGINAL_RECEIPT_OBSERVATION = {"status": "NOT_FOUND_IN_EXACT_INVENTORY", "inventory_sha256": "37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570", "result_sha256": "9f88d4565ef4658d0ddf64ddb9b84fa3a8a0454b13d6b2b62a730a420e7025f5", "list_calls": 1, "fetch_calls": 0}

if __name__ == "__main__":
    raise SystemExit(main())
