"""Two bounded CI-only executions on unchanged PR475 source and owned fixtures."""
import datetime
import hashlib
import json
import os
import pathlib
import platform
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET

SHA = "5e6f184311019b9982e8544a18f3dc02c1b16e98"
TREE = "1c671f446fa215c99a6a58a155806de394aa569a"
NODE = "catalog/federation/tests/test_control_plane_session_authority.py::test_reachable_isolated_leader_cannot_report_current_session_authority"
PREDECESSORS = [
    "catalog/federation/tests/test_control_plane_release_bootstrap_recovery.py::test_actual_release_voters_resume_interrupted_fresh_bootstrap[after-genesis]",
    "catalog/federation/tests/test_control_plane_release_bootstrap_recovery.py::test_actual_release_voters_resume_interrupted_fresh_bootstrap[after-journal-before-seal]",
    "catalog/federation/tests/test_control_plane_witnessed_chunk_recovery.py::test_different_voter_completes_exact_witnessed_prefix_after_first_real_chunk",
]


def git(*args):
    return subprocess.check_output(["git", *args], text=True, timeout=30).strip()


def main():
    output = pathlib.Path(os.environ["D16_EVIDENCE"])
    assert output.is_dir()
    assert os.name == "nt" and platform.python_version() == "3.12.10"
    assert os.environ["COMPUTERNAME"].upper() == "BEAST"
    assert os.environ["RUNNER_NAME"] == "Beast"
    assert git("rev-parse", "HEAD") == SHA
    assert git("rev-parse", "HEAD^{tree}") == TREE
    assert not git("status", "--porcelain", "--untracked-files=no")
    root = pathlib.Path("C:/fcp-qtmp") / ("d16-" + uuid.uuid4().hex[:16])
    assert root.parent.resolve() == pathlib.Path("C:/fcp-qtmp").resolve()
    root.mkdir()  # Exclusive new owned directory. Pytest bases below do not exist.
    observer = pathlib.Path(__file__).with_name("d16_beast_observer.py")
    context_mode = os.environ.get("D16_MODE", "isolated") == "context"
    receipt = {"recorded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "candidate_sha": SHA, "tree": TREE,
               "control_sha": os.environ["GITHUB_SHA"], "run_id": os.environ["GITHUB_RUN_ID"],
               "host": platform.node(), "runner": os.environ["RUNNER_NAME"],
               "python": sys.version, "executable": sys.executable, "source": os.getcwd(),
               "owned_tmp": str(root), "diagnostic_only": True,
               "protected_recorder_data": "UNTOUCHED", "physical_runtime": "UNCHANGED",
               "observer_sha256": hashlib.sha256(observer.read_bytes()).hexdigest(),
               "context_mode": context_mode, "executions": []}

    def save():
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")

    save()
    failed = False
    try:
        for mode in (("context",) if context_mode else ("original", "observed")):
            env = os.environ.copy()
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            env["TEMP"] = env["TMP"] = "C:/fcp-qtmp"
            command = [sys.executable, "-B", "-m", "pytest", "-o", "addopts=", "-p", "no:cacheprovider",
                       "-q", "--durations=5", "--basetemp=" + str(root / mode),
                       "--junitxml=" + str(output / (mode + ".xml"))]
            if mode in ("observed", "context"):
                env["PYTHONPATH"] = os.pathsep.join([str(observer.parent), os.getcwd()])
                env["D16_OBSERVER_OUTPUT"] = str(output / "observer.json")
                command += ["-p", "d16_beast_observer"]
            command += (PREDECESSORS if context_mode else []) + [NODE]
            if context_mode:
                command.append("-x")  # Stop at first failure; no misleading later context.
            started = time.monotonic()
            entry = {"mode": mode, "command": command,
                     "timeout_seconds": 300 if context_mode else 180}
            receipt["executions"].append(entry)
            save()
            try:
                with (output / (mode + ".log")).open("w", encoding="utf-8") as log:
                    completed = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT,
                                               text=True, timeout=entry["timeout_seconds"])
                entry["returncode"] = completed.returncode
                failed = failed or completed.returncode != 0
            except subprocess.TimeoutExpired:
                entry["timeout"] = True
                failed = True
                break
            finally:
                entry["elapsed_seconds"] = time.monotonic() - started
                save()
            xml = output / (mode + ".xml")
            if entry["returncode"] and mode == "original":
                # Continue only the already-known D16 boundary, never an unknown failure.
                failures = ET.parse(xml).findall(".//failure") if xml.exists() else []
                if len(failures) != 1 or "federation-quorum-leader-required" not in (failures[0].text or ""):
                    receipt["stopped"] = "unexpected_original_failure_requires_review"
                    break
    finally:
        receipt["final_sha"] = git("rev-parse", "HEAD")
        receipt["final_tracked_status"] = git("status", "--porcelain", "--untracked-files=no")
        receipt["completed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        save()
    assert receipt["final_sha"] == SHA and not receipt["final_tracked_status"]
    print(json.dumps(receipt))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
