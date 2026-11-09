"""Probe unchanged B0 tests against an isolated in-memory policy mutant.

Production files are never rewritten. The three research adapter mutants are
outside B0's execution scope and receive no artificial detection scores.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import subprocess
import sys
from pathlib import Path


PROBE = r'''
import json
import sys
import pytest
from catalog.flask_app.services import digital_twin_service as production

original = production._classify_rows
def unknown_as_non_active(frame):
    result = original(frame)
    unknown = result["state_class"].isna()
    result.loc[unknown, "state_class"] = "non_active"
    result.loc[unknown, "state_label"] = "DEFECT: unknown as non-active"
    return result

production._classify_rows = unknown_as_non_active

class Evidence:
    def __init__(self):
        self.assertion_failures = []
        self.other_call_failures = []
        self.setup_teardown_errors = []
        self.collection_errors = []
    def pytest_runtest_logreport(self, report):
        if not report.failed:
            return
        crash = getattr(report.longrepr, "reprcrash", None)
        message = getattr(crash, "message", str(report.longrepr))
        item = {"nodeid": report.nodeid, "when": report.when, "message": message}
        if report.when != "call":
            self.setup_teardown_errors.append(item)
        elif message.startswith("AssertionError") or message.startswith("assert "):
            self.assertion_failures.append(item)
        else:
            self.other_call_failures.append(item)
    def pytest_collectreport(self, report):
        if report.failed:
            self.collection_errors.append({"nodeid": report.nodeid, "message": str(report.longrepr)})

evidence = Evidence()
arguments = ["-o", "addopts=", "-q", "catalog/flask_app/tests/test_digital_twin_service.py",
             "--basetemp", sys.argv[1]]
result = pytest.main(arguments, plugins=[evidence])
print("DT_B0_PROBE_SUMMARY=" + json.dumps(vars(evidence), sort_keys=True))
raise SystemExit(int(result))
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--basetemp", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    command = [sys.executable, "-c", PROBE, str(args.basetemp.resolve())]
    child = subprocess.run(command, cwd=root, capture_output=True, text=True)
    summary = None
    for line in child.stdout.splitlines():
        if line.startswith("DT_B0_PROBE_SUMMARY="):
            summary = json.loads(line.partition("=")[2])
    clean_assertion_detection = bool(
        child.returncode == 1 and summary and summary["assertion_failures"]
        and not summary["other_call_failures"]
        and not summary["setup_teardown_errors"]
        and not summary["collection_errors"])
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    evidence = {
        "scope": "unchanged B0 tests; isolated in-memory production policy mutation; no historical input",
        "source_commit": commit,
        "python": sys.version,
        "dependencies": {name: importlib.metadata.version(name) for name in ("pytest", "pandas", "numpy")},
        "source_sha256": {str(path.relative_to(root)).replace("\\", "/"): hashlib.sha256(path.read_bytes()).hexdigest()
                          for path in (Path(__file__).resolve(),
                                       root / "catalog/flask_app/services/digital_twin_service.py",
                                       root / "catalog/flask_app/tests/test_digital_twin_service.py")},
        "command_argv": command,
        "working_directory": str(root),
        "mutant": "unknown_as_non_active",
        "exit_code": child.returncode,
        "stdout": child.stdout,
        "stderr": child.stderr,
        "pytest_evidence": summary,
        "assertion_failure_detection": clean_assertion_detection,
        "unexercised_research_adapters": ["wrong_unit_adapter", "omitted_rename_adapter", "omitted_provider_load"],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2)+"\n", encoding="utf-8")
    print(json.dumps({"mutant": evidence["mutant"], "exit_code": child.returncode,
                      "assertion_failure_detection": clean_assertion_detection,
                      "output": str(args.output)}))
    return 0 if clean_assertion_detection else 1


if __name__ == "__main__":
    raise SystemExit(main())
