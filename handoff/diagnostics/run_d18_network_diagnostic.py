"""One unchanged network execution; never upload private state or raw errors."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys

SHA = "5e6f184311019b9982e8544a18f3dc02c1b16e98"
PRIOR = Path("/opt/actions-runner/_work/_temp/fcp-icse-network-34734857086-1-Linux")
ERROR_TYPES = {"RuntimeError", "TimeoutError", "RelayRemoteError", "AuthorizationError",
               "QuorumUnavailable", "KeyError", "ValueError", "OSError", "ConnectionError"}
ERROR_CODES = {"federation-quorum-leader-required", "quorum-unavailable",
               "capability-node-mismatch", "ownership-lease-expired"}


def classify_trace(path, source):
    if not path.is_file():
        return {"exists": False}
    raw = path.read_bytes()
    if len(raw) > 512 * 1024:
        raise RuntimeError("CI error file exceeds bounded inspection size")
    text = raw.decode("utf-8", errors="replace")
    frames = []
    for file, line, function in re.findall(r'File "([^"]+)", line (\d+), in ([A-Za-z0-9_<>]+)', text):
        candidate = Path(file)
        try:
            name = candidate.relative_to(source).as_posix()
        except ValueError:
            name = "external/" + candidate.name
        frames.append({"file": name, "line": int(line), "function": function})
    # Never return exception messages, worker stderr, tokens or state contents.
    rpc = re.findall(r"(voter-[abc]|reviewer) (announce|discover|connect|join|invite|bootstrap) failed: ([A-Za-z]+)/(None|[a-z-]+)", text)
    return {
        "exists": True, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
        "frames": frames,
        "error_types": sorted(value for value in ERROR_TYPES if re.search(r"\b" + value + r"\b", text)),
        "known_error_codes": sorted(value for value in ERROR_CODES if value in text),
        "rpc_failures": [{"node_label": node, "operation": operation,
                          "type": kind if kind in ERROR_TYPES else "OTHER",
                          "code": code if code in ERROR_CODES else "OTHER_OR_NONE"}
                         for node, operation, kind, code in rpc],
    }


def main():
    source = Path.cwd().resolve()
    evidence = Path(os.environ["D18_EVIDENCE"]).resolve()
    expected = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
    assert source == expected == Path("/opt/actions-runner/_work/msh/msh")
    assert os.environ["RUNNER_NAME"] == "Beast-Linux-WSL"
    assert platform.python_version() == "3.12.13"
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    assert actual == SHA
    assert not subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()
    prior_file = PRIOR / "private-state/driver-failure.log"
    if prior_file.exists():
        assert prior_file.resolve().is_relative_to(PRIOR)
    prior = classify_trace(prior_file, source)
    receipt = {"recorded_at_utc": datetime.now(timezone.utc).isoformat(),
               "mode": "DIAGNOSTIC_ONLY_NOT_QUALIFICATION", "source_sha": actual,
               "control_sha": os.environ["GITHUB_SHA"], "host": platform.node(),
               "runner": os.environ["RUNNER_NAME"], "python": platform.python_version(),
               "prior_error": prior, "protected_recorder_data": "NOT_ACCESSED_OR_CHANGED"}
    if prior["exists"]:
        receipt["action"] = "retained_prior_classification_without_execution"
        code = 0
    else:
        output = Path(os.environ["RUNNER_TEMP"]) / ("d18-network-" + os.environ["GITHUB_RUN_ID"] + "-" + os.environ["GITHUB_RUN_ATTEMPT"])
        assert not output.exists(), "Do not overwrite an earlier diagnostic"
        command = [sys.executable, "-B", "-m", "demo.icse.network.run", "--source-sha", SHA, "--output", str(output)]
        receipt["action"] = "one_unchanged_original_host_execution"
        receipt["command"] = command
        code = subprocess.run(command, check=False).returncode
        receipt["exit_code"] = code
        receipt["error"] = classify_trace(output / "private-state/driver-failure.log", source)
        public = output / "public"
        # These are the original checked-in driver's redacted public outputs.
        for name in ("summary.json", "operator-report.html"):
            path = public / name
            if path.is_file():
                assert path.resolve().is_relative_to(output.resolve())
                shutil.copyfile(path, evidence / name)
    receipt["source_still_clean"] = not subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()
    assert receipt["source_still_clean"]
    (evidence / "diagnostic.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(receipt), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
