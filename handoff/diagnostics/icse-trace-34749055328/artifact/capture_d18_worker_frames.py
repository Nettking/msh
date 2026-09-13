"""One CI-owned ICSE execution; publish frames, never private messages or state."""

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

SOURCE = "f104038a2b77705adaa547cdd1df7d895bf80a41"
ERROR_TYPES = {"QuorumUnavailable", "TimeoutError", "CancelledError", "StaleTerm",
               "AuthorizationError", "ControlPlaneError", "RelayRemoteError",
               "ConnectionClosedError", "InvalidMessage", "EOFError"}


def safe_trace(path, source, owned_output, tracked):
    if not path.is_file():
        return {"exists": False}
    if not path.resolve().is_relative_to(owned_output.resolve()):
        raise RuntimeError("trace escaped this execution's output")
    size = path.stat().st_size
    if size > 512 * 1024:
        return {"exists": True, "bytes": size, "omitted": "oversize"}
    raw = path.read_bytes()
    text = raw.decode("utf-8", errors="replace")
    frames = []
    for filename, line in re.findall(r'File "([^"\r\n]+)", line (\d+), in ', text):
        candidate = Path(filename)
        try:
            relative = candidate.relative_to(source).as_posix()
        except ValueError:
            relative = None
        if relative in tracked:
            name = relative
        elif "websockets" in candidate.parts:
            name = "external/websockets"
        elif "asyncio" in candidate.parts:
            name = "external/asyncio"
        else:
            name = "external"
        frames.append({"file": name, "line": int(line)})
    return {
        "exists": True, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
        "frames": frames[-64:],
        "error_types": sorted(kind for kind in ERROR_TYPES
                              if re.search(r"\b" + kind + r"\b", text)),
    }


def main():
    source = Path.cwd().resolve()
    temp = Path(os.environ["RUNNER_TEMP"]).resolve()
    evidence = Path(os.environ["D18_FRAME_EVIDENCE"]).resolve()
    assert os.environ["RUNNER_NAME"] == "Beast-Linux-WSL"
    assert source == Path(os.environ["GITHUB_WORKSPACE"]).resolve()
    assert source == Path("/opt/actions-runner/_work/msh/msh")
    assert platform.python_version() == "3.12.13"
    assert evidence.is_relative_to(temp) and evidence != temp
    assert subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip() == SOURCE
    assert not subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()
    tracked = set(subprocess.check_output(
        ["git", "ls-files", "-z", "--", "*.py"], text=True,
    ).split("\0")) - {""}
    output = temp / ("d18-frames-network-" + os.environ["GITHUB_RUN_ID"]
                     + "-" + os.environ["GITHUB_RUN_ATTEMPT"])
    assert not output.exists(), "do not overwrite another execution"
    command = [sys.executable, "-B", "-m", "demo.icse.network.run",
               "--source-sha", SOURCE, "--output", str(output)]
    receipt = {
        "at": datetime.now(timezone.utc).isoformat(),
        "mode": "DIAGNOSTIC_ONLY_NOT_QUALIFICATION", "source_sha": SOURCE,
        "control_sha": os.environ["GITHUB_SHA"], "runner": os.environ["RUNNER_NAME"],
        "host": platform.node(), "python": platform.python_version(), "command": command,
        "packages": json.loads(subprocess.check_output(
            [sys.executable, "-m", "pip", "list", "--format=json"], text=True,
        )),
        "protected_recorder_data": "NOT_ACCESSED_OR_CHANGED",
        "physical_acceptance": "NOT_EVALUATED",
    }
    code = subprocess.run(command, check=False).returncode
    receipt["exit_code"] = code
    receipt["workers"] = {
        label: safe_trace(output / "private-state" / label / "worker.stderr.log",
                          source, output, tracked)
        for label in ("voter-a", "voter-b", "voter-c", "reviewer")
    }
    receipt["driver"] = safe_trace(output / "private-state/driver-failure.log",
                                  source, output, tracked)
    for name in ("summary.json", "events.jsonl", "operator-report.html"):
        item = output / "public" / name
        if item.is_file():
            assert item.resolve().is_relative_to(output.resolve())
            shutil.copyfile(item, evidence / name)
    receipt["source_still_clean"] = not subprocess.check_output(
        ["git", "status", "--porcelain"], text=True,
    ).strip()
    (evidence / "diagnostic.json").write_text(
        json.dumps(receipt, indent=2) + "\n", encoding="utf-8",
    )
    assert receipt["source_still_clean"]
    print(json.dumps({"mode": receipt["mode"], "exit_code": code,
                      "source": SOURCE, "report": "diagnostic.json"}), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
