"""Bounded snapshot of submitted actual-main gates; no dispatch or test execution."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, "C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance")
from github_qualification import client

parser = argparse.ArgumentParser()
parser.add_argument("--include-release", action="store_true")
args = parser.parse_args()
ROOT = Path(__file__).resolve().parent
SOURCE = "e6a9b74a1d555609eed6bf40c800e1258f1c9077"
api = client()
assert api("/git/ref/heads/main")["object"]["sha"] == SOURCE
startup = json.loads((ROOT / "main-e6a9b74a-qualification-startup.json").read_text())
latest = ROOT / "main-e6a9b74a-qualification-latest.json"
old = json.loads(latest.read_text()) if latest.exists() else {"runs": []}
retained = {r["id"]: r for r in old["runs"] if r["status"] == "completed"}
pending = [r for r in startup["runs"] if r["id"] not in retained and
           (args.include_release or r["id"] != 34751832493)]


def read(row):
    run_id = row["id"]
    run = api(f"/actions/runs/{run_id}")
    assert run["head_sha"] == SOURCE
    jobs = api(f"/actions/runs/{run_id}/jobs?filter=all&per_page=100")["jobs"]
    result = {k: run.get(k) for k in ("id", "path", "head_sha", "event", "status", "conclusion", "run_attempt", "created_at", "updated_at")}
    result["expected_jobs"] = row["expected_jobs"]
    result["jobs"] = [{k: j.get(k) for k in ("id", "name", "status", "conclusion", "run_attempt", "runner_name", "runner_id", "started_at", "completed_at", "steps")} for j in jobs]
    if run["status"] == "completed":
        result["artifacts"] = api(f"/actions/runs/{run_id}/artifacts?per_page=100")["artifacts"]
    return result


with ThreadPoolExecutor(max_workers=4) as pool:
    current = list(pool.map(read, pending))
stamp = datetime.now(timezone.utc)
result = {"at": stamp.isoformat(), "source": SOURCE,
          "runs": sorted([*retained.values(), *current], key=lambda r: r["id"]),
          "previous_terminal_runs_not_polled": sorted(retained),
          "release_deferred": not args.include_release and 34751832493 not in retained}
text = json.dumps(result, indent=2) + "\n"
latest.write_text(text, encoding="utf-8")
(ROOT / ("main-e6a9b74a-qualification-" + stamp.strftime("%Y%m%dT%H%M%SZ") + ".json")).write_text(text, encoding="utf-8")
print(json.dumps({"at": result["at"], "release_deferred": result["release_deferred"], "runs": [
    {"id": r["id"], "workflow": r["path"], "status": r["status"], "conclusion": r["conclusion"],
     "jobs": [{k: j.get(k) for k in ("id", "name", "status", "conclusion", "runner_name")} for j in r["jobs"]]}
    for r in result["runs"]]}))
