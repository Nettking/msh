"""Bounded read of only this handoff's unfinished gates; no dispatch or retry."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, "C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance")
from github_qualification import client

ROOT = Path(__file__).resolve().parent
SOURCE = "f104038a2b77705adaa547cdd1df7d895bf80a41"
RUNS = [34750443711, 34750445380, 34750447222, 34750448614, 34750523615, 34750525417]
api = client()


def read(run_id):
    attempt = "/attempts/2" if run_id == 34746641262 else ""
    run = api(f"/actions/runs/{run_id}{attempt}")
    jobs = api(f"/actions/runs/{run_id}{attempt}/jobs?per_page=100")["jobs"]
    result = {k: run.get(k) for k in ("id", "name", "path", "head_sha", "head_branch", "event", "status", "conclusion", "run_attempt", "created_at", "updated_at")}
    assert run["head_sha"] == ("06b956787ae63215982d5afd7198dead366e05ea" if attempt else SOURCE)
    result["jobs"] = [{k: j.get(k) for k in ("id", "name", "status", "conclusion", "run_attempt", "runner_name", "runner_id", "started_at", "completed_at", "steps")} for j in jobs]
    if run["status"] == "completed":
        result["artifacts"] = api(f"/actions/runs/{run_id}/artifacts?per_page=100")["artifacts"]
    return result


stamp = datetime.now(timezone.utc)
previous = ROOT / "f104-active-gates-latest.json"
old = json.loads(previous.read_text(encoding="utf-8")) if previous.exists() else {}
retained = {r["id"]: r for r in old.get("runs", []) if r["status"] == "completed"}
pending = [r for r in [34746641262, *RUNS] if r not in retained]
with ThreadPoolExecutor(max_workers=4) as pool:
    current = list(pool.map(read, pending))
out = {"at": stamp.isoformat(), "source": SOURCE,
       "runs": sorted([*retained.values(), *current], key=lambda r: r["id"]),
       "previous_terminal_runs_not_polled": sorted(retained)}
text = json.dumps(out, indent=2) + "\n"
(ROOT / ("f104-active-gates-" + stamp.strftime("%Y%m%dT%H%M%SZ") + ".json")).write_text(text, encoding="utf-8")
previous.write_text(text, encoding="utf-8")
print(json.dumps({"at": out["at"], "runs": [{"id": r["id"], "workflow": r["path"], "status": r["status"], "conclusion": r["conclusion"], "jobs": [{k: j.get(k) for k in ("id", "name", "status", "conclusion", "runner_name")} for j in r["jobs"]]} for r in out["runs"]]}))
