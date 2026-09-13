"""Dispatch only missing native PR475 qualification; persist every action."""
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import urllib.parse

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, r"C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance")
from github_qualification import REQUIRED, client

SHA = "5e6f184311019b9982e8544a18f3dc02c1b16e98"
REF = "codex/fix-d13-windows-refusal-response"
BRANCH = "codex/federation-v1-diagnostic-sweep-20260911"
RECEIPT = ROOT / "handoff/diagnostics/pr475-exact-head-dispatch.json"
WORKFLOWS = [*REQUIRED, "cfi2-onboarding-composition.yml", "release-image-metadata.yml"]


def git(*args):
    subprocess.run(["git", *args], cwd=ROOT, check=True)


def persist(records, stage):
    value = {
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "plan_checkpoint": "e70280bc",
        "source_commit": SHA,
        "ref": REF,
        "stage": stage,
        "records": records,
        "next_ordinary_check_utc": "2026-09-13T04:01:37Z",
        "physical_state_changed": False,
        "protected_recorder_data": "Not accessed or changed",
    }
    RECEIPT.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    git("add", "handoff/diagnostics/pr475-exact-head-dispatch.json",
        "handoff/diagnostics/dispatch_pr475_exact_head_gaps.py")
    git("commit", "-m", "docs(ci): checkpoint PR475 exact-head " + stage)
    git("push", "origin", BRANCH)


def main():
    api = client()
    records = json.loads(RECEIPT.read_text(encoding="utf-8"))["records"] if RECEIPT.exists() else []
    for workflow in WORKFLOWS:
        assert api("/git/ref/heads/" + REF)["object"]["sha"] == SHA
        assert api("/pulls/475")["head"]["sha"] == SHA
        query = urllib.parse.urlencode({"branch": REF, "event": "workflow_dispatch", "per_page": 100})
        result = api("/actions/workflows/" + workflow + "/runs?" + query)
        assert result["total_count"] < 100, "Paginate before deciding a gap exists"
        existing = [run for run in result["workflow_runs"] if run["head_sha"] == SHA]
        previous = [record for record in records if record["workflow"] == workflow]
        if existing:
            record = {"workflow": workflow, "action": "preserved_existing_exact_head_runs",
                      "runs": [{key: run.get(key) for key in ("id", "status", "conclusion", "run_attempt")} for run in existing]}
        elif previous:
            raise RuntimeError("Prior request exists but no visible run; investigate without redispatch: " + workflow)
        else:
            record = {"workflow": workflow, "action": "dispatch_requested",
                      "requested_at_utc": datetime.now(timezone.utc).isoformat()}
            try:
                record["response"] = api("/actions/workflows/" + workflow + "/dispatches", {"ref": REF})
            except Exception as error:
                record["action"] = "dispatch_response_uncertain"
                record["error_type"] = type(error).__name__
                records.append(record)
                persist(records, "uncertain-dispatch")
                raise RuntimeError("Inspect existing runs before any retry: " + workflow) from None
        records.append(record)
        persist(records, workflow.removesuffix(".yml"))
        print(json.dumps(record), flush=True)


if __name__ == "__main__":
    main()
