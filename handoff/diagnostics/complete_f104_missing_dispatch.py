"""Reconcile the one HTTP502 submission, then submit only the unstarted gates."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, "C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance")
from github_qualification import client

path = Path(__file__).with_name("f104-missing-qualification-dispatch.json")
state = json.loads(path.read_text(encoding="utf-8"))
api = client()
source, ref = state["source"], state["ref"]
assert api("/git/ref/heads/" + ref)["object"]["sha"] == source


def save():
    path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


for workflow in ("ci-test-sharding.yml", "cfi2-onboarding-composition.yml", "release-image-metadata.yml"):
    rows = api(f"/actions/workflows/{workflow}/runs?branch={ref}&per_page=100")["workflow_runs"]
    rows = [r for r in rows if r["head_sha"] == source]
    action = next((r for r in state["actions"] if r["workflow"] == workflow), None)
    if action is None:
        action = {"workflow": workflow, "action": "workflow_dispatch"}
        state["actions"].append(action)
    action["reconciled_at"] = datetime.now(timezone.utc).isoformat()
    action["existing_run_ids"] = [r["id"] for r in rows]
    if rows:
        action["status"] = "existing exact-source run; no dispatch repeated"
        save()
        print(json.dumps(action), flush=True)
        continue
    assert action.get("status") != "accepted", "Accepted action without visible run; wait, do not repeat"
    if workflow == "ci-test-sharding.yml":
        assert not action.get("bounded_resubmission"), "Only one recovery from the HTTP502 is allowed"
        action["previous_submission"] = "HTTP502; absent in subsequent repository branch listing and this workflow-specific exact-source listing"
        action["bounded_resubmission"] = True
    action["status"] = "submission uncertain until response/live run recorded"
    save()
    try:
        action["response"] = api(f"/actions/workflows/{workflow}/dispatches", {"ref": ref})
        action["status"] = "accepted"
    finally:
        save()
    print(json.dumps(action), flush=True)
