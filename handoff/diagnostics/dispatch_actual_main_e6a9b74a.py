"""Qualify the actual final merged main once; reuse every existing exact-source run."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, "C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance")
from github_qualification import REQUIRED, client

ROOT = Path(__file__).resolve().parent
SOURCE = "e6a9b74a1d555609eed6bf40c800e1258f1c9077"
GATES = {**REQUIRED, "cfi2-onboarding-composition.yml": 2, "release-image-metadata.yml": 1}
receipt = ROOT / "main-e6a9b74a-qualification-dispatch.json"
assert not receipt.exists(), "Inspect the existing receipt and runs before further dispatch"
api = client()
assert api("/git/ref/heads/main")["object"]["sha"] == SOURCE
merges = {}
for number, expected in [(475, "a039061e797dfde76d452f90c8888a7704e0c5b3"), (483, "02a90d3b9e26134a9d36a1ccb8bd660f54eed388"), (473, SOURCE)]:
    pr = api(f"/pulls/{number}")
    assert pr["merged"] and pr["merge_commit_sha"] == expected
    merges[number] = expected
runs = api(f"/actions/runs?head_sha={SOURCE}&per_page=100")["workflow_runs"]
existing = {r["path"].split("/")[-1]: r for r in runs if r["head_sha"] == SOURCE}
state = {"at": datetime.now(timezone.utc).isoformat(), "source": SOURCE, "ref": "main",
         "expected_tree": "cc9b29515d47d754f24199bf403213e7ff111315", "merges": merges,
         "scope": "37 required jobs plus3 companions on actual resulting main",
         "actions": [], "physical_acceptance": "NOT_EVALUATED", "deployed": False}


def save():
    receipt.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


save()
for workflow, count in GATES.items():
    if workflow in existing:
        run = existing[workflow]
        state["actions"].append({"workflow": workflow, "expected_jobs": count, "action": "reuse existing exact-source run", "run_id": run["id"], "status": run["status"]})
        save()
        continue
    assert api("/git/ref/heads/main")["object"]["sha"] == SOURCE
    action = {"workflow": workflow, "expected_jobs": count, "action": "workflow_dispatch", "status": "submission uncertain until response/live run retained"}
    state["actions"].append(action)
    save()
    try:
        action["response"] = api(f"/actions/workflows/{workflow}/dispatches", {"ref": "main"})
        action["status"] = "accepted"
    finally:
        save()
    print(json.dumps(action), flush=True)
print(json.dumps({"source": SOURCE, "total_scope": sum(GATES.values()), "actions": len(state["actions"])}), flush=True)
