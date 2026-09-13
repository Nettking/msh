"""Dispatch only absent qualification gates, with durable uncertain-action receipts."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from urllib.error import HTTPError

sys.path.insert(0, "C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance")
from github_qualification import client

ROOT = Path(__file__).resolve().parent
SOURCE = "f104038a2b77705adaa547cdd1df7d895bf80a41"
REF = "codex/federation-v1-qualify-f104038a"
GATES = [
    "cf7-acceptance-harness.yml", "cf7c-physical-test-readiness.yml",
    "cf8-role-retirement.yml", "phase-f85-operator-federation-surface.yml",
    "ci-test-sharding.yml", "cfi2-onboarding-composition.yml",
    "release-image-metadata.yml",
]
EXPECTED = {
    475: "5e6f184311019b9982e8544a18f3dc02c1b16e98",
    483: "06b956787ae63215982d5afd7198dead366e05ea",
    473: "440123f6bc6dc358eef3d233236bc14f91af60e0",
}
receipt = ROOT / "f104-missing-qualification-dispatch.json"
assert not receipt.exists(), "Inspect the existing receipt/live runs; never repeat blindly"
api = client()
state = {"at": datetime.now(timezone.utc).isoformat(), "source": SOURCE,
         "ref": REF, "actions": [], "preserve_green_gates": True}


def save():
    receipt.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


for number, sha in EXPECTED.items():
    pr = api(f"/pulls/{number}")
    assert pr["head"]["sha"] == sha and pr["state"] == "open" and not pr["merged"]
    if number == 483:
        assert pr["base"]["sha"] == EXPECTED[475] and pr["merge_commit_sha"] == SOURCE
assert api("/git/ref/heads/main")["object"]["sha"] == "b7194820d8f1940ae60b8c9639e09b7f61e65c55"
state["guarded_heads"] = EXPECTED
existing = {}
for workflow in GATES:
    runs = api(f"/actions/workflows/{workflow}/runs?head_sha={SOURCE}&per_page=100")["workflow_runs"]
    existing[workflow] = [{k: r.get(k) for k in ("id", "head_sha", "status", "conclusion", "event")} for r in runs]
state["existing_exact_source_runs"] = existing
save()
try:
    ref = api("/git/ref/heads/" + REF)
    assert ref["object"]["sha"] == SOURCE
    state["branch"] = "already present at exact source"
except HTTPError as error:
    if error.code != 404:
        raise
    state["branch"] = "creation pending; inspect live state if interrupted"
    save()
    api("/git/refs", {"ref": "refs/heads/" + REF, "sha": SOURCE})
    state["branch"] = "created at exact source"
save()
for workflow in GATES:
    if existing[workflow]:
        state["actions"].append({"workflow": workflow, "action": "reuse existing; inspect its result"})
        save()
        continue
    assert api("/git/ref/heads/" + REF)["object"]["sha"] == SOURCE
    action = {"workflow": workflow, "action": "workflow_dispatch", "status": "submission uncertain until response/live run recorded"}
    state["actions"].append(action)
    save()
    try:
        action["response"] = api(f"/actions/workflows/{workflow}/dispatches", {"ref": REF})
        action["status"] = "accepted"
    finally:
        save()
    print(json.dumps(action), flush=True)
print(json.dumps({"receipt": str(receipt), "source": SOURCE, "ref": REF}), flush=True)
