"""Review the completed single capture; never execute candidate code or retry CI."""
import hashlib
import json
from pathlib import Path
import re
import zipfile

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "icse-trace-34749055328"
SOURCE = "f104038a2b77705adaa547cdd1df7d895bf80a41"


def norm(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def installed(log):
    packages = {}
    for line in log.splitlines():
        if "Successfully installed " in line:
            for item in line.split("Successfully installed ", 1)[1].split():
                match = re.fullmatch(r"(.+)-(\d[^ ]*)", item)
                if match:
                    packages[norm(match[1])] = match[2]
    return packages


archive = OUT / "native-artifact.zip"
assert hashlib.sha256(archive.read_bytes()).hexdigest() == "b90089876397479a938b5821206cc8f744f9ee2e4b76d7bfe16a700043782a49"
with zipfile.ZipFile(archive) as z:
    assert set(z.namelist()) == {"capture_d18_worker_frames.py", "diagnostic.json", "summary.json", "events.jsonl", "operator-report.html"}
    receipt = json.loads(z.read("diagnostic.json"))
    summary = json.loads(z.read("summary.json"))
assert receipt["source_sha"] == SOURCE and receipt["source_still_clean"]
assert receipt["exit_code"] == 0 and receipt["runner"] == "Beast-Linux-WSL"
assert receipt["python"] == "3.12.13"
assert len(summary["checks"]) == 10 and set(summary["checks"].values()) == {"PASS"}
assert summary["all_owned_processes_stopped"]
assert receipt["driver"] == {"exists": False}
with zipfile.ZipFile(ROOT / "D18-stacked-explicit-native-evidence.zip") as z:
    original = z.read("job-logs/103695663389.log").decode()
before = installed(original)
after = {norm(p["name"]): p["version"] for p in receipt["packages"]}
assert before, "original installation evidence unavailable"
differences = {k: {"original": before.get(k), "capture": after.get(k)}
               for k in sorted(before.keys() | after.keys()) if before.get(k) != after.get(k)}
result = {
    "source_sha": SOURCE,
    "run_id": 34749055328,
    "job_id": 103702121271,
    "artifact_id": 10315421432,
    "artifact_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
    "mode": receipt["mode"],
    "command_exit_code": 0,
    "network_checks": summary["checks"],
    "source_still_clean": True,
    "all_owned_processes_stopped": True,
    "original_linux_job": 103695663389,
    "original_packages_recovered": len(before),
    "captured_packages": len(after),
    "package_differences": differences,
    "dependency_scope": "Original ICSE install commands, same Python and runner; package comparison uses original native installation log, not old differently constrained diagnostic.",
    "captured_frames": receipt["workers"],
    "frame_interpretation": {
        "voter-b": "worker.py:102 enrollment_token -> control_plane_product.py:669 -> require_quorum_leader:319. Expected final minority-enrollment refusal; bootstrap and successor connect passed.",
        "reviewer": "worker.py:136 announce -> client.py:592 -> request:540. Expected rejected forged capability ownership; discovery/owner-authorization check passed.",
    },
    "windows": {
        "classification": "unresolved but non-demonstrated candidate defect",
        "known_path": "voter-a bootstrap -> worker.py:90 bootstrap_new_federation; QuorumUnavailable. Original public evidence lacks the raising frame. This Linux-only capture cannot reproduce or exclude a Windows-specific cause.",
        "release_decision": "No demonstrated violated authority/correctness contract; preserve the failure. Windows ICSE qualification remains required, with unchanged assertions and deadlines.",
    },
    "linux": {
        "classification": "unresolved but non-demonstrated candidate defect",
        "known_path": "voter-b successor connect -> worker.py:118 RelayNodeClient.connect, following disconnect. Original TimeoutError lacks raising frame; handshake/auth/status/replay boundary cannot be selected from its type. Ready successor term2/index11 was observed before the failure.",
        "release_decision": "Same-source same-runner single capture passes reconnect/delivery. No product defect or deterministic harness mechanism demonstrated; infrastructure/transient cause remains a hypothesis, not an established classification.",
    },
    "relationship": "Distinct operations and phases. Neither a shared mechanism nor independent root causes are established. The successful capture supplies no failure stack for either original symptom.",
    "next_action": "One targeted failed-job recovery of original ICSE run34746641262 on unchanged f104; retain successful compose and all other green gates. Do not rerun this diagnostic or patch product code speculatively.",
    "physical_acceptance": "NOT_EVALUATED; P07/P12 not started",
    "protected_recorder_data": "NOT_ACCESSED_OR_CHANGED",
}
(OUT / "review.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
print(json.dumps({"checks": 10, "source": SOURCE, "original_packages": len(before), "capture_packages": len(after), "package_differences": differences, "classification": "unresolved but non-demonstrated candidate defect, both profiles"}))
