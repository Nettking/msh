"""Verify retained current-source artifacts; never rerun tests or build a bundle."""
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parent / "main-e6a9b74a-qualification"
REPO = "C:/wsl/fcp-v1-e6a9b74a-main-20260913"
SOURCE = "e6a9b74a1d555609eed6bf40c800e1258f1c9077"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def files(z):
    entries = z.infolist()
    assert len(entries) < 10000 and sum(i.file_size for i in entries) < 200 * 1024 * 1024
    names = [i.filename for i in entries]
    assert len(names) == len(set(names)) == len({n.casefold() for n in names})
    for i in entries:
        p = PurePosixPath(i.filename)
        assert not p.is_absolute() and ".." not in p.parts and "\\" not in i.filename
        assert not any(":" in part for part in p.parts)
        assert (i.external_attr >> 16) & 0o170000 != 0o120000
    return {i.filename: z.read(i) for i in entries if not i.is_dir()}


raw = (ROOT / "publication-native.zip").read_bytes()
assert sha(raw) == "eeee8d7b50c5d19bfdee85d1a2b861ec31e0b9892d5d419ada1f0f10084ce72e"
with zipfile.ZipFile(io.BytesIO(raw)) as z:
    outer = files(z)
manifest = json.loads(outer["artifact-manifest.json"])
assert manifest["source_revision"] == SOURCE
assert manifest["source_ref"] == "refs/heads/main"
assert manifest["workflow_run"] == "34751934925"
for line in outer["SHA256SUMS"].decode().splitlines():
    digest, name = line.split("  ", 1)
    assert re.fullmatch("[0-9a-f]{64}", digest) and sha(outer[name]) == digest
publication = manifest["publication_archive"]["file"]
digest, name = outer["ZENODO_SHA256"].decode().strip().split("  ", 1)
assert name == publication and sha(outer[publication]) == digest
with zipfile.ZipFile(io.BytesIO(outer[publication])) as z:
    nested = files(z)
sp = manifest["publication_archive"]["source_prefix"]
ap = manifest["publication_archive"]["artifact_prefix"]
assert all(n.startswith(sp) or n.startswith(ap) for n in nested)
public_source = {n.removeprefix(sp): b for n, b in nested.items() if n.startswith(sp)}
public_metadata = {n.removeprefix(ap): b for n, b in nested.items() if n.startswith(ap)}
assert public_metadata == {n: b for n, b in outer.items() if n not in (publication, "ZENODO_SHA256")}
assert "example-data/2026-03-23.jsonl" not in public_source
assert not any(".git" in PurePosixPath(n).parts or "private-state" in PurePosixPath(n).parts for n in nested)
reference = subprocess.check_output(["git", "-c", "core.autocrlf=false", "-c", "core.eol=lf", "archive", "--format=tar", SOURCE], cwd=REPO)
with tarfile.open(fileobj=io.BytesIO(reference), mode="r:") as z:
    expected = {i.name: z.extractfile(i).read() for i in z.getmembers() if i.isfile()}
assert public_source == expected
assert {e["execution"] for e in manifest["evidence"]} == {"Linux", "Windows", "compose"}
for e in manifest["evidence"]:
    data = outer[e["file"]]
    result = json.loads(data)
    assert sha(data) == e["sha256"] and result["implementation_commit"] == SOURCE
    assert result["passed"] == result["total"] == 4
    assert all(s["result"] == "pass" for s in result["scenarios"])
assert {e["execution"] for e in manifest["network_evidence"]} == {"Linux", "Windows"}
for e in manifest["network_evidence"]:
    prefix = "network-evidence/" + e["execution"] + "/"
    assert {f["file"] for f in e["files"]} == {prefix + n for n in ("summary.json", "events.jsonl", "operator-report.html")}
    for f in e["files"]:
        data = outer[f["file"]]
        assert sha(data) == f["sha256"]
        assert not any(t in data for t in (b"-----BEGIN ", b"ghp_", b"github_pat_"))
        assert not re.search(rb'"(?:transport_secret|enrollment_token|invitation_token|private_key)"\s*:', data)
    result = json.loads(outer[prefix + "summary.json"])
    assert result["source_sha"] == SOURCE and result["result"] == "PASS"
    assert len(result["checks"]) == 10 and set(result["checks"].values()) == {"PASS"}
    assert result["all_owned_processes_stopped"] and not result.get("failure") and not result.get("shutdown_errors")
    assert [json.loads(l) for l in outer[prefix + "events.jsonl"].decode().splitlines() if l.strip()] == result["events"]
    assert outer[prefix + "operator-report.html"].strip()
cf8 = []
for artifact_id, digest in [(10315951948, "ffd135cfc08b8a2ecd4196eb38a5e87e5eb81a5c7585dacd98673f3e1a6abc4f"), (10316670910, "6efe5c00431bab9f9c166ca4979d131675535e00aad115105a81056a43c6c51d")]:
    data = (ROOT / f"cf8-{artifact_id}.zip").read_bytes()
    assert sha(data) == digest
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        entries = files(z)
    assert set(entries) == {"cf8-acceptance.xml"}
    xml = ET.fromstring(entries["cf8-acceptance.xml"])
    suites = list(xml.iter("testsuite"))
    assert sum(int(s.get("tests", "0")) for s in suites) == 12
    assert all(int(s.get("errors", "0")) == int(s.get("failures", "0")) == 0 for s in suites)
    cf8.append({"artifact_id": artifact_id, "tests": 12, "failures": 0, "errors": 0, "sha256": digest})
logs = sorted(ROOT.glob("native-job-*.log"))
if logs:
    assert len(logs) == 24
    with zipfile.ZipFile(ROOT / "native-job-logs.zip", "w", compression=zipfile.ZIP_DEFLATED) as z:
        for path in logs:
            z.writestr(path.name, path.read_bytes())
with zipfile.ZipFile(ROOT / "native-job-logs.zip") as z:
    native_logs = files(z)
    assert len(native_logs) == 24
    assert all(native_logs[p.name] == p.read_bytes() for p in logs)
report = {"at": datetime.now(timezone.utc).isoformat(), "source": SOURCE,
          "publication_artifact": 10316707220, "publication_archive_sha256": sha(raw),
          "export_matches_exact_git_source": True, "exported_source_files": len(public_source),
          "component_checks": {"Linux": "4/4 PASS", "Windows": "4/4 PASS", "compose": "4/4 PASS on actual main"},
          "network_checks": {"Linux": "10/10 PASS", "Windows": "10/10 PASS"},
          "owned_teardown": "PASS both OS", "public_privacy": "PASS", "cf8_junit": cf8,
          "new_native_checkout_proofs": 24, "native_logs_sha256": sha((ROOT / "native-job-logs.zip").read_bytes()),
          "physical_acceptance": "NOT_EVALUATED", "protected_recorder_data": "NOT_ACCESSED_OR_CHANGED"}
(ROOT / "artifact-review.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
print(json.dumps(report))
