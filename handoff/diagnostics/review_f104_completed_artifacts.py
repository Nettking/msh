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

ROOT = Path(__file__).resolve().parent / "f104-final-qualification"
REPO = "C:/wsl/fcp-fix-d20-icse-failure-evidence-20260913"
SOURCE = "f104038a2b77705adaa547cdd1df7d895bf80a41"


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
assert sha(raw) == "46a22abb11f4cb71b961dde0adf742f2bac2445d93681dfea39a3d8bc62c3e34"
with zipfile.ZipFile(io.BytesIO(raw)) as z:
    outer = files(z)
manifest = json.loads(outer["artifact-manifest.json"])
assert manifest["source_revision"] == SOURCE
assert manifest["source_ref"] == "refs/pull/483/merge"
assert manifest["workflow_run"] == "34746641262"
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
for artifact_id, digest in [(10315499057, "465621d6e0856d4f2d85cdd5382f43109b11660ff4cb21d8bb40951ceffe1b4a"), (10314899213, "d419750a6aef54f2f8932017545087e4a474bb9a18affec7070b020e2cfb9421")]:
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
    assert len(logs) == 15
    with zipfile.ZipFile(ROOT / "native-job-logs.zip", "w", compression=zipfile.ZIP_DEFLATED) as z:
        for path in logs:
            z.writestr(path.name, path.read_bytes())
with zipfile.ZipFile(ROOT / "native-job-logs.zip") as z:
    native_logs = files(z)
    assert len(native_logs) == 15
    assert all(native_logs[p.name] == p.read_bytes() for p in logs)
report = {"at": datetime.now(timezone.utc).isoformat(), "source": SOURCE,
          "publication_artifact": 10315473711, "publication_archive_sha256": sha(raw),
          "export_matches_exact_git_source": True, "exported_source_files": len(public_source),
          "component_checks": {"Linux": "4/4 PASS", "Windows": "4/4 PASS", "compose": "4/4 PASS retained from attempt1"},
          "network_checks": {"Linux": "10/10 PASS", "Windows": "10/10 PASS"},
          "owned_teardown": "PASS both OS", "public_privacy": "PASS", "cf8_junit": cf8,
          "new_native_checkout_proofs": 15, "native_logs_sha256": sha((ROOT / "native-job-logs.zip").read_bytes()),
          "physical_acceptance": "NOT_EVALUATED", "protected_recorder_data": "NOT_ACCESSED_OR_CHANGED"}
(ROOT / "artifact-review.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
print(json.dumps(report))
