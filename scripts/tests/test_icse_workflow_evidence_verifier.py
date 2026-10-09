import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "icse-tool-demo.yml"


class IcseWorkflowEvidenceVerifierTests(unittest.TestCase):
    def verifier_source(self):
        lines = WORKFLOW.read_text(encoding="utf-8").splitlines()
        marker = "          python3 - <<'PY'"
        start = lines.index(marker) + 1
        end = lines.index("          PY", start)
        return "\n".join(line.removeprefix("          ") for line in lines[start:end])

    def fixture(self, root, wrong_artifact=None):
        run_id = "fixture-run"
        attempt = "3"
        source_sha = "source-sha"
        workflow_ref = "Nettking/msh/.github/workflows/icse-tool-demo.yml@refs/heads/main"
        workflow_sha = "workflow-sha"
        rows = [
            ("downloaded-evidence", "icse-component", "reviewer-entrypoint", "Linux", "Linux", {"os": "Linux"}, ["icse-summary.json"], "icse-summary-Linux"),
            ("downloaded-evidence", "icse-component", "reviewer-entrypoint", "Windows", "Windows", {"os": "Windows"}, ["icse-summary.json"], "icse-summary-Windows"),
            ("downloaded-evidence", "icse-component", "reviewer-compose", "reviewer-compose", "Linux", {}, ["icse-summary.json"], "icse-summary-compose"),
            ("downloaded-network-evidence", "icse-network-public", "reviewer-entrypoint", "Linux", "Linux", {"os": "Linux"}, ["events.jsonl", "operator-report.html", "summary.json"], "icse-network-summary-Linux"),
            ("downloaded-network-evidence", "icse-network-public", "reviewer-entrypoint", "Windows", "Windows", {"os": "Windows"}, ["events.jsonl", "operator-report.html", "summary.json"], "icse-network-summary-Windows"),
        ]
        for parent, kind, job, matrix_os, expected_os, matrix, filenames, artifact_suffix in rows:
            prefix = "icse-summary" if kind == "icse-component" else "icse-network-summary"
            artifact = f"{prefix}-{run_id}-a{attempt}-{job}" + (f"-{matrix_os}" if matrix_os in {"Linux", "Windows"} else "")
            artifact_root = root / parent / artifact
            artifact_root.mkdir(parents=True)
            entries = []
            for filename in filenames:
                data = b"fixture evidence\n"
                (artifact_root / filename).write_bytes(data)
                entries.append({"path": filename, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()})
            runner_os = "Linux" if artifact == wrong_artifact else expected_os
            manifest = {
                "schema": "fcp.ci-evidence-manifest.v1",
                "kind": kind,
                "repository": "Nettking/msh",
                "source_sha": source_sha,
                "checkout_sha": source_sha,
                "event_sha": source_sha,
                "workflow": "ICSE tool demo",
                "workflow_ref": workflow_ref,
                "workflow_sha": workflow_sha,
                "run_id": run_id,
                "run_attempt": int(attempt),
                "job_key": job,
                "matrix": matrix,
                "runner_name": "fixture-runner",
                "runner_os": runner_os,
                "files": entries,
            }
            (artifact_root / "archive-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        return {
            "SOURCE_SHA": source_sha,
            "WORKFLOW_REF": workflow_ref,
            "WORKFLOW_SHA": workflow_sha,
            "RUN_ID": run_id,
            "RUN_ATTEMPT": attempt,
            "REPOSITORY": "Nettking/msh",
            "WORKFLOW": "ICSE tool demo",
        }

    def run_verifier(self, root, env):
        return subprocess.run(
            [sys.executable, "-c", self.verifier_source()],
            cwd=root,
            env={**os.environ, **env},
            capture_output=True,
            text=True,
            check=False,
        )

    def test_accepts_matching_platform_manifests_including_compose_linux(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            result = self.run_verifier(root, self.fixture(root))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((root / "downloaded-evidence" / "icse-summary-compose").is_dir())

    def test_rejects_manifest_whose_runner_os_disagrees_with_platform_artifact(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            wrong = "icse-summary-fixture-run-a3-reviewer-entrypoint-Windows"
            result = self.run_verifier(root, self.fixture(root, wrong_artifact=wrong))
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("runner OS mismatch", result.stderr)


if __name__ == "__main__":
    unittest.main()
