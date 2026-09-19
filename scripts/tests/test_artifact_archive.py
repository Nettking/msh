import base64
import concurrent.futures
import copy
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import artifact_archive as archive
from scripts.artifact_archive_ci import trusted_event


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        # Files are tiny fixtures. Test host capacity separately from archive
        # semantics; real transport smoke still uses the unmodified disk guard.
        disk = patch.object(
            archive.shutil, "disk_usage", return_value=SimpleNamespace(free=1024**4)
        )
        self.disk_usage = disk.start()
        self.addCleanup(disk.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.input = self.root / "input"
        self.input.mkdir()
        (self.input / "junit.xml").write_text('<testsuite tests="1"/>')
        (self.input / "nested").mkdir()
        (self.input / "nested/log.txt").write_text("original failure preserved\n")
        self.metadata = {
            "repo": "Nettking/msh",
            "tested_sha": "a" * 40,
            "run_id": "unit-test",
            "run_attempt": 1,
            "job": "tests",
            "matrix": {"os": "Linux"},
            "artifact": "results",
            "workflow_ref": "local-unittest",
            "workflow_sha": None,
            "provenance_kind": "local-unit-test",
        }

    def package(self, name="package"):
        return archive.build_package(self.metadata, [str(self.input)], self.root / name)

    def test_local_capacity_refusal_retains_originals(self):
        self.disk_usage.return_value = SimpleNamespace(free=64 * 1024**3)
        with self.assertRaisesRegex(OSError, "free_bytes=.*required_bytes="):
            self.package()
        self.assertTrue((self.input / "junit.xml").is_file())
        self.assertFalse((self.root / "package/bundle.zip").exists())

    def test_streamed_fetch_verifies_without_a_second_zip_copy(self):
        package = self.package()
        manifest = json.loads((package / "manifest.json").read_bytes())
        payload = (package / "bundle.zip").read_bytes()
        receipt = {
            "reference": archive.reference(manifest),
            "manifest_sha256": archive.sha_file(package / "manifest.json"),
            "zip_sha256": archive.sha_file(package / "bundle.zip"),
            "zip_size": len(payload),
            "complete": True,
        }
        header = archive.canonical({"receipt": receipt, "manifest": manifest})
        for label, data, error in (
            ("good", payload, None),
            ("short", payload[:-1], "Truncated"),
            ("long", payload + b"x", "exceeds declared"),
            ("corrupt", b"x" + payload[1:], "checksum mismatch"),
        ):
            with self.subTest(label=label):
                wire = base64.b64encode(header + data).decode()
                command = [
                    sys.executable,
                    "-B",
                    "-c",
                    (
                        "import sys,base64;sys.stdin.buffer.read();"
                        f"sys.stdout.buffer.write(base64.b64decode({wire!r}))"
                    ),
                ]
                output = self.root / label
                with patch.object(archive, "ssh_command", return_value=command):
                    if error:
                        with self.assertRaisesRegex(ValueError, error):
                            archive.fetch({}, receipt, output)
                        self.assertFalse((output / "receipt.json").exists())
                    else:
                        self.assertEqual(archive.fetch({}, receipt, output), manifest)
                        self.assertEqual((output / "bundle.zip").read_bytes(), payload)
                        archive.extract(output, self.root / "fetched-files")
                self.assertFalse((output / "download.wire").exists())
                self.assertTrue((package / "bundle.zip").is_file())

    def test_round_trip_structure_and_checksums(self):
        package = self.package()
        archive.extract(package, self.root / "restored")
        for path in self.input.rglob("*"):
            if path.is_file():
                self.assertEqual(
                    path.read_bytes(),
                    (
                        self.root / "restored" / path.relative_to(self.input)
                    ).read_bytes(),
                )

    def test_corruption_is_rejected_before_extraction(self):
        package = self.package()
        manifest = json.loads((package / "manifest.json").read_text())
        manifest["files"][0]["sha256"] = "0" * 64
        (package / "manifest.json").write_bytes(archive.canonical(manifest))
        with self.assertRaisesRegex(ValueError, "checksum"):
            archive.extract(package, self.root / "restore")
        self.assertFalse((self.root / "restore").exists())

    def test_extraction_does_not_overwrite(self):
        package = self.package()
        archive.extract(package, self.root / "restored")
        with self.assertRaises(FileExistsError):
            archive.extract(package, self.root / "restored")

    def test_missing_files_are_not_success(self):
        with self.assertRaises(FileNotFoundError):
            archive.build_package(
                self.metadata, [str(self.root / "absent")], self.root / "absent-package"
            )

    def test_zip_traversal_and_windows_aliases(self):
        for name in (
            "../outside",
            "/absolute",
            "C:/file",
            "a/../b",
            "a\\b",
            "a//b",
            "CON",
            "x/NUL.txt",
            "x/file.",
            "x/file ",
        ):
            with self.subTest(name=name), self.assertRaises(ValueError):
                archive.safe_name(name)

    def test_identity_paths_cannot_escape(self):
        for field, value in (
            ("repo", "other/repo"),
            ("tested_sha", "main"),
            ("run_id", "../other"),
            ("artifact", ".."),
            ("run_attempt", True),
        ):
            metadata = dict(self.metadata, schema=archive.SCHEMA)
            metadata[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                archive.reference(metadata)

    def test_ssh_requires_pinned_host_and_batch_auth(self):
        cmd = archive.ssh_command(
            {"host": "archive@nitro", "identity_file": "key", "known_hosts": "pinned"}
        )
        self.assertIn("StrictHostKeyChecking=yes", cmd)
        self.assertIn("IdentitiesOnly=yes", cmd)
        self.assertIn("BatchMode=yes", cmd)
        self.assertNotIn("accept-new", " ".join(cmd))
        with self.assertRaises(ValueError):
            archive.ssh_command(
                {
                    "host": "-oProxyCommand=bad",
                    "identity_file": "key",
                    "known_hosts": "pinned",
                }
            )

    def test_fork_and_target_events_are_refused(self):
        env = {"GITHUB_REPOSITORY": "Nettking/msh", "GITHUB_EVENT_NAME": "pull_request"}
        with self.assertRaises(ValueError):
            trusted_event(
                env, {"pull_request": {"head": {"repo": {"full_name": "fork/msh"}}}}
            )
        trusted_event(
            env, {"pull_request": {"head": {"repo": {"full_name": "Nettking/msh"}}}}
        )
        with self.assertRaises(ValueError):
            trusted_event(dict(env, GITHUB_EVENT_NAME="pull_request_target"), {})

    def server(self, header, payload=b"", reserve=0):
        server_root = self.root / "server"
        server_root.mkdir(exist_ok=True)
        command = [
            sys.executable,
            "-B",
            "-c",
            "from scripts.artifact_archive import serve; import sys; serve(sys.argv[1], int(sys.argv[2]))",
            str(server_root),
            str(reserve),
        ]
        return subprocess.run(
            command,
            input=archive.canonical(header) + payload,
            capture_output=True,
            timeout=20,
            check=False,
        )

    @unittest.skipIf(
        os.name == "nt",
        "Server requires Linux flock and posix_fallocate; real SSH smoke covers Windows client",
    )
    def test_parallel_publish_idempotent_and_immutable(self):
        package = self.package()
        header = {
            "operation": "put",
            "manifest": json.loads((package / "manifest.json").read_bytes()),
            "zip_size": (package / "bundle.zip").stat().st_size,
            "zip_sha256": archive.sha_file(package / "bundle.zip"),
        }
        payload = (package / "bundle.zip").read_bytes()
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            responses = list(pool.map(lambda _: self.server(header, payload), range(2)))
        for response in responses:
            self.assertEqual(response.returncode, 0, response.stderr)
        self.assertEqual(responses[0].stdout, responses[1].stdout)
        changed = copy.deepcopy(header)
        changed["manifest"]["archived_at"] = "2026-01-01T00:00:00+00:00"
        conflict = self.server(changed, payload)
        self.assertNotEqual(conflict.returncode, 0)
        self.assertIn(b"Immutable reference", conflict.stderr)
        receipt = json.loads(responses[0].stdout)
        fetched = self.server({"operation": "get", "reference": receipt["reference"]})
        self.assertEqual(fetched.returncode, 0, fetched.stderr)
        stream = io.BytesIO(fetched.stdout)
        self.assertEqual(archive.read_header(stream)["receipt"], receipt)
        self.assertEqual(stream.read(), payload)

    @unittest.skipIf(os.name == "nt", "Linux receiver test")
    def test_truncation_and_full_disk_never_publish(self):
        package = self.package()
        header = {
            "operation": "put",
            "manifest": json.loads((package / "manifest.json").read_bytes()),
            "zip_size": (package / "bundle.zip").stat().st_size,
            "zip_sha256": archive.sha_file(package / "bundle.zip"),
        }
        truncated = self.server(header, b"bad")
        self.assertNotEqual(truncated.returncode, 0)
        self.assertFalse(list((self.root / "server").rglob("COMPLETE.json")))
        full = self.server(header, (package / "bundle.zip").read_bytes(), reserve=2**63)
        self.assertNotEqual(full.returncode, 0)
        self.assertIn(b"free-space reserve", full.stderr)
        self.assertTrue((package / "bundle.zip").exists())
        self.assertFalse(list((self.root / "server").rglob("COMPLETE.json")))


if __name__ == "__main__":
    unittest.main()
