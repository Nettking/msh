import base64
import concurrent.futures
import copy
import io
import json
import os
import runpy
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import artifact_archive as archive
from scripts.artifact_archive_ci import (
    native_job_identity,
    preserve_failed_inputs,
    trusted_event,
)


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
        with (
            patch.dict(os.environ, {"RUNNER_NAME": "Nettking"}),
            self.assertRaisesRegex(OSError, "free_bytes=.*required_bytes="),
        ):
            self.package()
        self.assertTrue((self.input / "junit.xml").is_file())
        self.assertFalse((self.root / "package/bundle.zip").exists())

    def test_ci_capacity_includes_job_growth_and_rejects_twelve_gib(self):
        env = {
            "GITHUB_ACTIONS": "true",
            "GITHUB_REPOSITORY": archive.REPO,
            "RUNNER_NAME": "Beast",
        }
        with patch.dict(os.environ, env):
            self.disk_usage.return_value = SimpleNamespace(free=13 * 1024**3)
            rows = archive.require_local_space(self.root, 8 * 1024**2 + 39)
            self.assertTrue(all(r["reserve_bytes"] == 12 * 1024**3 for r in rows))
            self.assertTrue(all(r["role"] == "ci-only" for r in rows))
            for free, extra in ((12 * 1024**3, 0), (13 * 1024**3, 1024**3)):
                self.disk_usage.return_value = SimpleNamespace(free=free)
                with self.assertRaises(OSError):
                    archive.require_local_space(self.root, extra)
        self.disk_usage.return_value = SimpleNamespace(free=13 * 1024**3)
        for runner in ("Beast", "Beast-Linux-WSL", "Nettking", "Nettking-Linux"):
            self.disk_usage.return_value = SimpleNamespace(free=13 * 1024**3)
            with patch.dict(
                os.environ,
                {
                    "RUNNER_NAME": runner,
                    "GITHUB_ACTIONS": "true",
                    "GITHUB_REPOSITORY": archive.REPO,
                },
            ):
                rows = archive.require_local_space(self.root, 1)
                self.assertTrue(all(r["reserve_bytes"] == 12 * 1024**3 for r in rows))
                self.assertTrue(all(r["role"] == "ci-only" for r in rows))
        for runner, actions, repo in (
            ("Nettking", "false", archive.REPO),
            ("Beast", "false", archive.REPO),
            ("Beast", "true", "fork/example"),
        ):
            self.disk_usage.return_value = SimpleNamespace(free=13 * 1024**3)
            with (
                patch.dict(
                    os.environ,
                    {
                        "RUNNER_NAME": runner,
                        "GITHUB_ACTIONS": actions,
                        "GITHUB_REPOSITORY": repo,
                    },
                ),
                self.assertRaises(OSError),
            ):
                archive.require_local_space(self.root, 1)

    def test_native_identity_waits_for_assignment_without_guessing(self):
        env = {
            "GITHUB_RUN_ID": "123",
            "GITHUB_RUN_ATTEMPT": "2",
            "RUNNER_NAME": "Beast-Linux-WSL",
            "ARCHIVE_GITHUB_TOKEN": "test-only",
            "RUNNER_TEMP": str(self.root),
        }
        job = {
            "id": 456,
            "run_id": 123,
            "run_attempt": 2,
            "status": "in_progress",
            "runner_id": 29,
            "runner_name": env["RUNNER_NAME"],
            "started_at": "2026-09-19T00:00:00Z",
        }
        pending = dict(job, status="queued", runner_name="")
        replies = [
            io.BytesIO(json.dumps({"total_count": 1, "jobs": [j]}).encode())
            for j in (pending, job)
        ]
        with (
            patch(
                "scripts.artifact_archive_ci.urllib.request.urlopen",
                side_effect=replies,
            ) as request,
            patch("scripts.artifact_archive_ci.time.sleep"),
        ):
            actual = native_job_identity(env)
        self.assertEqual(actual["id"], 456)
        self.assertEqual([x["matches"] for x in actual["binding_observations"]], [0, 1])
        self.assertIn("/attempts/2/jobs?", request.call_args.args[0].full_url)
        # Even a valid-looking record from another attempt/run is not eligible.
        wrong = dict(job, run_id=124, run_attempt=1)
        clock = [0.0]

        def tick(seconds):
            clock[0] += seconds

        def response(*args, **kwargs):
            return io.BytesIO(json.dumps({"total_count": 1, "jobs": [wrong]}).encode())

        with (
            patch(
                "scripts.artifact_archive_ci.urllib.request.urlopen",
                side_effect=response,
            ),
            patch(
                "scripts.artifact_archive_ci.time.monotonic",
                side_effect=lambda: clock[0],
            ),
            patch("scripts.artifact_archive_ci.time.sleep", side_effect=tick),
            self.assertRaisesRegex(ValueError, "missing or ambiguous"),
        ):
            native_job_identity(env)
        self.assertEqual(clock[0], 20)
        self.assertTrue(list(self.root.glob("fcp-native-job-binding-*.json")))
        ambiguous = io.BytesIO(
            json.dumps({"total_count": 2, "jobs": [job, dict(job, id=457)]}).encode()
        )
        with (
            patch(
                "scripts.artifact_archive_ci.urllib.request.urlopen",
                return_value=ambiguous,
            ),
            patch("scripts.artifact_archive_ci.time.sleep") as sleep,
            self.assertRaisesRegex(ValueError, "missing or ambiguous"),
        ):
            native_job_identity(env)
        sleep.assert_not_called()

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

    def test_failed_upload_survives_runner_temp_cleanup_without_secret_copy(self):
        job_temp = self.root / "runner-temp"
        job_temp.mkdir()
        source = job_temp / "junit.xml"
        original = b'<testsuite failures="1"/>'
        source.write_bytes(original)
        event = job_temp / "event.json"
        event.write_text("{}")
        env = {
            "GITHUB_ACTIONS": "true",
            "GITHUB_REPOSITORY": archive.REPO,
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_RUN_ID": "123",
            "GITHUB_RUN_ATTEMPT": "1",
            "GITHUB_JOB": "failed-test",
            "GITHUB_STEP_SUMMARY": str(job_temp / "summary.md"),
            "RUNNER_NAME": "Beast",
            "RUNNER_TEMP": str(job_temp),
            "ARCHIVE_OPERATION": "upload",
            "ARCHIVE_NAME": "failure-evidence",
            "ARCHIVE_PATH": str(source),
            "ARCHIVE_MATRIX": "null",
            "ARCHIVE_JOB_STATUS": "failure",
            "ARCHIVE_GITHUB_TOKEN": "native-api-fixture",
            "GITHUB_WORKFLOW_REF": "Nettking/msh/.github/workflows/test.yml@refs/heads/test",
            "GITHUB_WORKFLOW_SHA": "a" * 40,
            "GITHUB_SHA": "a" * 40,
            "FCP_ARCHIVE_SSH_KEY": "secret-fixture-never-copy",
            "FCP_ARCHIVE_HOST": "fcp-archive@nitro.invalid",
            "FCP_ARCHIVE_KNOWN_HOSTS": "not-a-real-host-key",
        }
        native = {
            "total_count": 1,
            "jobs": [
                {
                    "id": 456,
                    "run_id": 123,
                    "run_attempt": 1,
                    "status": "in_progress",
                    "runner_name": "Beast",
                }
            ],
        }
        if os.name == "nt":
            # Match the native PowerShell environment of the self-hosted action;
            # do not inherit PowerShell 7 module paths from the test launcher.
            env["PSMODULEPATH"] = str(
                Path(os.environ["SYSTEMROOT"])
                / "System32/WindowsPowerShell/v1.0/Modules"
            )
        original_output = subprocess.check_output

        def fixture_checkout(command, *args, **kwargs):
            if command == ["git", "rev-parse", "HEAD"]:
                return "a" * 40 + "\n"
            return original_output(command, *args, **kwargs)

        with (
            patch.dict(os.environ, env),
            patch("subprocess.check_output", side_effect=fixture_checkout),
            patch(
                "urllib.request.urlopen",
                return_value=io.BytesIO(json.dumps(native).encode()),
            ),
            patch.object(
                archive, "upload", side_effect=OSError("archive unavailable fixture")
            ),
            self.assertRaises(SystemExit) as error,
        ):
            runpy.run_path(
                str(Path(archive.__file__).with_name("artifact_archive_ci.py")),
                run_name="__main__",
            )
        self.assertNotEqual(error.exception.code, 0)
        self.assertIn("archive unavailable fixture", str(error.exception))
        self.assertEqual(source.read_bytes(), original)
        # Simulate the runner's documented end-of-job cleanup, only in this fixture.
        self.assertTrue(job_temp.resolve().is_relative_to(self.root.resolve()))
        shutil.rmtree(job_temp)
        receipts = list((self.root / "fcp-archive-pending").rglob("INCOMPLETE.json"))
        self.assertEqual(len(receipts), 1)
        receipt = json.loads(receipts[0].read_bytes())
        self.assertEqual(receipt["run_id"], "123")
        self.assertFalse(receipt["archive_complete"])
        self.assertEqual(len(receipt["files"]), 1)
        row = receipt["files"][0]
        saved = receipts[0].parent / "files" / row["path"]
        self.assertEqual(saved.read_bytes(), original)
        self.assertEqual(archive.sha_file(saved), row["sha256"])
        for file in receipts[0].parent.rglob("*"):
            if file.is_file():
                self.assertNotIn(b"secret-fixture-never-copy", file.read_bytes())

    def test_pending_retention_keeps_capacity_floor_and_reports_missing_inputs(self):
        job_temp = self.root / "runner-temp"
        job_temp.mkdir()
        event = job_temp / "event.json"
        event.write_text("{}")
        env = {
            "GITHUB_ACTIONS": "true",
            "GITHUB_REPOSITORY": archive.REPO,
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_EVENT_PATH": str(event),
            "RUNNER_NAME": "Beast",
            "RUNNER_TEMP": str(job_temp),
            "ARCHIVE_PATH": str(self.input) + "\n" + str(self.root / "missing.xml"),
        }
        with patch.dict(os.environ, env):
            self.disk_usage.return_value = SimpleNamespace(free=12 * 1024**3)
            with self.assertRaises(OSError):
                preserve_failed_inputs(env)
            self.assertFalse((self.root / "fcp-archive-pending").exists())
            self.assertTrue((self.input / "junit.xml").is_file())
            self.disk_usage.return_value = SimpleNamespace(free=13 * 1024**3)
            pending = preserve_failed_inputs(env)
        receipt = json.loads((pending / "INCOMPLETE.json").read_bytes())
        self.assertEqual(len(receipt["files"]), 2)
        self.assertEqual(receipt["missing_patterns"], [str(self.root / "missing.xml")])
        self.assertFalse(receipt["archive_complete"])
        self.assertTrue(all(x["method"] == "hardlink" for x in receipt["files"]))

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

    def test_windows_uses_existing_ssh_when_service_path_omits_it(self):
        system = self.root / "Windows"
        native = system / "System32/OpenSSH/ssh.exe"
        git = self.root / "Git/cmd/git.exe"
        bundled = self.root / "Git/usr/bin/ssh.exe"
        for file in (native, git, bundled):
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_bytes(b"synthetic executable path fixture")
        with (
            patch.object(archive.sys, "platform", "win32"),
            patch.dict(os.environ, {"SystemRoot": str(system)}),
            patch.object(
                archive.shutil,
                "which",
                side_effect=lambda name: str(git) if name == "git" else None,
            ),
        ):
            self.assertTrue(Path(archive.ssh_executable()).samefile(native))
            native.unlink()  # Only this test's synthetic executable fixture.
            # NetworkService temp paths may use Windows 8.3 aliases. Require
            # the same physical executable, not identical path spellings.
            self.assertTrue(Path(archive.ssh_executable()).samefile(bundled))
            bundled.unlink()
            with self.assertRaisesRegex(
                FileNotFoundError, "OpenSSH client unavailable"
            ):
                archive.ssh_executable()

    def test_fork_and_target_events_are_refused(self):
        env = {"GITHUB_REPOSITORY": "Nettking/msh", "GITHUB_EVENT_NAME": "pull_request"}
        with self.assertRaises(ValueError):
            trusted_event(
                env, {"pull_request": {"head": {"repo": {"full_name": "fork/example"}}}}
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
