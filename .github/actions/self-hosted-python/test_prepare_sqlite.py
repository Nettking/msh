"""Tests for source integrity, bounded writes and fresh-process library qualification."""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import os
import platform
import sys
import tempfile
import unittest
import warnings
import zipfile
from pathlib import Path
from unittest import mock

SPEC = importlib.util.spec_from_file_location(
    "prepare_ci_sqlite", Path(__file__).with_name("prepare_sqlite.py")
)
assert SPEC is not None and SPEC.loader is not None
SQLITE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SQLITE)


class SQLitePreparationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="ci-sqlite-test-")
        self.addCleanup(self.temporary.cleanup)
        self.parent = Path(self.temporary.name).resolve()
        self.root = self.parent / "fcp-job-test"
        self.root.mkdir()
        self.environment = {
            "RUNNER_TEMP": str(self.parent), "FCP_CI_JOB_ROOT": str(self.root),
            "GITHUB_ENV": str(self.parent / "github-env"),
        }
        self.environment_file = Path(self.environment["GITHUB_ENV"])
        self.environment_file.touch()

    def test_rejects_hash_before_writing_any_source(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "SHA3-256"):
            SQLITE.extract_source(b"unverified archive", self.root)
        self.assertEqual(list(self.root.iterdir()), [])

    def archive(self, *, duplicate: bool = False) -> bytes:
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            for name in ("sqlite3.c", "sqlite3.h", "sqlite3ext.h"):
                archive.writestr(f"{SQLITE.SQLITE_ARCHIVE}/{name}", name)
            archive.writestr("../../escaped", "must not be extracted")
            archive.writestr("/absolute-output", "must not be extracted")
            if duplicate:
                with warnings.catch_warnings(action="ignore", category=UserWarning):
                    archive.writestr(f"{SQLITE.SQLITE_ARCHIVE}/sqlite3.c", "duplicate")
        return output.getvalue()

    def test_extracts_only_fixed_source_names_without_archive_path_writes(self) -> None:
        payload = self.archive()
        with mock.patch.object(SQLITE, "SQLITE_SHA3", hashlib.sha3_256(payload).hexdigest()):
            SQLITE.extract_source(payload, self.root)
        self.assertEqual({path.name for path in self.root.iterdir()},
                         {"sqlite3.c", "sqlite3.h", "sqlite3ext.h"})
        self.assertFalse((self.parent / "escaped").exists())
        self.assertEqual((self.root / "sqlite3.c").read_text(), "sqlite3.c")

    def test_rejects_duplicate_required_source_member(self) -> None:
        payload = self.archive(duplicate=True)
        with mock.patch.object(SQLITE, "SQLITE_SHA3", hashlib.sha3_256(payload).hexdigest()):
            with self.assertRaisesRegex(RuntimeError, "Missing or duplicate"):
                SQLITE.extract_source(payload, self.root)

    def test_build_root_must_be_the_unique_direct_child(self) -> None:
        self.assertEqual(SQLITE.job_root(self.environment), self.root)
        for invalid in (str(self.parent), str(self.parent / ".."), "fcp-job-relative"):
            with self.subTest(path=invalid), self.assertRaises(RuntimeError):
                SQLITE.job_root({**self.environment, "FCP_CI_JOB_ROOT": invalid})

    def test_rejects_symlink_build_root(self) -> None:
        alias = self.parent / "fcp-job-alias"
        try:
            alias.symlink_to(self.root, target_is_directory=True)
        except OSError as error:
            self.skipTest(f"Creating directory symlinks is unavailable: {error}")
        with self.assertRaises(RuntimeError):
            SQLITE.job_root({**self.environment, "FCP_CI_JOB_ROOT": str(alias)})

    def test_library_prefix_preserves_inherited_paths_and_preload(self) -> None:
        library = self.root / "libsqlite3.so.0"
        for inherited in (None, "", "/trusted:/other", ":/trusted::"):
            original = {"LD_PRELOAD": "/security/preload.so", "OTHER": "keep"}
            if inherited is not None:
                original["LD_LIBRARY_PATH"] = inherited
            snapshot = original.copy()
            selected = SQLITE.library_environment(library, original)
            expected = str(self.root) + (f":{inherited}" if inherited else "")
            self.assertEqual(selected["LD_LIBRARY_PATH"], expected)
            self.assertEqual(selected["LD_PRELOAD"], original["LD_PRELOAD"])
            self.assertEqual(selected["OTHER"], "keep")
            self.assertEqual(original, snapshot)
        with self.assertRaises(RuntimeError):
            SQLITE.library_environment(library, {"LD_LIBRARY_PATH": "/path\nOTHER=injected"})

    def test_missing_compiler_fails_without_downloading_or_installing(self) -> None:
        with (mock.patch.object(SQLITE.shutil, "which", return_value=None),
              mock.patch.object(SQLITE.urllib.request, "urlopen") as download):
            with self.assertRaisesRegex(RuntimeError, "preinstalled C compiler"):
                SQLITE.build_library(self.root)
        download.assert_not_called()
        self.assertEqual(list(self.root.iterdir()), [])

    def test_existing_build_directory_cannot_be_reused_or_overwritten(self) -> None:
        existing = self.root / f"sqlite-{SQLITE.SQLITE_VERSION}"
        existing.mkdir()
        evidence = existing / "keep"
        evidence.write_text("retain")
        with (mock.patch.object(SQLITE.shutil, "which", return_value="/existing/cc"),
              mock.patch.object(SQLITE.urllib.request, "urlopen") as download):
            with self.assertRaises(FileExistsError):
                SQLITE.build_library(self.root)
        download.assert_not_called()
        self.assertEqual(evidence.read_text(), "retain")

    def test_real_probe_uses_a_fresh_process_and_tests_json_index(self) -> None:
        result = SQLITE.probe(dict(os.environ))
        self.assertNotEqual(result["pid"], os.getpid())
        self.assertEqual(result["python"], platform.python_version())
        self.assertEqual(result["executable"], sys.executable)
        self.assertIn("sqlite", result)
        self.assertTrue(result["supported"], result)

    def run_preparation(self, before: dict, after: dict | None = None,
                        *, probe_only: bool = False) -> tuple[mock.Mock, mock.Mock]:
        library = self.root / "sqlite-3.54.0" / "libsqlite3.so.0"
        results = [before] if after is None else [before, after]
        with (mock.patch.dict(os.environ, self.environment),
              mock.patch.object(SQLITE, "probe", side_effect=results) as probe,
              mock.patch.object(SQLITE, "build_library", return_value=library) as build,
              mock.patch.object(SQLITE.sys, "platform", "linux"),
              contextlib.redirect_stdout(io.StringIO())):
            SQLITE.prepare(probe_only=probe_only)
        return probe, build

    def test_capable_runtime_has_no_download_build_or_environment_change(self) -> None:
        probe, build = self.run_preparation({"python": platform.python_version(), "supported": True})
        self.assertEqual(probe.call_count, 1)
        build.assert_not_called()
        self.assertEqual(self.environment_file.read_text(), "")

    def test_probe_only_fails_without_build_or_environment_export(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "lacks the required"):
            self.run_preparation({"python": platform.python_version(), "supported": False},
                                 probe_only=True)
        self.assertEqual(self.environment_file.read_text(), "")

    def test_repair_requires_fresh_child_to_select_qualified_library(self) -> None:
        before = {"python": platform.python_version(), "supported": False}
        library = self.root / "sqlite-3.54.0" / "libsqlite3.so.0"
        after = {**before, "supported": True, "sqlite": SQLITE.SQLITE_VERSION,
                 "libraries": [str(library)]}
        with mock.patch.dict(os.environ, {"LD_PRELOAD": "/security/preload.so",
                                         "LD_LIBRARY_PATH": "/trusted"}):
            probe, build = self.run_preparation(before, after)
        build.assert_called_once_with(self.root)
        selected = probe.call_args_list[1].args[0]
        self.assertEqual(selected["LD_PRELOAD"], "/security/preload.so")
        self.assertEqual(selected["LD_LIBRARY_PATH"], f"{library.parent}:/trusted")
        self.assertEqual(self.environment_file.read_text(),
                         f"LD_LIBRARY_PATH={library.parent}:/trusted\n")

    def test_failed_version_feature_or_library_selection_never_exports(self) -> None:
        before = {"python": platform.python_version(), "supported": False}
        library = self.root / "sqlite-3.54.0" / "libsqlite3.so.0"
        qualified = {**before, "supported": True, "sqlite": SQLITE.SQLITE_VERSION,
                     "libraries": [str(library)]}
        for broken in ({"python": "0.0.0"}, {"sqlite": "3.37.2"},
                       {"libraries": ["/usr/lib/libsqlite3.so.0"]}, {"supported": False}):
            with self.subTest(broken=broken), self.assertRaisesRegex(RuntimeError, "did not select"):
                self.run_preparation(before, {**qualified, **broken})
        self.assertEqual(self.environment_file.read_text(), "")


if __name__ == "__main__":
    unittest.main()
