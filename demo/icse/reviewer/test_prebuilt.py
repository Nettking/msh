"""Negative tests for the prebuilt reviewer's checksum and archive handling."""
from __future__ import annotations
import hashlib
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from demo.icse.reviewer import prebuilt

class PublicationArchiveTests(unittest.TestCase):
    def test_canonical_archive_and_extraction(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            name = "fcp-icse-tool-demo-candidate.zip"
            archive = root / name
            with zipfile.ZipFile(archive, "w") as output:
                output.writestr("fcp-icse-tool-demo-candidate/source/demo/icse/network/run.py", "test")
                output.writestr("fcp-icse-tool-demo-candidate/artifact/artifact-manifest.json", json.dumps({}))
            sha = hashlib.sha256(archive.read_bytes()).hexdigest()
            (root / "ZENODO_SHA256").write_text(f"{sha}  {name}\n")
            with patch.object(prebuilt, "INPUT", root), patch.object(prebuilt, "OUTPUT", root):
                self.assertEqual(prebuilt.verified_archive(), (archive, sha))
                self.assertTrue((prebuilt.extract_checked(archive, root / "extracted") / "source").is_dir())

    def test_mismatched_archive_digest_refused(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            archive = root / "fcp-icse-tool-demo-candidate.zip"
            archive.write_bytes(b"tampered")
            (root / "ZENODO_SHA256").write_text("0" * 64 + "  " + archive.name + "\n")
            with patch.object(prebuilt, "INPUT", root), patch.object(prebuilt, "OUTPUT", root):
                with self.assertRaisesRegex(ValueError, "digest mismatch"):
                    prebuilt.verified_archive()

    def test_parent_traversal_refused(self):
        with tempfile.TemporaryDirectory() as raw:
            archive = Path(raw) / "fcp-icse-tool-demo-candidate.zip"
            with zipfile.ZipFile(archive, "w") as output:
                output.writestr("fcp-icse-tool-demo-candidate/../../outside", "unsafe")
            with self.assertRaisesRegex(ValueError, "archive path"):
                prebuilt.extract_checked(archive, Path(raw) / "extract")

if __name__ == "__main__":
    unittest.main()
