"""Publication provenance rejects source substitutions before runtime startup."""

from __future__ import annotations

import errno
import hashlib
import json
import stat
import zipfile
from pathlib import Path

import pytest

from demo.icse.network.provenance import ProvenanceError, verify_source

SHA = "a" * 40
TAG = "fcp-icse-tool-demo-v1.0.0"
PREFIX = "fcp-icse-tool-demo-1.0.0"
SOURCE_FILES = {"module.py": b"print('release')\n", "nested/config.json": b'{"ready": true}\n'}


def _archive(tmp_path: Path, *, additions=(), edit_manifest=None):
    source = tmp_path / "extracted" / PREFIX / "source"
    source.mkdir(parents=True)
    for name, content in SOURCE_FILES.items():
        target = source / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    manifest = {
        "schema": "fcp.icse-artifact-manifest.v1",
        "artifact_version": "1.0.0",
        "source_revision": SHA,
        "source_ref": "refs/tags/" + TAG,
        "publication_archive": {
            "file": PREFIX + ".zip",
            "source_prefix": PREFIX + "/source/",
            "artifact_prefix": PREFIX + "/artifact/",
            "sha256_sidecar": "ZENODO_SHA256",
        },
    }
    if edit_manifest is not None:
        edit_manifest(manifest)
    archive = tmp_path / "download.zip"
    with zipfile.ZipFile(archive, "w") as package:
        for name, content in SOURCE_FILES.items():
            package.writestr(PREFIX + "/source/" + name, content)
        package.writestr(PREFIX + "/artifact/artifact-manifest.json", json.dumps(manifest))
        for name, content in additions:
            package.writestr(name, content)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    return source, archive, digest


def _verify(source, archive, digest, *, sha=SHA, tag=TAG):
    return verify_source(source, sha, tag, source_archive=archive, archive_sha256=digest)


def test_complete_archive_binds_every_source_byte_and_only_the_artifact_tag(tmp_path):
    source, archive, digest = _archive(tmp_path)
    identity = _verify(source, archive, digest)
    assert identity["source_sha"] == SHA
    assert identity["archive_sha256"] == digest
    assert identity["source_files_verified"] == len(SOURCE_FILES)
    assert identity["source_bytes_verified"] == sum(map(len, SOURCE_FILES.values()))
    assert identity["artifact_source_ref"] == "refs/tags/" + TAG
    assert identity["artifact_tag_checked"] is True
    assert identity["release_tag"] is None
    assert identity["release_tag_checked"] is False
    assert identity["working_tree_clean"] is None


def test_full_zip_checksum_is_required_independently_of_embedded_manifest(tmp_path):
    source, archive, digest = _archive(tmp_path)
    with archive.open("ab") as stream:
        stream.write(b"changed after publication")
    with pytest.raises(ProvenanceError, match="archive-checksum-mismatch"):
        _verify(source, archive, digest)


@pytest.mark.parametrize("change", ["same_size_bytes", "missing", "extra_module", "bytecode", "empty_directory"])
def test_extracted_source_tamper_missing_and_extra_files_are_rejected(tmp_path, change):
    source, archive, digest = _archive(tmp_path)
    if change == "same_size_bytes":
        (source / "module.py").write_bytes(b"print('altered')\n")
    elif change == "missing":
        (source / "module.py").unlink()
    elif change == "extra_module":
        (source / "sitecustomize.py").write_text("raise RuntimeError('extra code')\n", encoding="utf-8")
    elif change == "bytecode":
        (source / "__pycache__").mkdir()
        (source / "__pycache__" / "module.pyc").write_bytes(b"unexpected bytecode")
    else:
        (source / "unmanifested-directory").mkdir()
    with pytest.raises(ProvenanceError, match="extracted-source-(bytes|file-set|directory-set)-mismatch"):
        _verify(source, archive, digest)


@pytest.mark.parametrize("unsafe", [
    "../escape.py", "/absolute.py", "C:/drive.py", "relative\\escape.py",
    PREFIX + "/source/../escape.py", PREFIX + "/source/NUL", PREFIX + "/source/name. ",
])
def test_unsafe_archive_names_are_rejected_without_extraction(tmp_path, unsafe):
    # ZipInfo's constructor normalizes Windows separators on Windows. Assign
    # after construction so the ZIP contains the deliberately unsafe raw name
    # on every platform; otherwise this fixture accidentally tests a safe name.
    member = zipfile.ZipInfo("placeholder")
    member.filename = unsafe
    member.orig_filename = unsafe
    source, archive, digest = _archive(tmp_path, additions=[(member, b"unsafe")])
    assert unsafe.encode("utf-8") in archive.read_bytes()
    with pytest.raises(ProvenanceError, match="archive-unsafe-path"):
        _verify(source, archive, digest)


def test_duplicate_archive_member_is_rejected(tmp_path):
    with pytest.warns(UserWarning, match="Duplicate name"):
        source, archive, digest = _archive(
            tmp_path, additions=[(PREFIX + "/source/module.py", SOURCE_FILES["module.py"])]
        )
    with pytest.raises(ProvenanceError, match="archive-duplicate-member"):
        _verify(source, archive, digest)


@pytest.mark.parametrize("name", ["MODULE.py", "NESTED/other.json"])
def test_case_collisions_in_files_or_implicit_directories_are_rejected(tmp_path, name):
    source, archive, digest = _archive(tmp_path, additions=[(PREFIX + "/source/" + name, b"collision")])
    with pytest.raises(ProvenanceError, match="archive-case-collision"):
        _verify(source, archive, digest)


def test_archive_symlink_is_rejected(tmp_path):
    link = zipfile.ZipInfo(PREFIX + "/source/link.py")
    link.create_system = 3
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    source, archive, digest = _archive(tmp_path, additions=[(link, b"module.py")])
    with pytest.raises(ProvenanceError, match="archive-nonregular-member"):
        _verify(source, archive, digest)


def test_extracted_source_symlink_is_rejected_even_with_identical_target_bytes(tmp_path):
    source, archive, digest = _archive(tmp_path)
    target = tmp_path / "same-bytes.py"
    target.write_bytes(SOURCE_FILES["module.py"])
    original = source / "module.py"
    original.unlink()
    try:
        original.symlink_to(target)
    except OSError as error:
        if getattr(error, "winerror", None) == 1314 or error.errno in {
            errno.EPERM, errno.EACCES, errno.ENOSYS, errno.ENOTSUP,
        }:
            pytest.skip("host privilege or filesystem does not support creating symlinks")
        raise
    with pytest.raises(ProvenanceError, match="source-linked-path"):
        _verify(source, archive, digest)


@pytest.mark.parametrize("mutation,code", [
    (lambda value: value.update(schema="unexpected"), "archive-manifest-schema"),
    (lambda value: value.update(source_revision="b" * 40), "archive-source-revision-mismatch"),
    (lambda value: value["publication_archive"].update(source_prefix="elsewhere/source/"), "archive-source-prefix-mismatch"),
    (lambda value: value["publication_archive"].update(artifact_prefix="elsewhere/artifact/"), "archive-artifact-prefix-mismatch"),
    (lambda value: value.update(source_ref="refs/heads/main"), "archive-artifact-tag-mismatch"),
])
def test_manifest_cannot_relabel_revision_prefix_or_artifact_ref(tmp_path, mutation, code):
    source, archive, digest = _archive(tmp_path, edit_manifest=mutation)
    with pytest.raises(ProvenanceError, match=code):
        _verify(source, archive, digest)


def test_product_tag_is_not_inferred_from_matching_source_sha(tmp_path):
    source, archive, digest = _archive(tmp_path)
    with pytest.raises(ProvenanceError, match="archive-artifact-tag-mismatch"):
        _verify(source, archive, digest, tag="v1.0.0")


def test_empty_tag_cannot_be_reported_as_checked(tmp_path):
    source, archive, digest = _archive(tmp_path)
    with pytest.raises(ProvenanceError, match="source-tag-format"):
        _verify(source, archive, digest, tag="")


def test_source_directory_must_match_embedded_archive_prefix(tmp_path):
    source, archive, digest = _archive(tmp_path)
    renamed = source.parent / "alternate-source"
    source.rename(renamed)
    with pytest.raises(ProvenanceError, match="extracted-source-prefix-mismatch"):
        _verify(renamed, archive, digest)


def test_archive_option_pair_never_falls_back_to_git(tmp_path):
    source, archive, digest = _archive(tmp_path)
    with pytest.raises(ProvenanceError, match="archive-options-must-be-paired"):
        verify_source(source, SHA, source_archive=archive)
    with pytest.raises(ProvenanceError, match="archive-options-must-be-paired"):
        verify_source(source, SHA, archive_sha256=digest)
