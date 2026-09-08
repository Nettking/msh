"""Fail-closed source identity checks shared by the driver and every node.

An archive is trusted only through the caller's independently obtained complete
ZIP SHA-256. Its embedded manifest is then used to locate and verify every byte
of extracted source. This module never extracts archive members or ignores files.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
import unicodedata
import zipfile
from pathlib import Path, PurePosixPath

MANIFEST_SCHEMA = "fcp.icse-artifact-manifest.v1"
_RESERVED = re.compile(r"(?:con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\..*)?", re.IGNORECASE)


class ProvenanceError(ValueError):
    """A bounded diagnostic code; contains no supplied path or secret text."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise ProvenanceError(code)


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _linked(metadata: os.stat_result) -> bool:
    return stat.S_ISLNK(metadata.st_mode) or bool(
        getattr(metadata, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _safe_name(name: str) -> tuple[str, ...]:
    _require(bool(name) and "\\" not in name and ":" not in name, "archive-unsafe-path")
    _require(not name.startswith("/") and not any(ord(char) < 32 or ord(char) == 127 for char in name), "archive-unsafe-path")
    parts = tuple(name.removesuffix("/").split("/"))
    _require(all(
        part not in {"", ".", ".."}
        and not part.endswith((".", " "))
        and not _RESERVED.fullmatch(part)
        for part in parts
    ), "archive-unsafe-path")
    return parts


def _members(archive: zipfile.ZipFile) -> dict[str, zipfile.ZipInfo]:
    result = {}
    spellings = {}
    kinds = {}
    for info in archive.infolist():
        _require(info.orig_filename == info.filename, "archive-unsafe-path")
        parts = _safe_name(info.filename)
        path = "/".join(parts)
        _require(path not in result, "archive-duplicate-member")
        mode = (info.external_attr >> 16) & 0xFFFF
        kind = stat.S_IFMT(mode)
        _require(kind in {0, stat.S_IFDIR, stat.S_IFREG}, "archive-nonregular-member")
        _require(not (info.external_attr & 0x400), "archive-reparse-member")
        _require(not (info.flag_bits & 1), "archive-encrypted-member")
        _require(kind != stat.S_IFDIR or info.is_dir(), "archive-directory-type-mismatch")
        _require(kind != stat.S_IFREG or not info.is_dir(), "archive-directory-type-mismatch")
        for count in range(1, len(parts) + 1):
            prefix = "/".join(parts[:count])
            folded = unicodedata.normalize("NFC", prefix).casefold()
            previous = spellings.setdefault(folded, prefix)
            _require(previous == prefix, "archive-case-collision")
            directory = count < len(parts) or info.is_dir()
            previous_kind = kinds.setdefault(prefix, directory)
            _require(previous_kind == directory, "archive-file-directory-collision")
        result[path] = info
    return result


def _regular_tree(root: Path) -> tuple[dict[str, Path], set[str]]:
    _require(root.is_absolute(), "source-root-must-be-absolute")
    # Reject aliasing through symlinks/junctions as well as links inside source.
    for part in (root, *root.parents):
        _require(not _linked(part.lstat()), "source-linked-path")
    files = {}
    directories = set()
    spellings = {}

    def refuse_unreadable(error: OSError) -> None:
        raise error

    for current, child_dirs, child_files in os.walk(root, followlinks=False, onerror=refuse_unreadable):
        for name in (*child_dirs, *child_files):
            path = Path(current) / name
            relative = path.relative_to(root).as_posix()
            metadata = path.lstat()
            _require(not _linked(metadata), "source-linked-path")
            _safe_name(relative)
            folded = unicodedata.normalize("NFC", relative).casefold()
            _require(folded not in spellings, "source-case-collision")
            spellings[folded] = relative
            if stat.S_ISREG(metadata.st_mode):
                files[relative] = path
            elif stat.S_ISDIR(metadata.st_mode):
                directories.add(relative)
            else:
                raise ProvenanceError("source-nonregular-file")
    return files, directories


def _archive_identity(
    root: Path, expected_sha: str, tag: str | None, archive_path: Path, trusted_digest: str
) -> dict:
    _require(bool(re.fullmatch(r"[0-9a-f]{64}", trusted_digest)), "archive-sha256-format")
    _require(not archive_path.resolve().is_relative_to(root.resolve()), "archive-must-be-outside-source")
    actual_digest = _digest(archive_path)
    _require(actual_digest == trusted_digest, "archive-checksum-mismatch")
    with zipfile.ZipFile(archive_path) as archive:
        members = _members(archive)
        manifests = [name for name in members if name.endswith("/artifact/artifact-manifest.json")]
        _require(len(manifests) == 1, "archive-manifest-count")
        info = members[manifests[0]]
        _require(not info.is_dir() and info.file_size <= 1024 * 1024, "archive-manifest-size")
        manifest = json.loads(archive.read(info))
        _require(isinstance(manifest, dict), "archive-manifest-object")
        _require(manifest.get("schema") == MANIFEST_SCHEMA, "archive-manifest-schema")
        _require(manifest.get("source_revision") == expected_sha, "archive-source-revision-mismatch")
        publication = manifest.get("publication_archive")
        _require(isinstance(publication, dict), "archive-publication-metadata")
        version = manifest.get("artifact_version")
        _require(isinstance(version, str) and bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", version)), "archive-version-format")
        prefix = f"fcp-icse-tool-demo-{version}"
        source_prefix = f"{prefix}/source/"
        artifact_prefix = f"{prefix}/artifact/"
        _require(publication.get("source_prefix") == source_prefix, "archive-source-prefix-mismatch")
        _require(publication.get("artifact_prefix") == artifact_prefix, "archive-artifact-prefix-mismatch")
        _require(publication.get("file") == prefix + ".zip", "archive-publication-filename-mismatch")
        _require(manifests[0] == artifact_prefix + "artifact-manifest.json", "archive-manifest-location-mismatch")
        _require(root.name == "source" and root.parent.name == prefix, "extracted-source-prefix-mismatch")
        allowed_roots = {prefix, source_prefix.rstrip("/"), artifact_prefix.rstrip("/")}
        _require(all(
            (name in allowed_roots and item.is_dir())
            or name.startswith((source_prefix, artifact_prefix))
            for name, item in members.items()
        ), "archive-unexpected-root")
        source_ref = manifest.get("source_ref")
        _require(
            isinstance(source_ref, str) and source_ref.startswith("refs/")
            and len(source_ref) <= 512
            and not any(ord(char) < 32 or ord(char) == 127 for char in source_ref),
            "archive-source-ref-format",
        )
        if tag:
            _require(source_ref == f"refs/tags/{tag}", "archive-artifact-tag-mismatch")
        expected_files = {}
        expected_directories = set()
        for name, item in members.items():
            if not name.startswith(source_prefix):
                continue
            relative = name.removeprefix(source_prefix)
            if item.is_dir():
                expected_directories.add(relative)
            else:
                expected_files[relative] = item
            expected_directories.update(
                str(parent) for parent in PurePosixPath(relative).parents if str(parent) != "."
            )
        _require(bool(expected_files), "archive-empty-source")
        local_files, local_directories = _regular_tree(root)
        _require(local_files.keys() == expected_files.keys(), "extracted-source-file-set-mismatch")
        _require(local_directories == expected_directories, "extracted-source-directory-set-mismatch")
        total_bytes = 0
        for relative, item in expected_files.items():
            local = local_files[relative]
            _require(local.stat().st_size == item.file_size, "extracted-source-size-mismatch")
            with archive.open(item) as archived, local.open("rb") as extracted:
                while data := archived.read(1024 * 1024):
                    _require(extracted.read(len(data)) == data, "extracted-source-bytes-mismatch")
                    total_bytes += len(data)
                _require(not extracted.read(1), "extracted-source-size-mismatch")
    # Detect an archive changed during comparison as well as before it.
    _require(_digest(archive_path) == trusted_digest, "archive-changed-during-verification")
    return {
        "source_sha": expected_sha,
        "source_mode": "verified-publication-archive",
        "archive_sha256": actual_digest,
        "source_files_verified": len(expected_files),
        "source_bytes_verified": total_bytes,
        "artifact_source_ref": source_ref,
        "artifact_tag": tag,
        "artifact_tag_checked": tag is not None,
        # The publication format has no product-release-tag mapping. Do not
        # reinterpret its artifact source_ref as an independently checked alias.
        "release_tag": None,
        "release_tag_checked": False,
        "working_tree_clean": None,
    }


def verify_source(
    root: Path,
    expected_sha: str,
    tag: str | None = None,
    *,
    source_archive: Path | None = None,
    archive_sha256: str | None = None,
) -> dict:
    """Verify Git or a complete trusted publication archive; never fall back."""
    _require(bool(re.fullmatch(r"[0-9a-f]{40}", expected_sha)), "source-sha-format")
    _require((source_archive is None) == (archive_sha256 is None), "archive-options-must-be-paired")
    if tag is not None:
        _require(bool(tag) and not tag.startswith("-") and not any(ord(char) < 32 for char in tag), "source-tag-format")
    if source_archive is not None:
        try:
            return _archive_identity(root, expected_sha, tag, source_archive, archive_sha256)
        except (zipfile.BadZipFile, zipfile.LargeZipFile) as error:
            raise ProvenanceError("archive-invalid-zip") from error

    def git(*arguments: str) -> str:
        return subprocess.check_output(
            ["git", "-C", str(root), *arguments], text=True, encoding="utf-8"
        ).strip()

    actual = git("rev-parse", "HEAD")
    _require(actual == expected_sha, "git-source-sha-mismatch")
    _require(not git("status", "--porcelain", "--untracked-files=normal"), "git-source-checkout-dirty")
    if tag:
        _require(git("rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}") == actual, "git-source-tag-mismatch")
    return {
        "source_sha": actual,
        "source_mode": "git",
        "release_tag": tag,
        "release_tag_checked": tag is not None,
        "working_tree_clean": True,
    }
