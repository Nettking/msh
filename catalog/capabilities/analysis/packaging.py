"""Deterministic, bounded packing of a JSONL data slice into one artifact body.

Packing keeps large JSONL out of every control message: the federation only ever
carries an :class:`~catalog.capabilities.jobs.ArtifactReference` with a verifiable
content hash, and the bytes move on the authorized data path.

Extraction is treated as parsing hostile input. Only regular files with safe
relative names are written, and both entry count and total size are bounded.
"""

from __future__ import annotations

import gzip
import os
import re
import tarfile
import tempfile
from collections.abc import Sequence
from pathlib import Path

from catalog.federation.errors import FederationValidationError

from .contracts import DEFAULT_MAX_SLICE_BYTES

MAX_SLICE_ENTRIES = 4_096
#: Deterministic archive metadata: the artifact identity must depend only on the
#: file names and contents, never on the packing machine's clock or accounts.
_FIXED_MTIME = 0
_FIXED_MODE = 0o644
_SAFE_MEMBER_RE = re.compile(r"^[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*$")
_COPY_CHUNK_BYTES = 1024 * 1024


def _member_name(path: Path, root: Path) -> str:
    try:
        relative = path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise FederationValidationError(
            "analysis-slice-path-outside-root",
            "path",
            "slice files must live under the declared data root",
        ) from exc
    name = relative.as_posix()
    if _SAFE_MEMBER_RE.fullmatch(name) is None:
        raise FederationValidationError(
            "analysis-slice-unsafe-name",
            "path",
            "slice file names must be safe relative logical names",
        )
    return name


def _validated_members(
    files: Sequence[Path], root: Path, max_bytes: int
) -> list[tuple[str, Path, int]]:
    if not files:
        raise FederationValidationError(
            "analysis-slice-empty", "files", "an analysis slice requires input files"
        )
    if len(files) > MAX_SLICE_ENTRIES:
        raise FederationValidationError(
            "analysis-slice-too-many-files",
            "files",
            f"must not exceed {MAX_SLICE_ENTRIES} files",
        )
    members = sorted(
        (
            (_member_name(item, root), item, item.stat().st_size)
            for item in files
        ),
        key=lambda item: item[0],
    )
    total = sum(size for _name, _item, size in members)
    if total > max_bytes:
        raise FederationValidationError(
            "analysis-slice-too-large",
            "files",
            f"packed analysis input must not exceed {max_bytes} bytes",
        )
    return members


def slice_archive_matches(
    archive_path: Path,
    *,
    files: Sequence[Path],
    root: Path,
    max_bytes: int = DEFAULT_MAX_SLICE_BYTES,
) -> bool:
    """Fully verify that an existing archive is the requested deterministic slice.

    This is intentionally stronger than checking that ``tarfile`` can open the
    path. Every member, fixed metadata field, and byte is compared with the
    declared source files, and the gzip stream is drained so truncation after
    the tar end marker still fails its footer/CRC check.
    """

    members = _validated_members(files, root, max_bytes)
    try:
        if not archive_path.is_file() or archive_path.stat().st_size > max_bytes:
            return False
        with (
            archive_path.open("rb") as raw,
            gzip.GzipFile(fileobj=raw, mode="rb") as compressed,
            tarfile.open(fileobj=compressed, mode="r|", format=tarfile.PAX_FORMAT)
            as archive,
        ):
            iterator = iter(archive)
            for expected_name, expected_path, expected_size in members:
                info = next(iterator, None)
                if (
                    info is None
                    or not info.isreg()
                    or info.name != expected_name
                    or info.size != expected_size
                    or info.mtime != _FIXED_MTIME
                    or info.mode != _FIXED_MODE
                    or info.uid != 0
                    or info.gid != 0
                    or info.uname != ""
                    or info.gname != ""
                ):
                    return False
                packed = archive.extractfile(info)
                if packed is None:  # pragma: no cover - defended by isreg above
                    return False
                with packed, expected_path.open("rb") as expected:
                    while True:
                        packed_chunk = packed.read(_COPY_CHUNK_BYTES)
                        expected_chunk = expected.read(_COPY_CHUNK_BYTES)
                        if packed_chunk != expected_chunk:
                            return False
                        if not expected_chunk:
                            break
            if next(iterator, None) is not None:
                return False
            while compressed.read(_COPY_CHUNK_BYTES):
                pass
    except (EOFError, OSError, tarfile.TarError):
        return False
    return True


def _sync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_slice_archive(
    destination: Path,
    *,
    files: Sequence[Path],
    root: Path,
    max_bytes: int = DEFAULT_MAX_SLICE_BYTES,
) -> int:
    """Atomically publish a deterministic ``tar.gz`` and return its byte size."""

    members = _validated_members(files, root, max_bytes)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".partial",
        dir=destination.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as raw:
            descriptor = -1
            with (
                gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0)
                as compressed,
                tarfile.open(
                    fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT
                ) as archive,
            ):
                for name, path, size in members:
                    info = tarfile.TarInfo(name=name)
                    info.size = size
                    info.mtime = _FIXED_MTIME
                    info.mode = _FIXED_MODE
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    info.type = tarfile.REGTYPE
                    with path.open("rb") as handle:
                        archive.addfile(info, handle)
            raw.flush()
            os.fsync(raw.fileno())
        size = temporary.stat().st_size
        if size > max_bytes:
            raise FederationValidationError(
                "analysis-slice-too-large",
                "size_bytes",
                f"packed analysis input must not exceed {max_bytes} bytes",
            )
        os.replace(temporary, destination)
        _sync_directory(destination.parent)
        return size
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        raise


def extract_slice_archive(
    archive_path: Path,
    *,
    destination: Path,
    max_bytes: int = DEFAULT_MAX_SLICE_BYTES,
) -> tuple[Path, ...]:
    """Materialize an already-verified slice archive into an isolated directory.

    The archive itself is delivered and integrity-checked by the F6 object
    transfer receiver; this function only parses it, and treats its contents as
    hostile input.
    """

    if archive_path.stat().st_size > max_bytes:
        raise FederationValidationError(
            "analysis-slice-too-large",
            "size_bytes",
            f"packed analysis input must not exceed {max_bytes} bytes",
        )
    destination.mkdir(parents=True, exist_ok=True)
    resolved_destination = destination.resolve()
    extracted: list[Path] = []
    total = 0
    with tarfile.open(archive_path, mode="r:gz") as archive:
        for info in archive:
            if not info.isreg():
                raise FederationValidationError(
                    "analysis-slice-unsupported-member",
                    "member",
                    "slice archives may only contain regular files",
                )
            if _SAFE_MEMBER_RE.fullmatch(info.name) is None:
                raise FederationValidationError(
                    "analysis-slice-unsafe-name",
                    "member",
                    "slice archive contains an unsafe member name",
                )
            target = (resolved_destination / info.name).resolve()
            if resolved_destination not in target.parents:
                raise FederationValidationError(
                    "analysis-slice-path-escape",
                    "member",
                    "slice archive member resolved outside the workspace",
                )
            total += info.size
            if total > max_bytes:
                raise FederationValidationError(
                    "analysis-slice-too-large",
                    "size_bytes",
                    f"unpacked analysis input must not exceed {max_bytes} bytes",
                )
            if len(extracted) >= MAX_SLICE_ENTRIES:
                raise FederationValidationError(
                    "analysis-slice-too-many-files",
                    "member",
                    f"must not exceed {MAX_SLICE_ENTRIES} files",
                )
            source = archive.extractfile(info)
            if source is None:  # pragma: no cover - defended by isreg above
                raise FederationValidationError(
                    "analysis-slice-unsupported-member",
                    "member",
                    "slice archive member could not be read",
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            with source, target.open("wb") as handle:
                while chunk := source.read(1024 * 1024):
                    handle.write(chunk)
            target.chmod(0o600)
            extracted.append(target)
    return tuple(extracted)


__all__ = [
    "MAX_SLICE_ENTRIES",
    "extract_slice_archive",
    "slice_archive_matches",
    "write_slice_archive",
]
