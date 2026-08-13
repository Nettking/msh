"""Deterministic, bounded packing of a JSONL data slice into one artifact body.

Packing keeps large JSONL out of every control message: the federation only ever
carries an :class:`~catalog.capabilities.jobs.ArtifactReference` with a verifiable
content hash, and the bytes move on the authorized data path.

Extraction is treated as parsing hostile input. Only regular files with safe
relative names are written, and both entry count and total size are bounded.
"""

from __future__ import annotations

import gzip
import re
import tarfile
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


def write_slice_archive(
    destination: Path,
    *,
    files: Sequence[Path],
    root: Path,
    max_bytes: int = DEFAULT_MAX_SLICE_BYTES,
) -> int:
    """Write a deterministic ``tar.gz`` of ``files`` and return its byte size."""

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
        ((_member_name(item, root), item) for item in files), key=lambda item: item[0]
    )
    total = sum(item.stat().st_size for _name, item in members)
    if total > max_bytes:
        raise FederationValidationError(
            "analysis-slice-too-large",
            "files",
            f"packed analysis input must not exceed {max_bytes} bytes",
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with (
        destination.open("wb") as raw,
        gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed,
        tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive,
    ):
        for name, path in members:
            info = tarfile.TarInfo(name=name)
            info.size = path.stat().st_size
            info.mtime = _FIXED_MTIME
            info.mode = _FIXED_MODE
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.type = tarfile.REGTYPE
            with path.open("rb") as handle:
                archive.addfile(info, handle)
    return destination.stat().st_size


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


__all__ = ["MAX_SLICE_ENTRIES", "extract_slice_archive", "write_slice_archive"]
