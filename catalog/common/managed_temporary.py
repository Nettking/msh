"""Owned temporary files with crash-safe, bounded scavenging.

This module is intentionally small and conservative.  A temporary file is
scavengeable only when it lives in an explicitly managed root, has a valid
durable ownership record authenticated by that root's private token, and its
ownership record can be acquired exclusively.  A prefix or an age by itself
is never sufficient for deletion.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import shutil
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Self

_ROOT_SCHEMA = "fcp-managed-temporary-root-v1"
_OWNER_SCHEMA = "fcp-managed-temporary-owner-v1"
_ROOT_MARKER = ".fcp-managed-root.json"
_OWNER_SUFFIX = ".fcp-owner.json"
_MAX_NAME_LENGTH = 240


class ManagedTemporaryError(RuntimeError):
    """The managed temporary boundary could not be verified safely."""


def _is_plain_directory(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    return stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode) and not bool(
        getattr(metadata, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    )


def _is_plain_file(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    return stat.S_ISREG(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode) and not bool(
        getattr(metadata, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    )


def _identity(metadata: os.stat_result) -> tuple[int, int]:
    return int(getattr(metadata, "st_dev", 0)), int(getattr(metadata, "st_ino", 0))


def _canonical(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _lock(handle: BinaryIO, *, blocking: bool) -> bool:
    """Acquire one-byte exclusive ownership lock without platform packages."""

    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        mode = msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK
        try:
            msvcrt.locking(handle.fileno(), mode, 1)
        except OSError:
            return False
        return True

    import fcntl

    flags = fcntl.LOCK_EX
    if not blocking:
        flags |= fcntl.LOCK_NB
    try:
        fcntl.flock(handle.fileno(), flags)
    except OSError:
        return False
    return True


def _unlock(handle: BinaryIO) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
        return

    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass


def _token_digest(token: str, name: str, root_identity: tuple[int, int]) -> str:
    message = f"{name}\0{root_identity[0]}\0{root_identity[1]}".encode()
    return hmac.new(token.encode("ascii"), message, hashlib.sha256).hexdigest()


def _valid_token(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


@dataclass
class ManagedTemporaryFile:
    """A writable file whose close operation removes only its owned identity."""

    path: Path
    owner_path: Path
    handle: BinaryIO
    owner_handle: BinaryIO
    file_identity: tuple[int, int]
    owner_identity: tuple[int, int]

    def __getattr__(self, name: str):
        return getattr(self.handle, name)

    def write(self, data: bytes) -> int:
        return self.handle.write(data)

    def prepare_for_replace(self) -> None:
        """Flush and close the data handle before same-filesystem replace.

        Windows does not permit replacing an open file.  The owner record and
        its lock deliberately remain live until ``close`` so a successful
        replace still cleans up the sidecar without exposing an unowned
        temporary path during the publication boundary.
        """

        if self.handle.closed:
            return
        self.handle.flush()
        self.handle.close()

    def close(self) -> None:
        if self.handle.closed and self.owner_handle.closed:
            return
        try:
            if not self.handle.closed:
                self.handle.flush()
        except OSError:
            pass
        try:
            if not self.handle.closed:
                self.handle.close()
        finally:
            try:
                if not self.owner_handle.closed:
                    _unlock(self.owner_handle)
                    self.owner_handle.close()
            finally:
                _unlink_if_identity(self.path, self.file_identity)
                _unlink_if_identity(self.owner_path, self.owner_identity)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def _unlink_if_identity(path: Path, expected: tuple[int, int]) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return True
    except OSError:
        return False
    if not _is_plain_file(path) or _identity(metadata) != expected:
        return False
    try:
        path.unlink()
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return True


@dataclass
class ManagedTemporaryDirectory:
    """A managed temporary directory whose owner record survives a crash."""

    path: Path
    owner_path: Path
    owner_handle: BinaryIO
    directory_identity: tuple[int, int]
    owner_identity: tuple[int, int]

    def close(self) -> None:
        if not self.owner_handle.closed:
            _unlock(self.owner_handle)
            self.owner_handle.close()
        try:
            metadata = self.path.lstat()
        except FileNotFoundError:
            pass
        except OSError:
            metadata = None
        else:
            if (
                _is_plain_directory(self.path)
                and _identity(metadata) == self.directory_identity
            ):
                _remove_plain_owned_tree(self.path, max_entries=4096)
        _unlink_if_identity(self.owner_path, self.owner_identity)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def _remove_plain_owned_tree(path: Path, *, max_entries: int) -> bool:
    """Remove an owned tree only when every traversed entry is unambiguous."""

    count = 0
    for directory, dir_names, file_names in os.walk(
        path, topdown=True, followlinks=False
    ):
        count += len(dir_names) + len(file_names) + 1
        if count > max_entries:
            return False
        for name in (*dir_names, *file_names):
            candidate = Path(directory) / name
            try:
                metadata = candidate.lstat()
            except OSError:
                return False
            if stat.S_ISLNK(metadata.st_mode) or bool(
                getattr(metadata, "st_file_attributes", 0)
                & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
            ):
                return False
    try:
        shutil.rmtree(path)
    except OSError:
        return False
    return True


class ManagedTemporaryRoot:
    """Explicit FCP-owned root for temporary files and crash re-entry."""

    def __init__(self, root: Path | str, *, namespace: str) -> None:
        self.root = Path(root)
        self.namespace = str(namespace)
        self._token: str | None = None
        self._root_identity: tuple[int, int] | None = None

    @property
    def marker_path(self) -> Path:
        return self.root / _ROOT_MARKER

    def ensure(self) -> tuple[int, int]:
        self.root.mkdir(parents=True, exist_ok=True)
        if not _is_plain_directory(self.root):
            raise ManagedTemporaryError("managed temporary root is not a plain directory")
        root_metadata = self.root.lstat()
        root_identity = _identity(root_metadata)
        marker = self.marker_path
        payload: dict[str, object]
        try:
            marker.lstat()
        except FileNotFoundError:
            marker_present = False
        except OSError as exc:
            raise ManagedTemporaryError(
                "managed temporary root marker could not be inspected"
            ) from exc
        else:
            marker_present = True
        if marker_present:
            if not _is_plain_file(marker):
                raise ManagedTemporaryError("managed temporary root marker is unsafe")
            try:
                payload = json.loads(marker.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise ManagedTemporaryError("managed temporary root marker is unreadable") from exc
            if not isinstance(payload, dict) or (
                payload.get("schema") != _ROOT_SCHEMA
                or payload.get("namespace") != self.namespace
                or not _valid_token(payload.get("token"))
            ):
                raise ManagedTemporaryError("managed temporary root marker ownership mismatch")
        else:
            payload = {
                "schema": _ROOT_SCHEMA,
                "namespace": self.namespace,
                "token": secrets.token_hex(32),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            # Publish the marker atomically. ``open("x")`` makes an empty file
            # visible before its content arrives, and a concurrent ``ensure()``
            # that reads it in that window parses nothing and reports a corrupt
            # root -- which is how first use from several threads failed. Write
            # a private temporary in the same directory and hard-link it into
            # place: the link is atomic and refuses to clobber, so a reader
            # sees either no marker or a complete one, and a concurrent creator
            # still loses the race exactly as before.
            staging = marker.with_name(f"{marker.name}.{secrets.token_hex(16)}.tmp")
            try:
                with staging.open("x", encoding="utf-8") as handle:
                    handle.write(json.dumps(payload, sort_keys=True) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.link(staging, marker)
            except FileExistsError:
                # A concurrent creator won the marker race. Re-read it after
                # the failed exclusive create; do not recurse on a dangling
                # marker symlink or an attacker-controlled replacement.
                try:
                    marker.lstat()
                except (FileNotFoundError, OSError) as exc:
                    raise ManagedTemporaryError(
                        "managed temporary root marker race left no safe marker"
                    ) from exc
                return self.ensure()
            except OSError as exc:
                raise ManagedTemporaryError("managed temporary root marker could not be created") from exc
            finally:
                try:
                    staging.unlink()
                except OSError:
                    pass
        self._token = str(payload["token"])
        self._root_identity = root_identity
        return root_identity

    def _validated(self) -> tuple[str, tuple[int, int]]:
        root_identity = self.ensure()
        assert self._token is not None
        return self._token, root_identity

    def allocate(self, *, prefix: str, suffix: str = "") -> ManagedTemporaryFile:
        token, root_identity = self._validated()
        if not prefix or Path(prefix).name != prefix or Path(suffix).name != suffix:
            raise ManagedTemporaryError("managed temporary name components are unsafe")
        for _attempt in range(128):
            name = f"{prefix}{secrets.token_hex(16)}{suffix}"
            if len(name) > _MAX_NAME_LENGTH:
                raise ManagedTemporaryError("managed temporary name is too long")
            path = self.root / name
            owner_path = self.root / f".{name}{_OWNER_SUFFIX}"
            handle: BinaryIO | None = None
            file_identity: tuple[int, int] | None = None
            try:
                handle = path.open("x+b")
                file_identity = _identity(path.lstat())
                owner_payload: dict[str, object] = {
                    "schema": _OWNER_SCHEMA,
                    "namespace": self.namespace,
                    "name": name,
                    "root_identity": list(root_identity),
                    "file_identity": list(file_identity),
                    "proof": _token_digest(token, name, root_identity),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "pid": os.getpid(),
                }
                # The scavenger reclaims any owner record it can authenticate
                # and lock. Writing the payload through one handle and locking
                # a second one leaves the sidecar briefly both -- a concurrent
                # scavenge then deletes live work, and this allocation fails on
                # the reopen. Claim the lock on the creating handle first and
                # write what authenticates the record only after: an empty
                # sidecar parses as nothing, which the scavenger treats as
                # ambiguous and never deletes.
                owner_handle = owner_path.open("x+b")
                owner_identity = _identity(os.fstat(owner_handle.fileno()))
                if not _lock(owner_handle, blocking=False):
                    owner_handle.close()
                    handle.close()
                    _unlink_if_identity(owner_path, owner_identity)
                    _unlink_if_identity(path, file_identity)
                    raise ManagedTemporaryError("managed temporary ownership lock failed")
                owner_handle.write(_canonical(owner_payload) + b"\n")
                owner_handle.flush()
                os.fsync(owner_handle.fileno())
                return ManagedTemporaryFile(
                    path=path,
                    owner_path=owner_path,
                    handle=handle,
                    owner_handle=owner_handle,
                    file_identity=file_identity,
                    owner_identity=owner_identity,
                )
            except FileExistsError:
                if handle is not None:
                    handle.close()
                if file_identity is not None:
                    _unlink_if_identity(path, file_identity)
                continue
            except BaseException:
                try:
                    if handle is not None:
                        handle.close()
                except OSError:
                    pass
                try:
                    if owner_path.is_file() and not owner_path.is_symlink():
                        owner_path.unlink()
                except OSError:
                    pass
                raise
        raise ManagedTemporaryError("could not allocate a unique managed temporary file")

    def allocate_directory(self, *, prefix: str) -> ManagedTemporaryDirectory:
        token, root_identity = self._validated()
        if not prefix or Path(prefix).name != prefix:
            raise ManagedTemporaryError("managed temporary directory name is unsafe")
        for _attempt in range(128):
            name = f"{prefix}{secrets.token_hex(16)}"
            if len(name) > _MAX_NAME_LENGTH:
                raise ManagedTemporaryError("managed temporary directory name is too long")
            path = self.root / name
            owner_path = self.root / f".{name}{_OWNER_SUFFIX}"
            directory_identity: tuple[int, int] | None = None
            try:
                path.mkdir()
                directory_identity = _identity(path.lstat())
                owner_payload: dict[str, object] = {
                    "schema": _OWNER_SCHEMA,
                    "kind": "directory",
                    "namespace": self.namespace,
                    "name": name,
                    "root_identity": list(root_identity),
                    "file_identity": list(directory_identity),
                    "proof": _token_digest(token, name, root_identity),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "pid": os.getpid(),
                }
                # Same ordering as allocate(): the sidecar must never be
                # authenticatable while unlocked, or a concurrent scavenge can
                # reclaim a directory that is still being filled.
                owner_handle = owner_path.open("x+b")
                owner_identity = _identity(os.fstat(owner_handle.fileno()))
                if not _lock(owner_handle, blocking=False):
                    owner_handle.close()
                    _unlink_if_identity(owner_path, owner_identity)
                    _remove_plain_owned_tree(path, max_entries=4096)
                    raise ManagedTemporaryError(
                        "managed temporary directory ownership lock failed"
                    )
                owner_handle.write(_canonical(owner_payload) + b"\n")
                owner_handle.flush()
                os.fsync(owner_handle.fileno())
                return ManagedTemporaryDirectory(
                    path=path,
                    owner_path=owner_path,
                    owner_handle=owner_handle,
                    directory_identity=directory_identity,
                    owner_identity=owner_identity,
                )
            except FileExistsError:
                if directory_identity is not None and _is_plain_directory(path):
                    _remove_plain_owned_tree(path, max_entries=4096)
                continue
            except BaseException:
                try:
                    if owner_path.is_file() and not owner_path.is_symlink():
                        owner_path.unlink()
                except OSError:
                    pass
                try:
                    if _is_plain_directory(path):
                        _remove_plain_owned_tree(path, max_entries=4096)
                except OSError:
                    pass
                raise
        raise ManagedTemporaryError(
            "could not allocate a unique managed temporary directory"
        )


@dataclass(frozen=True)
class ScavengeReport:
    scanned_entries: int
    reclaimed_files: int
    reclaimed_owner_records: int
    skipped_active: int
    skipped_ambiguous: int
    cleanup_failures: int


def scavenge_managed_temporary_root(
    root: Path | str,
    *,
    namespace: str,
    max_entries: int = 4096,
) -> ScavengeReport:
    """Reclaim only authenticated, unlocked, direct children of a managed root."""

    managed = ManagedTemporaryRoot(root, namespace=namespace)
    managed.ensure()
    entries = list(managed.root.iterdir())
    if len(entries) > max_entries:
        raise ManagedTemporaryError("managed temporary root traversal bound exceeded")
    token, root_identity = managed._validated()
    scanned = reclaimed = reclaimed_owner = active = ambiguous = failures = 0
    for owner_path in entries:
        if not owner_path.name.startswith(".") or not owner_path.name.endswith(_OWNER_SUFFIX):
            continue
        scanned += 1
        try:
            if not _is_plain_file(owner_path):
                ambiguous += 1
                continue
            try:
                payload = json.loads(owner_path.read_text(encoding="utf-8"))
            except OSError:
                # Windows may deny even a read while the live owner keeps the
                # sidecar open.  Leave it untouched for a later re-entry.
                active += 1
                continue
            except ValueError:
                ambiguous += 1
                continue
            if not isinstance(payload, dict):
                ambiguous += 1
                continue
            name = payload.get("name")
            file_identity = payload.get("file_identity")
            kind = payload.get("kind", "file")
            if not isinstance(payload, dict) or (
                payload.get("schema") != _OWNER_SCHEMA
                or payload.get("namespace") != namespace
                or kind not in {"file", "directory"}
                or not isinstance(name, str)
                or Path(name).name != name
                or not name
                or payload.get("root_identity") != list(root_identity)
                or payload.get("proof") != _token_digest(token, name, root_identity)
                or not isinstance(file_identity, list)
                or len(file_identity) != 2
                or any(
                    not isinstance(value, int) or isinstance(value, bool)
                    for value in file_identity
                )
            ):
                ambiguous += 1
                continue
            expected_file_identity = (int(file_identity[0]), int(file_identity[1]))
            if owner_path != managed.root / f".{name}{_OWNER_SUFFIX}":
                ambiguous += 1
                continue
            try:
                owner_handle: BinaryIO | None = owner_path.open("r+b")
            except OSError:
                # A live Windows owner can deny a second open while its
                # sidecar is locked.  Treat that as active and leave it for a
                # later re-entry; no deletion is attempted.
                active += 1
                continue
            if not _lock(owner_handle, blocking=False):
                owner_handle.close()
                owner_handle = None
                active += 1
                continue
            try:
                assert owner_handle is not None
                owner_identity = _identity(os.fstat(owner_handle.fileno()))
                data_path = managed.root / name
                try:
                    metadata = data_path.lstat()
                except FileNotFoundError:
                    metadata = None
                if kind == "directory":
                    if metadata is not None and (
                        not _is_plain_directory(data_path)
                        or _identity(metadata) != expected_file_identity
                        or not _remove_plain_owned_tree(
                            data_path, max_entries=max_entries
                        )
                    ):
                        ambiguous += 1
                        continue
                    if metadata is not None:
                        reclaimed += 1
                else:
                    if metadata is not None and (
                        not _is_plain_file(data_path)
                        or _identity(metadata) != expected_file_identity
                    ):
                        ambiguous += 1
                        continue
                    if metadata is not None:
                        data_path.unlink()
                        reclaimed += 1
                # Windows will not unlink an open sidecar.  The lock has
                # already excluded active owners, and the identity check is
                # repeated after closing so a replacement cannot be removed.
                _unlock(owner_handle)
                owner_handle.close()
                owner_handle = None
                if not _unlink_if_identity(owner_path, owner_identity):
                    failures += 1
                    continue
                reclaimed_owner += 1
            finally:
                if owner_handle is not None:
                    _unlock(owner_handle)
                    owner_handle.close()
        except FileNotFoundError:
            continue
        except OSError:
            failures += 1
    return ScavengeReport(
        scanned_entries=scanned,
        reclaimed_files=reclaimed,
        reclaimed_owner_records=reclaimed_owner,
        skipped_active=active,
        skipped_ambiguous=ambiguous,
        cleanup_failures=failures,
    )


__all__ = [
    "ManagedTemporaryDirectory",
    "ManagedTemporaryError",
    "ManagedTemporaryFile",
    "ManagedTemporaryRoot",
    "ScavengeReport",
    "scavenge_managed_temporary_root",
]
