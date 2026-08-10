"""Persistent Ed25519 identity for an outbound federation node.

The private key and public identity metadata are deliberately separate files.
Neither this module's return values nor their representations expose private
key material.
"""

from __future__ import annotations

import base64
import errno
import hashlib
import hmac
import json
import math
import os
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from catalog.federation.errors import FederationValidationError
from catalog.federation.models import NodeIdentity

PRIVATE_KEY_FILENAME: Final = "identity.pem"
PUBLIC_IDENTITY_FILENAME: Final = "identity.json"
IDENTITY_LOCK_FILENAME: Final = ".identity.lock"
PUBLIC_KEY_PREFIX: Final = "ed25519:"
WINDOWS_DPAPI_PREFIX: Final = b"FCP-DPAPI-v1\x00"
WINDOWS_DPAPI_ENTROPY: Final = b"FCP federated node identity v1"
MAX_PRIVATE_KEY_BYTES: Final = 16_384
MAX_PUBLIC_METADATA_BYTES: Final = 65_536
DEFAULT_IDENTITY_LOCK_TIMEOUT_SECONDS: Final = 10.0
MAX_IDENTITY_LOCK_TIMEOUT_SECONDS: Final = 60.0
IDENTITY_LOCK_POLL_SECONDS: Final = 0.05


class NodeIdentityStateError(FederationValidationError):
    """Persistent node identity is missing, malformed, or inconsistent."""


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _base64url_decode(value: str, *, field: str, expected_length: int) -> bytes:
    if not isinstance(value, str) or not value:
        raise NodeIdentityStateError("invalid-encoding", field, "must be base64url text")
    padded = value + ("=" * (-len(value) % 4))
    try:
        decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise NodeIdentityStateError(
            "invalid-encoding", field, "must be canonical base64url text"
        ) from exc
    if len(decoded) != expected_length or _base64url_encode(decoded) != value:
        raise NodeIdentityStateError(
            "invalid-encoding",
            field,
            f"must encode exactly {expected_length} bytes",
        )
    return decoded


def derive_node_id(raw_public_key: bytes) -> str:
    """Derive an opaque stable node ID from the complete raw Ed25519 key."""
    if not isinstance(raw_public_key, bytes) or len(raw_public_key) != 32:
        raise NodeIdentityStateError(
            "invalid-public-key", "public_key", "Ed25519 public keys must be 32 bytes"
        )
    digest = hashlib.sha256(raw_public_key).digest()
    return f"node-{_base64url_encode(digest)}"


def encode_public_key(public_key: Ed25519PublicKey) -> str:
    raw = public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return f"{PUBLIC_KEY_PREFIX}{_base64url_encode(raw)}"


def _decode_public_key(value: str) -> tuple[Ed25519PublicKey, bytes]:
    if not isinstance(value, str) or not value.startswith(PUBLIC_KEY_PREFIX):
        raise NodeIdentityStateError(
            "unsupported-key-algorithm",
            "public_key",
            "expected an Ed25519 public key",
        )
    raw = _base64url_decode(
        value.removeprefix(PUBLIC_KEY_PREFIX),
        field="public_key",
        expected_length=32,
    )
    return Ed25519PublicKey.from_public_bytes(raw), raw


def derive_node_id_from_public_key(public_key: str) -> str:
    """Derive the node ID from the encoded public identity representation."""

    _, raw = _decode_public_key(public_key)
    return derive_node_id(raw)


def verify_signature(public_key: str, message: bytes, signature: str) -> bool:
    """Verify a base64url Ed25519 signature without accepting key substitution."""
    if not isinstance(message, bytes):
        raise NodeIdentityStateError(
            "invalid-message", "message", "must be bytes"
        )
    verifier, _ = _decode_public_key(public_key)
    raw_signature = _base64url_decode(
        signature, field="signature", expected_length=64
    )
    try:
        verifier.verify(raw_signature, message)
    except InvalidSignature:
        return False
    return True


class NodeCredentials:
    """Signing boundary whose representation contains public identity only."""

    __slots__ = ("_private_key", "identity")

    def __init__(
        self, identity: NodeIdentity, private_key: Ed25519PrivateKey
    ) -> None:
        self.identity = identity
        self._private_key = private_key

    def __repr__(self) -> str:
        return f"NodeCredentials(node_id={self.identity.node_id!r})"

    def sign(self, message: bytes) -> str:
        if not isinstance(message, bytes):
            raise NodeIdentityStateError(
                "invalid-message", "message", "must be bytes"
            )
        return _base64url_encode(self._private_key.sign(message))

    def verify(self, message: bytes, signature: str) -> bool:
        return verify_signature(self.identity.public_key, message, signature)


def _restrict_directory(path: Path) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(path, 0o700)
    except OSError as exc:
        raise NodeIdentityStateError(
            "identity-permission-failed",
            "state_directory",
            "could not restrict identity state directory permissions",
        ) from exc


def _open_lock_file(path: Path) -> int:
    flags = os.O_RDWR | os.O_CREAT
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOINHERIT", 0)
    if os.name != "nt":
        flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags, 0o600)
        os.chmod(path, 0o600)
        if os.fstat(descriptor).st_size == 0:
            os.write(descriptor, b"\0")
            os.fsync(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        return descriptor
    except OSError as exc:
        if descriptor is not None:
            os.close(descriptor)
        raise NodeIdentityStateError(
            "identity-lock-failed",
            IDENTITY_LOCK_FILENAME,
            "could not open the persistent identity lock",
        ) from exc


def _try_lock_descriptor(descriptor: int) -> bool:
    os.lseek(descriptor, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            if exc.errno in {
                errno.EACCES,
                errno.EAGAIN,
                errno.EDEADLK,
                getattr(errno, "EDEADLOCK", errno.EDEADLK),
            }:
                return False
            raise
        return True

    import fcntl

    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def _unlock_descriptor(descriptor: int) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
        return

    import fcntl

    fcntl.flock(descriptor, fcntl.LOCK_UN)


@contextmanager
def _identity_lock(
    state_directory: Path, *, timeout_seconds: float
) -> Iterator[None]:
    """Serialize identity reads and creation across local processes."""

    _restrict_directory(state_directory)
    descriptor = _open_lock_file(state_directory / IDENTITY_LOCK_FILENAME)
    deadline = time.monotonic() + timeout_seconds
    acquired = False
    try:
        while not acquired:
            try:
                acquired = _try_lock_descriptor(descriptor)
            except OSError as exc:
                raise NodeIdentityStateError(
                    "identity-lock-failed",
                    IDENTITY_LOCK_FILENAME,
                    "could not acquire the persistent identity lock",
                ) from exc
            if acquired:
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise NodeIdentityStateError(
                    "identity-lock-timeout",
                    IDENTITY_LOCK_FILENAME,
                    "timed out waiting for another identity process",
                )
            time.sleep(min(IDENTITY_LOCK_POLL_SECONDS, remaining))
        yield
    finally:
        release_error: OSError | None = None
        try:
            if acquired:
                _unlock_descriptor(descriptor)
        except OSError as exc:
            release_error = exc
        finally:
            os.close(descriptor)
        if release_error is not None:
            raise NodeIdentityStateError(
                "identity-lock-release-failed",
                IDENTITY_LOCK_FILENAME,
                "could not release the persistent identity lock",
            ) from release_error


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, value: bytes, *, mode: int = 0o600) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.chmod(temporary, mode)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode)
        _fsync_directory(path.parent)
    except OSError as exc:
        raise NodeIdentityStateError(
            "identity-persistence-failed",
            path.name,
            "could not atomically persist identity state",
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _windows_dpapi_transform(value: bytes, *, protect: bool) -> bytes:
    """Protect or unprotect bytes with the current Windows user identity."""

    import ctypes
    from ctypes import wintypes

    class _DataBlob(ctypes.Structure):
        _fields_ = [
            ("size", wintypes.DWORD),
            ("data", ctypes.POINTER(ctypes.c_ubyte)),
        ]

    def blob(source: bytes) -> tuple[_DataBlob, ctypes.Array[ctypes.c_char]]:
        buffer = ctypes.create_string_buffer(source, len(source))
        return (
            _DataBlob(
                len(source),
                ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte)),
            ),
            buffer,
        )

    crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    input_blob, input_buffer = blob(value)
    entropy_blob, entropy_buffer = blob(WINDOWS_DPAPI_ENTROPY)
    output_blob = _DataBlob()
    blob_pointer = ctypes.POINTER(_DataBlob)
    common_arguments = [
        blob_pointer,
        ctypes.c_void_p,
        blob_pointer,
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.DWORD,
        blob_pointer,
    ]
    if protect:
        operation = crypt32.CryptProtectData
        operation.argtypes = common_arguments
        operation.restype = wintypes.BOOL
        result = operation(
            ctypes.byref(input_blob),
            ctypes.c_wchar_p("FCP federated node identity"),
            ctypes.byref(entropy_blob),
            None,
            None,
            0x01,  # CRYPTPROTECT_UI_FORBIDDEN
            ctypes.byref(output_blob),
        )
    else:
        operation = crypt32.CryptUnprotectData
        operation.argtypes = common_arguments
        operation.restype = wintypes.BOOL
        result = operation(
            ctypes.byref(input_blob),
            None,
            ctypes.byref(entropy_blob),
            None,
            None,
            0x01,  # CRYPTPROTECT_UI_FORBIDDEN
            ctypes.byref(output_blob),
        )
    # Keep input buffers alive through the native call.
    del input_buffer, entropy_buffer
    if not result:
        raise OSError(ctypes.get_last_error(), "Windows identity protection failed")
    try:
        return ctypes.string_at(output_blob.data, output_blob.size)
    finally:
        kernel32.LocalFree.argtypes = [ctypes.c_void_p]
        kernel32.LocalFree.restype = ctypes.c_void_p
        kernel32.LocalFree(ctypes.cast(output_blob.data, ctypes.c_void_p))


def _protect_private_bytes(value: bytes) -> bytes:
    if os.name != "nt":
        return value
    try:
        return WINDOWS_DPAPI_PREFIX + _windows_dpapi_transform(value, protect=True)
    except OSError as exc:
        raise NodeIdentityStateError(
            "identity-protection-failed",
            PRIVATE_KEY_FILENAME,
            "could not protect the private identity for the current Windows user",
        ) from exc


def _unprotect_private_bytes(value: bytes) -> bytes:
    if os.name != "nt":
        return value
    if not value.startswith(WINDOWS_DPAPI_PREFIX):
        raise NodeIdentityStateError(
            "malformed-private-key",
            PRIVATE_KEY_FILENAME,
            "private identity is not protected with Windows DPAPI",
        )
    try:
        return _windows_dpapi_transform(
            value.removeprefix(WINDOWS_DPAPI_PREFIX),
            protect=False,
        )
    except OSError as exc:
        raise NodeIdentityStateError(
            "identity-private-key-unavailable",
            PRIVATE_KEY_FILENAME,
            "private identity cannot be opened by the current Windows user",
        ) from exc


class IdentityStore:
    """Load or create one durable identity in a node-specific state directory."""

    def __init__(
        self,
        state_directory: Path | str,
        *,
        display_name: str,
        lock_timeout_seconds: float = DEFAULT_IDENTITY_LOCK_TIMEOUT_SECONDS,
    ) -> None:
        if not isinstance(display_name, str) or not display_name.strip():
            raise NodeIdentityStateError(
                "invalid-display-name",
                "display_name",
                "must be non-empty text",
            )
        if (
            isinstance(lock_timeout_seconds, bool)
            or not isinstance(lock_timeout_seconds, (int, float))
            or not math.isfinite(lock_timeout_seconds)
            or not 0 < lock_timeout_seconds <= MAX_IDENTITY_LOCK_TIMEOUT_SECONDS
        ):
            raise NodeIdentityStateError(
                "invalid-lock-timeout",
                "lock_timeout_seconds",
                (
                    "must be a finite positive number no greater than "
                    f"{MAX_IDENTITY_LOCK_TIMEOUT_SECONDS:g}"
                ),
            )
        self.state_directory = Path(state_directory)
        self.display_name = display_name
        self.lock_timeout_seconds = float(lock_timeout_seconds)
        self.private_key_path = self.state_directory / PRIVATE_KEY_FILENAME
        self.public_identity_path = (
            self.state_directory / PUBLIC_IDENTITY_FILENAME
        )

    def load_or_create(
        self, *, now: datetime | None = None
    ) -> NodeCredentials:
        with _identity_lock(
            self.state_directory, timeout_seconds=self.lock_timeout_seconds
        ):
            private_exists = self.private_key_path.exists()
            public_exists = self.public_identity_path.exists()
            if private_exists or public_exists:
                return self._load_unlocked()
            return self._create_unlocked(now=now)

    def create(self, *, now: datetime | None = None) -> NodeCredentials:
        with _identity_lock(
            self.state_directory, timeout_seconds=self.lock_timeout_seconds
        ):
            return self._create_unlocked(now=now)

    def _create_unlocked(
        self, *, now: datetime | None = None
    ) -> NodeCredentials:
        _restrict_directory(self.state_directory)
        if self.private_key_path.exists() or self.public_identity_path.exists():
            raise NodeIdentityStateError(
                "identity-state-exists",
                "identity",
                "identity state already exists",
            )
        created_at = now or datetime.now(timezone.utc)
        private_key = Ed25519PrivateKey.generate()
        public_key = private_key.public_key()
        raw_public_key = public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        identity = NodeIdentity(
            node_id=derive_node_id(raw_public_key),
            display_name=self.display_name,
            public_key=encode_public_key(public_key),
            created_at=created_at,
            identity_version=1,
        )
        private_bytes = _protect_private_bytes(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
        metadata = {
            **identity.to_dict(),
            "key_algorithm": "Ed25519",
            "private_key_file": PRIVATE_KEY_FILENAME,
            "private_key_protection": (
                "windows-dpapi-current-user"
                if os.name == "nt"
                else "filesystem-permissions"
            ),
        }
        metadata_bytes = (
            json.dumps(
                metadata,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            + b"\n"
        )
        _atomic_write(self.private_key_path, private_bytes)
        _atomic_write(self.public_identity_path, metadata_bytes)
        return NodeCredentials(identity, private_key)

    def load(self) -> NodeCredentials:
        if not self.state_directory.exists():
            raise NodeIdentityStateError(
                "identity-state-missing",
                "identity",
                "persistent identity has not been created",
            )
        with _identity_lock(
            self.state_directory, timeout_seconds=self.lock_timeout_seconds
        ):
            return self._load_unlocked()

    def _load_unlocked(self) -> NodeCredentials:
        private_exists = self.private_key_path.is_file()
        public_exists = self.public_identity_path.is_file()
        if not private_exists and not public_exists:
            raise NodeIdentityStateError(
                "identity-state-missing",
                "identity",
                "persistent identity has not been created",
            )
        if private_exists != public_exists:
            missing = (
                PRIVATE_KEY_FILENAME if not private_exists else PUBLIC_IDENTITY_FILENAME
            )
            raise NodeIdentityStateError(
                "identity-state-incomplete",
                missing,
                "identity state is incomplete and will not be regenerated",
            )
        _restrict_directory(self.state_directory)
        protected_private_bytes = self._read_bounded(
            self.private_key_path,
            maximum=MAX_PRIVATE_KEY_BYTES,
            field=PRIVATE_KEY_FILENAME,
        )
        private_bytes = _unprotect_private_bytes(protected_private_bytes)
        metadata_bytes = self._read_bounded(
            self.public_identity_path,
            maximum=MAX_PUBLIC_METADATA_BYTES,
            field=PUBLIC_IDENTITY_FILENAME,
        )
        try:
            loaded_key = serialization.load_pem_private_key(
                private_bytes, password=None
            )
        except (TypeError, ValueError) as exc:
            raise NodeIdentityStateError(
                "malformed-private-key",
                PRIVATE_KEY_FILENAME,
                "private identity is not a valid unencrypted PKCS8 key",
            ) from exc
        if not isinstance(loaded_key, Ed25519PrivateKey):
            raise NodeIdentityStateError(
                "unsupported-private-key",
                PRIVATE_KEY_FILENAME,
                "private identity must use Ed25519",
            )
        try:
            raw_metadata = json.loads(metadata_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeIdentityStateError(
                "malformed-public-identity",
                PUBLIC_IDENTITY_FILENAME,
                "public identity metadata is not valid JSON",
            ) from exc
        try:
            identity = NodeIdentity.from_dict(raw_metadata)
        except FederationValidationError as exc:
            raise NodeIdentityStateError(
                "malformed-public-identity",
                exc.field,
                "public identity metadata does not satisfy fcp.node.v1",
            ) from exc
        _, stored_raw_public = _decode_public_key(identity.public_key)
        actual_raw_public = loaded_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        expected_node_id = derive_node_id(stored_raw_public)
        if (
            not hmac.compare_digest(stored_raw_public, actual_raw_public)
            or not hmac.compare_digest(identity.node_id, expected_node_id)
        ):
            raise NodeIdentityStateError(
                "identity-state-replaced",
                "identity",
                "private key, public identity, and node ID are not the same identity",
            )
        try:
            os.chmod(self.private_key_path, 0o600)
            os.chmod(self.public_identity_path, 0o600)
        except OSError as exc:
            raise NodeIdentityStateError(
                "identity-permission-failed",
                "identity",
                "could not restrict identity file permissions",
            ) from exc
        return NodeCredentials(identity, loaded_key)

    @staticmethod
    def _read_bounded(path: Path, *, maximum: int, field: str) -> bytes:
        try:
            if path.stat().st_size > maximum:
                raise NodeIdentityStateError(
                    "identity-file-too-large", field, "identity file exceeds its bound"
                )
            return path.read_bytes()
        except NodeIdentityStateError:
            raise
        except OSError as exc:
            raise NodeIdentityStateError(
                "identity-state-unreadable",
                field,
                "identity state could not be read",
            ) from exc
