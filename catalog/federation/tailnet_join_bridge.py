"""Shared pieces of the host-side automatic Federation join.

Automatic joining is split across a trust boundary on purpose:

* the **responder** runs on the host, where ``tailscaled`` can say who the
  connecting peer really is (see :mod:`tailscale_peer_identity`);
* the **web application** runs in a container and owns the Federation pairing
  authority, but it can see neither the tailnet nor the peer's real address.

The responder therefore verifies identity and then asks the application for one
ordinary pairing grant. Nothing here mints membership: the grant is created by
the existing pairing authority, with its existing signature, one-use enrollment,
one-use invitation, and bounded lifetime.

The two sides authenticate to each other with a secret written into the mounted
data directory, which only host processes and the application container can
read. A tailnet peer that can reach the published web port still cannot forge a
grant request, because it cannot read that file. The secret is mutable state and
is therefore removed by ``--fresh`` along with everything else under ``data``.
"""

from __future__ import annotations

import json
import os
import secrets
import stat
from pathlib import Path

SECRET_ENV = "FCP_AUTO_JOIN_SECRET_FILE"
GRANT_ENV = "FCP_AUTO_JOIN_GRANT_FILE"
PORT_ENV = "FCP_AUTO_JOIN_PORT"
DEFAULT_SECRET_FILE = "data/federation/onboarding/auto_join_secret"
DEFAULT_GRANT_FILE = "data/federation/onboarding/auto_join_grant.json"
DEFAULT_AUTO_JOIN_PORT = 5151
SECRET_HEADER = "X-FCP-Auto-Join-Secret"
GRANT_SCHEMA = "fcp.federation.tailnet-auto-join-grant.v1"
GRANT_REQUEST_SCHEMA = "fcp.federation.tailnet-auto-join-request.v1"
SECRET_BYTES = 32
MAX_SECRET_BYTES = 4096


def auto_join_port(environ: dict[str, str] | None = None) -> int:
    """Return the bounded port the host responder listens on."""

    values = os.environ if environ is None else environ
    raw = str(values.get(PORT_ENV, "") or DEFAULT_AUTO_JOIN_PORT)
    try:
        port = int(raw)
    except ValueError:
        return DEFAULT_AUTO_JOIN_PORT
    if not 1 <= port <= 65_535:
        return DEFAULT_AUTO_JOIN_PORT
    return port


def secret_path(environ: dict[str, str] | None = None) -> Path:
    values = os.environ if environ is None else environ
    return Path(values.get(SECRET_ENV) or DEFAULT_SECRET_FILE)


def grant_path(environ: dict[str, str] | None = None) -> Path:
    values = os.environ if environ is None else environ
    return Path(values.get(GRANT_ENV) or DEFAULT_GRANT_FILE)


def read_secret(path: Path | str) -> str:
    """Return the shared secret, or an empty string when it is unusable."""

    candidate = Path(path)
    try:
        if candidate.stat().st_size > MAX_SECRET_BYTES:
            return ""
        value = candidate.read_text(encoding="utf-8").strip()
    except (OSError, ValueError, UnicodeDecodeError):
        return ""
    return value if len(value) >= 32 else ""


def ensure_secret(path: Path | str) -> str:
    """Create the shared secret once, then return it on every later call."""

    candidate = Path(path)
    existing = read_secret(candidate)
    if existing:
        return existing
    candidate.parent.mkdir(parents=True, exist_ok=True)
    value = secrets.token_urlsafe(SECRET_BYTES)
    # Write private, then move into place so a reader never sees a partial file.
    temporary = candidate.with_name(candidate.name + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    try:
        os.chmod(temporary, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        # Windows filesystems may not honour POSIX modes; the mounted data
        # directory is still the boundary that protects this file.
        pass
    os.replace(temporary, candidate)
    return value


def store_grant(path: Path | str, code: str) -> None:
    """Hand one pairing grant to the local application, private to this host."""

    candidate = Path(path)
    candidate.parent.mkdir(parents=True, exist_ok=True)
    temporary = candidate.with_name(candidate.name + ".tmp")
    temporary.write_text(
        json.dumps({"schema": GRANT_SCHEMA, "pairing_code": code}),
        encoding="utf-8",
    )
    try:
        os.chmod(temporary, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass
    os.replace(temporary, candidate)


def load_grant(path: Path | str) -> str:
    """Return a stored pairing grant, or an empty string when there is none."""

    candidate = Path(path)
    try:
        if candidate.stat().st_size > MAX_SECRET_BYTES * 16:
            return ""
        payload = json.loads(candidate.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return ""
    if not isinstance(payload, dict) or payload.get("schema") != GRANT_SCHEMA:
        return ""
    code = payload.get("pairing_code")
    return code.strip() if isinstance(code, str) and code.strip() else ""


def clear_grant(path: Path | str) -> None:
    """Remove a stored grant. A grant is one-use and must not be retried."""

    try:
        Path(path).unlink(missing_ok=True)
    except OSError:
        pass


__all__ = [
    "DEFAULT_AUTO_JOIN_PORT",
    "GRANT_REQUEST_SCHEMA",
    "GRANT_SCHEMA",
    "SECRET_HEADER",
    "auto_join_port",
    "clear_grant",
    "ensure_secret",
    "grant_path",
    "load_grant",
    "read_secret",
    "secret_path",
    "store_grant",
]
