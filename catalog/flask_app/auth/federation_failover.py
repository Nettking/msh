"""Human-auth integration for replicated Federation leader failover.

Legacy/standalone installations retain creator-backed semantics. When a
coordinator exposes durable leadership, credential/sign-in authority follows
the current fenced operational leader while immutable creator provenance stays
unchanged.
"""

from __future__ import annotations

import json
from pathlib import Path

from flask import current_app

from catalog.federation.control_plane_runtime import AUTH_GENERATION_FILE
from catalog.federation.errors import FederationOperationError

from .federation import FederationHumanAuthService
from .models import db

_SHARED_AUTH_DIRECTORY = Path("/app/data/auth")


class CurrentLeaderFederationHumanAuthService(FederationHumanAuthService):
    """Resolve credential/sign-in authority from durable Federation leadership."""

    def _leader_node_id(self, context: object) -> str | None:
        _federation_id, session_id, _node_id, _credentials, coordinator = (
            self._context_parts(context)
        )
        resolver = getattr(coordinator, "session_leadership", None)
        if callable(resolver):
            try:
                leadership = resolver(session_id)
            except FederationOperationError:
                raise
            except Exception as exc:  # noqa: BLE001 - authority resolution must fail closed
                raise FederationOperationError(
                    "human-auth-leadership-unavailable",
                    "the current Federation human-auth authority cannot be resolved",
                    "session_id",
                ) from exc
            value = getattr(leadership, "leader_node_id", None)
            if isinstance(value, str) and value:
                return value
            raise FederationOperationError(
                "human-auth-leadership-unavailable",
                "the current Federation human-auth authority is missing",
                "session_id",
            )
        return super()._leader_node_id(context)


class HumanAuthReplicaGenerationWatcher:
    """Refresh Flask auth state after the relay restores a credential snapshot."""

    def __init__(
        self,
        generation_file: Path | str = _SHARED_AUTH_DIRECTORY / AUTH_GENERATION_FILE,
        password_salt_file: Path | str = _SHARED_AUTH_DIRECTORY / "password-salt",
    ) -> None:
        self.generation_file = Path(generation_file)
        self.password_salt_file = Path(password_salt_file)
        self._generation: tuple[int, str] | None = None
        self._primed = False

    def _read_generation(self) -> tuple[int, str] | None:
        try:
            value = json.loads(self.generation_file.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        if (
            not isinstance(value, dict)
            or value.get("schema") != "fcp.human-auth.replica-generation.v1"
            or isinstance(value.get("version"), bool)
            or not isinstance(value.get("version"), int)
            or value["version"] < 0
            or not isinstance(value.get("snapshot_id"), str)
            or not value["snapshot_id"]
        ):
            return None
        return int(value["version"]), str(value["snapshot_id"])

    def prime(self) -> None:
        """Record the generation visible when the Flask app opens its DB."""
        self._generation = self._read_generation()
        self._primed = True

    def refresh_if_changed(self) -> bool:
        if not self._primed:
            self.prime()
            return False
        generation = self._read_generation()
        if generation is None or generation == self._generation:
            return False

        try:
            salt = self.password_salt_file.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise FederationOperationError(
                "human-auth-replica-salt-unavailable",
                "the restored human-auth password salt is unavailable",
            ) from exc
        if len(salt) < 32:
            raise FederationOperationError(
                "human-auth-replica-salt-invalid",
                "the restored human-auth password salt is invalid",
            )

        # SQLAlchemy can otherwise retain an open connection to the SQLite inode
        # that was atomically replaced by credential recovery.
        db.session.remove()
        db.engine.dispose()
        current_app.config["SECURITY_PASSWORD_SALT"] = salt
        self._generation = generation
        return True


__all__ = [
    "CurrentLeaderFederationHumanAuthService",
    "HumanAuthReplicaGenerationWatcher",
]
