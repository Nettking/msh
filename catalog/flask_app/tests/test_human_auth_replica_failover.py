"""Human-auth continuity checks for C03 credential replica restoration."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from flask import Flask

from catalog.federation.errors import FederationOperationError
from catalog.flask_app.auth import federation_failover
from catalog.flask_app.auth.federation_failover import HumanAuthReplicaGenerationWatcher


def _generation(path: Path, *, version: int, snapshot_id: str) -> None:
    path.write_text(
        json.dumps(
            {
                "schema": "fcp.human-auth.replica-generation.v1",
                "federation_id": "fed-a",
                "session_id": "session-a",
                "snapshot_id": snapshot_id,
                "version": version,
                "term": 3,
                "leader_id": "node-successor",
            }
        ),
        encoding="utf-8",
    )


class _Calls:
    def __init__(self) -> None:
        self.removed = 0
        self.disposed = 0

    def remove(self) -> None:
        self.removed += 1

    def dispose(self) -> None:
        self.disposed += 1


def _fake_db(calls: _Calls) -> SimpleNamespace:
    return SimpleNamespace(
        session=SimpleNamespace(remove=calls.remove),
        engine=SimpleNamespace(dispose=calls.dispose),
    )


def test_absent_to_committed_generation_reloads_restored_database_and_salt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation = tmp_path / "human-auth-replica-generation.json"
    salt_file = tmp_path / "password-salt"
    watcher = HumanAuthReplicaGenerationWatcher(generation, salt_file)
    calls = _Calls()
    monkeypatch.setattr(federation_failover, "db", _fake_db(calls))

    app = Flask(__name__)
    app.config["SECURITY_PASSWORD_SALT"] = "old-salt-" + "x" * 40
    with app.app_context():
        watcher.prime()
        assert watcher.refresh_if_changed() is False

        restored_salt = "restored-" + "s" * 48
        salt_file.write_text(restored_salt + "\n", encoding="utf-8")
        _generation(generation, version=4, snapshot_id="snapshot-committed-4")

        assert watcher.refresh_if_changed() is True
        assert calls.removed == 1
        assert calls.disposed == 1
        assert app.config["SECURITY_PASSWORD_SALT"] == restored_salt

        # The same committed generation is idempotent and does not repeatedly
        # tear down SQLAlchemy connections on every request.
        assert watcher.refresh_if_changed() is False
        assert calls.removed == 1
        assert calls.disposed == 1


def test_changed_generation_with_invalid_restored_salt_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation = tmp_path / "human-auth-replica-generation.json"
    salt_file = tmp_path / "password-salt"
    watcher = HumanAuthReplicaGenerationWatcher(generation, salt_file)
    calls = _Calls()
    monkeypatch.setattr(federation_failover, "db", _fake_db(calls))

    app = Flask(__name__)
    app.config["SECURITY_PASSWORD_SALT"] = "unchanged-" + "x" * 40
    with app.app_context():
        watcher.prime()
        salt_file.write_text("too-short\n", encoding="utf-8")
        _generation(generation, version=1, snapshot_id="snapshot-invalid")

        with pytest.raises(
            FederationOperationError,
            match="restored human-auth password salt is invalid",
        ):
            watcher.refresh_if_changed()

        assert calls.removed == 0
        assert calls.disposed == 0
        assert app.config["SECURITY_PASSWORD_SALT"].startswith("unchanged-")


def test_missing_or_malformed_generation_never_claims_a_restore(tmp_path: Path) -> None:
    generation = tmp_path / "human-auth-replica-generation.json"
    salt_file = tmp_path / "password-salt"
    watcher = HumanAuthReplicaGenerationWatcher(generation, salt_file)
    watcher.prime()

    generation.write_text("{not-json", encoding="utf-8")
    assert watcher.refresh_if_changed() is False

    generation.write_text(
        json.dumps(
            {
                "schema": "fcp.human-auth.replica-generation.v1",
                "version": -1,
                "snapshot_id": "bad",
            }
        ),
        encoding="utf-8",
    )
    assert watcher.refresh_if_changed() is False


@pytest.mark.parametrize("replicated", [False, True])
def test_human_auth_leadership_respects_the_configured_credential_boundary(
    monkeypatch: pytest.MonkeyPatch, replicated: bool
) -> None:
    monkeypatch.delenv("FCP_REPLICATED_CONTROL_PLANE_CONFIG", raising=False)
    coordinator = SimpleNamespace(
        store=SimpleNamespace(
            get_session=lambda _session: SimpleNamespace(created_by_node_id="creator")
        ),
        session_leadership=lambda _session: SimpleNamespace(leader_node_id="successor"),
    )
    context = SimpleNamespace(
        binding=SimpleNamespace(internal_session_id="session", federation_id="fed"),
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id="creator")),
        coordinator=coordinator,
    )
    service = federation_failover.CurrentLeaderFederationHumanAuthService()
    app = Flask(__name__)
    if replicated:
        app.config["FCP_REPLICATED_CONTROL_PLANE_CONFIG"] = "/configured/control.json"
    with app.app_context():
        assert service._leader_node_id(context) == ("successor" if replicated else "creator")
        assert service._is_authority_context(context) is not replicated
        context.credentials.identity.node_id = "successor"
        assert service._is_authority_context(context) is replicated

        def unavailable(_session):
            raise RuntimeError("authority unavailable")

        coordinator.session_leadership = unavailable
        if replicated:
            with pytest.raises(FederationOperationError) as error:
                service._leader_node_id(context)
            assert error.value.code == "human-auth-leadership-unavailable"
        else:
            # An operational leader does not carry the legacy creator's passwords.
            assert service._leader_node_id(context) == "creator"
            monkeypatch.setenv("FCP_REPLICATED_CONTROL_PLANE_CONFIG", "/environment.json")
            app.config["FCP_REPLICATED_CONTROL_PLANE_CONFIG"] = ""
            assert service._leader_node_id(context) == "creator"
