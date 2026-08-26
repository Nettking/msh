"""B09: unreadable pairing state must not answer "this device is not a member".

``saved_remote_member`` is the one signal that keeps an established Federation
member from behaving like a fresh standalone installation. Four separate
boundaries are anchored on it: device-local password login, local user
administration, first-run human-admin bootstrap, and the pre-auth pairing /
enrollment endpoints.

``RemotePairingStore.load`` returns ``None`` only when the binding file is
absent; a file that exists but cannot be read or validated raises. Collapsing
both into ``False`` reopened every one of those boundaries on a paired device
whose binding file was merely unreadable -- and it did so *behind* the callers'
own ``except ... - must fail closed`` guards, which never saw a failure because
it had already been turned into an answer.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from flask import Blueprint, Flask
from werkzeug.exceptions import HTTPException

from catalog.federation.errors import FederationValidationError
from catalog.flask_app.auth import federation as human_auth
from catalog.flask_app.auth import federation_enrollment
from catalog.flask_app.auth import policy as auth_policy
from catalog.flask_app.auth import routes as auth_routes
from catalog.flask_app.auth.extension import init_human_auth
from catalog.flask_app.services.federation_pairing_service import (
    PAIRING_STATE_SCHEMA,
    RemotePairingStore,
)

PAIR_ENDPOINT = "federation_pairing_web.pair_device"
BOOTSTRAP_ENDPOINT = "auth_users.bootstrap_user"


@pytest.fixture()
def member_app(tmp_path, monkeypatch) -> Flask:
    monkeypatch.delenv("FCP_AUTH_DISABLED", raising=False)
    monkeypatch.setenv("FCP_FLASK_SECRET", "s" * 48)
    monkeypatch.setenv("FCP_PASSWORD_SALT", "p" * 48)
    monkeypatch.setenv("FCP_AUTH_DATABASE", str(tmp_path / "users.sqlite3"))
    app = Flask(__name__, template_folder="../templates")
    app.testing = True
    app.config["WTF_CSRF_ENABLED"] = False
    init_human_auth(app)
    pairing = Blueprint("federation_pairing_web", __name__)
    pairing.add_url_rule(
        "/onboarding/federation/pair",
        endpoint="pair_device",
        view_func=lambda: "paired",
        methods=["POST"],
    )
    app.register_blueprint(pairing)
    return app


def _unreadable_pairing_state(tmp_path: Path) -> RemotePairingStore:
    """A binding file that exists on disk but cannot be validated."""

    path = tmp_path / "remote_pairing.json"
    path.write_bytes(
        b'{"schema": "' + PAIRING_STATE_SCHEMA.encode("ascii") + b'", "relay_'
    )
    store = RemotePairingStore(path)
    with pytest.raises(FederationValidationError):
        store.load()
    return store


def _install_store(monkeypatch, store: object | None) -> None:
    import catalog.flask_app.services.capability_onboarding_service as onboarding_module

    class _Onboarding:
        remote_store = store

    monkeypatch.setattr(
        onboarding_module,
        "get_capability_onboarding_service",
        lambda: _Onboarding(),
    )


def test_an_unreadable_binding_still_reports_an_established_member(
    tmp_path, monkeypatch
) -> None:
    _install_store(monkeypatch, _unreadable_pairing_state(tmp_path))
    assert human_auth.saved_remote_member() is True


def test_an_absent_binding_still_reports_a_standalone_installation(
    tmp_path, monkeypatch
) -> None:
    _install_store(monkeypatch, RemotePairingStore(tmp_path / "missing.json"))
    assert human_auth.saved_remote_member() is False


def test_a_readable_binding_is_unaffected(tmp_path, monkeypatch) -> None:
    from datetime import datetime, timezone

    from catalog.federation.onboarding_models import (
        FederationConnectionState,
        FederationSessionBinding,
    )
    from catalog.flask_app.services.federation_pairing_service import (
        RemotePairingState,
    )

    store = RemotePairingStore(tmp_path / "remote_pairing.json")
    store.save(
        RemotePairingState(
            relay_url="wss://relay.example",
            binding=FederationSessionBinding(
                federation_id="fed-1",
                internal_session_id="session-1",
                device_id="node-1",
                state=FederationConnectionState.CONNECTED,
                revision=1,
                trusted=True,
                created_at=datetime(2026, 8, 24, tzinfo=timezone.utc),
                last_verified_at=datetime(2026, 8, 24, tzinfo=timezone.utc),
            ),
        )
    )
    _install_store(monkeypatch, store)
    assert human_auth.saved_remote_member() is True


def test_a_device_with_no_pairing_capability_is_not_forced_into_member_mode(
    monkeypatch,
) -> None:
    """Preserved: a standalone install must not be locked out of local sign-in."""

    _install_store(monkeypatch, None)
    assert human_auth.saved_remote_member() is False


def test_an_onboarding_service_that_cannot_be_built_is_not_evidence_of_pairing(
    monkeypatch,
) -> None:
    """Preserved availability: no positive evidence means no member claim."""

    import catalog.flask_app.services.capability_onboarding_service as onboarding_module

    def _explode():
        raise RuntimeError("onboarding service unavailable")

    monkeypatch.setattr(
        onboarding_module, "get_capability_onboarding_service", _explode
    )
    assert human_auth.saved_remote_member() is False


def test_an_unreadable_binding_still_blocks_device_local_password_login(
    member_app, tmp_path, monkeypatch
) -> None:
    """The consequence the docstring promises to prevent."""

    _install_store(monkeypatch, _unreadable_pairing_state(tmp_path))
    with member_app.test_request_context("/login", method="POST"):
        from flask import request

        assert request.endpoint == "security.login"
        blocked = human_auth.guard_member_local_password_login()
    assert blocked is not None, "device-local password login was re-enabled"


def test_an_unreadable_binding_still_blocks_local_user_administration(
    member_app, tmp_path, monkeypatch
) -> None:
    _install_store(monkeypatch, _unreadable_pairing_state(tmp_path))
    with (
        member_app.test_request_context("/"),
        pytest.raises(HTTPException) as refused,
    ):
        auth_routes._require_local_user_authority()
    assert refused.value.code == 403


def test_an_unreadable_binding_is_not_a_fresh_installation_needing_setup(
    member_app, tmp_path, monkeypatch
) -> None:
    """An established member must not be pushed into first-run admin bootstrap."""

    _install_store(monkeypatch, _unreadable_pairing_state(tmp_path))
    with member_app.test_request_context("/"):
        assert auth_routes.first_user_bootstrap_gate() is None


def test_an_unreadable_binding_does_not_open_the_pre_auth_pairing_endpoint(
    member_app, tmp_path, monkeypatch
) -> None:
    _install_store(monkeypatch, _unreadable_pairing_state(tmp_path))
    with member_app.test_request_context("/"):
        assert (
            auth_policy.pre_auth_federation_bootstrap_allowed(PAIR_ENDPOINT) is False
        )


def test_an_unreadable_binding_is_not_a_fresh_enrollment_device(
    member_app, tmp_path, monkeypatch
) -> None:
    _install_store(monkeypatch, _unreadable_pairing_state(tmp_path))
    with member_app.test_request_context("/"):
        assert federation_enrollment._is_fresh_device() is False
