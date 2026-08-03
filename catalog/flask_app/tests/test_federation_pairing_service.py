from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.capabilities.benchmarking import OLLAMA_BENCHMARK_ID
from catalog.federation.errors import (
    AuthenticationError,
    FederationValidationError,
)
from catalog.federation.onboarding_compat import federation_id_from_session_id
from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationSessionBinding,
)
from catalog.flask_app import federation_pairing_routes
from catalog.flask_app.app import create_app
from catalog.flask_app.capability_onboarding_routes import _CSRF_SESSION_KEY
from catalog.flask_app.services.federation_pairing_service import (
    PAIRING_CODE_PREFIX,
    PairingCodeCodec,
    RemotePairingState,
    RemotePairingStore,
)
from catalog.node.identity import IdentityStore
from reset_msh import reset_repository_state

NOW = datetime(2026, 8, 3, 18, 0, tzinfo=timezone.utc)
SESSION_ID = "session-pairing-one"


def _credentials(tmp_path: Path):
    return IdentityStore(
        tmp_path / "host-identity",
        display_name="Pairing host",
    ).load_or_create(now=NOW)


def _code(tmp_path: Path, *, clock=lambda: NOW) -> tuple[PairingCodeCodec, str]:
    codec = PairingCodeCodec(clock=clock)
    code = codec.encode(
        credentials=_credentials(tmp_path),
        relay_url="ws://192.168.10.10:8765",
        federation_id=federation_id_from_session_id(SESSION_ID),
        internal_session_id=SESSION_ID,
        enrollment_token="enrollment-secret",
        invitation_token="invitation-secret",
        ttl_seconds=300,
    )
    return codec, code


def test_pairing_code_round_trip_is_signed_and_secret_safe_in_repr(
    tmp_path: Path,
) -> None:
    codec, code = _code(tmp_path)

    offer = codec.decode(code)

    assert code.startswith(PAIRING_CODE_PREFIX)
    assert offer.relay_url == "ws://192.168.10.10:8765"
    assert offer.internal_session_id == SESSION_ID
    assert offer.enrollment_token == "enrollment-secret"
    assert offer.invitation_token == "invitation-secret"
    assert "enrollment-secret" not in repr(offer)
    assert "invitation-secret" not in repr(offer)


def test_pairing_code_rejects_tampering(tmp_path: Path) -> None:
    codec, code = _code(tmp_path)
    replacement = "A" if code[-1] != "A" else "B"

    with pytest.raises((AuthenticationError, FederationValidationError)):
        codec.decode(code[:-1] + replacement)


def test_pairing_code_expires(tmp_path: Path) -> None:
    _codec, code = _code(tmp_path)
    expired = PairingCodeCodec(clock=lambda: NOW + timedelta(minutes=6))

    with pytest.raises(AuthenticationError) as error:
        expired.decode(code)

    assert error.value.code == "pairing-code-expired"


def test_remote_pairing_store_persists_no_one_use_tokens(tmp_path: Path) -> None:
    binding = FederationSessionBinding(
        federation_id=federation_id_from_session_id(SESSION_ID),
        internal_session_id=SESSION_ID,
        device_id=_credentials(tmp_path).identity.node_id,
        state=FederationConnectionState.CONNECTED,
        revision=2,
        trusted=True,
        created_at=NOW,
        last_verified_at=NOW,
    )
    store = RemotePairingStore(tmp_path / "remote-pairing.json")

    store.save(
        RemotePairingState(
            relay_url="ws://192.168.10.10:8765",
            binding=binding,
        )
    )

    persisted = store.path.read_text(encoding="utf-8")
    loaded = store.load()
    assert loaded is not None
    assert loaded.binding == binding
    assert "enrollment-secret" not in persisted
    assert "invitation-secret" not in persisted


def test_pairing_routes_and_ui_are_registered(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    app = create_app()
    app.config.update(TESTING=True)
    rules = {str(rule): rule.methods for rule in app.url_map.iter_rules()}

    assert rules["/onboarding/federation/pairing-code"] == {
        "POST",
        "OPTIONS",
    }
    assert rules["/onboarding/federation/pair"] == {"POST", "OPTIONS"}

    page = app.test_client().get("/onboarding?step=federation")
    body = page.get_data(as_text=True)
    assert page.status_code == 200
    assert (
        "Create pairing code" in body
        or "Pairing code from the other MSH device" in body
    )
    assert "A public device ID alone never grants membership" in body


def test_pairing_code_route_accepts_the_onboarding_csrf_token(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    app = create_app()
    app.config.update(
        TESTING=True,
        CAPABILITY_ONBOARDING_PAIRING_RELAY_URL="ws://192.168.10.10:8765",
    )
    calls: list[str] = []

    class PairingServiceStub:
        def create_pairing_code(self, *, relay_url: str) -> str:
            calls.append(relay_url)
            return "MSH1-test-code"

    monkeypatch.setattr(
        federation_pairing_routes,
        "_pairing_service",
        lambda: PairingServiceStub(),
    )
    client = app.test_client()
    page = client.get("/onboarding?step=federation")
    assert page.status_code == 200
    with client.session_transaction() as browser:
        token = browser[_CSRF_SESSION_KEY]

    response = client.post(
        "/onboarding/federation/pairing-code",
        data={"_csrf_token": token},
        follow_redirects=True,
    )

    assert response.status_code == 200
    with client.session_transaction() as browser:
        flashes = tuple(browser.get("_flashes", ()))
    assert calls == ["ws://192.168.10.10:8765"], flashes
    assert "Pairing code created" in response.get_data(as_text=True)


def test_fresh_install_registers_ollama_check_before_legacy_setup(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    app = create_app()
    app.config["CAPABILITY_ONBOARDING_SETUP_LOADER"] = lambda: {
        "configured": False,
        "user_setup_complete": False,
        "ai_enabled": True,
        "ai_model": "llama3.2:3b",
        "ollama_base_url": "http://ollama:11434",
    }

    with app.app_context():
        factory = app.config["CAPABILITY_ONBOARDING_INSPECTION_ADAPTERS"]
        adapters = factory()

    benchmark_ids = {
        adapter.definition.benchmark_id
        for adapter in adapters
        if hasattr(adapter, "definition")
    }
    assert OLLAMA_BENCHMARK_ID in benchmark_ids


def test_fresh_reset_preserves_only_recorder_jsonl(tmp_path: Path) -> None:
    (tmp_path / "docker-compose.yml").write_text("services: {}\n", encoding="utf-8")
    preserved = (
        tmp_path
        / "data"
        / "sources"
        / "mtconnect_recorder"
        / "jsonl"
        / "machine-one"
        / "batch.jsonl"
    )
    preserved.parent.mkdir(parents=True)
    preserved.write_text('{"sequence":1}\n', encoding="utf-8")
    unwanted_nearby = preserved.with_suffix(".txt")
    unwanted_nearby.write_text("remove", encoding="utf-8")
    federation_state = tmp_path / "data" / "federation" / "onboarding.sqlite3"
    federation_state.parent.mkdir(parents=True)
    federation_state.write_bytes(b"state")
    recorder_state = tmp_path / "data" / "source_state" / "recorder.json"
    recorder_state.parent.mkdir(parents=True)
    recorder_state.write_text("{}", encoding="utf-8")
    result = tmp_path / "results" / "derived.jsonl"
    result.parent.mkdir(parents=True)
    result.write_text("derived\n", encoding="utf-8")

    summary = reset_repository_state(tmp_path, stop_compose=False)

    assert preserved.read_text(encoding="utf-8") == '{"sequence":1}\n'
    assert not unwanted_nearby.exists()
    assert not federation_state.exists()
    assert not recorder_state.exists()
    assert not result.exists()
    assert summary.preserved_jsonl_files == 1
