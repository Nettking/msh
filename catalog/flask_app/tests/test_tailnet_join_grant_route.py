"""The grant endpoint delegates to the pairing authority and never mints trust.

It is reachable without a browser session because the host responder has none,
so its only gate is a secret written into the mounted data directory. A tailnet
peer that can reach the published port cannot read that file.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from flask import Flask

from catalog.federation.tailnet_join_bridge import SECRET_HEADER, ensure_secret
from catalog.flask_app import federation_pairing_routes as pairing_routes
from catalog.flask_app.auth.policy import PUBLIC_ENDPOINTS

GRANT = "FCP1-test-grant-not-real"


def _service(*, remote=None, code=GRANT, calls=None):
    def create_pairing_code(*, relay_url, ttl_seconds, remember=True):
        if calls is not None:
            calls.append({"remember": remember, "ttl_seconds": ttl_seconds})
        return code

    return SimpleNamespace(
        remote_store=SimpleNamespace(load=lambda: remote),
        create_pairing_code=create_pairing_code,
    )


def _call(monkeypatch, tmp_path: Path, *, secret_value, service=None, calls=None):
    secret_file = tmp_path / "auto_join_secret"
    real = ensure_secret(secret_file)
    monkeypatch.setattr(pairing_routes, "secret_path", lambda: secret_file)
    monkeypatch.setattr(
        pairing_routes,
        "_pairing_service",
        lambda: _service(calls=calls) if service is None else service,
    )
    monkeypatch.setattr(pairing_routes, "_relay_url", lambda: "ws://100.90.80.70:8765")

    headers = {}
    if secret_value is not None:
        headers[SECRET_HEADER] = real if secret_value == "correct" else secret_value

    app = Flask(__name__)
    with app.test_request_context(
        "/internal/federation/tailnet-join-grant",
        method="POST",
        headers=headers,
        json={"peer_node_name": "laptop.tail0abc.ts.net"},
    ):
        return pairing_routes.tailnet_join_grant()


def test_the_local_responder_receives_a_grant(monkeypatch, tmp_path: Path) -> None:
    calls: list[dict] = []
    response = _call(monkeypatch, tmp_path, secret_value="correct", calls=calls)

    payload = response.get_json()
    assert payload["accepted"] is True
    assert payload["pairing_code"] == GRANT
    assert response.headers["Cache-Control"] == "no-store"
    # An automatic grant must not become the code shown in this device's UI.
    assert calls == [{"remember": False, "ttl_seconds": 600}]


def test_a_wrong_or_absent_secret_receives_nothing(monkeypatch, tmp_path: Path) -> None:
    for value in (None, "", "wrong-secret-value-that-is-long-enough-x"):
        response, status = _call(monkeypatch, tmp_path, secret_value=value)

        assert status == 403
        payload = response.get_json()
        assert payload["accepted"] is False
        assert payload["error"] == "unauthenticated"
        assert "pairing_code" not in json.dumps(payload)


def test_a_remote_member_cannot_issue_grants(monkeypatch, tmp_path: Path) -> None:
    response, status = _call(
        monkeypatch,
        tmp_path,
        secret_value="correct",
        service=_service(remote=object()),
    )

    assert status == 409
    assert response.get_json()["accepted"] is False


def test_an_unavailable_pairing_authority_fails_closed(
    monkeypatch, tmp_path: Path
) -> None:
    response, status = _call(
        monkeypatch,
        tmp_path,
        secret_value="correct",
        service=_service(code=""),
    )

    assert status == 503
    payload = response.get_json()
    assert payload["accepted"] is False
    assert "pairing_code" not in json.dumps(payload)


def test_there_is_no_unauthenticated_token_minting_endpoint() -> None:
    # The grant endpoint is session-free by necessity, but it is not open: the
    # handler's first act is the secret check. Human-facing pairing endpoints
    # keep their ordinary permission gate.
    assert "federation_pairing_web.tailnet_join_grant" in PUBLIC_ENDPOINTS
    assert "federation_pairing_web.create_pairing_code" not in PUBLIC_ENDPOINTS
    assert "federation_pairing_web.pair_device" not in PUBLIC_ENDPOINTS


def test_a_missing_secret_file_refuses_every_caller(monkeypatch, tmp_path: Path) -> None:
    missing = tmp_path / "never_created"
    monkeypatch.setattr(pairing_routes, "secret_path", lambda: missing)
    monkeypatch.setattr(pairing_routes, "_pairing_service", _service)
    monkeypatch.setattr(pairing_routes, "_relay_url", lambda: "ws://100.90.80.70:8765")

    app = Flask(__name__)
    with app.test_request_context(
        "/internal/federation/tailnet-join-grant",
        method="POST",
        headers={SECRET_HEADER: "anything-at-all-long-enough-to-pass-x"},
    ):
        response, status = pairing_routes.tailnet_join_grant()

    assert status == 403
    assert response.get_json()["error"] == "unauthenticated"
