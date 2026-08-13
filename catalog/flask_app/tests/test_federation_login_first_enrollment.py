from __future__ import annotations

from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from flask import Blueprint, Flask

from catalog.flask_app.auth import federation_enrollment as enrollment


def _app() -> Flask:
    app = Flask(__name__, template_folder="../templates")
    app.testing = True
    app.secret_key = "test-enrollment-secret"
    security = Blueprint("security", __name__)
    security.add_url_rule("/login", endpoint="login", view_func=lambda: "login")
    app.register_blueprint(security)
    app.register_blueprint(enrollment.federation_enrollment)
    return app


def test_tailscale_origin_accepts_only_tailnet_ipv4() -> None:
    assert (
        enrollment._normalize_tailscale_origin("http://100.64.10.20:5000/")
        == "http://100.64.10.20:5000"
    )
    assert (
        enrollment._normalize_tailscale_origin("https://100.127.255.254/")
        == "https://100.127.255.254"
    )

    for value in (
        "http://127.0.0.1:5000/",
        "http://192.168.1.10:5000/",
        "http://8.8.8.8:5000/",
        "http://100.128.0.1:5000/",
        "ftp://100.64.10.20/",
        "http://user@example.com/",
    ):
        with pytest.raises(ValueError):
            enrollment._normalize_tailscale_origin(value)


def test_discovery_candidates_are_revalidated_and_allowlisted(monkeypatch) -> None:
    monkeypatch.setattr(
        enrollment,
        "load_snapshot",
        lambda _path: {
            "federations": [
                {
                    "federation_label": "Lab",
                    "federation_fingerprint": "a" * 32,
                    "device_name": "Authority",
                    "tailscale_ip": "100.70.1.2",
                    "web_port": 5000,
                    "unexpected_secret": "must-not-propagate",
                },
                {
                    "federation_label": "Public host",
                    "federation_fingerprint": "b" * 32,
                    "device_name": "Invalid",
                    "tailscale_ip": "203.0.113.10",
                    "web_port": 5000,
                },
            ]
        },
    )

    candidates = enrollment.discovered_federations()
    assert candidates == (
        {
            "federation_label": "Lab",
            "federation_fingerprint": "a" * 32,
            "device_name": "Authority",
            "tailscale_ip": "100.70.1.2",
            "web_port": 5000,
        },
    )
    assert "unexpected_secret" not in candidates[0]


def test_start_redirects_fresh_device_to_discovered_authority(monkeypatch) -> None:
    app = _app()
    monkeypatch.setattr(enrollment, "_is_fresh_device", lambda: True)
    monkeypatch.setattr(
        enrollment,
        "_candidate",
        lambda fingerprint: {
            "federation_label": "Existing Federation",
            "federation_fingerprint": fingerprint,
            "device_name": "Authority",
            "tailscale_ip": "100.70.1.2",
            "web_port": 5000,
        },
    )
    fake_service = SimpleNamespace(
        create_identity=lambda: SimpleNamespace(
            identity=SimpleNamespace(node_id="node-fresh", display_name="New device")
        )
    )
    monkeypatch.setattr(enrollment, "_pairing_service", lambda: fake_service)

    client = app.test_client()
    response = client.post(
        "/federation-auth/enroll/start",
        data={"federation_fingerprint": "a" * 32},
        base_url="http://100.70.1.3:5000",
    )

    assert response.status_code == 302
    location = urlsplit(response.headers["Location"])
    assert location.scheme == "http"
    assert location.netloc == "100.70.1.2:5000"
    assert location.path == "/federation-auth/enroll/authorize"
    query = parse_qs(location.query)
    assert query["target_node_id"] == ["node-fresh"]
    assert query["target_name"] == ["New device"]
    assert query["return_url"] == [
        "http://100.70.1.3:5000/federation-auth/enroll/callback"
    ]
    assert query["federation_fingerprint"] == ["a" * 32]
    assert len(query["state"][0]) >= 32


def test_authority_returns_pairing_grant_in_fragment_not_http_query(monkeypatch) -> None:
    app = _app()
    monkeypatch.setattr(
        enrollment,
        "current_user",
        SimpleNamespace(is_authenticated=True, is_active=True),
    )
    monkeypatch.setattr(enrollment, "_authority_relay_url", lambda: "ws://100.70.1.2:8765")
    fake_remote_store = SimpleNamespace(load=lambda: None)
    fake_context = SimpleNamespace(binding=SimpleNamespace(federation_id="session-fed-1"))
    fake_service = SimpleNamespace(
        remote_store=fake_remote_store,
        authorized_context=lambda: fake_context,
        create_pairing_code=lambda **_kwargs: "FCP1-one-use-secret",
    )
    monkeypatch.setattr(enrollment, "_pairing_service", lambda: fake_service)
    fingerprint = enrollment._authority_fingerprint(fake_service)

    client = app.test_client()
    response = client.post(
        "/federation-auth/enroll/authorize",
        data={
            "state": "state-value",
            "return_url": "http://100.70.1.3:5000/federation-auth/enroll/callback",
            "target_node_id": "node-fresh",
            "target_name": "New device",
            "federation_fingerprint": fingerprint,
        },
        base_url="http://100.70.1.2:5000",
    )

    assert response.status_code == 302
    location = response.headers["Location"]
    parsed = urlsplit(location)
    assert parsed.scheme == "http"
    assert parsed.netloc == "100.70.1.3:5000"
    assert parsed.path == "/federation-auth/enroll/callback"
    assert parsed.query == ""
    fragment = parse_qs(parsed.fragment)
    assert fragment["state"] == ["state-value"]
    assert fragment["pairing_code"] == ["FCP1-one-use-secret"]
    assert "pairing_code" not in location.split("#", 1)[0]


def test_complete_rejects_federation_fingerprint_swap(monkeypatch) -> None:
    app = _app()
    monkeypatch.setattr(enrollment, "_is_fresh_device", lambda: True)
    redeemed: list[str] = []
    fake_service = SimpleNamespace(
        pairing_codec=SimpleNamespace(
            decode=lambda _code: SimpleNamespace(federation_id="session-other")
        ),
        redeem_pairing_code=lambda code: redeemed.append(code),
    )
    monkeypatch.setattr(enrollment, "_pairing_service", lambda: fake_service)

    client = app.test_client()
    with client.session_transaction() as state:
        state[enrollment._STATE_KEY] = "expected-state"
        state[enrollment._ISSUED_KEY] = enrollment._stamp(enrollment._utc_now())
        state[enrollment._FINGERPRINT_KEY] = "a" * 32

    response = client.post(
        "/federation-auth/enroll/complete",
        data={"state": "expected-state", "pairing_code": "FCP1-returned"},
        base_url="http://100.70.1.3:5000",
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/login")
    assert redeemed == []
