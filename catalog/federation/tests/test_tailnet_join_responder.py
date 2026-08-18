"""The host responder turns verified identity into one ordinary pairing grant.

It mints nothing itself. Every path that cannot prove the caller is a
same-tailnet, same-owner peer must end without a grant.
"""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from catalog.federation import tailnet_join_responder as responder
from catalog.federation.tailnet_join_bridge import (
    SECRET_HEADER,
    ensure_secret,
    read_secret,
)
from catalog.federation.tailscale_peer_identity import PeerVerification, TailnetPeer

PEER = TailnetPeer(
    login_name="owner@example.com",
    tailnet="tail0abc.ts.net",
    node_name="laptop.tail0abc.ts.net",
    address="100.90.80.71",
)
GRANT = "FCP1-test-grant-not-real"


def _serve(verifier, grant_requester, secret_file=None):
    server = responder.build_server(
        bind_host="127.0.0.1",
        port=0,
        app_url="http://127.0.0.1:1",
        secret_file=secret_file,
        verifier=verifier,
        grant_requester=grant_requester,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, server.server_address[1]


def _post(port: int, path: str = responder.JOIN_PATH):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=b"{}",
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8"))


def test_verified_peer_receives_a_grant_from_the_pairing_authority() -> None:
    calls: list[str] = []

    def grant(*, app_url, secret, peer_node_name):
        calls.append(peer_node_name)
        return GRANT, "granted"

    server, port = _serve(lambda _address: PeerVerification(PEER, "verified"), grant)
    try:
        status, payload = _post(port)
    finally:
        server.shutdown()

    assert status == 200
    assert payload["accepted"] is True
    assert payload["pairing_code"] == GRANT
    assert calls == [PEER.node_name]


@pytest.mark.parametrize(
    "reason",
    [
        "not-a-tailnet-address",
        "peer-owned-by-another-user",
        "peer-in-another-tailnet",
        "peer-shared-from-another-tailnet",
        "peer-is-tag-owned",
        "peer-identity-unavailable",
        "local-tailnet-identity-unavailable",
    ],
)
def test_unverified_peer_never_reaches_the_pairing_authority(reason: str) -> None:
    def grant(*, app_url, secret, peer_node_name):
        raise AssertionError("the pairing authority must not be asked")

    server, port = _serve(lambda _address: PeerVerification(None, reason), grant)
    try:
        status, payload = _post(port)
    finally:
        server.shutdown()

    assert status == 403
    assert payload["accepted"] is False
    assert payload["error"] == reason
    assert "pairing_code" not in payload


def test_unavailable_pairing_authority_fails_closed() -> None:
    def grant(*, app_url, secret, peer_node_name):
        return "", "pairing-authority-unreachable"

    server, port = _serve(lambda _address: PeerVerification(PEER, "verified"), grant)
    try:
        status, payload = _post(port)
    finally:
        server.shutdown()

    assert status == 503
    assert payload["accepted"] is False
    assert payload["error"] == "pairing-authority-unreachable"
    assert "pairing_code" not in payload


def test_unknown_paths_are_refused() -> None:
    def grant(*, app_url, secret, peer_node_name):
        raise AssertionError("must not be asked")

    server, port = _serve(lambda _address: PeerVerification(PEER, "verified"), grant)
    try:
        status, _payload = _post(port, "/anything-else")
    finally:
        server.shutdown()

    assert status == 404


def test_health_endpoint_reports_readiness_without_granting(monkeypatch) -> None:
    monkeypatch.setattr(responder, "local_tailnet_identity", lambda: PEER)

    def grant(*, app_url, secret, peer_node_name):
        raise AssertionError("health must not mint anything")

    server, port = _serve(lambda _address: PeerVerification(PEER, "verified"), grant)
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}{responder.HEALTH_PATH}", timeout=5
        ) as response:
            payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()

    assert payload["responder"] == "ready"
    assert payload["tailscale"] is True
    assert "pairing_code" not in payload


def test_grant_request_carries_the_secret_and_reads_the_response() -> None:
    seen: dict[str, object] = {}

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def read(self, _size=-1):
            return json.dumps({"accepted": True, "pairing_code": GRANT}).encode()

    def opener(request, timeout=None):
        seen["url"] = request.full_url
        seen["secret"] = request.get_header(SECRET_HEADER.capitalize())
        seen["timeout"] = timeout
        return _Response()

    code, reason = responder.request_grant(
        app_url="http://127.0.0.1:5000",
        secret="s" * 40,
        peer_node_name=PEER.node_name,
        opener=opener,
    )

    assert code == GRANT
    assert reason == "granted"
    assert seen["url"].endswith("/internal/federation/tailnet-join-grant")
    assert seen["secret"] == "s" * 40
    assert seen["timeout"] == responder.GRANT_TIMEOUT_SECONDS


def test_grant_request_fails_closed_when_the_application_refuses() -> None:
    def opener(request, timeout=None):
        raise urllib.error.HTTPError(request.full_url, 403, "no", None, None)

    code, reason = responder.request_grant(
        app_url="http://127.0.0.1:5000",
        secret="s" * 40,
        peer_node_name=PEER.node_name,
        opener=opener,
    )

    assert code == ""
    assert reason == "pairing-authority-refused-403"


def test_grant_request_fails_closed_when_the_application_is_unreachable() -> None:
    def opener(request, timeout=None):
        raise urllib.error.URLError("connection refused")

    code, reason = responder.request_grant(
        app_url="http://127.0.0.1:5000",
        secret="s" * 40,
        peer_node_name=PEER.node_name,
        opener=opener,
    )

    assert code == ""
    assert reason == "pairing-authority-unreachable"


def test_secret_is_created_once_and_is_not_guessable(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "auto_join_secret"

    first = ensure_secret(path)
    second = ensure_secret(path)

    assert first == second
    assert len(first) >= 32
    assert read_secret(path) == first
    assert ensure_secret(tmp_path / "other_secret") != first


def test_a_short_or_missing_secret_is_never_accepted(tmp_path: Path) -> None:
    missing = tmp_path / "absent"
    assert read_secret(missing) == ""

    short = tmp_path / "short"
    short.write_text("tooshort", encoding="utf-8")
    assert read_secret(short) == ""


def test_preflight_reports_a_useful_diagnostic_when_logged_out(monkeypatch) -> None:
    monkeypatch.setattr(responder, "local_tailnet_identity", lambda: None)

    code, lines = responder.preflight(bind_host="100.90.80.70")

    assert code == 1
    assert any("tailscale" in line and "FAIL" in line for line in lines)
    assert any("logged out" in line for line in lines)


def test_preflight_reports_ready_when_signed_in(monkeypatch) -> None:
    monkeypatch.setattr(responder, "local_tailnet_identity", lambda: PEER)

    code, lines = responder.preflight(bind_host="100.90.80.70")

    assert code == 0
    assert all(line.startswith("OK") for line in lines)
    assert any(PEER.login_name in line for line in lines)


def test_preflight_fails_without_a_tailnet_bind_address(monkeypatch) -> None:
    monkeypatch.setattr(responder, "local_tailnet_identity", lambda: PEER)

    code, lines = responder.preflight(bind_host="")

    assert code == 1
    assert any("bind address" in line and "FAIL" in line for line in lines)


def test_the_secret_is_reread_after_a_fresh_reset_removes_it(tmp_path: Path) -> None:
    """A --fresh reset deletes the secret while this responder keeps running.

    A value cached at startup would make every later grant fail until someone
    restarted the host, which is exactly the launch where joining matters.
    """

    secret_file = tmp_path / "auto_join_secret"
    seen: list[str] = []

    def grant(*, app_url, secret, peer_node_name):
        seen.append(secret)
        return GRANT, "granted"

    server, port = _serve(
        lambda _address: PeerVerification(PEER, "verified"),
        grant,
        secret_file=secret_file,
    )
    try:
        assert _post(port)[0] == 200
        first = read_secret(secret_file)

        # The factory reset removes the whole data directory beneath us.
        secret_file.unlink()

        assert _post(port)[0] == 200
        second = read_secret(secret_file)
    finally:
        server.shutdown()

    assert first and second
    assert first != second
    assert seen == [first, second]
