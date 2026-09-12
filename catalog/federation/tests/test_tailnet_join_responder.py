"""The host responder turns verified identity into one ordinary pairing grant.

It mints nothing itself. Every path that cannot prove the caller is a
same-tailnet, same-owner peer must end without a grant.
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
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


def test_a_stale_responder_is_replaced_instead_of_blocking_the_port(
    tmp_path: Path, monkeypatch
) -> None:
    """A responder from an earlier start must not keep the port.

    The launcher runs on every start, including the restart after --fresh. If
    the previous instance survives, the replacement cannot bind and automatic
    joining is silently dead while the console says it is listening.
    """

    pid_file = tmp_path / "responder.pid"
    pid_file.write_text(
        json.dumps(
            {
                "schema": responder.PROCESS_RECORD_SCHEMA,
                "pid": 4242,
                "start_token": "boot-a:100",
            }
        ),
        encoding="utf-8",
    )
    terminated: list[tuple[int, str, bool]] = []
    monkeypatch.setattr(
        responder,
        "terminate_process_if_same_instance",
        lambda pid, token, *, wait_for_exit: (
            terminated.append((pid, token, wait_for_exit)) or True
        ),
    )

    replaced = responder.stop_previous_instance(pid_file)

    assert replaced == 4242
    assert terminated == [(4242, "boot-a:100", True)]


def test_a_reused_pid_is_not_terminated(tmp_path: Path, monkeypatch) -> None:
    pid_file = tmp_path / "responder.pid"
    pid_file.write_text(
        json.dumps(
            {
                "schema": responder.PROCESS_RECORD_SCHEMA,
                "pid": 4242,
                "start_token": "boot-a:100",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        responder,
        "terminate_process_if_same_instance",
        lambda pid, token, **kwargs: False,
    )

    assert responder.stop_previous_instance(pid_file) is None


def test_a_legacy_bare_pid_file_is_never_trusted(tmp_path: Path, monkeypatch) -> None:
    pid_file = tmp_path / "responder.pid"
    pid_file.write_text("4242", encoding="utf-8")
    terminated: list[tuple[int, str]] = []
    monkeypatch.setattr(
        responder,
        "terminate_process_if_same_instance",
        lambda pid, token, **kwargs: terminated.append((pid, token)) or True,
    )

    assert responder.stop_previous_instance(pid_file) is None
    assert terminated == []


def test_a_dead_or_missing_pid_is_not_treated_as_a_running_responder(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        responder,
        "terminate_process_if_same_instance",
        lambda pid, token, **kwargs: False,
    )

    missing = tmp_path / "absent.pid"
    assert responder.stop_previous_instance(missing) is None

    dead = tmp_path / "dead.pid"
    dead.write_text(
        json.dumps(
            {
                "schema": responder.PROCESS_RECORD_SCHEMA,
                "pid": 4242,
                "start_token": "boot-a:100",
            }
        ),
        encoding="utf-8",
    )
    assert responder.stop_previous_instance(dead) is None

    garbage = tmp_path / "garbage.pid"
    garbage.write_text("not-a-pid", encoding="utf-8")
    assert responder.stop_previous_instance(garbage) is None


def test_the_responder_never_terminates_itself(tmp_path: Path) -> None:
    import os as _os

    pid_file = tmp_path / "self.pid"
    pid_file.write_text(
        json.dumps(
            {
                "schema": responder.PROCESS_RECORD_SCHEMA,
                "pid": _os.getpid(),
                "start_token": "current-process",
            }
        ),
        encoding="utf-8",
    )

    assert responder.stop_previous_instance(pid_file) is None


def test_the_pid_file_records_the_live_responder(tmp_path: Path, monkeypatch) -> None:
    import os as _os

    pid_file = tmp_path / "nested" / "responder.pid"
    monkeypatch.setattr(
        responder,
        "process_start_token",
        lambda pid: "boot-a:100" if pid == _os.getpid() else None,
    )
    responder.write_pid_file(pid_file)

    assert json.loads(pid_file.read_text(encoding="utf-8")) == {
        "schema": responder.PROCESS_RECORD_SCHEMA,
        "pid": _os.getpid(),
        "start_token": "boot-a:100",
    }


def test_linux_process_identity_is_checked_after_pinning_the_process(
    monkeypatch,
) -> None:
    opened: list[int] = []
    signalled: list[tuple[int, int]] = []
    closed: list[int] = []
    monkeypatch.setattr(
        responder,
        "process_start_token",
        lambda pid: "boot-b:1",
    )
    monkeypatch.setattr(responder.os, "name", "posix")
    monkeypatch.setattr(
        responder.sys,
        "platform",
        "linux",
    )
    monkeypatch.setattr(
        responder.os,
        "pidfd_open",
        lambda pid, flags=0: opened.append(pid) or 91,
        raising=False,
    )
    monkeypatch.setattr(
        responder.signal,
        "pidfd_send_signal",
        lambda descriptor, sig: signalled.append((descriptor, sig)),
        raising=False,
    )
    monkeypatch.setattr(
        responder.os,
        "close",
        lambda descriptor: closed.append(descriptor),
    )

    assert not responder.terminate_process_if_same_instance(4242, "boot-a:100")
    assert opened == [4242]
    assert signalled == []
    assert closed == [91]


@pytest.mark.parametrize("wait_for_exit", [False, True])
def test_linux_matching_identity_signals_only_the_pinned_process(
    monkeypatch,
    wait_for_exit,
) -> None:
    signalled: list[tuple[int, int]] = []
    waited: list[int] = []
    closed: list[int] = []
    monkeypatch.setattr(responder.os, "name", "posix")
    monkeypatch.setattr(responder.sys, "platform", "linux")
    monkeypatch.setattr(
        responder,
        "process_start_token",
        lambda pid: "boot-a:100",
    )
    monkeypatch.setattr(
        responder.os,
        "pidfd_open",
        lambda pid, flags=0: 91,
        raising=False,
    )
    monkeypatch.setattr(
        responder.signal,
        "pidfd_send_signal",
        lambda descriptor, sig: signalled.append((descriptor, sig)),
        raising=False,
    )
    monkeypatch.setattr(
        responder.os,
        "close",
        lambda descriptor: closed.append(descriptor),
    )
    monkeypatch.setattr(
        responder,
        "_wait_linux_process_exit",
        lambda descriptor: waited.append(descriptor) or True,
    )

    if wait_for_exit:
        assert responder.terminate_process_if_same_instance(
            4242, "boot-a:100", wait_for_exit=True
        )
    else:
        assert responder.terminate_process_if_same_instance(4242, "boot-a:100")
    assert signalled == [(91, responder.signal.SIGTERM)]
    assert waited == ([91] if wait_for_exit else [])
    assert closed == [91]


def test_non_linux_posix_without_a_pinned_handle_fails_closed(monkeypatch) -> None:
    monkeypatch.setattr(responder.os, "name", "posix")
    monkeypatch.setattr(responder.sys, "platform", "darwin")
    monkeypatch.setattr(
        responder,
        "process_start_token",
        lambda pid: "start-token",
    )
    monkeypatch.setattr(
        responder.os,
        "kill",
        lambda pid, sig: pytest.fail("an unpinned PID must not be signalled"),
    )

    assert not responder.terminate_process_if_same_instance(4242, "start-token")


def test_a_matching_child_process_instance_can_be_terminated() -> None:
    if responder.os.name != "nt" and not responder.sys.platform.startswith("linux"):
        pytest.skip("this POSIX platform has no pinned process signalling primitive")
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
    )
    try:
        token = None
        deadline = time.monotonic() + 5.0
        while token is None and time.monotonic() < deadline:
            token = responder.process_start_token(child.pid)
            if token is None:
                time.sleep(0.05)
        assert token is not None
        assert responder.terminate_process_if_same_instance(
            child.pid, token, wait_for_exit=True
        )
        assert child.poll() is not None, "success must confirm exit, not just signal"
        child.wait(timeout=5.0)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5.0)


def test_a_port_already_in_use_is_reported_instead_of_announced_as_listening(
    tmp_path: Path, monkeypatch
) -> None:
    """The success line must come after the socket exists, never before."""

    holder = responder.build_server(
        bind_host="127.0.0.1",
        port=0,
        app_url="http://127.0.0.1:1",
        secret_file=tmp_path / "secret",
    )
    port = holder.server_address[1]
    try:
        monkeypatch.setattr(responder, "local_tailnet_identity", lambda: PEER)
        printed: list[str] = []
        monkeypatch.setattr(
            "builtins.print",
            lambda *args, **kwargs: printed.append(" ".join(str(a) for a in args)),
        )

        code = responder.main(
            [
                "--bind",
                "127.0.0.1",
                "--port",
                str(port),
                "--secret-file",
                str(tmp_path / "secret"),
                "--pid-file",
                str(tmp_path / "responder.pid"),
            ]
        )
    finally:
        holder.server_close()

    assert code == 1
    joined = "\n".join(printed)
    assert "could not listen" in joined
    assert "manual pairing codes still work" in joined
    assert "listening on" not in joined
