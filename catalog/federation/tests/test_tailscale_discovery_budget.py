"""Bound real delayed/streaming responders without treating partial scans as unique."""

from __future__ import annotations

import json
import socket
import subprocess
import threading
import time
from contextlib import contextmanager
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import Request

import pytest

from catalog.federation import tailscale_host_discovery as discovery


def _advertisement(fingerprint="a" * 32):
    return {
        "schema": discovery.ADVERTISEMENT_SCHEMA,
        "federation_label": "Fixture Federation",
        "federation_fingerprint": fingerprint,
        "device_name": "Fixture host",
        "relay_port": 8765,
        "pairing_required": True,
    }


def _runner(count):
    def run(command, **_kwargs):
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps(
                {
                    "Peer": {
                        str(i): {"Online": True, "TailscaleIPs": [f"100.64.0.{i + 1}"]}
                        for i in range(count)
                    }
                }
            ),
            "",
        )

    return run


@contextmanager
def _server(*, delay=0.0, body=None, trickle=False, trickle_headers=False, status=200):
    stopped = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if stopped.wait(delay):
                return
            encoded = json.dumps(_advertisement()).encode() if body is None else body
            try:
                if trickle_headers:
                    for value in b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n":
                        self.wfile.write(bytes([value]))
                        self.wfile.flush()
                        if stopped.wait(0.03):
                            return
                    return
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                if status == 302:
                    self.send_header("Location", "/must-not-follow")
                self.end_headers()
                for piece in (
                    [encoded] if not trickle else [bytes([c]) for c in encoded]
                ):
                    self.wfile.write(piece)
                    self.wfile.flush()
                    if trickle and stopped.wait(0.03):
                        return
            except OSError:
                pass

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield server.server_address[1]
    finally:
        stopped.set()
        server.shutdown()
        server.server_close()
        worker.join(timeout=3)


def _local_opener(port):
    def open_local(request, *, timeout):
        # Substitute only the transport endpoint; exercise the production HTTP
        # deadline and decoder with actual sockets on an isolated loopback port.
        target = Request(f"http://127.0.0.1:{port}{urlsplit(request.full_url).path}")
        return discovery._open_probe(target, timeout=timeout)

    return open_local


def test_normal_budget_accepts_the_observed_one_second_responder():
    with _server(delay=1.05) as port:
        begin = time.monotonic()
        result = discovery.discover(runner=_runner(1), opener=_local_opener(port))
        elapsed = time.monotonic() - begin
    assert 0.75 < elapsed < discovery.DEFAULT_TIMEOUT_SECONDS + 0.5
    assert len(result["federations"]) == 1
    assert result["federations"][0]["pairing_required"] is True
    assert "pairing_code" not in json.dumps(result)


def test_normal_recorder_start_finds_slow_peer_but_cannot_join_without_grant(
    tmp_path, monkeypatch
):
    from urllib.request import urlopen

    from catalog.federation import (
        tailnet_join_bridge,
        tailnet_join_client,
        tailnet_join_responder,
    )
    from catalog.federation.tailscale_peer_identity import PeerVerification, TailnetPeer
    from scripts import start_tailscale_recorder as launcher

    peer = TailnetPeer(
        login_name="owner@example.com",
        tailnet="fixture.ts.net",
        node_name="fixture",
        address="100.64.0.1",
    )
    verified = []

    def verify(address):
        verified.append(address)
        return PeerVerification(peer, "verified")

    server = tailnet_join_responder.build_server(
        bind_host="127.0.0.1",
        port=0,
        app_url="http://127.0.0.1:1",
        secret_file=tmp_path / "unused-secret",
        verifier=verify,
        grant_requester=lambda **_kw: ("", "quorum-unavailable"),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def grant_opener(request, *, timeout):
        return urlopen(
            Request(
                f"http://127.0.0.1:{server.server_address[1]}{urlsplit(request.full_url).path}",
                data=request.data,
                headers=dict(request.header_items()),
                method=request.method,
            ),
            timeout=timeout,
        )

    try:
        with _server(delay=1.05) as port:
            modules = {
                "tailscale_host_discovery": SimpleNamespace(
                    discover=partial(
                        discovery.discover,
                        runner=_runner(1),
                        opener=_local_opener(port),
                    ),
                    write_snapshot=discovery.write_snapshot,
                ),
                "tailnet_join_client": SimpleNamespace(
                    join_discovered_federation=partial(
                        tailnet_join_client.join_discovered_federation,
                        opener=grant_opener,
                    )
                ),
                "tailnet_join_bridge": tailnet_join_bridge,
            }
            monkeypatch.setattr(launcher, "load_host_module", modules.__getitem__)
            monkeypatch.setenv(
                "FCP_RECORDER_FEDERATION_KEY", "FCP1-inherited-legacy-must-not-bypass"
            )
            with pytest.raises(RuntimeError, match="quorum-unavailable"):
                launcher._pairing_key(["--data-dir", str(tmp_path)])
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
    assert verified
    assert (
        len(
            discovery.load_snapshot(tmp_path / "tailscale_discovery.json")[
                "federations"
            ]
        )
        == 1
    )
    assert not (tmp_path / launcher.PAIRING_STATE_RELATIVE).exists()
    assert not (tmp_path / tailnet_join_bridge.GRANT_RELATIVE).exists()
    assert "FCP_RECORDER_FEDERATION_KEY" not in launcher.os.environ


def test_unreachable_peer_is_not_an_advertisement():
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        # Bound but not listening: a deterministic refusal without another
        # process taking ownership between a free-port probe and this request.
        result = discovery.discover(
            runner=_runner(1), opener=_local_opener(reserved.getsockname()[1])
        )
    assert result["federations"] == []


def test_two_actual_advertisements_remain_ambiguous_for_normal_recorder_start(
    tmp_path, monkeypatch
):
    from scripts import start_tailscale_recorder as launcher

    with (
        _server(body=json.dumps(_advertisement("a" * 32)).encode()) as first,
        _server(body=json.dumps(_advertisement("b" * 32)).encode()) as second,
    ):

        def opener(request, *, timeout):
            port = (
                first if urlsplit(request.full_url).hostname.endswith(".1") else second
            )
            return _local_opener(port)(request, timeout=timeout)

        module = SimpleNamespace(
            discover=partial(discovery.discover, runner=_runner(2), opener=opener),
            write_snapshot=discovery.write_snapshot,
        )

        def load(name):
            assert name == "tailscale_host_discovery", (
                "ambiguous discovery must never request a grant"
            )
            return module

        monkeypatch.setattr(launcher, "load_host_module", load)
        with pytest.raises(RuntimeError, match="More than one"):
            launcher._pairing_key(["--data-dir", str(tmp_path)])
    assert (
        len(
            discovery.load_snapshot(tmp_path / "tailscale_discovery.json")[
                "federations"
            ]
        )
        == 2
    )
    assert not (tmp_path / launcher.PAIRING_STATE_RELATIVE).exists()


@pytest.mark.parametrize("body", [b"not json", b"{}", b'"scalar"', b"x" * 16385])
def test_malformed_or_oversized_reply_is_refused(body):
    with _server(body=body) as port:
        result = discovery.discover(runner=_runner(1), opener=_local_opener(port))
    assert result["federations"] == []


@pytest.mark.parametrize("part", ["body", "headers"])
def test_trickled_response_cannot_extend_the_request_budget(part):
    with _server(trickle=part == "body", trickle_headers=part == "headers") as port:
        begin = time.monotonic()
        result = discovery.discover(
            runner=_runner(1), opener=_local_opener(port), timeout_seconds=0.15
        )
        elapsed = time.monotonic() - begin
    assert elapsed < 0.7
    assert result["federations"] == []


def test_discovery_does_not_follow_redirects():
    with _server(status=302) as port:
        result = discovery.discover(runner=_runner(1), opener=_local_opener(port))
    assert result["federations"] == []


def test_total_budget_exhaustion_discards_even_a_valid_partial_result():
    with _server() as fast, _server(delay=1) as slow:

        def opener(request, *, timeout):
            return _local_opener(
                fast if urlsplit(request.full_url).hostname.endswith(".1") else slow
            )(request, timeout=timeout)

        begin = time.monotonic()
        with pytest.raises(discovery.DiscoveryBudgetExceeded):
            discovery.discover(
                runner=_runner(2), opener=opener, total_timeout_seconds=0.15
            )
        elapsed = time.monotonic() - begin
    assert elapsed < 0.7
    assert not any(t.name.startswith("fcp-discovery") for t in threading.enumerate())


def test_normal_recorder_exhaustion_cannot_request_a_grant(tmp_path, monkeypatch):
    from scripts import start_tailscale_recorder as launcher

    with _server(delay=1) as port:
        module = SimpleNamespace(
            discover=partial(
                discovery.discover,
                runner=_runner(1),
                opener=_local_opener(port),
                total_timeout_seconds=0.15,
            ),
            write_snapshot=lambda *_a: pytest.fail("incomplete snapshot"),
        )

        def load(name):
            assert name == "tailscale_host_discovery", "no grant after exhaustion"
            return module

        monkeypatch.setattr(launcher, "load_host_module", load)
        monkeypatch.setenv("FCP_RECORDER_FEDERATION_KEY", "FCP1-inherited")
        with pytest.raises(discovery.DiscoveryBudgetExceeded):
            launcher._pairing_key(["--data-dir", str(tmp_path)])
    assert not (tmp_path / launcher.PAIRING_STATE_RELATIVE).exists()
    assert not (tmp_path / "tailscale_discovery.json").exists()
    assert "FCP_RECORDER_FEDERATION_KEY" not in launcher.os.environ


def test_timeout_limited_by_total_budget_fails_closed_for_direct_and_url_errors():
    timeout_errors = (
        TimeoutError("probe consumed remaining scan budget"),
        URLError(TimeoutError("probe consumed remaining scan budget")),
    )
    for timeout_error in timeout_errors:

        def opener(_request, *, timeout, timeout_error=timeout_error):
            assert 0.05 <= timeout <= 0.15
            time.sleep(max(0.0, timeout - 0.01))
            raise timeout_error

        with pytest.raises(discovery.DiscoveryBudgetExceeded):
            discovery.discover(
                runner=_runner(1),
                opener=opener,
                timeout_seconds=2.0,
                total_timeout_seconds=0.15,
            )


def test_short_peer_timeout_before_total_budget_is_a_complete_negative_probe():
    def opener(_request, *, timeout):
        assert timeout == 0.05
        time.sleep(timeout)
        raise TimeoutError("peer-specific timeout")

    result = discovery.discover(
        runner=_runner(1),
        opener=opener,
        timeout_seconds=0.05,
        total_timeout_seconds=1.0,
    )

    assert result["federations"] == []


def test_peers_and_ports_share_a_bounded_worker_pool_and_scan_deadline():
    active = peak = calls = 0
    lock = threading.Lock()

    def opener(_request, *, timeout):
        nonlocal active, peak, calls
        with lock:
            active += 1
            calls += 1
            peak = max(peak, active)
        try:
            threading.Event().wait(timeout)
            raise TimeoutError("unreachable fixture peer")
        finally:
            with lock:
                active -= 1

    begin = time.monotonic()
    with pytest.raises(discovery.DiscoveryBudgetExceeded):
        discovery.discover(
            runner=_runner(32),
            opener=opener,
            web_ports=(5000, 5001, 5002, 5003),
            timeout_seconds=0.1,
            total_timeout_seconds=0.3,
        )
    assert time.monotonic() - begin < 0.9
    assert 1 < peak <= discovery.MAX_PROBE_WORKERS
    assert calls < 32 * 4
    assert active == 0


def test_excess_ports_are_rejected_before_any_network_operation():
    with pytest.raises(ValueError, match="at most"):
        discovery.discover(
            web_ports=range(5000, 5100),
            runner=lambda *_a, **_kw: pytest.fail("no scan"),
        )


@pytest.mark.parametrize("budget", [0, -1, float("nan"), float("inf"), True, 11])
def test_invalid_scan_budget_fails_closed(budget):
    with pytest.raises(ValueError):
        discovery.discover(
            total_timeout_seconds=budget,
            runner=lambda *_a, **_kw: pytest.fail("no scan"),
        )


def test_cli_exhaustion_replaces_a_previous_snapshot_and_returns_failure(
    tmp_path, monkeypatch
):
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps({"old": "snapshot"}))

    def exhausted(**_kwargs):
        raise discovery.DiscoveryBudgetExceeded("scan budget exhausted")

    monkeypatch.setattr(discovery, "discover", exhausted)
    with pytest.raises(SystemExit, match="scan budget exhausted"):
        discovery.main(["--output", str(path)])
    assert discovery.load_snapshot(path)["federations"] == []


@pytest.mark.parametrize("peer_timeout", [2.0, 0.15])
@pytest.mark.parametrize("wrapped", [False, True])
def test_repeated_clock_read_caps_total_probe_and_preserves_exhaustion(
    monkeypatch, peer_timeout, wrapped
):
    # The deadline addition rounds up at this ordinary monotonic magnitude.
    # Repeated readings must neither enlarge the configured cap nor classify
    # an equal peer/scan timeout as a complete negative discovery result.
    monkeypatch.setattr(discovery.time, "monotonic", lambda: 1024.0)
    timeout_error = TimeoutError("remaining scan budget exhausted")
    failure = URLError(timeout_error) if wrapped else timeout_error
    seen = []

    def opener(_request, *, timeout):
        seen.append(timeout)
        assert timeout == 0.15
        raise failure

    with pytest.raises(discovery.DiscoveryBudgetExceeded) as result:
        discovery.discover(
            runner=_runner(1), opener=opener,
            timeout_seconds=peer_timeout, total_timeout_seconds=0.15,
        )

    assert seen == [0.15]
    assert result.value.__cause__ is failure
    assert not any(t.name.startswith("fcp-discovery") for t in threading.enumerate())


@pytest.mark.parametrize("wrapped", [False, True])
def test_repeated_clock_read_keeps_shorter_peer_timeout_a_complete_negative(
    monkeypatch, wrapped
):
    monkeypatch.setattr(discovery.time, "monotonic", lambda: 1024.0)
    timeout_error = TimeoutError("short peer-specific timeout")
    failure = URLError(timeout_error) if wrapped else timeout_error
    seen = []

    def opener(_request, *, timeout):
        seen.append(timeout)
        assert timeout == 0.05
        raise failure

    result = discovery.discover(
        runner=_runner(1), opener=opener,
        timeout_seconds=0.05, total_timeout_seconds=0.15,
    )

    assert seen == [0.05]
    assert result["federations"] == []


def test_repeated_clock_read_caps_the_whole_scan_wait(monkeypatch):
    monkeypatch.setattr(discovery.time, "monotonic", lambda: 1024.0)
    waits = []

    def completed(futures, *, timeout):
        assert futures == []
        waits.append(timeout)
        return iter(())

    monkeypatch.setattr(discovery, "as_completed", completed)
    result = discovery.discover(runner=_runner(0), total_timeout_seconds=0.15)

    assert waits == [0.15]
    assert result["federations"] == []


def test_repeated_clock_read_caps_http_interrupt_timer(monkeypatch):
    monkeypatch.setattr(discovery.time, "monotonic", lambda: 1024.0)
    waits = []
    events = []
    response = SimpleNamespace(close=lambda: events.append("response-closed"))

    class Connection:
        sock = SimpleNamespace(shutdown=lambda *_args: None)

        def __init__(self, _host, _port, *, timeout):
            assert timeout == 0.15

        def connect(self):
            pass

        def request(self, *_args, **_kwargs):
            pass

        def getresponse(self):
            return response

        def close(self):
            events.append("connection-closed")

    class Timer:
        def __init__(self, interval, _function):
            waits.append(interval)

        def start(self):
            pass

        def cancel(self):
            events.append("timer-cancelled")

        def join(self):
            events.append("timer-joined")

    monkeypatch.setattr(discovery.http.client, "HTTPConnection", Connection)
    monkeypatch.setattr(discovery.threading, "Timer", Timer)
    with discovery._open_probe(Request("http://100.64.0.1:5000/probe"), timeout=0.15) as value:
        assert value is response

    assert waits == [0.15]
    assert events == ["timer-cancelled", "timer-joined", "response-closed", "connection-closed"]
