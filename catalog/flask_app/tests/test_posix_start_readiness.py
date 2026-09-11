"""The launcher must probe the app without rendering stateful login pages."""

from __future__ import annotations

import re
import subprocess
import sys
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest


@contextmanager
def _application(status: int, location: str | None = None):
    requests: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            # Real login rendering can publish credential/user metadata and wait
            # for voters. Receiving this request already crosses that boundary.
            self.send_response(status if self.path == "/onboarding" else 503)
            if location is not None and self.path == "/onboarding":
                self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1], requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _launcher_probe(port: int) -> subprocess.CompletedProcess[str]:
    source = (Path(__file__).resolve().parents[3] / "start.sh").read_text()
    # Execute the actual checked-in Python command, substituting only our
    # isolated HTTP server's ephemeral port. No Docker or campaign state.
    match = re.search(
        r'if docker compose exec -T flask python -c \\\n\s*"([^"\n]+)"', source
    )
    assert match is not None, "launcher readiness command was not found"
    code = match.group(1).replace("127.0.0.1:5000", f"127.0.0.1:{port}")
    code = code.replace("'127.0.0.1', 5000", f"'127.0.0.1', {port}")
    return subprocess.run(
        [sys.executable, "-B", "-c", code],
        capture_output=True, text=True, timeout=8, check=False,
    )


def test_launcher_accepts_the_app_auth_gate_without_requesting_login():
    with _application(302, "/login") as (port, requests):
        result = _launcher_probe(port)
    assert requests == ["/onboarding"], "readiness must not render/publish login state"
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("status", [200, 204])
def test_launcher_accepts_a_successful_direct_response(status):
    with _application(status) as (port, requests):
        result = _launcher_probe(port)
    assert result.returncode == 0, result.stderr
    assert requests == ["/onboarding"]


@pytest.mark.parametrize("status", [300, 304, 305, 400, 401, 403, 404, 500, 503])
def test_launcher_still_rejects_an_http_error(status):
    with _application(status) as (port, requests):
        result = _launcher_probe(port)
    assert result.returncode != 0
    assert requests == ["/onboarding"]


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_launcher_rejects_a_redirect_without_a_destination(status):
    with _application(status) as (port, requests):
        result = _launcher_probe(port)
    assert result.returncode != 0
    assert requests == ["/onboarding"]


def test_launcher_never_follows_a_redirect_to_another_origin():
    with (
        _application(200) as (other_port, other_requests),
        _application(302, f"http://127.0.0.1:{other_port}/login") as (port, requests),
    ):
        _launcher_probe(port)
    assert requests == ["/onboarding"]
    assert other_requests == []
