"""Host-side responder that turns verified tailnet identity into a pairing grant.

This process runs on the host, beside ``tailscaled``, because only there can the
real peer address of an incoming connection be observed and resolved to a
tailnet identity. The web application runs in a container behind published
ports, so it sees the Docker gateway rather than the peer and has no
``tailscale`` CLI; it therefore owns the pairing authority but not the identity
check, and this responder owns the identity check but not the authority.

Flow for one join:

1. A joining host connects over the tailnet and asks to join.
2. This responder reads the real peer address from the socket.
3. :func:`verify_tailnet_peer` accepts only a same-tailnet, same-owner peer that
   is neither shared in from another tailnet nor tag-owned.
4. Only then does this responder ask the local application for one grant, over
   the loopback/published web address, authenticated with the shared host secret.
5. The grant is returned to the verified peer and never written to a log.

Everything fails closed. There is no mode in which an unverified caller receives
a grant, and no environment variable turns the identity check off.
"""

from __future__ import annotations

import argparse
import http.server
import json
import os
import socketserver
import sys
import threading
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path

from .tailnet_join_bridge import (
    GRANT_REQUEST_SCHEMA,
    SECRET_HEADER,
    auto_join_port,
    ensure_secret,
    secret_path,
)
from .tailscale_peer_identity import (
    PeerVerification,
    local_tailnet_identity,
    verify_tailnet_peer,
)

JOIN_PATH = "/fcp/federation/tailnet-join"
HEALTH_PATH = "/fcp/federation/tailnet-join/health"
RESPONSE_SCHEMA = "fcp.federation.tailnet-auto-join.v1"
MAX_REQUEST_BYTES = 4096
GRANT_TIMEOUT_SECONDS = 10.0
DEFAULT_APP_URL = "http://127.0.0.1:5000"


def application_url(environ: dict[str, str] | None = None) -> str:
    values = os.environ if environ is None else environ
    configured = str(values.get("FCP_AUTO_JOIN_APP_URL", "") or "").strip()
    if configured:
        return configured.rstrip("/")
    port = str(values.get("FCP_WEB_PORT", "") or "5000").strip() or "5000"
    return f"http://127.0.0.1:{port}"


def request_grant(
    *,
    app_url: str,
    secret: str,
    peer_node_name: str,
    opener: Callable[..., object] = urllib.request.urlopen,
) -> tuple[str, str]:
    """Ask the application for one pairing grant. Returns ``(code, reason)``."""

    payload = json.dumps(
        {"schema": GRANT_REQUEST_SCHEMA, "peer_node_name": peer_node_name}
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{app_url}/internal/federation/tailnet-join-grant",
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            SECRET_HEADER: secret,
        },
    )
    try:
        with opener(request, timeout=GRANT_TIMEOUT_SECONDS) as response:
            body = json.loads(response.read(MAX_REQUEST_BYTES * 8).decode("utf-8"))
    except urllib.error.HTTPError as error:
        return "", f"pairing-authority-refused-{error.code}"
    except (urllib.error.URLError, OSError, TimeoutError):
        return "", "pairing-authority-unreachable"
    except (ValueError, json.JSONDecodeError):
        return "", "pairing-authority-unreadable"
    if not isinstance(body, dict) or body.get("accepted") is not True:
        reason = "pairing-authority-refused"
        if isinstance(body, dict) and isinstance(body.get("error"), str):
            reason = f"pairing-authority-{body['error']}"
        return "", reason
    code = body.get("pairing_code")
    if not isinstance(code, str) or not code.strip():
        return "", "pairing-authority-returned-no-grant"
    return code.strip(), "granted"


class _Handler(http.server.BaseHTTPRequestHandler):
    server_version = "FCPTailnetJoin/1.0"
    sys_version = ""

    # Injected by the server factory.
    app_url = DEFAULT_APP_URL
    secret_file: Path | None = None
    verifier: Callable[[object], PeerVerification] = staticmethod(verify_tailnet_peer)
    grant_requester = staticmethod(request_grant)

    def log_message(self, format: str, *args) -> None:
        # Never let request text reach the log; a grant must not be logged.
        sys.stderr.write("tailnet-join responder: %s\n" % (format % args))

    def _json(self, status: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path != HEALTH_PATH:
            self._json(404, {"accepted": False, "error": "unknown-path"})
            return
        identity = local_tailnet_identity()
        self._json(
            200,
            {
                "schema": RESPONSE_SCHEMA,
                "responder": "ready",
                "tailscale": identity is not None,
            },
        )

    def do_POST(self) -> None:
        if self.path != JOIN_PATH:
            self._json(404, {"accepted": False, "error": "unknown-path"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
        except ValueError:
            length = 0
        if length < 0 or length > MAX_REQUEST_BYTES:
            self._json(413, {"accepted": False, "error": "request-too-large"})
            return
        if length:
            self.rfile.read(length)

        peer_address = self.client_address[0] if self.client_address else ""
        verification = self.verifier(peer_address)
        if not verification.trusted or verification.peer is None:
            self.log_message("refused (%s)", verification.reason_code)
            self._json(403, {"accepted": False, "error": verification.reason_code})
            return

        # Resolve the secret per request: a --fresh factory reset deletes it
        # while this responder keeps running, and a value cached at startup
        # would refuse every later grant until someone restarted the host.
        secret = ensure_secret(self.secret_file or secret_path())
        code, reason = self.grant_requester(
            app_url=self.app_url,
            secret=secret,
            peer_node_name=verification.peer.node_name,
        )
        if not code:
            self.log_message("grant unavailable (%s)", reason)
            self._json(503, {"accepted": False, "error": reason})
            return
        self.log_message("authorized a verified same-owner peer")
        self._json(
            200,
            {"schema": RESPONSE_SCHEMA, "accepted": True, "pairing_code": code},
        )


class _Server(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True
    # A stuck peer must never hold the responder open.
    timeout = GRANT_TIMEOUT_SECONDS


def build_server(
    *,
    bind_host: str,
    port: int,
    app_url: str,
    secret_file: Path | None = None,
    verifier: Callable[[object], PeerVerification] = verify_tailnet_peer,
    grant_requester: Callable[..., tuple[str, str]] = request_grant,
) -> _Server:
    handler = type(
        "_BoundHandler",
        (_Handler,),
        {
            "app_url": app_url.rstrip("/"),
            "secret_file": secret_file,
            "verifier": staticmethod(verifier),
            "grant_requester": staticmethod(grant_requester),
        },
    )
    return _Server((bind_host, port), handler)


def serve(
    *,
    bind_host: str,
    port: int,
    app_url: str,
    secret_file: Path | None = None,
    ready: threading.Event | None = None,
) -> None:
    server = build_server(
        bind_host=bind_host,
        port=port,
        app_url=app_url,
        secret_file=secret_file,
    )
    with server:
        if ready is not None:
            ready.set()
        server.serve_forever(poll_interval=0.5)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Authorize same-tailnet, same-owner FCP devices to join this "
            "Federation without a copied pairing code."
        )
    )
    parser.add_argument(
        "--bind",
        default="",
        help="host address to listen on; defaults to this device's Tailscale IP",
    )
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--app-url", default="")
    parser.add_argument("--secret-file", default="")
    parser.add_argument(
        "--check",
        action="store_true",
        help="report readiness and exit without serving",
    )
    return parser


def preflight(*, bind_host: str) -> tuple[int, list[str]]:
    """Return an exit code and human-readable readiness lines."""

    lines: list[str] = []
    identity = local_tailnet_identity()
    if identity is None:
        lines.append(
            "FAIL tailscale: the tailscale CLI is unavailable or this host is "
            "logged out. Run 'tailscale status' and sign in, then start FCP again."
        )
        return 1, lines
    lines.append(f"OK   tailscale: signed in as {identity.login_name}")
    lines.append(f"OK   tailnet: {identity.tailnet}")
    if not bind_host:
        lines.append(
            "FAIL bind address: no Tailscale IP was detected for this host."
        )
        return 1, lines
    lines.append(f"OK   bind address: {bind_host}")
    return 0, lines


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    bind_host = arguments.bind or os.getenv("FCP_TAILSCALE_IP", "")
    if not bind_host:
        identity = local_tailnet_identity()
        bind_host = identity.address if identity is not None else ""

    if arguments.check:
        code, lines = preflight(bind_host=bind_host)
        for line in lines:
            print(line)
        return code

    if not bind_host:
        print(
            "tailnet-join responder: no Tailscale address; automatic joining is "
            "disabled. Manual pairing codes still work.",
            file=sys.stderr,
        )
        return 1
    port = arguments.port or auto_join_port()
    secret_file = Path(arguments.secret_file) if arguments.secret_file else secret_path()
    ensure_secret(secret_file)
    app_url = arguments.app_url or application_url()
    print(
        f"tailnet-join responder: listening on {bind_host}:{port}",
        file=sys.stderr,
    )
    try:
        serve(
            bind_host=bind_host,
            port=port,
            app_url=app_url,
            secret_file=secret_file,
        )
    except KeyboardInterrupt:
        return 0
    except OSError as error:
        print(
            f"tailnet-join responder: could not listen on {bind_host}:{port} ({error})",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
