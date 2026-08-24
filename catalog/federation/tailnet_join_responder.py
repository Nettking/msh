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
import signal
import socketserver
import subprocess
import sys
import tempfile
import threading
import urllib.error
import urllib.request
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path

from .tailnet_join_bridge import (
    GRANT_REQUEST_SCHEMA,
    SECRET_HEADER,
    auto_join_port,
    ensure_secret,
    pid_path,
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
PROCESS_RECORD_SCHEMA = "fcp.federation.tailnet-auto-join-process.v2"
MAX_PROCESS_RECORD_BYTES = 4096
MAX_PROCESS_START_TOKEN_LENGTH = 512


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
    # Windows SO_REUSEADDR permits two live listeners to bind the same port.
    # The replacement must either own the port or fail explicitly, never race
    # an older or unrelated listener for join requests.
    allow_reuse_address = os.name != "nt"
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


def _windows_start_token_from_handle(handle: object) -> str | None:
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetProcessTimes.argtypes = (
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
    )
    kernel32.GetProcessTimes.restype = wintypes.BOOL
    creation = wintypes.FILETIME()
    exit_time = wintypes.FILETIME()
    kernel_time = wintypes.FILETIME()
    user_time = wintypes.FILETIME()
    if not kernel32.GetProcessTimes(
        handle,
        ctypes.byref(creation),
        ctypes.byref(exit_time),
        ctypes.byref(kernel_time),
        ctypes.byref(user_time),
    ):
        return None
    value = (int(creation.dwHighDateTime) << 32) | int(creation.dwLowDateTime)
    return f"windows:{value}"


def _windows_process_start_token(pid: int) -> str | None:
    import ctypes
    from ctypes import wintypes

    process_query_limited_information = 0x1000
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
    if not handle:
        return None
    try:
        return _windows_start_token_from_handle(handle)
    finally:
        kernel32.CloseHandle(handle)


def _terminate_windows_process_if_same_instance(
    pid: int, expected_start_token: str
) -> bool:
    import ctypes
    from ctypes import wintypes

    process_terminate = 0x0001
    process_query_limited_information = 0x1000
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.TerminateProcess.argtypes = (wintypes.HANDLE, wintypes.UINT)
    kernel32.TerminateProcess.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(
        process_query_limited_information | process_terminate, False, pid
    )
    if not handle:
        return False
    try:
        if _windows_start_token_from_handle(handle) != expected_start_token:
            return False
        return bool(kernel32.TerminateProcess(handle, 1))
    finally:
        kernel32.CloseHandle(handle)


def _proc_process_start_token(pid: int) -> str | None:
    try:
        raw_stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(
            encoding="utf-8"
        ).strip()
    except (OSError, UnicodeDecodeError):
        return None
    closing_parenthesis = raw_stat.rfind(")")
    if closing_parenthesis < 0 or not boot_id:
        return None
    # Fields after ``comm`` begin with field 3; process start time is field 22.
    fields = raw_stat[closing_parenthesis + 2 :].split()
    if len(fields) <= 19 or not fields[19].isdigit():
        return None
    return f"linux:{boot_id}:{fields[19]}"


def _ps_process_start_token(pid: int) -> str | None:
    try:
        completed = subprocess.run(
            ("ps", "-o", "lstart=", "-p", str(pid)),
            capture_output=True,
            text=True,
            check=False,
            timeout=5.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    value = " ".join((completed.stdout or "").split())
    if completed.returncode != 0 or not value:
        return None
    return f"posix:{value}"


def process_start_token(pid: int) -> str | None:
    """Return an OS process-creation identity, not merely process existence."""

    if pid <= 0:
        return None
    if os.name == "nt":
        return _windows_process_start_token(pid)
    return _proc_process_start_token(pid) or _ps_process_start_token(pid)


def terminate_process_if_same_instance(pid: int, expected_start_token: str) -> bool:
    """Terminate only when the PID still names the recorded process instance."""

    if pid <= 0 or not expected_start_token:
        return False
    if os.name == "nt":
        # OpenProcess pins the kernel process object while creation identity is
        # checked and termination is requested, closing the PID-reuse race.
        return _terminate_windows_process_if_same_instance(
            pid, expected_start_token
        )
    if process_start_token(pid) != expected_start_token:
        return False
    # Re-read immediately before signalling. A stale record or PID reuse fails
    # closed instead of allowing an unrelated host process to be terminated.
    if process_start_token(pid) != expected_start_token:
        return False
    try:
        os.kill(pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError):
        return False
    return True


def stop_previous_instance(pid_file: Path) -> int | None:
    """Terminate a responder left over from an earlier start.

    Without this, a stale responder keeps the port and the replacement cannot
    bind, which on Windows previously left the launcher announcing a listener
    that was never there.
    """

    path = Path(pid_file)
    try:
        if path.stat().st_size > MAX_PROCESS_RECORD_BYTES:
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or payload.get("schema") != PROCESS_RECORD_SCHEMA:
        return None
    pid = payload.get("pid")
    start_token = payload.get("start_token")
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return None
    if not isinstance(start_token, str):
        return None
    start_token = start_token.strip()
    if not start_token or len(start_token) > MAX_PROCESS_START_TOKEN_LENGTH:
        return None
    if pid == os.getpid():
        return None
    return pid if terminate_process_if_same_instance(pid, start_token) else None


def write_pid_file(pid_file: Path) -> None:
    path = Path(pid_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    pid = os.getpid()
    start_token = process_start_token(pid)
    if start_token is None:
        raise RuntimeError("could not determine responder process identity")
    payload = json.dumps(
        {
            "schema": PROCESS_RECORD_SCHEMA,
            "pid": pid,
            "start_token": start_token,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        with suppress(OSError):
            temporary_path.unlink(missing_ok=True)


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
    parser.add_argument("--pid-file", default="")
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
    pid_file = Path(arguments.pid_file) if arguments.pid_file else pid_path()

    # A responder from an earlier start still owns the port. Replace it rather
    # than failing to bind behind a launcher that already claimed success.
    replaced = stop_previous_instance(pid_file)
    if replaced is not None:
        print(
            f"tailnet-join responder: replaced earlier instance (pid {replaced})",
            file=sys.stderr,
        )

    try:
        server = build_server(
            bind_host=bind_host,
            port=port,
            app_url=app_url,
            secret_file=secret_file,
        )
    except OSError as error:
        print(
            f"tailnet-join responder: could not listen on {bind_host}:{port} "
            f"({error}). Automatic Federation joining is unavailable; manual "
            "pairing codes still work.",
            file=sys.stderr,
        )
        return 1

    # Only now is the listener real. Refuse to advertise it unless future
    # replacement can prove this exact process instance rather than trust a PID.
    try:
        write_pid_file(pid_file)
    except (OSError, RuntimeError) as error:
        server.server_close()
        print(
            "tailnet-join responder: could not record a safe process identity "
            f"({error}). Automatic Federation joining is unavailable; manual "
            "pairing codes still work.",
            file=sys.stderr,
        )
        return 1
    print(
        f"tailnet-join responder: listening on {bind_host}:{port}",
        file=sys.stderr,
    )
    try:
        with server:
            server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        return 0
    # Retain the final process record. A later start can prove that the process
    # is gone before overwriting it; unlinking here could race a replacement and
    # delete the replacement's identity record.
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
