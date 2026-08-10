from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import socket

from .source_inventory_service import SourceInventoryService


@dataclass(frozen=True)
class ConnectionTestResult:
    ok: bool
    title: str
    message: str
    target: str
    detail: str = ""


class SourceConnectionTestService:
    """Connection checks for machine-side data sources.

    The tests run from the Flask server/container. That is intentional: the
    relevant question is whether FCP can reach the machine network, not whether
    the user's browser can reach it.
    """

    def __init__(
        self,
        inventory_service: SourceInventoryService | None = None,
        *,
        urlopen_func: Callable[..., Any] = urlopen,
        socket_connect_func: Callable[..., Any] = socket.create_connection,
        timeout_seconds: float = 4.0,
    ) -> None:
        self.inventory_service = inventory_service or SourceInventoryService()
        self.urlopen_func = urlopen_func
        self.socket_connect_func = socket_connect_func
        self.timeout_seconds = timeout_seconds

    def test_mtconnect(self, machine_id: str) -> ConnectionTestResult:
        machine = self._machine(machine_id)
        if machine is None:
            return ConnectionTestResult(False, "MTConnect test failed", "Machine was not found.", machine_id)
        url = _mtconnect_current_url(str(machine.get("mtconnect_url") or ""))
        if not url:
            return ConnectionTestResult(
                False,
                "MTConnect test not configured",
                "Add an MTConnect URL for this machine before testing.",
                str(machine.get("name") or machine_id),
            )
        request = Request(url, headers={"User-Agent": "FCP-connection-test/1.0"})
        try:
            with self.urlopen_func(request, timeout=self.timeout_seconds) as response:
                status = getattr(response, "status", None) or getattr(response, "code", None) or response.getcode()
                sample = response.read(512)
        except HTTPError as exc:
            return ConnectionTestResult(
                False,
                "MTConnect HTTP error",
                f"Reached the MTConnect host, but it returned HTTP {exc.code}.",
                url,
                str(exc),
            )
        except (URLError, TimeoutError, OSError) as exc:
            return ConnectionTestResult(False, "MTConnect test failed", "FCP could not reach the MTConnect endpoint.", url, str(exc))
        detail = f"HTTP {status}. Received {len(sample or b'')} byte(s)."
        return ConnectionTestResult(True, "MTConnect reachable", "FCP reached the MTConnect endpoint from the server/container.", url, detail)

    def test_vpn_network(self, machine_id: str) -> ConnectionTestResult:
        machine = self._machine(machine_id)
        if machine is None:
            return ConnectionTestResult(False, "VPN/network test failed", "Machine was not found.", machine_id)
        host, port = _vpn_target(machine)
        if not host:
            return ConnectionTestResult(
                False,
                "VPN/network test not configured",
                "Add a VPN test host or MTConnect URL for this machine before testing.",
                str(machine.get("name") or machine_id),
            )
        target = f"{host}:{port}"
        try:
            connection = self.socket_connect_func((host, port), timeout=self.timeout_seconds)
            close = getattr(connection, "close", None)
            if callable(close):
                close()
        except (TimeoutError, OSError) as exc:
            return ConnectionTestResult(
                False,
                "VPN/network test failed",
                "FCP could not open a TCP connection to the configured machine-network host.",
                target,
                str(exc),
            )
        return ConnectionTestResult(
            True,
            "VPN/network reachable",
            "FCP can open a TCP connection to the configured machine-network host. This indicates the machine network/VPN path is reachable from the server/container.",
            target,
        )

    def _machine(self, machine_id: str) -> dict[str, Any] | None:
        machine_id = str(machine_id or "").strip()
        for machine in self.inventory_service.load().get("machines", []):
            if str(machine.get("id") or "") == machine_id:
                return machine
        return None


def _mtconnect_current_url(raw_url: str) -> str:
    raw_url = str(raw_url or "").strip()
    if not raw_url:
        return ""
    if "://" not in raw_url:
        raw_url = f"http://{raw_url}"
    parsed = urlparse(raw_url)
    if not parsed.netloc:
        return ""
    path = parsed.path.rstrip("/")
    if not path:
        path = "/current"
    elif path.lower().endswith(("/current", "/probe", "/sample")):
        path = parsed.path
    else:
        path = f"{path}/current"
    return parsed._replace(path=path, query="", fragment="").geturl()


def _vpn_target(machine: dict[str, Any]) -> tuple[str, int]:
    host = str(machine.get("vpn_test_host") or "").strip()
    port_text = str(machine.get("vpn_test_port") or "").strip()
    if host and ":" in host and not port_text:
        host_part, _, port_part = host.rpartition(":")
        host = host_part.strip()
        port_text = port_part.strip()
    if not host:
        parsed = urlparse(_mtconnect_current_url(str(machine.get("mtconnect_url") or "")))
        host = parsed.hostname or ""
        if not port_text and parsed.port:
            port_text = str(parsed.port)
        if not port_text and parsed.scheme == "https":
            port_text = "443"
    port = _safe_port(port_text, default=80)
    return host, port


def _safe_port(value: str, *, default: int) -> int:
    try:
        port = int(value)
    except (TypeError, ValueError):
        return default
    return port if 1 <= port <= 65535 else default
