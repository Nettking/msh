"""Bounded, read-only relay listener probe for the product health surface."""

from __future__ import annotations

import ipaddress
import queue
import socket
import threading
import time
from collections.abc import Callable
from typing import Any

RESOLUTION_TIMEOUT_SECONDS = 0.2
TOTAL_PROBE_TIMEOUT_SECONDS = 0.5
MAX_RESOLVED_ENDPOINTS = 4

AddressInfo = tuple[int, int, int, str, tuple[Any, ...]]
Resolver = Callable[..., list[AddressInfo]]


class BoundedRelayListenerProbe:
    """Resolve and connect with bounded caller time and bounded worker growth.

    ``socket.create_connection`` applies its timeout only after name resolution.
    The product uses the Compose service name ``relay``, so DNS must be bounded
    independently. One daemon worker owns resolution and the request queue holds
    at most one additional lookup. A wedged resolver therefore cannot create an
    unbounded thread/process backlog, and callers fail closed after the deadline.
    """

    def __init__(
        self,
        *,
        resolver: Resolver = socket.getaddrinfo,
        resolution_timeout: float = RESOLUTION_TIMEOUT_SECONDS,
        total_timeout: float = TOTAL_PROBE_TIMEOUT_SECONDS,
    ) -> None:
        self._resolver = resolver
        self._resolution_timeout = max(0.001, float(resolution_timeout))
        self._total_timeout = max(self._resolution_timeout, float(total_timeout))
        self._requests: queue.Queue[
            tuple[str, int, queue.Queue[tuple[AddressInfo, ...]]]
        ] = queue.Queue(maxsize=1)
        self._worker_lock = threading.Lock()
        self._worker: threading.Thread | None = None

    def _ensure_worker(self) -> None:
        with self._worker_lock:
            if self._worker is not None and self._worker.is_alive():
                return
            self._worker = threading.Thread(
                target=self._resolution_loop,
                name="fcp-relay-health-resolver",
                daemon=True,
            )
            self._worker.start()

    def _resolution_loop(self) -> None:
        while True:
            host, port, response = self._requests.get()
            try:
                values = self._resolver(
                    host,
                    port,
                    type=socket.SOCK_STREAM,
                )
                endpoints = tuple(values[:MAX_RESOLVED_ENDPOINTS])
            except (OSError, socket.gaierror):
                endpoints = ()
            try:
                response.put_nowait(endpoints)
            except queue.Full:
                pass
            finally:
                self._requests.task_done()

    @staticmethod
    def _numeric_endpoint(host: str, port: int) -> tuple[AddressInfo, ...] | None:
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            return None
        family = socket.AF_INET6 if address.version == 6 else socket.AF_INET
        sockaddr: tuple[Any, ...]
        if family == socket.AF_INET6:
            sockaddr = (host, port, 0, 0)
        else:
            sockaddr = (host, port)
        return ((family, socket.SOCK_STREAM, 0, "", sockaddr),)

    def _resolve(self, host: str, port: int) -> tuple[AddressInfo, ...]:
        numeric = self._numeric_endpoint(host, port)
        if numeric is not None:
            return numeric

        self._ensure_worker()
        response: queue.Queue[tuple[AddressInfo, ...]] = queue.Queue(maxsize=1)
        try:
            self._requests.put_nowait((host, port, response))
        except queue.Full:
            return ()
        try:
            return response.get(timeout=self._resolution_timeout)
        except queue.Empty:
            return ()

    def __call__(self, host: str, port: int) -> bool:
        started = time.monotonic()
        endpoints = self._resolve(host, port)
        for family, socktype, proto, _canonname, sockaddr in endpoints:
            remaining = self._total_timeout - (time.monotonic() - started)
            if remaining <= 0:
                return False
            candidate = socket.socket(family, socktype, proto)
            try:
                candidate.settimeout(remaining)
                candidate.connect(sockaddr)
                return True
            except (OSError, TimeoutError):
                continue
            finally:
                candidate.close()
        return False


bounded_relay_listener_probe = BoundedRelayListenerProbe()

__all__ = ["BoundedRelayListenerProbe", "bounded_relay_listener_probe"]
