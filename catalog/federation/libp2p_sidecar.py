"""Async JSON-lines client for the F6.2 Go libp2p sidecar."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path

from .adaptive_transport import DirectTransportUnavailable
from .direct_peer import ChannelReady, JSON
from .errors import FederationValidationError

MAX_SIDECAR_LINE_BYTES = 2_097_152


class Libp2pSidecarClient:
    """Own one sidecar subprocess and multiplex direct packet requests."""

    def __init__(
        self,
        executable: Path | str,
        *,
        listen_multiaddr: str = "/ip4/127.0.0.1/tcp/0",
        extra_args: Sequence[str] = (),
        environment: Mapping[str, str] | None = None,
        request_timeout: float = 15.0,
        start_timeout: float = 15.0,
    ) -> None:
        if request_timeout <= 0 or start_timeout <= 0:
            raise ValueError("sidecar timeouts must be positive")
        self.executable = str(executable)
        self.listen_multiaddr = listen_multiaddr
        self.extra_args = tuple(extra_args)
        self.environment = dict(environment) if environment is not None else None
        self.request_timeout = float(request_timeout)
        self.start_timeout = float(start_timeout)
        self._process: asyncio.subprocess.Process | None = None
        self._handler: Callable[[str, JSON], Awaitable[JSON]] | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._pending: dict[str, asyncio.Future[JSON]] = {}
        self._write_lock = asyncio.Lock()
        self._ready: ChannelReady | None = None
        self._closed = False

    async def start(
        self,
        handler: Callable[[str, JSON], Awaitable[JSON]],
    ) -> ChannelReady:
        if self._closed:
            raise RuntimeError("libp2p sidecar client is closed")
        if self._ready is not None:
            self._handler = handler
            return self._ready
        self._handler = handler
        self._process = await asyncio.create_subprocess_exec(
            self.executable,
            "--listen",
            self.listen_multiaddr,
            *self.extra_args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self.environment,
        )
        try:
            event = await asyncio.wait_for(
                self._read_event(),
                timeout=self.start_timeout,
            )
        except Exception:
            await self.close()
            raise
        if event.get("event") != "ready":
            await self.close()
            raise DirectTransportUnavailable(
                "direct-sidecar-start-failed",
                "sidecar did not publish a ready event",
            )
        peer_id = event.get("peer_id")
        listen_addrs = event.get("listen_addrs")
        if not isinstance(peer_id, str) or not isinstance(listen_addrs, list):
            await self.close()
            raise DirectTransportUnavailable(
                "direct-sidecar-start-failed",
                "sidecar ready event is malformed",
            )
        self._ready = ChannelReady(peer_id, tuple(listen_addrs))
        self._reader_task = asyncio.create_task(
            self._reader_loop(),
            name=f"msh-libp2p-sidecar-{peer_id}",
        )
        self._stderr_task = asyncio.create_task(
            self._drain_stderr(),
            name=f"msh-libp2p-sidecar-stderr-{peer_id}",
        )
        return self._ready

    async def request(
        self,
        *,
        target_peer_id: str,
        target_multiaddr: str,
        payload: JSON,
    ) -> JSON:
        if self._ready is None or self._process is None:
            raise DirectTransportUnavailable(
                "direct-sidecar-not-started", "direct sidecar is not running"
            )
        correlation_id = str(uuid.uuid4())
        future: asyncio.Future[JSON] = asyncio.get_running_loop().create_future()
        self._pending[correlation_id] = future
        try:
            await self._write(
                {
                    "command": "request",
                    "correlation_id": correlation_id,
                    "target_peer_id": target_peer_id,
                    "target_multiaddr": target_multiaddr,
                    "payload": payload,
                }
            )
            return await asyncio.wait_for(future, timeout=self.request_timeout)
        except asyncio.TimeoutError as exc:
            raise DirectTransportUnavailable(
                "direct-sidecar-timeout", "direct sidecar request timed out"
            ) from exc
        finally:
            self._pending.pop(correlation_id, None)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        process, self._process = self._process, None
        if process is not None and process.returncode is None:
            try:
                await self._write({"command": "shutdown"}, process=process)
                await asyncio.wait_for(process.wait(), timeout=2.0)
            except (BrokenPipeError, ConnectionError, asyncio.TimeoutError):
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
        tasks = tuple(
            task
            for task in (self._reader_task, self._stderr_task, *self._handler_tasks)
            if task is not None
        )
        self._reader_task = None
        self._stderr_task = None
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._handler_tasks.clear()
        error = DirectTransportUnavailable(
            "direct-sidecar-closed", "direct sidecar connection closed"
        )
        for future in tuple(self._pending.values()):
            if not future.done():
                future.set_exception(error)
        self._pending.clear()
        self._ready = None

    async def _reader_loop(self) -> None:
        try:
            while not self._closed:
                event = await self._read_event()
                event_kind = event.get("event")
                correlation_id = event.get("correlation_id")
                if event_kind == "response" and isinstance(correlation_id, str):
                    future = self._pending.get(correlation_id)
                    payload = event.get("payload")
                    if future is not None and not future.done() and isinstance(payload, dict):
                        future.set_result(payload)
                    continue
                if event_kind == "error" and isinstance(correlation_id, str):
                    future = self._pending.get(correlation_id)
                    if future is None or future.done():
                        continue
                    code = event.get("code")
                    message = event.get("message")
                    if code == "remote-request-rejected":
                        future.set_exception(
                            FederationValidationError(
                                "direct-peer-rejected",
                                "packet",
                                "remote peer rejected the authenticated request",
                            )
                        )
                    else:
                        future.set_exception(
                            DirectTransportUnavailable(
                                str(code or "direct-sidecar-error"),
                                str(message or "direct sidecar request failed"),
                            )
                        )
                    continue
                if event_kind == "incoming_request" and isinstance(correlation_id, str):
                    task = asyncio.create_task(self._handle_incoming(event))
                    self._handler_tasks.add(task)
                    task.add_done_callback(self._finish_handler)
                    continue
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            for future in tuple(self._pending.values()):
                if not future.done():
                    future.set_exception(exc)

    async def _handle_incoming(self, event: JSON) -> None:
        correlation_id = event["correlation_id"]
        source_peer_id = event.get("source_peer_id")
        payload = event.get("payload")
        if (
            self._handler is None
            or not isinstance(source_peer_id, str)
            or not isinstance(payload, dict)
        ):
            await self._write(
                {
                    "command": "respond",
                    "correlation_id": correlation_id,
                    "error": "direct-peer-request-rejected",
                }
            )
            return
        try:
            response = await self._handler(source_peer_id, payload)
        except Exception:  # noqa: BLE001
            await self._write(
                {
                    "command": "respond",
                    "correlation_id": correlation_id,
                    "error": "direct-peer-request-rejected",
                }
            )
            return
        await self._write(
            {
                "command": "respond",
                "correlation_id": correlation_id,
                "payload": response,
            }
        )

    def _finish_handler(self, task: asyncio.Task[None]) -> None:
        self._handler_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    async def _read_event(self) -> JSON:
        process = self._process
        if process is None or process.stdout is None:
            raise DirectTransportUnavailable(
                "direct-sidecar-not-started", "sidecar stdout is unavailable"
            )
        line = await process.stdout.readline()
        if not line:
            raise DirectTransportUnavailable(
                "direct-sidecar-exited", "sidecar exited before completing the request"
            )
        if len(line) > MAX_SIDECAR_LINE_BYTES:
            raise FederationValidationError(
                "direct-sidecar-line-too-large", "sidecar", "sidecar event exceeds its bound"
            )
        try:
            value = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "invalid-direct-sidecar-event", "sidecar", "sidecar event is not JSON"
            ) from exc
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-direct-sidecar-event", "sidecar", "sidecar event must be an object"
            )
        return value

    async def _write(
        self,
        command: JSON,
        *,
        process: asyncio.subprocess.Process | None = None,
    ) -> None:
        process = process or self._process
        if process is None or process.stdin is None or process.returncode is not None:
            raise DirectTransportUnavailable(
                "direct-sidecar-exited", "sidecar stdin is unavailable"
            )
        line = json.dumps(
            command,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8") + b"\n"
        if len(line) > MAX_SIDECAR_LINE_BYTES:
            raise FederationValidationError(
                "direct-sidecar-line-too-large", "command", "sidecar command exceeds its bound"
            )
        async with self._write_lock:
            process.stdin.write(line)
            await process.stdin.drain()

    async def _drain_stderr(self) -> None:
        process = self._process
        if process is None or process.stderr is None:
            return
        while not self._closed:
            line = await process.stderr.readline()
            if not line:
                return
