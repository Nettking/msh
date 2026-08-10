"""Deterministic, cross-platform helpers for capability-first acceptance tests.

The helpers reopen existing stores and call existing authority services. They do
not define a replacement federation, membership, storage, or provider model.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Self

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import AuthenticationError, AuthorizationError
from catalog.federation.onboarding_discovery import SessionOnboardingAuthority
from catalog.federation.onboarding_models import FederationSessionBinding
from catalog.node.identity import IdentityStore, NodeCredentials


class HarnessTimeoutError(RuntimeError):
    """A deterministic harness operation did not complete within its bound."""


@dataclass(frozen=True)
class DeviceState:
    """One independent FCP device state root used only by tests."""

    name: str
    root: Path

    @property
    def identity_directory(self) -> Path:
        return self.root / "identity"

    def identity_store(self) -> IdentityStore:
        return IdentityStore(self.identity_directory, display_name=self.name)

    def create_identity(self, *, now: datetime) -> NodeCredentials:
        return self.identity_store().create(now=now)

    def reopen_identity(self) -> NodeCredentials:
        return self.identity_store().load()


@dataclass(frozen=True)
class MultiDeviceState:
    """Separate state directories for independent FCP devices."""

    root: Path
    devices: tuple[DeviceState, ...]

    @classmethod
    def create(cls, root: Path, names: Sequence[str]) -> MultiDeviceState:
        normalized = tuple(str(name).strip() for name in names)
        if len(normalized) < 2 or any(not name for name in normalized):
            raise ValueError("at least two non-empty device names are required")
        if len({name.casefold() for name in normalized}) != len(normalized):
            raise ValueError("device names must be unique")
        devices = tuple(
            DeviceState(name=name, root=root / f"device-{index:02d}")
            for index, name in enumerate(normalized, start=1)
        )
        return cls(root=root, devices=devices)

    def by_name(self, name: str) -> DeviceState:
        for device in self.devices:
            if device.name == name:
                return device
        raise KeyError(name)


@dataclass(frozen=True)
class ReservedPort:
    host: str
    port: int


@contextmanager
def reserved_tcp_port(host: str = "127.0.0.1") -> Iterator[ReservedPort]:
    """Hold an OS-assigned TCP port for the duration of the context."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind((host, 0))
        listener.listen(1)
        yield ReservedPort(host=host, port=int(listener.getsockname()[1]))


@dataclass(frozen=True)
class ProcessSpec:
    """A shell-free process specification that is portable to Windows."""

    args: tuple[str, ...]
    cwd: Path | None = None
    env: Mapping[str, str] | None = None

    def start(self) -> ManagedProcess:
        environment = None
        if self.env is not None:
            environment = {**os.environ, **dict(self.env)}
        process = subprocess.Popen(
            self.args,
            cwd=self.cwd,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=False,
        )
        return ManagedProcess(spec=self, process=process)


@dataclass
class ManagedProcess:
    spec: ProcessSpec
    process: subprocess.Popen[str]

    def stop(self, *, timeout_seconds: float = 5.0) -> None:
        if self.process.poll() is not None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=timeout_seconds)

    def restart(self, *, timeout_seconds: float = 5.0) -> ManagedProcess:
        self.stop(timeout_seconds=timeout_seconds)
        return self.spec.start()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.stop()


def python_marker_process(marker: Path) -> ProcessSpec:
    """Return a deterministic child process that publishes its PID then waits."""

    interpreter = getattr(sys, "_base_executable", None) or sys.executable
    script = (
        "import os, sys, time; "
        "from pathlib import Path; "
        "Path(sys.argv[1]).write_text(str(os.getpid()), encoding='utf-8'); "
        "time.sleep(60)"
    )
    return ProcessSpec((interpreter, "-c", script, str(marker)))


def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout_seconds: float = 5.0,
    interval_seconds: float = 0.02,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval_seconds)
    raise HarnessTimeoutError("condition did not become true before the timeout")


def atomic_write_json(path: Path, value: Any) -> None:
    """Write test state with replace semantics that work on Windows and Linux."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            descriptor = -1
            json.dump(value, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def write_partial_json(path: Path) -> None:
    """Inject a deterministic interrupted JSON write into an existing format."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'{"schema":"fcp.node.v1","node_id":')


def write_corrupted_bytes(path: Path) -> None:
    """Inject bounded invalid bytes without inventing a new state schema."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\x00CF7-A-corrupt-state\xff")


def restart_coordinator(
    database: Path,
    *,
    clock: Callable[[], datetime],
) -> SessionCoordinator:
    """Reopen the existing durable coordinator store after a simulated restart."""

    return SessionCoordinator(database, clock=clock)


def reconnect_after_restart(
    *,
    database: Path,
    device: DeviceState,
    binding: FederationSessionBinding,
    clock: Callable[[], datetime],
) -> tuple[SessionCoordinator, NodeCredentials, FederationSessionBinding]:
    coordinator = restart_coordinator(database, clock=clock)
    credentials = device.reopen_identity()
    authority = SessionOnboardingAuthority(coordinator, clock=clock)
    reconnected = authority.reconnect(credentials.identity, binding)
    return coordinator, credentials, reconnected


def assert_same_identity(first: NodeCredentials, second: NodeCredentials) -> None:
    assert first.identity == second.identity
    assert first.identity.node_id == second.identity.node_id
    assert first.identity.public_key == second.identity.public_key


def assert_member(
    coordinator: SessionCoordinator,
    *,
    session_id: str,
    node_id: str,
) -> None:
    coordinator.store.require_membership(session_id=session_id, node_id=node_id)


def assert_not_member(
    coordinator: SessionCoordinator,
    *,
    session_id: str,
    node_id: str,
) -> None:
    try:
        coordinator.store.require_membership(session_id=session_id, node_id=node_id)
    except (AuthenticationError, AuthorizationError):
        return
    raise AssertionError(f"{node_id} unexpectedly remains a member of {session_id}")


def assert_route_authorized(
    coordinator: SessionCoordinator,
    *,
    session_id: str,
    actor_node_id: str,
    target_node_id: str,
) -> dict[str, Any]:
    route = coordinator.authorize_route(
        session_id=session_id,
        actor_node_id=actor_node_id,
        target_node_id=target_node_id,
    )
    assert route["kind"] == "validated-session-membership"
    assert route["validated_by"] == coordinator.coordinator_id
    return route


def assert_fenced(
    coordinator: SessionCoordinator,
    *,
    session_id: str,
    actor_node_id: str,
    target_node_id: str,
) -> None:
    try:
        coordinator.authorize_route(
            session_id=session_id,
            actor_node_id=actor_node_id,
            target_node_id=target_node_id,
        )
    except (AuthenticationError, AuthorizationError):
        return
    raise AssertionError("removed membership still has route authority")


def assert_no_private_data(
    value: Any,
    *,
    secrets: Sequence[str] = (),
    forbidden_fragments: Sequence[str] = (),
) -> None:
    """Assert that public output omits supplied secrets and private locations."""

    rendered = json.dumps(value, sort_keys=True, default=str).casefold()
    for secret in secrets:
        assert secret.casefold() not in rendered
    fragments = (
        "identity.pem",
        "server_settings.json",
        "sqlite://",
        "postgresql://",
        "mongodb://",
        "redis://",
        "127.0.0.1:",
        "localhost:",
        *forbidden_fragments,
    )
    for fragment in fragments:
        assert fragment.casefold() not in rendered


def load_scenario_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
