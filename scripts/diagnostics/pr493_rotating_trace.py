"""Bounded, observer-only pytest trace for PR493's rotating release failure.

The product checkout is supplied separately by the workflow. This file only
wraps existing methods to record timings and request identities; it never
changes return values, synchronization, assertions, or timeouts.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import resource
import re
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable


class Trace:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.events = (root / "diagnostic.jsonl").open("a", encoding="utf-8", buffering=1)
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.sampler: threading.Thread | None = None

    def write(self, kind: str, **fields: Any) -> None:
        record = {
            "utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "monotonic": time.monotonic(),
            "kind": kind,
            **fields,
        }
        with self.lock:
            self.events.write(json.dumps(record, sort_keys=True, default=str) + "\n")
            self.events.flush()

    def start_resources(self) -> None:
        def sample() -> None:
            while not self.stop.wait(5):
                self.resource_sample("periodic")

        self.resource_sample("start")
        self.sampler = threading.Thread(target=sample, name="diagnostic-resource-sampler", daemon=True)
        self.sampler.start()

    def resource_sample(self, reason: str) -> None:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        fields: dict[str, Any] = {
            "reason": reason,
            "platform": platform.platform(),
            "pid": os.getpid(),
            "ru_maxrss": usage.ru_maxrss,
            "ru_utime": usage.ru_utime,
            "ru_stime": usage.ru_stime,
        }
        for path in (Path.cwd(), Path("/mnt/c")):
            try:
                disk = shutil.disk_usage(path)
                fields[f"disk_{path}"] = {"free": disk.free, "total": disk.total}
            except OSError as exc:
                fields[f"disk_{path}"] = {"error": type(exc).__name__}
        try:
            fields["loadavg"] = os.getloadavg()
        except (AttributeError, OSError):
            pass
        self.write("resource", **fields)

    def close(self) -> None:
        self.stop.set()
        if self.sampler is not None:
            self.sampler.join(timeout=2)
        self.resource_sample("finish")
        self.events.close()


TRACE: Trace


def _safe_error(exc: BaseException) -> dict[str, str]:
    return {"type": type(exc).__name__, "message": str(exc)[:500]}


_SENSITIVE = re.compile(
    r"(?i)(?P<key>authorization|password|passwd|secret|token|signature|private[_ -]?key)"
    r"(?P<sep>\s*[:=]\s*)(?P<value>[^\s,;]+)"
)


def _redact_traceback(value: Any) -> str:
    """Keep a useful traceback while removing common credential-shaped fields."""
    return _SENSITIVE.sub(lambda match: f"{match.group('key')}{match.group('sep')}<redacted>", str(value))[:20000]


def install_observers() -> None:
    from catalog.federation.persistence import CoordinatorStore
    from catalog.node.client import RelayNodeClient
    from catalog.relay.service import RelayServer

    envelope = RelayNodeClient._envelope

    def observed_envelope(self, *args: Any, **kwargs: Any):
        result = envelope(self, *args, **kwargs)
        TRACE.write(
            "client.envelope_created",
            message_type=result.message_type,
            request_id=result.request_id,
            session_id=result.session_id,
        )
        return result

    RelayNodeClient._envelope = observed_envelope  # type: ignore[method-assign]

    request = RelayNodeClient.request

    async def observed_request(self, message_type: str, *args: Any, **kwargs: Any):
        started = time.monotonic()
        TRACE.write("client.request_start", message_type=message_type, request_id=kwargs.get("request_id"))
        try:
            result = await request(self, message_type, *args, **kwargs)
        except BaseException as exc:
            TRACE.write("client.request_error", message_type=message_type, elapsed=time.monotonic() - started, error=_safe_error(exc))
            raise
        TRACE.write(
            "client.request_response",
            message_type=message_type,
            elapsed=time.monotonic() - started,
            response_keys=sorted(result) if isinstance(result, dict) else type(result).__name__,
        )
        return result

    RelayNodeClient.request = observed_request  # type: ignore[method-assign]

    dispatch = RelayServer._dispatch

    async def observed_dispatch(self, record: Any, request_envelope: Any):
        started = time.monotonic()
        TRACE.write(
            "server.dispatch_start",
            message_type=request_envelope.message_type,
            request_id=request_envelope.request_id,
            actor_node_id=record.node_id,
        )
        try:
            result = await dispatch(self, record, request_envelope)
        except BaseException as exc:
            TRACE.write(
                "server.dispatch_error",
                message_type=request_envelope.message_type,
                request_id=request_envelope.request_id,
                elapsed=time.monotonic() - started,
                error=_safe_error(exc),
            )
            raise
        TRACE.write(
            "server.dispatch_return",
            message_type=request_envelope.message_type,
            request_id=request_envelope.request_id,
            elapsed=time.monotonic() - started,
        )
        return result

    RelayServer._dispatch = observed_dispatch  # type: ignore[method-assign]

    send_live = RelayServer._send_live

    async def observed_send_live(self, record: Any, envelope_to_send: Any):
        started = time.monotonic()
        TRACE.write(
            "server.send_start",
            message_type=envelope_to_send.message_type,
            request_id=envelope_to_send.request_id,
            target_node_id=record.node_id,
        )
        try:
            result = await send_live(self, record, envelope_to_send)
        except BaseException as exc:
            TRACE.write(
                "server.send_error",
                message_type=envelope_to_send.message_type,
                request_id=envelope_to_send.request_id,
                elapsed=time.monotonic() - started,
                error=_safe_error(exc),
            )
            raise
        TRACE.write(
            "server.send_return",
            message_type=envelope_to_send.message_type,
            request_id=envelope_to_send.request_id,
            elapsed=time.monotonic() - started,
        )
        return result

    RelayServer._send_live = observed_send_live  # type: ignore[method-assign]

    create_session = CoordinatorStore.create_session

    def observed_create_session(self, *args: Any, **kwargs: Any):
        started = time.monotonic()
        TRACE.write("coordinator.create_session_start", request_id=kwargs.get("request_id"), actor_node_id=kwargs.get("actor_node_id"))
        try:
            result = create_session(self, *args, **kwargs)
        except BaseException as exc:
            TRACE.write("coordinator.create_session_error", elapsed=time.monotonic() - started, error=_safe_error(exc))
            raise
        TRACE.write("coordinator.create_session_return", elapsed=time.monotonic() - started, session_id=result.session_id)
        return result

    CoordinatorStore.create_session = observed_create_session  # type: ignore[method-assign]


class PytestTracePlugin:
    def pytest_sessionstart(self, session: Any) -> None:
        TRACE.write("pytest.session_start", pytest_args=list(getattr(session.config, "args", [])))

    def pytest_collection_finish(self, session: Any) -> None:
        items = [item.nodeid for item in session.items]
        TRACE.write("pytest.collection", count=len(items), nodeids=items)

    def pytest_runtest_logstart(self, nodeid: str, location: Any) -> None:
        TRACE.write("test.start", nodeid=nodeid, location=location)

    def pytest_runtest_setup(self, item: Any) -> None:
        TRACE.write("test.setup_start", nodeid=item.nodeid)

    def pytest_runtest_call(self, item: Any) -> None:
        TRACE.write("test.call_start", nodeid=item.nodeid)

    def pytest_runtest_teardown(self, item: Any) -> None:
        TRACE.write("test.teardown_start", nodeid=item.nodeid)

    def pytest_runtest_logreport(self, report: Any) -> None:
        TRACE.write("test.phase_result", nodeid=report.nodeid, phase=report.when, outcome=report.outcome, duration=report.duration)
        if report.failed:
            TRACE.write("test.failure_traceback", nodeid=report.nodeid, phase=report.when, traceback=_redact_traceback(report.longrepr))

    def pytest_runtest_logfinish(self, nodeid: str, location: Any) -> None:
        TRACE.write("test.finish", nodeid=nodeid, location=location)

    def pytest_sessionfinish(self, session: Any, exitstatus: int) -> None:
        TRACE.write("pytest.session_finish", exitstatus=exitstatus)


def git_identity() -> dict[str, Any]:
    def git(*args: str) -> str:
        return subprocess.check_output(["git", *args], text=True).strip()

    return {"sha": git("rev-parse", "HEAD"), "tree": git("rev-parse", "HEAD^{tree}"), "status": git("status", "--porcelain")}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--junitxml", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=1727)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    identity = git_identity()
    (args.output_dir / "source-identity.json").write_text(json.dumps(identity, indent=2) + "\n", encoding="utf-8")
    global TRACE
    TRACE = Trace(args.output_dir)
    TRACE.write("diagnostic.start", source=identity, seed=args.seed, python=platform.python_version(), argv=os.sys.argv)
    TRACE.start_resources()
    install_observers()
    import pytest

    pytest_args = [
        "-o", "addopts=", "-vv", "-x", "--tb=long", "--durations=20",
        "-p", "randomly", f"--randomly-seed={args.seed}", f"--junitxml={args.junitxml}",
    ]
    try:
        return int(pytest.main(pytest_args, plugins=[PytestTracePlugin()]))
    finally:
        TRACE.write("diagnostic.finish", junit_exists=args.junitxml.exists())
        TRACE.close()


if __name__ == "__main__":
    raise SystemExit(main())
