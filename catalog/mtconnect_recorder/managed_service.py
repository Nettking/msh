"""Compose-managed recorder entry point with an independent Federation runtime.

Local MTConnect capture remains the primary process commitment.  Federation
membership, capability advertisement, recorder control, and durable publication
run in a background companion and may fail or reconnect without stopping local
capture.

The companion never consumes a pairing key.  First pairing may be completed by
a separate bounded bootstrap while the recorder is already running; once the
public-safe reconnect state appears in the shared data directory, this runtime
notices it and connects automatically.
"""

from __future__ import annotations

from collections.abc import Callable
import os
from pathlib import Path
import sys
import threading
from typing import Any

from catalog.flask_app.services.capability_config_service import (
    CapabilityConfigError,
    load_capability_config,
    parse_recorder_sources,
)
from catalog.mtconnect_recorder import run
from catalog.mtconnect_recorder.federation_control import (
    RecorderFederationControlWorker,
)
from catalog.mtconnect_recorder.federation_node import RecorderFederationNode
from catalog.mtconnect_recorder.upgrade_compat import ensure_recorder_upgrade_config


DEFAULT_DEVICE_NAME = "FCP MTConnect recorder"
DEFAULT_POLL_SECONDS = 2.0
DEFAULT_REQUEST_TIMEOUT = 15.0


def _positive_float(value: str | None, default: float) -> float:
    if value is None or not value.strip():
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def configured_source_names(data_directory: Path) -> tuple[str, ...]:
    """Return only public-safe source names from the current recorder config."""

    config_path = data_directory / "capabilities" / "config.json"
    try:
        config = load_capability_config(config_path)
        sources = parse_recorder_sources(config.recorder_sources)
    except (CapabilityConfigError, OSError, ValueError) as exc:
        print(
            "Federation companion could not read recorder source names "
            f"({type(exc).__name__}); capability advertisement will omit them.",
            file=sys.stderr,
            flush=True,
        )
        return ()
    return tuple(sorted(sources))


class ManagedRecorderFederationRuntime:
    """Maintain recorder Federation services without owning local capture."""

    def __init__(
        self,
        *,
        data_directory: Path | str,
        device_name: str = DEFAULT_DEVICE_NAME,
        storage_group: str | None = None,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        node_factory: Callable[..., Any] = RecorderFederationNode,
        control_factory: Callable[..., Any] = RecorderFederationControlWorker,
        source_names_loader: Callable[[Path], tuple[str, ...]] = configured_source_names,
    ) -> None:
        if request_timeout <= 0 or poll_seconds <= 0:
            raise ValueError("Federation companion intervals must be positive")
        self.data_directory = Path(data_directory).resolve()
        self.device_name = device_name.strip() or DEFAULT_DEVICE_NAME
        self.storage_group = (
            storage_group.strip()
            if isinstance(storage_group, str) and storage_group.strip()
            else None
        )
        self.request_timeout = float(request_timeout)
        self.poll_seconds = float(poll_seconds)
        self.node_factory = node_factory
        self.control_factory = control_factory
        self.source_names_loader = source_names_loader
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._node: Any | None = None
        self._control: Any | None = None

    def connect_once(self) -> str:
        """Connect saved membership once, or report that pairing is still absent."""

        if self._node is not None:
            return "connected"
        node = self.node_factory(
            data_directory=self.data_directory,
            display_name=self.device_name,
            source_names=self.source_names_loader(self.data_directory),
            requested_storage_group=self.storage_group,
            request_timeout=self.request_timeout,
        )
        try:
            if not node.has_saved_membership():
                node.stop()
                return "waiting_for_pairing"
            snapshot = node.bootstrap()
            control = self.control_factory(
                node,
                data_directory=self.data_directory,
            )
            control.start()
        except Exception:
            node.stop()
            raise
        self._node = node
        self._control = control
        print("Federation companion: connected", flush=True)
        print(f"  Device:     {snapshot.node_id}", flush=True)
        print(f"  Federation: {snapshot.federation_id}", flush=True)
        print(f"  Session:    {snapshot.session_id}", flush=True)
        return "connected"

    def _run(self) -> None:
        waiting_reported = False
        failures = 0
        while not self._stop.is_set():
            if self._node is not None:
                self._stop.wait(self.poll_seconds)
                continue
            try:
                state = self.connect_once()
                failures = 0
                if state == "waiting_for_pairing":
                    if not waiting_reported:
                        print(
                            "Federation companion: waiting for saved pairing; "
                            "local recording continues.",
                            flush=True,
                        )
                        waiting_reported = True
                    self._stop.wait(self.poll_seconds)
                    continue
                waiting_reported = False
            except Exception as exc:  # noqa: BLE001 - optional background boundary
                failures += 1
                waiting_reported = False
                print(
                    "Federation companion unavailable "
                    f"({type(exc).__name__}); local recording continues.",
                    file=sys.stderr,
                    flush=True,
                )
                delay = min(
                    10.0,
                    self.poll_seconds * float(2 ** min(failures - 1, 3)),
                )
                self._stop.wait(delay)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="fcp-managed-recorder-federation",
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, timeout: float = 3.0) -> None:
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
        control, self._control = self._control, None
        node, self._node = self._node, None
        if control is not None:
            control.stop(timeout=timeout)
        if node is not None:
            node.stop(timeout=timeout)


def run_managed_recorder(
    *,
    capture_runner: Callable[[], Any] | None = None,
    companion_factory: Callable[..., ManagedRecorderFederationRuntime] = (
        ManagedRecorderFederationRuntime
    ),
) -> Any:
    """Start capture immediately and keep Federation lifecycle secondary."""

    data_directory = Path(os.environ.get("FCP_RECORDER_DATA_DIR", "data")).resolve()
    ensure_recorder_upgrade_config(data_directory=data_directory)
    companion = companion_factory(
        data_directory=data_directory,
        device_name=os.environ.get("FCP_RECORDER_DEVICE_NAME", DEFAULT_DEVICE_NAME),
        storage_group=os.environ.get("FCP_RECORDER_STORAGE_GROUP"),
        request_timeout=_positive_float(
            os.environ.get("FCP_RECORDER_FEDERATION_TIMEOUT"),
            DEFAULT_REQUEST_TIMEOUT,
        ),
        poll_seconds=_positive_float(
            os.environ.get("FCP_RECORDER_FEDERATION_POLL_SECONDS"),
            DEFAULT_POLL_SECONDS,
        ),
    )
    companion.start()
    try:
        return (capture_runner or run)()
    finally:
        companion.stop()


def main() -> int:
    run_managed_recorder()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
