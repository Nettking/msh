"""Compose-managed recorder entry point with an independent Federation runtime.

Local MTConnect capture remains the primary process commitment. Federation
membership, capability advertisement, recorder control, durable publication,
and bounded software-update intent processing run in a background companion and
may fail or reconnect without stopping local capture.

The companion never consumes a pairing key. First pairing may be completed by a
separate bounded bootstrap while the recorder is already running; once the
public-safe reconnect state appears in the shared data directory, this runtime
notices it and connects automatically.

Software updates preserve the existing authority split: the recorder container
only replays authenticated Federation intents and writes bounded declarative
handoff files under ``data/federation/update-agent``. A separately owned host
update agent must independently revalidate Git, rebuild/restart Compose, and
prove the running commit before success is reported.
"""

from __future__ import annotations

import os
import sys
import threading
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from catalog.federation.service_incarnation import (
    STOP_COMPLETED,
    STOP_FAILURE,
    STOP_OPERATOR,
    STOP_UPDATE,
    incarnation_state_file,
    record_service_start,
    record_service_stop,
)
from catalog.flask_app.services.capability_config_service import (
    CapabilityConfigError,
    load_capability_config,
    parse_recorder_sources,
)
from catalog.flask_app.services.federation_active_leader_runtime import (
    ActiveLeaderFederationUpdateEventProcessor as FederationUpdateEventProcessor,
)
from catalog.flask_app.services.federation_update_handoff import HostUpdateHandoff
from catalog.mtconnect_recorder import run
from catalog.mtconnect_recorder.acceptance_observability import process_provenance
from catalog.mtconnect_recorder.federation_control import (
    RecorderFederationControlWorker,
)
from catalog.mtconnect_recorder.federation_node import RecorderFederationNode
from catalog.mtconnect_recorder.upgrade_compat import ensure_recorder_upgrade_config

DEFAULT_DEVICE_NAME = "FCP MTConnect recorder"
DEFAULT_POLL_SECONDS = 2.0
DEFAULT_REQUEST_TIMEOUT = 15.0


def _health_provenance() -> dict[str, Any]:
    try:
        value = process_provenance()
        return dict(value)
    except Exception:  # noqa: BLE001 - unavailable evidence cannot change workload lifecycle
        return {"candidate_sha": None, "runtime_generation": None,
                "supervisor_generation": None, "pid": None}


def _health_generation() -> str | None:
    try:
        return uuid.uuid4().hex
    except Exception:  # noqa: BLE001 - an observer UUID is never a startup prerequisite
        return None


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
        update_processor_factory: Callable[..., Any] = FederationUpdateEventProcessor,
        handoff_factory: Callable[..., Any] = HostUpdateHandoff,
        source_names_loader: Callable[
            [Path], tuple[str, ...]
        ] = configured_source_names,
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
        self.update_processor_factory = update_processor_factory
        self.handoff_factory = handoff_factory
        self.source_names_loader = source_names_loader
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._node: Any | None = None
        self._control: Any | None = None
        self._update_processor: Any | None = None
        # These references observe the existing lifecycle; they never control it.
        # Keep a joined-out thread/future visible if its ordinary stop times out.
        self._health_lock = threading.Lock()
        self._health_process = _health_provenance()
        self._health_generation: str | None = None
        self._health_thread: threading.Thread | None = None
        self._health_control: Any | None = None
        self._health_control_thread: threading.Thread | None = None
        self._health_control_generation: str | None = None
        self._health_publication: Any | None = None
        self._health_publication_thread: threading.Thread | None = None
        self._health_publication_loop: Any | None = None
        self._health_publication_generation: str | None = None
        self._health_node_id: str | None = None
        self._health_session_id: str | None = None
        self._health_outcome = "not-started"
        self._health_activity_ns: int | None = None
        self._health_failures = 0
        self._health_error = ""
        self._health_stop_requested = False
        self._health_generation_overlap = False
        self._health_overlapped_workers: tuple[tuple[str, str, Any], ...] = ()
        self._health_overlap_evidence_complete = True

    def _observe_cycle(
        self,
        generation: str | None,
        outcome: str,
        *,
        error: BaseException | None = None,
    ) -> None:
        """Update constant-space evidence, without changing a worker decision."""

        try:
            with self._health_lock:
                if generation != self._health_generation:
                    return
                self._health_outcome = outcome
                self._health_activity_ns = time.monotonic_ns()
                if error is not None:
                    self._health_failures = min(1_000_000_000, self._health_failures + 1)
                    # Exception text can contain endpoints or credentials.
                    self._health_error = type(error).__name__[:96]
                elif outcome == "completed":
                    self._health_failures = 0
                    self._health_error = ""
        except Exception:  # noqa: BLE001, S110 - observer failure cannot affect or log workload data
            # An observer must never decide whether capture or reconnect runs.
            pass

    @staticmethod
    def _public_identity(value: object) -> str | None:
        if isinstance(value, str) and 0 < len(value) <= 192 and all(
            character.isascii() and (character.isalnum() or character in "._:-")
            for character in value
        ):
            return value
        return None

    def _observe_connected(self, generation: str | None, node: Any, control: Any, snapshot: Any) -> None:
        try:
            with self._health_lock:
                if generation != self._health_generation:
                    return
                self._health_control = control
                self._health_control_thread = getattr(control, "_thread", None)
                self._health_control_generation = (
                    _health_generation() if self._health_control_thread is not None else None
                )
                self._health_publication = getattr(node, "_publication_future", None)
                runtime = getattr(node, "runtime", None)
                self._health_publication_thread = getattr(runtime, "_thread", None)
                self._health_publication_loop = getattr(runtime, "_loop", None)
                self._health_publication_generation = (
                    _health_generation() if self._health_publication is not None else None
                )
                self._health_node_id = self._public_identity(getattr(snapshot, "node_id", None))
                self._health_session_id = self._public_identity(getattr(snapshot, "session_id", None))
        except Exception:  # noqa: BLE001, S110 - observer failure leaves evidence unavailable
            pass

    def federation_snapshot(self) -> dict[str, Any]:
        """Read the already-connected node's immutable in-memory status only."""

        try:
            current = _health_provenance()
            with self._health_lock:
                if (
                    current.get("runtime_generation") is None
                    or current.get("runtime_generation") != self._health_process.get("runtime_generation")
                    or self._health_generation_overlap
                    or self._health_stop_requested
                ):
                    return {"status": "unavailable"}
                node = self._node
            if node is None:
                return {"status": "not-started"}
            snapshot = node.snapshot()
            return {
                key: self._public_identity(getattr(snapshot, key, None))
                for key in (
                    "status", "node_id", "federation_id", "session_id",
                    "storage_state", "storage_group", "storage_authority_node_id",
                )
            }
        except Exception:  # noqa: BLE001 - read-only status must never prevent capture
            return {"status": "unavailable"}

    def health_snapshot(self, *, expected_runtime_generation: str | None = None) -> dict[str, Any]:
        """Sample actual managed workers through the existing Recorder heartbeat.

        Thread/future liveness is distinct from a completed healthy cycle. A
        retrying worker remains alive, while absent context, an old generation,
        and a worker that silently exited cannot be reported healthy.
        """

        current = _health_provenance()
        observed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        observed_ns = time.monotonic_ns()
        with self._health_lock:
            matching = (
                current.get("runtime_generation") is not None
                and current.get("runtime_generation") == self._health_process.get("runtime_generation")
                and (expected_runtime_generation is None or expected_runtime_generation == current.get("runtime_generation"))
                and not self._health_generation_overlap
            )
            base = {
                **current,
                "observed_at_utc": observed_at,
                "observed_at_monotonic_ns": observed_ns,
                "node_id": self._health_node_id,
                "session_id": self._health_session_id,
                "generation_current": matching,
                "generation_overlap": self._health_generation_overlap,
                "overlap_evidence_complete": self._health_overlap_evidence_complete,
                "overlapped_workers": [
                    {"worker": name, "kind": kind,
                     "alive" if kind == "thread" else "pending": (
                         reference.is_alive() if kind == "thread" else not reference.done()
                     )}
                    for name, kind, reference in self._health_overlapped_workers
                ],
                "supervisor": "compose",
                "stop_requested": self._health_stop_requested,
            }
            thread = self._health_thread
            companion_alive = bool(thread is not None and thread.is_alive())
            control_thread = self._health_control_thread
            control_alive = bool(control_thread is not None and control_thread.is_alive())
            control_health = getattr(self._health_control, "health", None)
            control_record = control_health.snapshot() if control_health is not None else {}
            future = self._health_publication
            publication_thread = self._health_publication_thread
            publication_loop = self._health_publication_loop
            publication_thread_alive = bool(
                publication_thread is not None and publication_thread.is_alive()
            )
            publication_loop_running = bool(
                publication_loop is not None and publication_loop.is_running()
            )
            publication_alive = bool(
                future is not None and not future.done()
                and publication_thread_alive and publication_loop_running
            )

            def entry(alive: bool, generation: str | None, *, outcome: str, failures: int = 0, error: str = "", activity_ns: int | None = None) -> dict[str, Any]:
                started = generation is not None
                if not matching:
                    state = "stale-generation"
                elif self._health_stop_requested:
                    state = "stopping" if alive else "stopped"
                elif not started:
                    state = "starting"
                elif not alive:
                    state = "failed"
                elif failures:
                    state = "recovering"
                else:
                    state = "alive"
                return {
                    **base,
                    "worker_generation": generation,
                    "started": started,
                    "alive": alive,
                    "state": state,
                    # False means no completed healthy-cycle proof. Liveness is
                    # separate: control/future observations alone never invent
                    # a successful operation for an idle or recovering loop.
                    "healthy": bool(matching and started and alive and not self._health_stop_requested and outcome == "completed" and not failures),
                    "last_cycle_outcome": outcome,
                    "last_activity_monotonic_ns": activity_ns,
                    "consecutive_failures": failures,
                    "last_error_code": error,
                }

            publication_outcome = "running" if publication_alive else "not-started"
            if future is not None and future.done():
                publication_outcome = "cancelled" if future.cancelled() else "exited"
            return {
                "managed_companion": entry(companion_alive, self._health_generation,
                    outcome=self._health_outcome, failures=self._health_failures,
                    error=self._health_error, activity_ns=self._health_activity_ns),
                "federation_control": entry(control_alive, self._health_control_generation,
                    outcome="observed" if control_record.get("started") else "not-started",
                    failures=int(control_record.get("consecutive_failures", 0)),
                    error=str(control_record.get("last_error_code") or "")[:200]),
                "recorder_publication": {
                    **entry(publication_alive, self._health_publication_generation,
                        outcome=publication_outcome),
                    "event_loop_thread_alive": publication_thread_alive,
                    "event_loop_running": publication_loop_running,
                    "future_pending": bool(future is not None and not future.done()),
                },
            }

    def _build_update_processor(self, node: Any) -> Any:
        federation_root = self.data_directory / "federation"
        return self.update_processor_factory(
            node.service,
            self.handoff_factory(federation_root / "update-agent"),
            federation_root / "update-events" / "processor.json",
        )

    def connect_once(self) -> str:
        """Connect saved membership once, or report that pairing is still absent."""

        generation = self._health_generation
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
            update_processor = self._build_update_processor(node)
        except Exception:
            node.stop()
            raise
        self._node = node
        self._control = control
        self._update_processor = update_processor
        self._observe_connected(generation, node, control, snapshot)
        print("Federation companion: connected", flush=True)
        print(f"  Device:     {snapshot.node_id}", flush=True)
        print(f"  Federation: {snapshot.federation_id}", flush=True)
        print(f"  Session:    {snapshot.session_id}", flush=True)
        return "connected"

    def process_update_events_once(self) -> bool:
        """Replay one bounded update-control cycle when membership is connected."""

        node = self._node
        processor = self._update_processor
        if node is None or processor is None:
            return False
        context_loader = getattr(node.service, "authorized_context", None)
        context = context_loader() if callable(context_loader) else None
        if context is None:
            return False
        processor.process(context)
        return True

    def _run(self, generation: str | None) -> None:
        self._observe_cycle(generation, "started")
        try:
            self._run_companion(generation)
        except BaseException as exc:
            self._observe_cycle(generation, "failed", error=exc)
            raise
        finally:
            if self._stop.is_set():
                self._observe_cycle(generation, "stopped")

    def _run_companion(self, generation: str | None) -> None:
        waiting_reported = False
        failures = 0
        while not self._stop.is_set():
            if self._node is not None:
                try:
                    processed = self.process_update_events_once()
                    self._observe_cycle(generation, "completed" if processed else "no-context")
                except Exception as exc:  # noqa: BLE001 - update path fails closed
                    self._observe_cycle(generation, "retrying", error=exc)
                    print(
                        "Federation update processing unavailable "
                        f"({type(exc).__name__}); local recording continues.",
                        file=sys.stderr,
                        flush=True,
                    )
                self._stop.wait(self.poll_seconds)
                continue
            try:
                state = self.connect_once()
                self._observe_cycle(generation, "completed" if state == "connected" else "waiting-for-pairing")
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
                self._observe_cycle(generation, "retrying", error=exc)
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
        try:
            with self._health_lock:
                if (
                    (self._health_thread is not None and self._health_thread.is_alive())
                    or (self._health_control_thread is not None and self._health_control_thread.is_alive())
                    or (self._health_publication is not None and not self._health_publication.done())
                    or (self._health_publication_thread is not None and self._health_publication_thread.is_alive())
                ):
                    self._health_generation_overlap = True
                    if not self._health_overlapped_workers:
                        # Retain one actual overlapping cohort, bounded at four
                        # references. More overlaps invalidate completeness;
                        # they cannot turn an unobserved survivor into healthy.
                        self._health_overlapped_workers = tuple(
                            (name, kind, reference)
                            for name, kind, reference in (
                                ("managed_companion", "thread", self._health_thread),
                                ("federation_control", "thread", self._health_control_thread),
                                ("recorder_publication", "future", self._health_publication),
                                ("publication_event_loop", "thread", self._health_publication_thread),
                            )
                            if reference is not None
                        )
                    else:
                        self._health_overlap_evidence_complete = False
                self._health_generation = _health_generation()
                self._health_stop_requested = False
                self._health_outcome = "starting"
                self._health_failures = 0
                self._health_error = ""
                self._health_activity_ns = None
        except Exception:  # noqa: BLE001 - observation failure cannot prevent ordinary startup
            self._health_generation = None
        self._thread = threading.Thread(
            target=self._run,
            args=(self._health_generation,),
            name="fcp-managed-recorder-federation",
            daemon=True,
        )
        self._health_thread = self._thread
        self._thread.start()

    def stop(self, *, timeout: float = 3.0) -> None:
        try:
            with self._health_lock:
                self._health_stop_requested = True
        except Exception:  # noqa: BLE001, S110 - observation cannot interfere with ordinary stop
            pass
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
        self._update_processor = None
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
    # The managed path has different workers from start_recorder.py. Register
    # its actual map before capture starts; do not invent native updater loops.
    from .runtime import set_federation_status_provider, set_worker_health_provider

    health_provider = getattr(companion, "health_snapshot", None)
    federation_provider = getattr(companion, "federation_snapshot", None)
    def observe_providers(health: Any, federation: Any) -> None:
        try:
            set_worker_health_provider(health)
            set_federation_status_provider(federation)
        except Exception:  # noqa: BLE001, S110 - no status-provider failure may change lifecycle
            pass

    observe_providers(
        health_provider if callable(health_provider) else None,
        federation_provider if callable(federation_provider) else None,
    )
    try:
        companion.start()
    except BaseException:
        observe_providers(None, None)
        raise
    try:
        return (capture_runner or run)()
    finally:
        try:
            companion.stop()
        finally:
            observe_providers(None, None)


def main() -> int:
    """Run the managed recorder and journal its actual lifecycle outcome."""

    incarnation = incarnation_state_file(
        Path(os.environ.get("FCP_RECORDER_DATA_DIR", "data")),
        "recorder",
    )
    record_service_start(incarnation, service="recorder")
    try:
        run_managed_recorder()
    except KeyboardInterrupt:
        record_service_stop(
            incarnation,
            service="recorder",
            reason=STOP_OPERATOR,
        )
        return 0
    except Exception:
        # The process observed the exception but will still exit nonzero and
        # Docker remains the only supervisor. Keep this stop explicitly
        # restart-worthy so the next incarnation contributes to crash-loop
        # evidence instead of being mistaken for a clean completion.
        record_service_stop(
            incarnation,
            service="recorder",
            reason=STOP_FAILURE,
        )
        raise

    # The capture runtime handles its own signals and external activation
    # requests. Preserve that reason across the launcher boundary: update/trial
    # exits and operator signals are intentional, while an ordinary return is
    # a successful completion.
    from .runtime import EXTERNAL_STOP_REASON, SIGNAL_STOP_REASON, last_stop_reason

    runtime_reason = last_stop_reason()
    if runtime_reason == SIGNAL_STOP_REASON:
        stop_reason = STOP_OPERATOR
    elif runtime_reason == EXTERNAL_STOP_REASON:
        stop_reason = STOP_UPDATE
    else:
        stop_reason = STOP_COMPLETED
    record_service_stop(incarnation, service="recorder", reason=stop_reason)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
