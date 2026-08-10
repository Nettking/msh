"""Recorder-side execution of bounded Federation control requests.

The worker consumes authenticated session events after the recorder has joined a
Federation.  It deliberately exposes only two operations: run the existing
bounded RFC1918 MTConnect scan, and add/remove recorder sources selected from the
latest scan.  It never executes remote shell text and never accepts arbitrary
source URLs from another device.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
from dataclasses import replace
from datetime import datetime, timezone
from ipaddress import IPv4Address, IPv4Network, ip_address, ip_network
from pathlib import Path
from typing import Any, Mapping

from catalog.federation.errors import FederationValidationError
from catalog.federation.recorder_control_events import (
    SCAN_REPORT_EVENT,
    SCAN_REQUEST_EVENT,
    SOURCES_REPORT_EVENT,
    SOURCES_REQUEST_EVENT,
    parse_command,
    scan_report_payload,
    sources_report_payload,
)
from catalog.flask_app.services.capability_config_service import (
    CapabilityConfig,
    format_recorder_sources,
    load_capability_config,
    parse_recorder_sources,
    save_capability_config,
    update_recorder_config,
)
from catalog.flask_app.services.mtconnect_discovery_service import (
    DEFAULT_MTCONNECT_PORT,
    MtconnectDiscoveryError,
    MtconnectDiscoveryService,
    validate_scan_cidr,
)

_STATE_SCHEMA = "fcp.recorder-control-processor.v1"
_CONTROL_SCHEMA = "fcp.mtconnect_recorder.control.v1"
_RFC1918 = (
    IPv4Network("10.0.0.0/8"),
    IPv4Network("172.16.0.0/12"),
    IPv4Network("192.168.0.0/16"),
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _private_cidr_for(value: str) -> str:
    try:
        address = ip_address(value)
    except ValueError:
        return ""
    if not isinstance(address, IPv4Address):
        return ""
    if not any(address in private for private in _RFC1918):
        return ""
    return str(ip_network(f"{address}/24", strict=False))


def _route_address(host: str, port: int) -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(0.5)
            sock.connect((host, port))
            return str(sock.getsockname()[0])
    except OSError:
        return ""


def infer_private_scan_cidr(preferred_host: str | None = None) -> str:
    """Infer one bounded /24 from the host's normal private-network routes.

    Route selection is tried before hostname enumeration so Docker/virtual
    adapters do not normally win.  This function performs no network scan and
    sends no application payload; UDP ``connect`` is used only to ask the OS
    which local address it would route from.
    """

    candidates: list[str] = []
    if preferred_host:
        candidates.append(_route_address(preferred_host, DEFAULT_MTCONNECT_PORT))
    candidates.append(_route_address("8.8.8.8", 53))
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            candidates.append(str(info[4][0]))
    except OSError:
        pass
    for candidate in candidates:
        cidr = _private_cidr_for(candidate)
        if cidr:
            return cidr
    return ""


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(dict(value), sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)
    try:
        path.chmod(0o600)
    except OSError:
        pass


def _safe_sources(config: CapabilityConfig) -> dict[str, str]:
    try:
        return parse_recorder_sources(config.recorder_sources)
    except Exception:  # noqa: BLE001 - caller reports a bounded control error
        return {}


class RecorderFederationControlWorker:
    """Apply recorder-control events on the recorder host only."""

    def __init__(
        self,
        node: Any,
        *,
        data_directory: Path | str,
        poll_seconds: float = 1.0,
        discovery: MtconnectDiscoveryService | None = None,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        self.node = node
        self.data_directory = Path(data_directory).resolve()
        self.poll_seconds = float(poll_seconds)
        self.config_path = self.data_directory / "capabilities" / "config.json"
        self.control_path = (
            self.data_directory / "source_state" / "mtconnect_recorder_control.json"
        )
        self.state_path = (
            self.data_directory / "federation" / "recorder_control" / "processor.json"
        )
        self.discovery = discovery or MtconnectDiscoveryService(
            scan_path=(
                self.data_directory
                / "source_state"
                / "mtconnect_network_scan.json"
            ),
            config_path=self.config_path,
            checkpoint_path=(
                self.data_directory
                / "source_state"
                / "mtconnect_recorder_state.json"
            ),
        )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        if self.node.remote_store.load() is None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="fcp-recorder-federation-control",
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, timeout: float = 3.0) -> None:
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)

    def _state(self) -> dict[str, Any]:
        value = _read_json(self.state_path)
        if value.get("schema") != _STATE_SCHEMA:
            return {
                "schema": _STATE_SCHEMA,
                "last_revision": 0,
                "last_scan_id": "",
                "pending_report": None,
            }
        value.setdefault("last_revision", 0)
        value.setdefault("last_scan_id", "")
        value.setdefault("pending_report", None)
        return value

    def _save_state(self, value: dict[str, Any]) -> None:
        _write_json_atomic(self.state_path, {**value, "schema": _STATE_SCHEMA})

    @staticmethod
    def _report_request_id(revision: int, request_id: str) -> str:
        digest = hashlib.sha256(request_id.encode("utf-8")).hexdigest()[:20]
        return f"recorder-control-report-{revision}-{digest}"

    def _flush_pending(self, remote: Any, state: dict[str, Any]) -> bool:
        pending = state.get("pending_report")
        if not isinstance(pending, dict):
            return True
        event_type = pending.get("event_type")
        payload = pending.get("payload")
        revision = pending.get("revision")
        request_id = pending.get("request_id")
        if (
            event_type not in {SCAN_REPORT_EVENT, SOURCES_REPORT_EVENT}
            or not isinstance(payload, dict)
            or isinstance(revision, bool)
            or not isinstance(revision, int)
            or revision <= 0
            or not isinstance(request_id, str)
        ):
            state["pending_report"] = None
            self._save_state(state)
            return True
        self.node.runtime.append_session_event(
            remote,
            session_id=remote.binding.internal_session_id,
            event_type=event_type,
            payload=payload,
            request_id=self._report_request_id(revision, request_id),
        )
        state["last_revision"] = revision
        state["pending_report"] = None
        self._save_state(state)
        return True

    def _queue_report(
        self,
        state: dict[str, Any],
        *,
        revision: int,
        request_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        state["pending_report"] = {
            "revision": revision,
            "request_id": request_id,
            "event_type": event_type,
            "payload": payload,
        }
        self._save_state(state)

    def _set_recording_desired(self, enabled: bool, *, requested_by: str) -> None:
        _write_json_atomic(
            self.control_path,
            {
                "schema": _CONTROL_SCHEMA,
                "enabled": bool(enabled),
                "updated_at": _stamp(_utc_now()),
                "requested_by": requested_by[:128],
            },
        )

    def _reannounce_sources(self, remote: Any, config: CapabilityConfig) -> None:
        sources = _safe_sources(config)
        self.node.source_names = tuple(sorted(sources, key=str.casefold))
        # RecorderFederationNode owns the capability identity and validates the
        # exact bound node/session before publication. Reuse that narrow method
        # rather than creating a second announcement authority here.
        self.node._announce(remote)

    def _scan(
        self,
        command: dict[str, Any],
        *,
        remote: Any,
        state: dict[str, Any],
        revision: int,
    ) -> tuple[str, dict[str, Any]]:
        request_id = str(command["request_id"])
        scan_id = (
            "scan-"
            + hashlib.sha256(
                f"{remote.binding.internal_session_id}:{revision}:{request_id}".encode(
                    "utf-8"
                )
            ).hexdigest()[:24]
        )
        config = load_capability_config(self.config_path)
        requested_cidr = str(command.get("cidr") or "").strip()
        cidr = requested_cidr or self.discovery.recommended_cidr(config)
        if not cidr:
            preferred_host = None
            try:
                preferred_host = socket.gethostbyname(
                    str(remote.relay_url).split("://", 1)[-1].split(":", 1)[0]
                )
            except OSError:
                preferred_host = None
            cidr = infer_private_scan_cidr(preferred_host)
        if not cidr:
            raise MtconnectDiscoveryError(
                "No private IPv4 /24 could be inferred on the recorder host. "
                "Enter an explicit CIDR from any Federation device."
            )
        cidr = str(validate_scan_cidr(cidr))
        result = self.discovery.scan(cidr, port=int(command["port"]))
        state["last_scan_id"] = scan_id
        self._save_state(state)
        configured = tuple(_safe_sources(config))
        compact = [
            {
                "source_id": item.get("source_id"),
                "source_name": item.get("source_name"),
                "display_name": item.get("display_name"),
                "machine_count": item.get("machine_count"),
            }
            for item in result.get("results", [])
            if isinstance(item, dict)
        ]
        return SCAN_REPORT_EVENT, scan_report_payload(
            request_id=request_id,
            target_node_id=remote.binding.device_id,
            scan_id=scan_id,
            state="complete",
            cidr=cidr,
            port=int(command["port"]),
            results=compact,
            configured_source_names=configured,
            message=(
                f"Scan complete: {result.get('machines_found', 0)} machine(s) "
                f"from {result.get('agents_found', 0)} MTConnect Agent(s)."
            ),
            now=_utc_now(),
        )

    def _change_sources(
        self,
        command: dict[str, Any],
        *,
        remote: Any,
        state: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        request_id = str(command["request_id"])
        scan_id = str(command["scan_id"])
        additions = tuple(command["add_source_ids"])
        removals = tuple(command["remove_source_names"])
        if additions and scan_id != str(state.get("last_scan_id") or ""):
            raise MtconnectDiscoveryError(
                "The selected scan is no longer current on the recorder. Run a new scan first."
            )
        config = load_capability_config(self.config_path)
        sources = _safe_sources(config)
        for name in removals:
            sources.pop(name, None)
        config = update_recorder_config(
            config,
            recorder_sources=format_recorder_sources(sources),
        )
        if additions:
            config = self.discovery.merge_selected_results(config, additions)
            config = update_recorder_config(
                config,
                recorder_sources=config.recorder_sources,
            )
        save_capability_config(config, self.config_path)
        configured = _safe_sources(config)
        if configured:
            # Source management is an explicit request to record these sources.
            self._set_recording_desired(True, requested_by="federation-recorder-control")
        else:
            self._set_recording_desired(False, requested_by="federation-recorder-control")
        self._reannounce_sources(remote, config)
        return SOURCES_REPORT_EVENT, sources_report_payload(
            request_id=request_id,
            target_node_id=remote.binding.device_id,
            scan_id=scan_id,
            state="complete",
            configured_source_names=tuple(configured),
            message=(
                f"Recorder sources updated. {len(configured)} source(s) are configured."
            ),
            now=_utc_now(),
        )

    def _error_report(
        self,
        command: dict[str, Any],
        *,
        remote: Any,
        error: BaseException,
    ) -> tuple[str, dict[str, Any]]:
        code = str(getattr(error, "code", type(error).__name__))[:128]
        message = str(getattr(error, "message", str(error) or type(error).__name__))[:512]
        configured = tuple(_safe_sources(load_capability_config(self.config_path)))
        if command.get("command") == "sources":
            return SOURCES_REPORT_EVENT, sources_report_payload(
                request_id=str(command["request_id"]),
                target_node_id=remote.binding.device_id,
                scan_id=str(command.get("scan_id") or "unknown"),
                state="error",
                configured_source_names=configured,
                message=message,
                error_code=code,
                now=_utc_now(),
            )
        return SCAN_REPORT_EVENT, scan_report_payload(
            request_id=str(command["request_id"]),
            target_node_id=remote.binding.device_id,
            scan_id="scan-error",
            state="error",
            cidr=str(command.get("cidr") or ""),
            port=int(command.get("port") or DEFAULT_MTCONNECT_PORT),
            results=[],
            configured_source_names=configured,
            message=message,
            error_code=code,
            now=_utc_now(),
        )

    def _run(self) -> None:
        remote = self.node.remote_store.load()
        if remote is None:
            return
        state = self._state()
        while not self._stop.is_set():
            try:
                if not self._flush_pending(remote, state):
                    self._stop.wait(self.poll_seconds)
                    continue
                events = self.node.runtime.session_events(
                    remote,
                    session_id=remote.binding.internal_session_id,
                    after_revision=int(state.get("last_revision") or 0),
                )
                for event in events:
                    revision = int(event.revision)
                    if event.event_type not in {
                        SCAN_REQUEST_EVENT,
                        SOURCES_REQUEST_EVENT,
                    }:
                        state["last_revision"] = revision
                        self._save_state(state)
                        continue
                    try:
                        command = parse_command(event.event_type, event.payload)
                    except FederationValidationError:
                        # Malformed/expired events cannot become valid on retry.
                        state["last_revision"] = revision
                        self._save_state(state)
                        continue
                    if command["target_node_id"] != remote.binding.device_id:
                        state["last_revision"] = revision
                        self._save_state(state)
                        continue
                    try:
                        if command["command"] == "scan":
                            report_type, report = self._scan(
                                command,
                                remote=remote,
                                state=state,
                                revision=revision,
                            )
                        else:
                            report_type, report = self._change_sources(
                                command,
                                remote=remote,
                                state=state,
                            )
                    except Exception as exc:  # noqa: BLE001 - report bounded failure
                        report_type, report = self._error_report(
                            command,
                            remote=remote,
                            error=exc,
                        )
                    self._queue_report(
                        state,
                        revision=revision,
                        request_id=str(command["request_id"]),
                        event_type=report_type,
                        payload=report,
                    )
                    self._flush_pending(remote, state)
                self._stop.wait(self.poll_seconds)
            except Exception:  # noqa: BLE001 - transport failures retry safely
                self._stop.wait(self.poll_seconds)


__all__ = [
    "RecorderFederationControlWorker",
    "infer_private_scan_cidr",
]
