"""Federation-facing orchestration for standalone recorder control.

Any authenticated member of the same Federation may request a bounded scan on a
standalone recorder or change its configured MTConnect source selection.  The
caller never gains host execution authority: requests are declarative session
events, additions can name only source IDs returned by the recorder's latest
scan, and the recorder independently revalidates every operation locally.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from catalog.federation.errors import (
    AuthenticationError,
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.recorder_control_events import (
    SCAN_REPORT_EVENT,
    SCAN_REQUEST_EVENT,
    SOURCES_REPORT_EVENT,
    SOURCES_REQUEST_EVENT,
    scan_command_payload,
    sources_command_payload,
)

from .capability_onboarding_service import get_capability_onboarding_service
from .mtconnect_discovery_service import validate_scan_cidr

_CONNECTED = frozenset({"connected", "online", "ready", "active"})


class FederationRecorderControlError(RuntimeError):
    """Raised when a recorder control request cannot be issued safely."""


def _text(value: object) -> str:
    return str(value or "").strip()


def _sections() -> list[dict[str, str]]:
    return [
        {"key": "overview", "label": "Overview", "url": "/federation"},
        {"key": "devices", "label": "Devices", "url": "/federation/devices"},
        {"key": "services", "label": "Services", "url": "/federation/services"},
        {"key": "storage", "label": "Storage", "url": "/federation/storage"},
        {"key": "activity", "label": "Activity", "url": "/federation/activity"},
    ]


class FederationRecorderControlService:
    """Issue and observe recorder control commands over authenticated events."""

    def _context(self) -> tuple[Any, Any, str]:
        onboarding = get_capability_onboarding_service()
        context = onboarding.authorized_context()
        if context is None:
            raise AuthenticationError(
                "recorder-control-federation-required",
                "a trusted Federation membership is required",
                "binding",
            )
        actor = context.credentials.identity.node_id
        return onboarding, context, actor

    @staticmethod
    def _status(context: Any, actor: str) -> dict[str, Any]:
        value = context.coordinator.status(actor_node_id=actor, cursor=None)
        if not isinstance(value, dict):
            raise FederationOperationError(
                "recorder-control-status-unavailable",
                "Federation status is unavailable",
            )
        return value

    @staticmethod
    def _recorders_from_status(status: dict[str, Any]) -> dict[str, dict[str, Any]]:
        nodes = status.get("nodes")
        capabilities = status.get("capabilities")
        if not isinstance(nodes, list):
            nodes = []
        if not isinstance(capabilities, list):
            capabilities = []

        node_by_id: dict[str, dict[str, Any]] = {
            _text(node.get("node_id")): node
            for node in nodes
            if isinstance(node, dict) and _text(node.get("node_id"))
        }
        recorders: dict[str, dict[str, Any]] = {}
        for capability in capabilities:
            if not isinstance(capability, dict):
                continue
            properties = capability.get("properties")
            if not isinstance(properties, dict):
                properties = {}
            if (
                capability.get("type") != "recorder"
                or capability.get("protocol") != "mtconnect"
                or _text(capability.get("status")).casefold() != "ready"
                or properties.get("kind") != "standalone-recorder"
            ):
                continue
            node_id = _text(capability.get("node_id"))
            if not node_id:
                continue
            node = node_by_id.get(node_id, {})
            connection_state = _text(node.get("connection_state") or "unknown")
            raw_names = properties.get("source_names")
            source_names = (
                sorted(
                    {
                        _text(item)
                        for item in raw_names
                        if isinstance(item, str) and _text(item)
                    },
                    key=str.casefold,
                )
                if isinstance(raw_names, list)
                else []
            )
            recorders[node_id] = {
                "node_id": node_id,
                "label": _text(node.get("display_name")) or node_id,
                "connection_state": connection_state or "unknown",
                "connected": connection_state.casefold() in _CONNECTED,
                "source_names": source_names,
            }
        return recorders

    @staticmethod
    def _append_event(
        onboarding: Any,
        context: Any,
        *,
        event_type: str,
        payload: dict[str, Any],
        request_id: str,
    ) -> None:
        remote_store = getattr(onboarding, "remote_store", None)
        runtime = getattr(onboarding, "relay_runtime", None)
        remote = None
        if remote_store is not None and callable(getattr(remote_store, "load", None)):
            remote = remote_store.load()
        if remote is not None:
            if runtime is None or not callable(getattr(runtime, "append_session_event", None)):
                raise FederationOperationError(
                    "recorder-control-relay-unavailable",
                    "the paired Federation relay cannot publish recorder control events",
                )
            runtime.append_session_event(
                remote,
                session_id=context.binding.internal_session_id,
                event_type=event_type,
                payload=payload,
                request_id=request_id,
            )
            return
        context.coordinator.append_event(
            session_id=context.binding.internal_session_id,
            actor_node_id=context.credentials.identity.node_id,
            request_id=request_id,
            event_type=event_type,
            payload=payload,
        )

    @staticmethod
    def _events(context: Any, actor: str) -> tuple[Any, ...]:
        collected: list[Any] = []
        last_revision = 0
        for _ in range(128):
            events, current_revision = context.coordinator.replay_page(
                session_id=context.binding.internal_session_id,
                actor_node_id=actor,
                last_applied_revision=last_revision,
                limit=1000,
            )
            for event in events:
                collected.append(event)
                last_revision = int(event.revision)
            if not events or last_revision >= int(current_revision):
                break
        return tuple(collected)

    @staticmethod
    def _report_for_recorder(event: Any, recorder_id: str) -> dict[str, Any] | None:
        payload = getattr(event, "payload", None)
        actor = _text(getattr(event, "actor_node_id", ""))
        if (
            not isinstance(payload, dict)
            or payload.get("schema") != "fcp.recorder-control.v1"
            or actor != recorder_id
            or _text(payload.get("target_node_id")) != recorder_id
        ):
            return None
        return payload

    def _latest_reports(
        self,
        context: Any,
        actor: str,
        recorders: dict[str, dict[str, Any]],
    ) -> None:
        latest_scan: dict[str, tuple[int, dict[str, Any]]] = {}
        latest_sources: dict[str, tuple[int, dict[str, Any]]] = {}
        pending_scan: dict[str, tuple[int, str]] = {}
        pending_sources: dict[str, tuple[int, str]] = {}

        for event in self._events(context, actor):
            event_type = _text(getattr(event, "event_type", ""))
            payload = getattr(event, "payload", None)
            revision = int(getattr(event, "revision", 0) or 0)
            if not isinstance(payload, dict):
                continue
            target = _text(payload.get("target_node_id"))
            if target not in recorders:
                continue
            request_id = _text(payload.get("request_id"))
            if event_type == SCAN_REQUEST_EVENT and request_id:
                pending_scan[target] = (revision, request_id)
                continue
            if event_type == SOURCES_REQUEST_EVENT and request_id:
                pending_sources[target] = (revision, request_id)
                continue
            if event_type == SCAN_REPORT_EVENT:
                report = self._report_for_recorder(event, target)
                if report is None:
                    continue
                latest_scan[target] = (revision, report)
                if pending_scan.get(target, (0, ""))[1] == _text(report.get("request_id")):
                    pending_scan.pop(target, None)
                continue
            if event_type == SOURCES_REPORT_EVENT:
                report = self._report_for_recorder(event, target)
                if report is None:
                    continue
                latest_sources[target] = (revision, report)
                if pending_sources.get(target, (0, ""))[1] == _text(report.get("request_id")):
                    pending_sources.pop(target, None)

        for node_id, recorder in recorders.items():
            scan = latest_scan.get(node_id)
            sources = latest_sources.get(node_id)
            recorder["latest_scan"] = dict(scan[1]) if scan is not None else None
            recorder["latest_source_change"] = (
                dict(sources[1]) if sources is not None else None
            )
            recorder["scan_pending"] = node_id in pending_scan
            recorder["source_change_pending"] = node_id in pending_sources
            if scan is not None:
                configured = scan[1].get("configured_source_names")
                if isinstance(configured, list):
                    recorder["source_names"] = sorted(
                        {
                            _text(item)
                            for item in configured
                            if isinstance(item, str) and _text(item)
                        },
                        key=str.casefold,
                    )
            if sources is not None:
                configured = sources[1].get("configured_source_names")
                if isinstance(configured, list):
                    recorder["source_names"] = sorted(
                        {
                            _text(item)
                            for item in configured
                            if isinstance(item, str) and _text(item)
                        },
                        key=str.casefold,
                    )

    def snapshot(self) -> dict[str, Any]:
        onboarding, context, actor = self._context()
        del onboarding
        recorders = self._recorders_from_status(self._status(context, actor))
        self._latest_reports(context, actor, recorders)
        return {
            "title": "Recorder control",
            "intro": (
                "Manage standalone MTConnect recorder sources from any trusted "
                "Federation device. Network scans execute on the recorder host "
                "and remain bounded to a private IPv4 /24 or smaller."
            ),
            "sections": _sections(),
            "active_section": "recorders",
            "recorders": list(recorders.values()),
        }

    def _target(
        self,
        context: Any,
        actor: str,
        target_node_id: str,
    ) -> dict[str, Any]:
        target = _text(target_node_id)
        recorders = self._recorders_from_status(self._status(context, actor))
        recorder = recorders.get(target)
        if recorder is None:
            raise FederationRecorderControlError(
                "The selected device is not an active standalone recorder in this Federation."
            )
        if not recorder["connected"]:
            raise FederationRecorderControlError(
                "The selected recorder is offline. Reconnect it before sending control requests."
            )
        return recorder

    def request_scan(
        self,
        target_node_id: str,
        *,
        cidr: str = "",
        port: int = 5000,
    ) -> str:
        onboarding, context, actor = self._context()
        self._target(context, actor, target_node_id)
        normalized_cidr = _text(cidr)
        if normalized_cidr:
            normalized_cidr = str(validate_scan_cidr(normalized_cidr))
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise FederationRecorderControlError(
                "The MTConnect port must be between 1 and 65535."
            )
        request_id = f"recorder-scan-{uuid.uuid4().hex}"
        payload = scan_command_payload(
            request_id=request_id,
            target_node_id=target_node_id,
            cidr=normalized_cidr,
            port=port,
            now=datetime.now(timezone.utc),
        )
        self._append_event(
            onboarding,
            context,
            event_type=SCAN_REQUEST_EVENT,
            payload=payload,
            request_id=f"recorder-control-{request_id}",
        )
        return request_id

    def request_source_change(
        self,
        target_node_id: str,
        *,
        scan_id: str,
        add_source_ids: list[str],
        remove_source_names: list[str],
    ) -> str:
        onboarding, context, actor = self._context()
        recorder = self._target(context, actor, target_node_id)
        self._latest_reports(context, actor, {target_node_id: recorder})
        latest_scan = recorder.get("latest_scan")
        current_scan_id = (
            _text(latest_scan.get("scan_id"))
            if isinstance(latest_scan, dict)
            else ""
        )
        if add_source_ids:
            if not current_scan_id or _text(scan_id) != current_scan_id:
                raise FederationRecorderControlError(
                    "Run a new recorder scan before adding discovered machines."
                )
            allowed_ids = {
                _text(item.get("source_id"))
                for item in latest_scan.get("results", [])
                if isinstance(item, dict) and _text(item.get("source_id"))
            }
            if any(_text(item) not in allowed_ids for item in add_source_ids):
                raise FederationRecorderControlError(
                    "One or more selected machines are not part of the recorder's latest scan."
                )
        configured = set(recorder.get("source_names") or [])
        if any(_text(item) not in configured for item in remove_source_names):
            raise FederationRecorderControlError(
                "One or more selected recorder sources are no longer configured."
            )
        request_id = f"recorder-sources-{uuid.uuid4().hex}"
        payload = sources_command_payload(
            request_id=request_id,
            target_node_id=target_node_id,
            scan_id=current_scan_id or _text(scan_id) or "no-scan",
            add_source_ids=add_source_ids,
            remove_source_names=remove_source_names,
            now=datetime.now(timezone.utc),
        )
        self._append_event(
            onboarding,
            context,
            event_type=SOURCES_REQUEST_EVENT,
            payload=payload,
            request_id=f"recorder-control-{request_id}",
        )
        return request_id


_RECORDER_CONTROL: FederationRecorderControlService | None = None


def get_federation_recorder_control_service() -> FederationRecorderControlService:
    global _RECORDER_CONTROL
    if _RECORDER_CONTROL is None:
        _RECORDER_CONTROL = FederationRecorderControlService()
    return _RECORDER_CONTROL


__all__ = [
    "FederationRecorderControlError",
    "FederationRecorderControlService",
    "get_federation_recorder_control_service",
]
