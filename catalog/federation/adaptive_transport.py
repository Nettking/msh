"""Adaptive relay/direct storage transport selection for Phase F6."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol

from .storage_protocol import StorageRequestEnvelope, StorageResponseEnvelope

ADAPTIVE_TRANSPORT_STATUS_SCHEMA = "fcp.adaptive_storage_transport.status.v1"
DIRECT_CONTINUITY_STATUS_SCHEMA = "fcp.direct_continuity.status.v1"


class StorageRequestTransport(Protocol):
    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope: ...


class DirectTransportUnavailable(RuntimeError):
    """Expected direct-path establishment or liveness failure eligible for relay fallback."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class TransportPathUnavailable(RuntimeError):
    """No correctness-preserving data path is currently available."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class TransportKind(str, Enum):
    DIRECT = "direct"
    RELAY = "relay"
    NONE = "none"


class DirectTransportState(str, Enum):
    DISABLED = "disabled"
    CONNECTING = "connecting"
    READY = "ready"
    UNAVAILABLE = "unavailable"


class ControlPlaneState(str, Enum):
    AVAILABLE = "available"
    INTERRUPTED = "interrupted"
    RECOVERING = "recovering"


@dataclass(frozen=True)
class TransportDecision:
    selected: TransportKind
    direct_state: DirectTransportState
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {
            "selected": self.selected.value,
            "direct_state": self.direct_state.value,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class AdaptiveTransportStatus:
    direct_transport_enabled: bool
    direct_state: DirectTransportState
    selected_transport: TransportKind
    request_count: int
    relay_request_count: int
    direct_request_count: int
    fallback_count: int
    last_reason: str
    last_outcome: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": ADAPTIVE_TRANSPORT_STATUS_SCHEMA,
            "direct_transport_enabled": self.direct_transport_enabled,
            "direct_state": self.direct_state.value,
            "selected_transport": self.selected_transport.value,
            "request_count": self.request_count,
            "relay_request_count": self.relay_request_count,
            "direct_request_count": self.direct_request_count,
            "fallback_count": self.fallback_count,
            "last_reason": self.last_reason,
            "last_outcome": self.last_outcome,
        }


@dataclass(frozen=True)
class DirectContinuityStatus:
    control_plane_state: ControlPlaneState
    direct_state: DirectTransportState
    direct_data_plane_available: bool
    relay_fallback_available: bool
    authority_refresh_available: bool
    membership_updates_available: bool
    coordinated_failover_available: bool
    limitations: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": DIRECT_CONTINUITY_STATUS_SCHEMA,
            "control_plane_state": self.control_plane_state.value,
            "direct_state": self.direct_state.value,
            "direct_data_plane_available": self.direct_data_plane_available,
            "relay_fallback_available": self.relay_fallback_available,
            "authority_refresh_available": self.authority_refresh_available,
            "membership_updates_available": self.membership_updates_available,
            "coordinated_failover_available": self.coordinated_failover_available,
            "limitations": list(self.limitations),
        }


class AdaptiveStorageTransport:
    """Prefer a ready direct transport and preserve relay as bounded fallback.

    Only ``DirectTransportUnavailable`` permits downgrade to relay. Protocol,
    identity, signature, and ciphertext validation errors fail closed and are not
    hidden by fallback. F6.3 additionally keeps a ready direct data path usable
    during a reported control-plane interruption without pretending that leases,
    membership, routing, or coordinated failover can be refreshed.
    """

    def __init__(
        self,
        relay_transport: StorageRequestTransport,
        *,
        direct_transport: StorageRequestTransport | None = None,
        direct_state: DirectTransportState = DirectTransportState.DISABLED,
        control_plane_state: ControlPlaneState = ControlPlaneState.AVAILABLE,
        decision_observer: Callable[[TransportDecision], None] | None = None,
    ) -> None:
        self._relay_transport = relay_transport
        self._direct_transport = direct_transport
        self._direct_state = DirectTransportState(direct_state)
        self._control_plane_state = ControlPlaneState(control_plane_state)
        self._decision_observer = decision_observer
        self._request_count = 0
        self._relay_request_count = 0
        self._direct_request_count = 0
        self._fallback_count = 0
        self._last_decision = self._decide()
        self._last_outcome = "not-run"

    @property
    def direct_state(self) -> DirectTransportState:
        return self._direct_state

    @property
    def control_plane_state(self) -> ControlPlaneState:
        return self._control_plane_state

    def report_direct_state(self, state: DirectTransportState) -> None:
        self._direct_state = DirectTransportState(state)
        self._last_decision = self._decide()

    def report_control_plane_state(self, state: ControlPlaneState) -> None:
        """Report relay/control reachability without changing storage authority."""

        self._control_plane_state = ControlPlaneState(state)
        self._last_decision = self._decide()

    def decision(self) -> TransportDecision:
        return self._decide()

    def status(self) -> AdaptiveTransportStatus:
        decision = self._last_decision
        return AdaptiveTransportStatus(
            direct_transport_enabled=self._direct_transport is not None,
            direct_state=self._direct_state,
            selected_transport=decision.selected,
            request_count=self._request_count,
            relay_request_count=self._relay_request_count,
            direct_request_count=self._direct_request_count,
            fallback_count=self._fallback_count,
            last_reason=decision.reason,
            last_outcome=self._last_outcome,
        )

    def continuity_status(self) -> DirectContinuityStatus:
        direct_available = (
            self._direct_transport is not None
            and self._direct_state is DirectTransportState.READY
        )
        control_available = self._control_plane_state is ControlPlaneState.AVAILABLE
        limitations: tuple[str, ...] = ()
        if not control_available:
            limitations = (
                "authority-refresh-unavailable",
                "membership-updates-unavailable",
                "route-updates-unavailable",
                "coordinated-failover-unavailable",
                "relay-fallback-unavailable",
            )
        return DirectContinuityStatus(
            control_plane_state=self._control_plane_state,
            direct_state=self._direct_state,
            direct_data_plane_available=direct_available,
            relay_fallback_available=control_available,
            authority_refresh_available=control_available,
            membership_updates_available=control_available,
            coordinated_failover_available=control_available,
            limitations=limitations,
        )

    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        decision = self._decide()
        self._last_decision = decision
        self._request_count += 1
        self._observe(decision)

        if decision.selected is TransportKind.NONE:
            self._last_outcome = "failed"
            raise TransportPathUnavailable(
                "control-plane-interrupted-no-direct-path",
                "relay/control is unavailable and no direct data path is ready",
            )

        if decision.selected is TransportKind.DIRECT:
            self._direct_request_count += 1
            direct_transport = self._direct_transport
            if direct_transport is None:
                raise RuntimeError("direct transport decision without an implementation")
            try:
                response = await direct_transport.request(
                    target_node_id=target_node_id,
                    envelope=envelope,
                )
            except DirectTransportUnavailable as exc:
                if self._control_plane_state is not ControlPlaneState.AVAILABLE:
                    blocked = TransportDecision(
                        selected=TransportKind.NONE,
                        direct_state=DirectTransportState.UNAVAILABLE,
                        reason="direct-failed-relay-unavailable-f63",
                    )
                    self._last_decision = blocked
                    self._last_outcome = "failed"
                    self._observe(blocked)
                    raise TransportPathUnavailable(
                        "direct-and-relay-unavailable",
                        "direct path failed while relay/control was unavailable",
                    ) from exc
                self._fallback_count += 1
                fallback = TransportDecision(
                    selected=TransportKind.RELAY,
                    direct_state=DirectTransportState.UNAVAILABLE,
                    reason="direct-failed-relay-fallback",
                )
                self._last_decision = fallback
                self._observe(fallback)
            else:
                self._last_outcome = "succeeded"
                return response

        elif self._direct_state is not DirectTransportState.DISABLED:
            self._fallback_count += 1

        if self._control_plane_state is not ControlPlaneState.AVAILABLE:
            blocked = TransportDecision(
                selected=TransportKind.NONE,
                direct_state=self._direct_state,
                reason="relay-unavailable-control-interrupted-f63",
            )
            self._last_decision = blocked
            self._last_outcome = "failed"
            self._observe(blocked)
            raise TransportPathUnavailable(
                "relay-control-unavailable",
                "relay/control data path is unavailable",
            )

        self._relay_request_count += 1
        try:
            response = await self._relay_transport.request(
                target_node_id=target_node_id,
                envelope=envelope,
            )
        except Exception:
            self._last_outcome = "failed"
            raise
        self._last_outcome = "succeeded"
        return response

    def _observe(self, decision: TransportDecision) -> None:
        if self._decision_observer is None:
            return
        try:
            self._decision_observer(decision)
        except Exception:  # noqa: BLE001
            self._decision_observer = None

    def _decide(self) -> TransportDecision:
        if (
            self._direct_transport is not None
            and self._direct_state is DirectTransportState.READY
        ):
            reasons = {
                ControlPlaneState.AVAILABLE: "direct-ready-f62",
                ControlPlaneState.INTERRUPTED: "direct-ready-control-interrupted-f63",
                ControlPlaneState.RECOVERING: "direct-ready-control-recovering-f63",
            }
            return TransportDecision(
                selected=TransportKind.DIRECT,
                direct_state=self._direct_state,
                reason=reasons[self._control_plane_state],
            )
        if self._control_plane_state is not ControlPlaneState.AVAILABLE:
            return TransportDecision(
                selected=TransportKind.NONE,
                direct_state=self._direct_state,
                reason="no-transport-control-interrupted-f63",
            )
        reasons = {
            DirectTransportState.DISABLED: "direct-disabled-f61",
            DirectTransportState.CONNECTING: "direct-connecting-relay-selected",
            DirectTransportState.READY: "direct-not-enabled-until-f62",
            DirectTransportState.UNAVAILABLE: "direct-unavailable-relay-selected",
        }
        return TransportDecision(
            selected=TransportKind.RELAY,
            direct_state=self._direct_state,
            reason=reasons[self._direct_state],
        )
