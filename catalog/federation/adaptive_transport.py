"""Phase F6.1 relay-only adaptive storage transport boundary."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol

from .storage_protocol import StorageRequestEnvelope, StorageResponseEnvelope

ADAPTIVE_TRANSPORT_STATUS_SCHEMA = "msh.adaptive_storage_transport.status.v1"


class StorageRequestTransport(Protocol):
    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope: ...


class TransportKind(str, Enum):
    DIRECT = "direct"
    RELAY = "relay"


class DirectTransportState(str, Enum):
    DISABLED = "disabled"
    CONNECTING = "connecting"
    READY = "ready"
    UNAVAILABLE = "unavailable"


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
            "direct_transport_enabled": False,
            "direct_state": self.direct_state.value,
            "selected_transport": self.selected_transport.value,
            "request_count": self.request_count,
            "relay_request_count": self.relay_request_count,
            "direct_request_count": self.direct_request_count,
            "fallback_count": self.fallback_count,
            "last_reason": self.last_reason,
            "last_outcome": self.last_outcome,
        }


class AdaptiveStorageTransport:
    """Relay-only F6.1 transport boundary with explicit future direct state.

    F6.1 never calls a direct transport. Even when reachability is reported as
    ``ready``, the decision remains relay with a stable reason explaining that the
    direct implementation belongs to F6.2. This prevents a diagnostic state from
    accidentally becoming an authority or correctness decision.
    """

    def __init__(
        self,
        relay_transport: StorageRequestTransport,
        *,
        direct_state: DirectTransportState = DirectTransportState.DISABLED,
        decision_observer: Callable[[TransportDecision], None] | None = None,
    ) -> None:
        self._relay_transport = relay_transport
        self._direct_state = DirectTransportState(direct_state)
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

    def report_direct_state(self, state: DirectTransportState) -> None:
        """Update diagnostics only; it cannot activate a direct data path."""

        self._direct_state = DirectTransportState(state)
        self._last_decision = self._decide()

    def decision(self) -> TransportDecision:
        return self._decide()

    def status(self) -> AdaptiveTransportStatus:
        decision = self._last_decision
        return AdaptiveTransportStatus(
            direct_state=self._direct_state,
            selected_transport=decision.selected,
            request_count=self._request_count,
            relay_request_count=self._relay_request_count,
            direct_request_count=self._direct_request_count,
            fallback_count=self._fallback_count,
            last_reason=decision.reason,
            last_outcome=self._last_outcome,
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
        self._relay_request_count += 1
        if self._direct_state is not DirectTransportState.DISABLED:
            self._fallback_count += 1
        if self._decision_observer is not None:
            try:
                self._decision_observer(decision)
            except Exception:
                # Disable a faulty diagnostic sink without blocking relay traffic.
                self._decision_observer = None
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

    def _decide(self) -> TransportDecision:
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
