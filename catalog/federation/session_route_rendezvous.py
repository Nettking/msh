"""F6.6.2 signed, session-scoped route rendezvous contracts."""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Any

from .direct_peer import DirectPeerDescriptor
from .errors import FederationValidationError, ProtocolCompatibilityError
from .peer_stream import validate_peer_public_text

SESSION_ROUTE_DESCRIPTOR_SCHEMA = "fcp.session_route.descriptor.v1"
SESSION_ROUTE_PROTOCOL_VERSION = "1.0"
SESSION_ROUTE_PROTOCOL_MAJOR = 1
MIN_ROUTE_TTL_SECONDS = 5
MAX_ROUTE_TTL_SECONDS = 600
MAX_ROUTE_CLOCK_SKEW_SECONDS = 30
DEFAULT_MAX_ROUTE_ENTRIES = 2_048
MAX_ROUTE_GENERATION = 2**63 - 1

JSON = dict[str, Any]


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _utc(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise FederationValidationError(
            "invalid-session-route-time", field, "must be timezone-aware"
        )
    return value.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return _utc(value, "time").isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise FederationValidationError(
            "invalid-session-route-time", field, "must be canonical UTC text"
        )
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise FederationValidationError(
            "invalid-session-route-time", field, "must be canonical UTC text"
        ) from exc
    if _utc_text(parsed) != value:
        raise FederationValidationError(
            "invalid-session-route-time", field, "must use canonical microsecond UTC text"
        )
    return parsed.astimezone(timezone.utc)


def _protocol_version(value: Any) -> str:
    value = validate_peer_public_text(value, "protocol_version")
    try:
        major = int(value.split(".", 1)[0])
    except ValueError as exc:
        raise ProtocolCompatibilityError(
            "invalid-session-route-protocol",
            "protocol_version",
            "must begin with a number",
        ) from exc
    if major != SESSION_ROUTE_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-session-route-protocol",
            "protocol_version",
            f"supported major is {SESSION_ROUTE_PROTOCOL_MAJOR}",
        )
    return value


def _generation(value: Any) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= MAX_ROUTE_GENERATION
    ):
        raise FederationValidationError(
            "invalid-session-route-generation",
            "generation",
            f"must be between 1 and {MAX_ROUTE_GENERATION}",
        )
    return value


@dataclass(frozen=True)
class SessionRouteDescriptor:
    """A short-lived route signed by the enrolled node that owns it."""

    session_id: str
    node_id: str
    peer_id: str
    multiaddr: str
    encryption_public_key: str
    generation: int
    issued_at: datetime
    expires_at: datetime
    signature: str
    protocol_version: str = SESSION_ROUTE_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "session_id",
            validate_peer_public_text(self.session_id, "session_id"),
        )
        direct = DirectPeerDescriptor(
            node_id=self.node_id,
            peer_id=self.peer_id,
            multiaddr=self.multiaddr,
            encryption_public_key=self.encryption_public_key,
        )
        if not direct.multiaddr.endswith(f"/p2p/{direct.peer_id}"):
            raise FederationValidationError(
                "session-route-peer-mismatch",
                "multiaddr",
                "route must bind the advertised libp2p peer identity",
            )
        object.__setattr__(self, "node_id", direct.node_id)
        object.__setattr__(self, "peer_id", direct.peer_id)
        object.__setattr__(self, "multiaddr", direct.multiaddr)
        object.__setattr__(
            self, "encryption_public_key", direct.encryption_public_key
        )
        object.__setattr__(self, "generation", _generation(self.generation))
        issued_at = _utc(self.issued_at, "issued_at")
        expires_at = _utc(self.expires_at, "expires_at")
        if expires_at <= issued_at:
            raise FederationValidationError(
                "invalid-session-route-window",
                "expires_at",
                "must be later than issued_at",
            )
        object.__setattr__(self, "issued_at", issued_at)
        object.__setattr__(self, "expires_at", expires_at)
        object.__setattr__(
            self,
            "signature",
            validate_peer_public_text(self.signature, "signature"),
        )
        object.__setattr__(
            self, "protocol_version", _protocol_version(self.protocol_version)
        )

    @classmethod
    def create(
        cls,
        *,
        session_id: str,
        descriptor: DirectPeerDescriptor,
        generation: int,
        issued_at: datetime,
        ttl_seconds: int,
        sign: Callable[[bytes], str],
    ) -> SessionRouteDescriptor:
        if (
            isinstance(ttl_seconds, bool)
            or not isinstance(ttl_seconds, int)
            or not MIN_ROUTE_TTL_SECONDS <= ttl_seconds <= MAX_ROUTE_TTL_SECONDS
        ):
            raise FederationValidationError(
                "invalid-session-route-ttl",
                "ttl_seconds",
                f"must be between {MIN_ROUTE_TTL_SECONDS} and {MAX_ROUTE_TTL_SECONDS}",
            )
        issued_at = _utc(issued_at, "issued_at")
        unsigned = cls._unsigned(
            session_id=validate_peer_public_text(session_id, "session_id"),
            node_id=descriptor.node_id,
            peer_id=descriptor.peer_id,
            multiaddr=descriptor.multiaddr,
            encryption_public_key=descriptor.encryption_public_key,
            generation=_generation(generation),
            issued_at=_utc_text(issued_at),
            expires_at=_utc_text(issued_at + timedelta(seconds=ttl_seconds)),
            protocol_version=SESSION_ROUTE_PROTOCOL_VERSION,
        )
        return cls.from_dict({**unsigned, "signature": sign(_canonical(unsigned))})

    @classmethod
    def from_dict(cls, value: Any) -> SessionRouteDescriptor:
        if not isinstance(value, dict) or value.get("schema") != SESSION_ROUTE_DESCRIPTOR_SCHEMA:
            raise ProtocolCompatibilityError(
                "unsupported-session-route-descriptor",
                "schema",
                "unexpected route descriptor schema",
            )
        required = {
            "session_id",
            "node_id",
            "peer_id",
            "multiaddr",
            "encryption_public_key",
            "generation",
            "issued_at",
            "expires_at",
            "signature",
            "protocol_version",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        return cls(
            session_id=value["session_id"],
            node_id=value["node_id"],
            peer_id=value["peer_id"],
            multiaddr=value["multiaddr"],
            encryption_public_key=value["encryption_public_key"],
            generation=value["generation"],
            issued_at=_parse_utc(value["issued_at"], "issued_at"),
            expires_at=_parse_utc(value["expires_at"], "expires_at"),
            signature=value["signature"],
            protocol_version=value["protocol_version"],
        )

    @staticmethod
    def _unsigned(**value: Any) -> JSON:
        return {"schema": SESSION_ROUTE_DESCRIPTOR_SCHEMA, **value}

    def signing_bytes(self) -> bytes:
        return _canonical(
            self._unsigned(
                session_id=self.session_id,
                node_id=self.node_id,
                peer_id=self.peer_id,
                multiaddr=self.multiaddr,
                encryption_public_key=self.encryption_public_key,
                generation=self.generation,
                issued_at=_utc_text(self.issued_at),
                expires_at=_utc_text(self.expires_at),
                protocol_version=self.protocol_version,
            )
        )

    def to_dict(self) -> JSON:
        return {**json.loads(self.signing_bytes()), "signature": self.signature}

    def to_direct_peer_descriptor(self) -> DirectPeerDescriptor:
        return DirectPeerDescriptor(
            node_id=self.node_id,
            peer_id=self.peer_id,
            multiaddr=self.multiaddr,
            encryption_public_key=self.encryption_public_key,
        )

    def validate_window(self, *, now: datetime, max_ttl_seconds: int) -> None:
        now = _utc(now, "now")
        ttl = (self.expires_at - self.issued_at).total_seconds()
        if (
            not math.isfinite(ttl)
            or ttl < MIN_ROUTE_TTL_SECONDS
            or ttl > max_ttl_seconds
        ):
            raise FederationValidationError(
                "invalid-session-route-window",
                "expires_at",
                "route lifetime is outside the configured bound",
            )
        if self.issued_at > now + timedelta(seconds=MAX_ROUTE_CLOCK_SKEW_SECONDS):
            raise FederationValidationError(
                "session-route-issued-in-future",
                "issued_at",
                "route exceeds the allowed clock skew",
            )
        if self.expires_at <= now:
            raise FederationValidationError(
                "session-route-expired", "expires_at", "route is already expired"
            )


class SessionRouteRegistry:
    """Bounded, process-local rendezvous state; never an authority source."""

    durable = False

    def __init__(
        self,
        *,
        max_entries: int = DEFAULT_MAX_ROUTE_ENTRIES,
        max_ttl_seconds: int = MAX_ROUTE_TTL_SECONDS,
    ) -> None:
        if (
            isinstance(max_entries, bool)
            or not isinstance(max_entries, int)
            or not 1 <= max_entries <= 100_000
        ):
            raise ValueError("max_entries must be between 1 and 100000")
        if (
            isinstance(max_ttl_seconds, bool)
            or not isinstance(max_ttl_seconds, int)
            or not MIN_ROUTE_TTL_SECONDS <= max_ttl_seconds <= MAX_ROUTE_TTL_SECONDS
        ):
            raise ValueError(
                f"max_ttl_seconds must be between {MIN_ROUTE_TTL_SECONDS} and {MAX_ROUTE_TTL_SECONDS}"
            )
        self.max_entries = max_entries
        self.max_ttl_seconds = max_ttl_seconds
        self._items: dict[tuple[str, str], SessionRouteDescriptor] = {}
        self._lock = RLock()

    def publish(
        self,
        descriptor: SessionRouteDescriptor,
        *,
        public_key: str,
        verify_signature: Callable[[str, bytes, str], bool],
        now: datetime,
    ) -> bool:
        descriptor.validate_window(now=now, max_ttl_seconds=self.max_ttl_seconds)
        try:
            valid_signature = verify_signature(
                public_key, descriptor.signing_bytes(), descriptor.signature
            )
        except Exception as exc:
            raise FederationValidationError(
                "invalid-session-route-signature",
                "signature",
                "route signature could not be verified",
            ) from exc
        if not valid_signature:
            raise FederationValidationError(
                "invalid-session-route-signature",
                "signature",
                "route is not signed by its enrolled node",
            )
        key = (descriptor.session_id, descriptor.node_id)
        with self._lock:
            self._purge_expired_locked(now)
            current = self._items.get(key)
            if current is not None:
                if descriptor.generation < current.generation:
                    raise FederationValidationError(
                        "session-route-rollback",
                        "generation",
                        "route generation cannot move backwards",
                    )
                if descriptor.generation == current.generation:
                    if descriptor == current:
                        return False
                    raise FederationValidationError(
                        "session-route-generation-conflict",
                        "generation",
                        "one generation cannot identify two routes",
                    )
            elif len(self._items) >= self.max_entries:
                raise FederationValidationError(
                    "session-route-capacity-exhausted",
                    "descriptor",
                    "bounded rendezvous capacity is exhausted",
                )
            self._items[key] = descriptor
            return True

    def resolve(
        self, *, session_id: str, node_id: str, now: datetime
    ) -> SessionRouteDescriptor | None:
        key = (
            validate_peer_public_text(session_id, "session_id"),
            validate_peer_public_text(node_id, "node_id"),
        )
        with self._lock:
            self._purge_expired_locked(now)
            return self._items.get(key)

    def withdraw(
        self,
        *,
        session_id: str,
        node_id: str,
        generation: int,
        now: datetime,
    ) -> bool:
        key = (
            validate_peer_public_text(session_id, "session_id"),
            validate_peer_public_text(node_id, "node_id"),
        )
        generation = _generation(generation)
        with self._lock:
            self._purge_expired_locked(now)
            current = self._items.get(key)
            if current is None:
                return False
            if generation < current.generation:
                raise FederationValidationError(
                    "session-route-withdrawal-rollback",
                    "generation",
                    "withdrawal generation cannot be older than the route",
                )
            self._items.pop(key, None)
            return True

    def remove_node(self, node_id: str) -> int:
        node_id = validate_peer_public_text(node_id, "node_id")
        with self._lock:
            keys = [key for key in self._items if key[1] == node_id]
            for key in keys:
                self._items.pop(key, None)
            return len(keys)

    def count(self, *, now: datetime) -> int:
        with self._lock:
            self._purge_expired_locked(now)
            return len(self._items)

    def _purge_expired_locked(self, now: datetime) -> None:
        now = _utc(now, "now")
        expired = [key for key, value in self._items.items() if value.expires_at <= now]
        for key in expired:
            self._items.pop(key, None)
