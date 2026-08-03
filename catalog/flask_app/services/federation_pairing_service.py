"""Authenticated copy-and-paste pairing for physical MSH devices.

The public device ID identifies a key but grants no membership. Pairing therefore
uses a signed, short-lived bundle containing one-use enrollment and invitation
material issued by the existing coordinator. The joining device consumes the
bundle through the existing relay/node client. Tokens are never persisted.
"""

from __future__ import annotations

import asyncio
import base64
import json
import math
import os
import re
import ssl
import tempfile
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Final
from urllib.parse import urlsplit

from catalog.federation.errors import (
    AuthenticationError,
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.models import NodeIdentity
from catalog.federation.onboarding_compat import federation_id_matches_session
from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationSessionBinding,
)
from catalog.node.client import (
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_REQUEST_TIMEOUT,
    MAX_CLIENT_INTERVAL_SECONDS,
    MAX_INBOUND_MESSAGES,
    RelayNodeClient,
)
from catalog.node.identity import IdentityStore, NodeCredentials, verify_signature
from catalog.node.state import NodeState, ReconnectPolicy

from .capability_onboarding_service import (
    AuthorizedOnboardingContext,
    CapabilityOnboardingService,
)

PAIRING_CODE_PREFIX: Final = "MSH1-"
PAIRING_CODE_SCHEMA: Final = "msh.federation.pairing-code.v1"
PAIRING_PAYLOAD_SCHEMA: Final = "msh.federation.pairing-offer.v1"
PAIRING_STATE_SCHEMA: Final = "msh.federation.remote-pairing.v1"
MAX_PAIRING_CODE_BYTES: Final = 32_768
MAX_PAIRING_TTL_SECONDS: Final = 600
DEFAULT_PAIRING_TTL_SECONDS: Final = 300
DEFAULT_PAIRING_TIMEOUT_SECONDS: Final = 20.0
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError(
            "invalid-pairing-timestamp",
            "timestamp",
            "must be timezone-aware",
        )
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_stamp(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise FederationValidationError(
            "invalid-pairing-timestamp",
            field_name,
            "must be RFC 3339 text",
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FederationValidationError(
            "invalid-pairing-timestamp",
            field_name,
            "must be RFC 3339 text",
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FederationValidationError(
            "invalid-pairing-timestamp",
            field_name,
            "must be timezone-aware",
        )
    return parsed.astimezone(timezone.utc)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64decode(value: str) -> bytes:
    if not isinstance(value, str) or not value:
        raise FederationValidationError(
            "invalid-pairing-code",
            "pairing_code",
            "must contain canonical base64url text",
        )
    padded = value + "=" * (-len(value) % 4)
    try:
        decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise FederationValidationError(
            "invalid-pairing-code",
            "pairing_code",
            "must contain canonical base64url text",
        ) from exc
    if _b64encode(decoded) != value:
        raise FederationValidationError(
            "invalid-pairing-code",
            "pairing_code",
            "must use canonical base64url encoding",
        )
    return decoded


def _safe_identifier(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise FederationValidationError(
            "invalid-pairing-identifier",
            field_name,
            "must be printable text",
        )
    normalized = value.strip()
    if not _SAFE_ID.fullmatch(normalized):
        raise FederationValidationError(
            "invalid-pairing-identifier",
            field_name,
            "contains unsupported characters or exceeds its bound",
        )
    return normalized


def _validate_relay_url(value: object) -> str:
    if not isinstance(value, str) or len(value.encode("utf-8")) > 2_048:
        raise FederationValidationError(
            "invalid-pairing-relay-url",
            "relay_url",
            "must be a bounded WebSocket URL",
        )
    normalized = value.strip().rstrip("/")
    parsed = urlsplit(normalized)
    try:
        port = parsed.port
    except ValueError as exc:
        raise FederationValidationError(
            "invalid-pairing-relay-url",
            "relay_url",
            "contains an invalid port",
        ) from exc
    if (
        parsed.scheme not in {"ws", "wss"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or (port is not None and not 1 <= port <= 65_535)
    ):
        raise FederationValidationError(
            "invalid-pairing-relay-url",
            "relay_url",
            "must be a credential-free WebSocket service root",
        )
    return normalized


@dataclass(frozen=True, repr=False)
class PairingOffer:
    relay_url: str
    federation_id: str
    internal_session_id: str
    host_identity: NodeIdentity
    enrollment_token: str = field(repr=False)
    invitation_token: str = field(repr=False)
    issued_at: datetime
    expires_at: datetime

    def __repr__(self) -> str:
        return (
            "PairingOffer("
            f"relay_url={self.relay_url!r}, "
            f"federation_id={self.federation_id!r}, "
            f"host_node_id={self.host_identity.node_id!r}, "
            f"expires_at={self.expires_at!r})"
        )


class PairingCodeCodec:
    """Sign and verify bounded one-use pairing bundles."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._clock = clock

    def encode(
        self,
        *,
        credentials: NodeCredentials,
        relay_url: str,
        federation_id: str,
        internal_session_id: str,
        enrollment_token: str,
        invitation_token: str,
        ttl_seconds: int = DEFAULT_PAIRING_TTL_SECONDS,
    ) -> str:
        if (
            isinstance(ttl_seconds, bool)
            or not isinstance(ttl_seconds, int)
            or not 1 <= ttl_seconds <= MAX_PAIRING_TTL_SECONDS
        ):
            raise FederationValidationError(
                "invalid-pairing-ttl",
                "ttl_seconds",
                f"must be between 1 and {MAX_PAIRING_TTL_SECONDS}",
            )
        now = self._clock().astimezone(timezone.utc)
        session_id = _safe_identifier(
            internal_session_id,
            "internal_session_id",
        )
        federation = _safe_identifier(federation_id, "federation_id")
        if not federation_id_matches_session(federation, session_id):
            raise FederationValidationError(
                "pairing-federation-mismatch",
                "federation_id",
                "does not match the session authority",
            )
        for field_name, token in (
            ("enrollment_token", enrollment_token),
            ("invitation_token", invitation_token),
        ):
            if (
                not isinstance(token, str)
                or not token.strip()
                or len(token.encode("utf-8")) > 4_096
            ):
                raise FederationValidationError(
                    "invalid-pairing-token",
                    field_name,
                    "must be bounded non-empty text",
                )
        payload = {
            "schema": PAIRING_PAYLOAD_SCHEMA,
            "relay_url": _validate_relay_url(relay_url),
            "federation_id": federation,
            "internal_session_id": session_id,
            "host_identity": credentials.identity.to_dict(),
            "enrollment_token": enrollment_token,
            "invitation_token": invitation_token,
            "issued_at": _stamp(now),
            "expires_at": _stamp(now + timedelta(seconds=ttl_seconds)),
        }
        envelope = {
            "schema": PAIRING_CODE_SCHEMA,
            "payload": payload,
            "signature": credentials.sign(_canonical(payload)),
        }
        encoded = PAIRING_CODE_PREFIX + _b64encode(_canonical(envelope))
        if len(encoded.encode("ascii")) > MAX_PAIRING_CODE_BYTES:
            raise FederationValidationError(
                "pairing-code-too-large",
                "pairing_code",
                "exceeds the bounded code size",
            )
        return encoded

    def decode(self, code: object) -> PairingOffer:
        if not isinstance(code, str):
            raise FederationValidationError(
                "invalid-pairing-code",
                "pairing_code",
                "must be text",
            )
        normalized = "".join(code.split())
        if (
            not normalized.startswith(PAIRING_CODE_PREFIX)
            or len(normalized.encode("utf-8")) > MAX_PAIRING_CODE_BYTES
        ):
            raise FederationValidationError(
                "invalid-pairing-code",
                "pairing_code",
                "does not use the supported pairing-code format",
            )
        try:
            envelope = json.loads(
                _b64decode(normalized.removeprefix(PAIRING_CODE_PREFIX)).decode(
                    "utf-8"
                )
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "invalid-pairing-code",
                "pairing_code",
                "does not contain valid JSON",
            ) from exc
        if (
            not isinstance(envelope, dict)
            or envelope.get("schema") != PAIRING_CODE_SCHEMA
            or not isinstance(envelope.get("payload"), dict)
            or not isinstance(envelope.get("signature"), str)
        ):
            raise FederationValidationError(
                "invalid-pairing-code",
                "pairing_code",
                "does not satisfy the supported schema",
            )
        payload = envelope["payload"]
        if payload.get("schema") != PAIRING_PAYLOAD_SCHEMA:
            raise FederationValidationError(
                "invalid-pairing-code",
                "pairing_code",
                "contains an unsupported payload schema",
            )
        try:
            identity = NodeIdentity.from_dict(payload.get("host_identity"))
        except (TypeError, FederationValidationError) as exc:
            raise FederationValidationError(
                "invalid-pairing-host-identity",
                "host_identity",
                "does not contain a valid public host identity",
            ) from exc
        if not verify_signature(
            identity.public_key,
            _canonical(payload),
            envelope["signature"],
        ):
            raise AuthenticationError(
                "pairing-signature-invalid",
                "the pairing code was not signed by its advertised host",
                "pairing_code",
            )
        issued_at = _parse_stamp(payload.get("issued_at"), "issued_at")
        expires_at = _parse_stamp(payload.get("expires_at"), "expires_at")
        now = self._clock().astimezone(timezone.utc)
        if issued_at > now + timedelta(seconds=30):
            raise AuthenticationError(
                "pairing-code-not-yet-valid",
                "the pairing code timestamp is in the future",
                "pairing_code",
            )
        if expires_at <= now:
            raise AuthenticationError(
                "pairing-code-expired",
                "the pairing code has expired",
                "pairing_code",
            )
        if expires_at - issued_at > timedelta(seconds=MAX_PAIRING_TTL_SECONDS):
            raise AuthenticationError(
                "pairing-code-lifetime-invalid",
                "the pairing code lifetime exceeds the supported bound",
                "pairing_code",
            )
        session_id = _safe_identifier(
            payload.get("internal_session_id"),
            "internal_session_id",
        )
        federation_id = _safe_identifier(
            payload.get("federation_id"),
            "federation_id",
        )
        if not federation_id_matches_session(federation_id, session_id):
            raise AuthenticationError(
                "pairing-federation-mismatch",
                "the pairing code session does not match its federation",
                "pairing_code",
            )
        enrollment_token = payload.get("enrollment_token")
        invitation_token = payload.get("invitation_token")
        if not isinstance(enrollment_token, str) or not enrollment_token:
            raise FederationValidationError(
                "invalid-pairing-token",
                "enrollment_token",
                "is missing",
            )
        if not isinstance(invitation_token, str) or not invitation_token:
            raise FederationValidationError(
                "invalid-pairing-token",
                "invitation_token",
                "is missing",
            )
        return PairingOffer(
            relay_url=_validate_relay_url(payload.get("relay_url")),
            federation_id=federation_id,
            internal_session_id=session_id,
            host_identity=identity,
            enrollment_token=enrollment_token,
            invitation_token=invitation_token,
            issued_at=issued_at,
            expires_at=expires_at,
        )


class PairingRelayNodeClient(RelayNodeClient):
    """Existing relay client with explicit trusted-network plaintext opt-in."""

    def __init__(
        self,
        *,
        state_directory: Path | str,
        relay_url: str,
        display_name: str,
        ssl_context: ssl.SSLContext | None = None,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
        reconnect_policy: ReconnectPolicy | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        relay_url = _validate_relay_url(relay_url)
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not 0 < value <= MAX_CLIENT_INTERVAL_SECONDS
            for value in (heartbeat_interval, request_timeout)
        ):
            raise FederationValidationError(
                "invalid-client-timeout",
                "timeout",
                "heartbeat and request timeouts must be finite and positive",
            )
        self.state_directory = Path(state_directory)
        self.relay_url = relay_url
        self.credentials = IdentityStore(
            self.state_directory,
            display_name=display_name,
        ).load_or_create(now=clock())
        self.state = NodeState(
            self.state_directory / "node_state.sqlite3",
            node_id=self.credentials.identity.node_id,
            now=clock(),
        )
        self.ssl_context = ssl_context
        self.heartbeat_interval = heartbeat_interval
        self.request_timeout = request_timeout
        self.reconnect_policy = reconnect_policy or ReconnectPolicy()
        self._clock = clock
        self._sleep = asyncio.sleep
        self._websocket = None
        self._send_lock = asyncio.Lock()
        self._pending = {}
        self._replay_pending = set()
        self._gap_replay_tasks = {}
        self._receiver_task = None
        self._heartbeat_task = None
        self._inbound = asyncio.Queue(maxsize=MAX_INBOUND_MESSAGES)
        self.connected_event = asyncio.Event()
        self.disconnected_event = asyncio.Event()
        self.disconnected_event.set()


@dataclass(frozen=True)
class RemotePairingState:
    relay_url: str
    binding: FederationSessionBinding


class RemotePairingStore:
    """Persist only the relay root and public-safe trusted binding."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def load(self) -> RemotePairingState | None:
        if not self.path.exists():
            return None
        try:
            raw = self.path.read_bytes()
            if len(raw) > 65_536:
                raise ValueError("pairing state is too large")
            value = json.loads(raw.decode("utf-8"))
            if not isinstance(value, dict) or value.get("schema") != PAIRING_STATE_SCHEMA:
                raise ValueError("unsupported pairing state")
            relay_url = _validate_relay_url(value.get("relay_url"))
            binding = FederationSessionBinding.from_dict(value.get("binding"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise FederationValidationError(
                "malformed-remote-pairing-state",
                "pairing_state",
                "remote pairing state could not be read safely",
            ) from exc
        return RemotePairingState(relay_url=relay_url, binding=binding)

    def save(self, state: RemotePairingState) -> RemotePairingState:
        payload = _canonical(
            {
                "schema": PAIRING_STATE_SCHEMA,
                "relay_url": _validate_relay_url(state.relay_url),
                "binding": state.binding.to_dict(),
            }
        )
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.",
            dir=self.path.parent,
        )
        temporary = Path(temporary_name)
        try:
            os.chmod(temporary, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                descriptor = -1
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            os.chmod(self.path, 0o600)
        except OSError as exc:
            raise FederationValidationError(
                "remote-pairing-state-write-failed",
                "pairing_state",
                "remote pairing state could not be persisted safely",
            ) from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)
        return state


class PairingRelayRuntime:
    """Own one persistent outbound relay client in a private event-loop thread."""

    def __init__(
        self,
        *,
        state_directory: Path | str,
        display_name: str,
        clock: Callable[[], datetime] = _utc_now,
        timeout_seconds: float = DEFAULT_PAIRING_TIMEOUT_SECONDS,
    ) -> None:
        self.state_directory = Path(state_directory)
        self.display_name = display_name
        self._clock = clock
        self.timeout_seconds = timeout_seconds
        self._lock = threading.RLock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._client: PairingRelayNodeClient | None = None
        self._relay_url: str | None = None

    def _start_loop(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._loop is not None and self._thread is not None and self._thread.is_alive():
                return self._loop
            ready = threading.Event()

            def run() -> None:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                with self._lock:
                    self._loop = loop
                ready.set()
                loop.run_forever()
                loop.close()

            thread = threading.Thread(
                target=run,
                name="msh-pairing-relay",
                daemon=True,
            )
            self._thread = thread
            thread.start()
        if not ready.wait(timeout=5):
            raise FederationOperationError(
                "pairing-runtime-start-failed",
                "the relay pairing runtime did not start",
            )
        assert self._loop is not None
        return self._loop

    def _submit(self, coroutine: Any) -> Any:
        loop = self._start_loop()
        future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        try:
            return future.result(timeout=self.timeout_seconds)
        except TimeoutError as exc:
            future.cancel()
            raise FederationOperationError(
                "pairing-relay-timeout",
                "the remote relay did not respond in time",
            ) from exc

    async def _disconnect_current(self) -> None:
        client = self._client
        self._client = None
        self._relay_url = None
        if client is not None and client.connected_event.is_set():
            await client.disconnect()

    async def _redeem(self, offer: PairingOffer) -> FederationSessionBinding:
        await self._disconnect_current()
        client = PairingRelayNodeClient(
            state_directory=self.state_directory,
            relay_url=offer.relay_url,
            display_name=self.display_name,
            clock=self._clock,
            request_timeout=min(self.timeout_seconds, 60.0),
        )
        try:
            await client.connect(enrollment_token=offer.enrollment_token)
            session = await client.join_session(offer.invitation_token)
        except BaseException:
            if client.connected_event.is_set():
                await client.disconnect(error_code="pairing-failed")
            raise
        session_id = str(session.get("session_id") or "")
        if session_id != offer.internal_session_id:
            await client.disconnect(error_code="pairing-session-mismatch")
            raise AuthenticationError(
                "pairing-session-mismatch",
                "the relay joined a different session than the signed offer",
                "pairing_code",
            )
        revision = session.get("revision")
        if isinstance(revision, bool) or not isinstance(revision, int) or revision <= 0:
            revision = 1
        created_at = _parse_stamp(
            session.get("created_at") or _stamp(self._clock()),
            "created_at",
        )
        self._client = client
        self._relay_url = offer.relay_url
        return FederationSessionBinding(
            federation_id=offer.federation_id,
            internal_session_id=offer.internal_session_id,
            device_id=client.node_id,
            state=FederationConnectionState.CONNECTED,
            revision=revision,
            trusted=True,
            created_at=created_at,
            last_verified_at=self._clock().astimezone(timezone.utc),
        )

    def redeem(self, offer: PairingOffer) -> FederationSessionBinding:
        return self._submit(self._redeem(offer))

    async def _ensure_connected(self, state: RemotePairingState) -> None:
        if (
            self._client is not None
            and self._client.connected_event.is_set()
            and self._relay_url == state.relay_url
        ):
            return
        await self._disconnect_current()
        client = PairingRelayNodeClient(
            state_directory=self.state_directory,
            relay_url=state.relay_url,
            display_name=self.display_name,
            clock=self._clock,
            request_timeout=min(self.timeout_seconds, 60.0),
        )
        await client.connect()
        joined = {
            item.session_id for item in client.state.joined_sessions()
        }
        if state.binding.internal_session_id not in joined:
            await client.disconnect(error_code="pairing-membership-missing")
            raise AuthenticationError(
                "pairing-membership-missing",
                "the saved remote federation membership is no longer active",
                "binding",
            )
        self._client = client
        self._relay_url = state.relay_url

    def ensure_connected(self, state: RemotePairingState) -> None:
        self._submit(self._ensure_connected(state))

    def coordinator_status(self) -> dict[str, Any]:
        client = self._client
        if client is None or not client.connected_event.is_set():
            raise FederationOperationError(
                "pairing-relay-disconnected",
                "the paired relay is not connected",
            )
        return self._submit(client.coordinator_status())


class RemoteCoordinatorFacade:
    """Read-only coordinator shape backed by the authenticated relay client."""

    def __init__(
        self,
        runtime: PairingRelayRuntime,
        state: RemotePairingState,
    ) -> None:
        self.runtime = runtime
        self.state = state
        self.store = self

    def _status(self) -> dict[str, Any]:
        self.runtime.ensure_connected(self.state)
        return self.runtime.coordinator_status()

    @property
    def coordinator_id(self) -> str:
        value = self._status().get("coordinator_id")
        return str(value or "remote-coordinator")

    def get_session(self, session_id: str) -> object | None:
        for session in self._status().get("sessions", ()):
            if isinstance(session, dict) and session.get("session_id") == session_id:
                return SimpleNamespace(**session)
        return None

    def status(
        self,
        *,
        actor_node_id: str,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if actor_node_id != self.state.binding.device_id:
            raise AuthenticationError(
                "pairing-actor-mismatch",
                "remote status must use the paired device identity",
                "actor_node_id",
            )
        if cursor is not None:
            raise FederationValidationError(
                "pairing-status-cursor-unsupported",
                "cursor",
                "the relay client already returns one complete snapshot",
            )
        return self._status()

    def require_membership(self, *, session_id: str, node_id: str) -> None:
        if (
            session_id != self.state.binding.internal_session_id
            or node_id != self.state.binding.device_id
        ):
            raise AuthenticationError(
                "pairing-membership-mismatch",
                "the requested membership is not the paired binding",
                "binding",
            )
        self._status()

    def session_ids_for_node(self, node_id: str) -> tuple[str, ...]:
        if node_id != self.state.binding.device_id:
            return ()
        self._status()
        return (self.state.binding.internal_session_id,)


class PairingAwareCapabilityOnboardingService(CapabilityOnboardingService):
    """Add authenticated remote pairing while retaining all local CFI behavior."""

    def __init__(
        self,
        *,
        remote_store: RemotePairingStore,
        relay_runtime: PairingRelayRuntime,
        pairing_codec: PairingCodeCodec | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.remote_store = remote_store
        self.relay_runtime = relay_runtime
        self.pairing_codec = pairing_codec or PairingCodeCodec(clock=self._clock)
        self._last_pairing_code: str | None = None

    def binding_or_none(self) -> FederationSessionBinding | None:
        remote = self.remote_store.load()
        return remote.binding if remote is not None else super().binding_or_none()

    def authorized_context(self) -> AuthorizedOnboardingContext | None:
        remote = self.remote_store.load()
        if remote is None:
            return super().authorized_context()
        credentials = self.identity_or_none()
        if credentials is None or credentials.identity.node_id != remote.binding.device_id:
            return None
        self.relay_runtime.ensure_connected(remote)
        return AuthorizedOnboardingContext(
            credentials=credentials,
            binding=remote.binding,
            coordinator=RemoteCoordinatorFacade(self.relay_runtime, remote),  # type: ignore[arg-type]
        )

    def reconnect(self) -> FederationSessionBinding:
        remote = self.remote_store.load()
        if remote is None:
            return super().reconnect()
        context = self.authorized_context()
        if context is None:
            raise FederationOperationError(
                "pairing-reconnect-failed",
                "the saved remote pairing could not be revalidated",
                "binding",
            )
        refreshed = FederationSessionBinding(
            federation_id=remote.binding.federation_id,
            internal_session_id=remote.binding.internal_session_id,
            device_id=remote.binding.device_id,
            state=FederationConnectionState.CONNECTED,
            revision=remote.binding.revision,
            trusted=True,
            created_at=remote.binding.created_at,
            last_verified_at=self._clock().astimezone(timezone.utc),
        )
        self.remote_store.save(
            RemotePairingState(remote.relay_url, refreshed)
        )
        return refreshed

    def create_pairing_code(
        self,
        *,
        relay_url: str,
        ttl_seconds: int = DEFAULT_PAIRING_TTL_SECONDS,
    ) -> str:
        if self.remote_store.load() is not None:
            raise FederationOperationError(
                "pairing-host-must-be-local-authority",
                "a remotely paired device cannot issue a new host code",
                "binding",
            )
        context = super().authorized_context()
        if context is None:
            raise FederationOperationError(
                "pairing-federation-required",
                "create or connect a local federation before generating a code",
                "binding",
            )
        enrollment = context.coordinator.create_enrollment_token(
            ttl_seconds=ttl_seconds,
            max_uses=1,
        )
        invitation = context.coordinator.create_invitation(
            session_id=context.binding.internal_session_id,
            actor_node_id=context.credentials.identity.node_id,
            ttl_seconds=ttl_seconds,
            max_uses=1,
            request_id=f"pairing-invite-{os.urandom(12).hex()}",
        )
        code = self.pairing_codec.encode(
            credentials=context.credentials,
            relay_url=relay_url,
            federation_id=context.binding.federation_id,
            internal_session_id=context.binding.internal_session_id,
            enrollment_token=str(enrollment["token"]),
            invitation_token=str(invitation["token"]),
            ttl_seconds=ttl_seconds,
        )
        self._last_pairing_code = code
        return code

    def last_pairing_code(self) -> str | None:
        return self._last_pairing_code

    def redeem_pairing_code(self, code: str) -> FederationSessionBinding:
        credentials = self.create_identity()
        offer = self.pairing_codec.decode(code)
        binding = self.relay_runtime.redeem(offer)
        if binding.device_id != credentials.identity.node_id:
            raise AuthenticationError(
                "pairing-device-mismatch",
                "the relay used a different local device identity",
                "device_id",
            )
        self.remote_store.save(
            RemotePairingState(offer.relay_url, binding)
        )
        return binding

    def build_view_model(self, **kwargs: Any) -> dict[str, object]:
        model = super().build_view_model(**kwargs)
        federation = model.get("federation")
        if isinstance(federation, dict):
            remote = self.remote_store.load()
            federation["pairing_code"] = self.last_pairing_code() or ""
            federation["pairing_mode"] = "remote" if remote is not None else "local"
            federation["can_generate_pairing_code"] = (
                remote is None and kwargs.get("connected_binding") is not None
            )
            federation["can_redeem_pairing_code"] = True
        actions = model.get("actions")
        if isinstance(actions, dict):
            actions["pairing_code_url"] = "/onboarding/federation/pairing-code"
            actions["pair_url"] = "/onboarding/federation/pair"
        return model


__all__ = [
    "PairingAwareCapabilityOnboardingService",
    "PairingCodeCodec",
    "PairingOffer",
    "PairingRelayRuntime",
    "RemoteCoordinatorFacade",
    "RemotePairingState",
    "RemotePairingStore",
]
