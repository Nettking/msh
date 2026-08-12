"""Federation-scoped human single sign-on without replicating passwords.

The Federation creator is the human-authentication authority. Passwords and
password hashes remain only in that installation's authentication database.
Other Federation members authenticate by redirecting the browser to the
authority and accepting a short-lived Ed25519-signed assertion targeted to the
requesting device.

Only non-secret authorization metadata (email, active state, roles, web base
URLs, and the authority's public node identity) is written to the Federation
session log. Coordinator policy constrains authority/user events to the session
creator and member endpoint announcements to the authenticated announcing node.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

from email_validator import EmailNotValidError, validate_email
from flask import (
    Blueprint,
    abort,
    current_app,
    flash,
    redirect,
    request,
    session,
    url_for,
)
from flask_login import login_user, logout_user
from flask_security import current_user

from catalog.federation.errors import FederationOperationError
from catalog.federation.human_auth import (
    AUTHORITY_EVENT,
    AUTHORITY_SCHEMA,
    MEMBER_ENDPOINT_EVENT,
    MEMBER_ENDPOINT_SCHEMA,
    USER_EVENT,
    USER_SCHEMA,
)
from catalog.federation.models import SessionEvent
from catalog.node.identity import derive_node_id_from_public_key, verify_signature

from .models import Role, User, db

ASSERTION_SCHEMA = "fcp.human-auth.assertion.v1"

FEDERATED_UNIQUIFIER_PREFIX = "federated:"
ASSERTION_TTL_SECONDS = 90
ASSERTION_CLOCK_SKEW_SECONDS = 30
MAX_ASSERTION_BYTES = 16_384
MAX_STATE_BYTES = 256
MAX_BASE_URL_BYTES = 2_048
EVENT_PAGE_SIZE = 500
MAX_EVENT_PAGES = 2_000
EVENT_CACHE_SECONDS = 2.0
_SERVICE_EXTENSION_KEY = "federated_human_auth_service"

_SESSION_STATE_KEY = "fcp_federated_auth_state"
_SESSION_NEXT_KEY = "fcp_federated_auth_next"
_SESSION_ISSUED_KEY = "fcp_federated_auth_issued"

federated_human_auth = Blueprint(
    "federated_human_auth", __name__, url_prefix="/federation-auth"
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_stamp(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be RFC 3339 text")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64decode(value: str) -> bytes:
    if not isinstance(value, str) or not value:
        raise ValueError("must be canonical base64url text")
    padded = value + ("=" * (-len(value) % 4))
    decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
    if _b64encode(decoded) != value:
        raise ValueError("must use canonical base64url encoding")
    return decoded


def _normalize_email(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("invalid email")
    try:
        return validate_email(
            value.strip(), check_deliverability=False
        ).normalized.lower()
    except EmailNotValidError as exc:
        raise ValueError("invalid email") from exc


def _normalize_base_url(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("missing web base URL")
    normalized = value.strip()
    if len(normalized.encode("utf-8")) > MAX_BASE_URL_BYTES:
        raise ValueError("web base URL is too large")
    parsed = urlsplit(normalized)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("invalid web base URL port") from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or (port is not None and not 1 <= port <= 65_535)
    ):
        raise ValueError("invalid web base URL")
    path = parsed.path or "/"
    if path != "/":
        # FCP currently has no supported non-root application mount. Refusing
        # path-bearing authority URLs also prevents path-confusion redirects.
        raise ValueError("web base URL must point at the FCP origin root")
    return urlunsplit((parsed.scheme.lower(), parsed.netloc, "", "", "")).rstrip("/")


def _safe_next(value: object) -> str:
    if not isinstance(value, str) or not value:
        return "/"
    if len(value) > 2_048 or not value.startswith("/") or value.startswith("//"):
        return "/"
    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc:
        return "/"
    return value


def configured_base_url() -> str:
    configured = current_app.config.get("FCP_HUMAN_AUTH_BASE_URL")
    if configured is None:
        configured = os.getenv("FCP_HUMAN_AUTH_BASE_URL")
    return _normalize_base_url(configured or request.url_root)


def local_fallback_enabled() -> bool:
    value = current_app.config.get("FCP_HUMAN_AUTH_LOCAL_FALLBACK")
    if value is None:
        value = os.getenv("FCP_HUMAN_AUTH_LOCAL_FALLBACK", "")
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def saved_remote_member() -> bool:
    """Return whether this installation has a persisted remote Federation binding.

    This deliberately does not require the relay to be reachable. It prevents a
    network outage from silently re-enabling device-local password login on a
    member that is configured to trust the Federation human-auth authority.
    """

    try:
        from catalog.flask_app.services.capability_onboarding_service import (
            get_capability_onboarding_service,
        )

        onboarding = get_capability_onboarding_service()
        remote_store = getattr(onboarding, "remote_store", None)
        if remote_store is None:
            return False
        return remote_store.load() is not None
    except Exception:  # noqa: BLE001 - absence of pairing state is not fatal
        return False


@dataclass(frozen=True)
class AuthorityRecord:
    node_id: str
    public_key: str
    base_url: str
    revision: int


@dataclass(frozen=True)
class LoginMode:
    mode: str
    federation_id: str | None
    session_id: str | None
    local_node_id: str | None
    authority: AuthorityRecord | None

    @property
    def is_member(self) -> bool:
        return self.mode == "member"

    @property
    def is_authority(self) -> bool:
        return self.mode == "authority"


class FederationHumanAuthService:
    """Project the Federation's human-auth authority and signed login handoff."""

    def __init__(
        self,
        *,
        context_provider: Callable[[], object | None] | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._context_provider = context_provider or self._default_context
        self._clock = clock
        self._cache_lock = threading.RLock()
        self._event_cache: tuple[str, float, tuple[SessionEvent, ...]] | None = None

    @staticmethod
    def _default_context() -> object | None:
        try:
            from catalog.flask_app.services.capability_onboarding_service import (
                get_capability_onboarding_service,
            )

            return get_capability_onboarding_service().authorized_context()
        except Exception:  # noqa: BLE001 - login must remain available standalone
            return None

    def _context(self) -> object | None:
        try:
            return self._context_provider()
        except Exception:  # noqa: BLE001 - federation outage is a mode, not a crash
            return None

    @staticmethod
    def _context_parts(context: object) -> tuple[str, str, str, object, object]:
        binding = getattr(context, "binding", None)
        credentials = getattr(context, "credentials", None)
        identity = getattr(credentials, "identity", None)
        coordinator = getattr(context, "coordinator", None)
        session_id = getattr(binding, "internal_session_id", None)
        federation_id = getattr(binding, "federation_id", None)
        node_id = getattr(identity, "node_id", None)
        if not all(isinstance(value, str) and value for value in (session_id, federation_id, node_id)):
            raise FederationOperationError(
                "human-auth-context-incomplete",
                "the trusted Federation context is incomplete",
                "binding",
            )
        if coordinator is None:
            raise FederationOperationError(
                "human-auth-context-incomplete",
                "the trusted Federation coordinator is unavailable",
                "coordinator",
            )
        return federation_id, session_id, node_id, credentials, coordinator

    @staticmethod
    def _session(coordinator: object, session_id: str) -> object | None:
        store = getattr(coordinator, "store", None)
        getter = getattr(store, "get_session", None)
        if callable(getter):
            return getter(session_id)
        getter = getattr(coordinator, "get_session", None)
        if callable(getter):
            return getter(session_id)
        return None

    def _leader_node_id(self, context: object) -> str | None:
        _federation_id, session_id, _node_id, _credentials, coordinator = (
            self._context_parts(context)
        )
        session_value = self._session(coordinator, session_id)
        value = getattr(session_value, "created_by_node_id", None)
        return value if isinstance(value, str) and value else None

    def _is_authority_context(self, context: object) -> bool:
        _federation_id, _session_id, node_id, _credentials, _coordinator = (
            self._context_parts(context)
        )
        return self._leader_node_id(context) == node_id

    @staticmethod
    def _read_events_uncached(context: object) -> tuple[SessionEvent, ...]:
        binding = getattr(context, "binding")
        credentials = getattr(context, "credentials")
        coordinator = getattr(context, "coordinator")
        session_id = binding.internal_session_id
        actor_node_id = credentials.identity.node_id

        runtime = getattr(coordinator, "runtime", None)
        remote_state = getattr(coordinator, "state", None)
        if runtime is not None and remote_state is not None:
            reader = getattr(runtime, "session_events", None)
            if callable(reader):
                return tuple(
                    reader(
                        remote_state,
                        session_id=session_id,
                        after_revision=0,
                    )
                )

        replay_page = getattr(coordinator, "replay_page", None)
        if callable(replay_page):
            events: list[SessionEvent] = []
            last_revision = 0
            for _ in range(MAX_EVENT_PAGES):
                page, current_revision = replay_page(
                    session_id=session_id,
                    actor_node_id=actor_node_id,
                    last_applied_revision=last_revision,
                    limit=EVENT_PAGE_SIZE,
                )
                if not page:
                    break
                events.extend(page)
                last_revision = page[-1].revision
                if last_revision >= current_revision:
                    break
            return tuple(events)

        replay = getattr(coordinator, "replay", None)
        if callable(replay):
            return tuple(
                replay(
                    session_id=session_id,
                    actor_node_id=actor_node_id,
                    last_applied_revision=0,
                )
            )
        raise FederationOperationError(
            "human-auth-replay-unavailable",
            "the connected Federation does not expose its authenticated event log",
        )

    def _events(
        self, context: object, *, refresh: bool = False
    ) -> tuple[SessionEvent, ...]:
        _federation_id, session_id, _node_id, _credentials, _coordinator = (
            self._context_parts(context)
        )
        now = time.monotonic()
        with self._cache_lock:
            cached = self._event_cache
            if (
                not refresh
                and cached is not None
                and cached[0] == session_id
                and now - cached[1] <= EVENT_CACHE_SECONDS
            ):
                return cached[2]
        events = self._read_events_uncached(context)
        with self._cache_lock:
            self._event_cache = (session_id, now, events)
        return events

    def _invalidate_events(self) -> None:
        with self._cache_lock:
            self._event_cache = None

    @staticmethod
    def _append(
        context: object,
        *,
        event_type: str,
        payload: dict[str, Any],
        request_id: str,
    ) -> SessionEvent:
        binding = getattr(context, "binding")
        credentials = getattr(context, "credentials")
        coordinator = getattr(context, "coordinator")
        session_id = binding.internal_session_id
        actor_node_id = credentials.identity.node_id

        runtime = getattr(coordinator, "runtime", None)
        remote_state = getattr(coordinator, "state", None)
        if runtime is not None and remote_state is not None:
            writer = getattr(runtime, "append_session_event", None)
            if callable(writer):
                return writer(
                    remote_state,
                    session_id=session_id,
                    event_type=event_type,
                    payload=payload,
                    request_id=request_id,
                )

        writer = getattr(coordinator, "append_event", None)
        if not callable(writer):
            raise FederationOperationError(
                "human-auth-append-unavailable",
                "the connected Federation does not accept human-auth metadata",
            )
        event, _created = writer(
            session_id=session_id,
            actor_node_id=actor_node_id,
            request_id=request_id,
            event_type=event_type,
            payload=payload,
        )
        return event

    def mode(self, *, refresh: bool = False) -> LoginMode:
        context = self._context()
        if context is None:
            return LoginMode("local", None, None, None, None)
        try:
            federation_id, session_id, node_id, _credentials, _coordinator = (
                self._context_parts(context)
            )
            events = self._events(context, refresh=refresh)
            authority = self._authority_from_events(context, events)
            mode = "authority" if self._is_authority_context(context) else "member"
            return LoginMode(mode, federation_id, session_id, node_id, authority)
        except Exception:  # noqa: BLE001 - local login page must fail safely
            return LoginMode("local", None, None, None, None)

    def _authority_from_events(
        self, context: object, events: Iterable[SessionEvent]
    ) -> AuthorityRecord | None:
        leader_node_id = self._leader_node_id(context)
        if leader_node_id is None:
            return None
        latest: AuthorityRecord | None = None
        for event in sorted(events, key=lambda item: item.revision):
            if (
                event.event_type != AUTHORITY_EVENT
                or event.actor_node_id != leader_node_id
                or not isinstance(event.payload, dict)
                or event.payload.get("schema") != AUTHORITY_SCHEMA
                or event.payload.get("node_id") != leader_node_id
            ):
                continue
            public_key = event.payload.get("public_key")
            base_url = event.payload.get("base_url")
            if not isinstance(public_key, str) or not isinstance(base_url, str):
                continue
            try:
                if derive_node_id_from_public_key(public_key) != leader_node_id:
                    continue
                normalized_url = _normalize_base_url(base_url)
            except Exception:  # noqa: BLE001 - malformed history is ignored
                continue
            latest = AuthorityRecord(
                node_id=leader_node_id,
                public_key=public_key,
                base_url=normalized_url,
                revision=event.revision,
            )
        return latest

    def authority(self, *, refresh: bool = False) -> AuthorityRecord | None:
        context = self._context()
        if context is None:
            return None
        return self._authority_from_events(
            context, self._events(context, refresh=refresh)
        )

    def ensure_authority(self, base_url: str) -> AuthorityRecord | None:
        context = self._context()
        if context is None or not self._is_authority_context(context):
            return self.authority(refresh=True) if context is not None else None
        _federation_id, _session_id, node_id, credentials, _coordinator = (
            self._context_parts(context)
        )
        normalized = _normalize_base_url(base_url)
        events = self._events(context, refresh=True)
        current = self._authority_from_events(context, events)
        public_key = credentials.identity.public_key
        if (
            current is not None
            and current.node_id == node_id
            and current.public_key == public_key
            and current.base_url == normalized
        ):
            return current
        content_hash = hashlib.sha256(
            f"{node_id}\0{public_key}\0{normalized}".encode("utf-8")
        ).hexdigest()
        payload = {
            "schema": AUTHORITY_SCHEMA,
            "node_id": node_id,
            "public_key": public_key,
            "base_url": normalized,
        }
        event = self._append(
            context,
            event_type=AUTHORITY_EVENT,
            payload=payload,
            request_id=f"human-auth-authority-{content_hash[:32]}",
        )
        self._invalidate_events()
        return AuthorityRecord(node_id, public_key, normalized, event.revision)

    def ensure_member_endpoint(self, base_url: str) -> str:
        context = self._context()
        if context is None:
            raise FederationOperationError(
                "human-auth-federation-required",
                "Federation membership is required for Federation sign-in",
            )
        _federation_id, _session_id, node_id, _credentials, _coordinator = (
            self._context_parts(context)
        )
        normalized = _normalize_base_url(base_url)
        events = self._events(context, refresh=True)
        current = self._member_endpoint_from_events(events, node_id)
        if current == normalized:
            return normalized
        content_hash = hashlib.sha256(
            f"{node_id}\0{normalized}".encode("utf-8")
        ).hexdigest()
        payload = {
            "schema": MEMBER_ENDPOINT_SCHEMA,
            "node_id": node_id,
            "base_url": normalized,
        }
        self._append(
            context,
            event_type=MEMBER_ENDPOINT_EVENT,
            payload=payload,
            request_id=f"human-auth-member-endpoint-{content_hash[:32]}",
        )
        self._invalidate_events()
        return normalized

    @staticmethod
    def _member_endpoint_from_events(
        events: Iterable[SessionEvent], node_id: str
    ) -> str | None:
        latest: str | None = None
        for event in sorted(events, key=lambda item: item.revision):
            if (
                event.event_type != MEMBER_ENDPOINT_EVENT
                or event.actor_node_id != node_id
                or not isinstance(event.payload, dict)
                or event.payload.get("schema") != MEMBER_ENDPOINT_SCHEMA
                or event.payload.get("node_id") != node_id
            ):
                continue
            try:
                latest = _normalize_base_url(event.payload.get("base_url"))
            except ValueError:
                continue
        return latest

    def member_endpoint(self, node_id: str, *, refresh: bool = False) -> str | None:
        context = self._context()
        if context is None:
            return None
        return self._member_endpoint_from_events(
            self._events(context, refresh=refresh), node_id
        )

    @staticmethod
    def _user_payload(user: User) -> dict[str, Any]:
        roles = sorted(
            role.name
            for role in user.roles
            if isinstance(role.name, str) and role.name
        )
        return {
            "schema": USER_SCHEMA,
            "email": _normalize_email(user.email),
            "active": bool(user.active),
            "roles": roles,
        }

    @staticmethod
    def _latest_user_event(
        events: Iterable[SessionEvent],
        *,
        authority_node_id: str,
        email: str,
    ) -> SessionEvent | None:
        latest: SessionEvent | None = None
        for event in sorted(events, key=lambda item: item.revision):
            if (
                event.event_type != USER_EVENT
                or event.actor_node_id != authority_node_id
                or not isinstance(event.payload, dict)
                or event.payload.get("schema") != USER_SCHEMA
            ):
                continue
            try:
                event_email = _normalize_email(event.payload.get("email"))
            except ValueError:
                continue
            if event_email == email:
                latest = event
        return latest

    def publish_user(self, user: User) -> SessionEvent | None:
        context = self._context()
        if context is None or not self._is_authority_context(context):
            return None
        authority_node_id = self._leader_node_id(context)
        if authority_node_id is None:
            return None
        desired = self._user_payload(user)
        email = desired["email"]
        events = self._events(context, refresh=True)
        latest = self._latest_user_event(
            events, authority_node_id=authority_node_id, email=email
        )
        if latest is not None and latest.payload == desired:
            return latest
        base_revision = 0 if latest is None else latest.revision
        encoded = _canonical(desired)
        digest = hashlib.sha256(encoded).hexdigest()
        subject = hashlib.sha256(email.encode("utf-8")).hexdigest()[:16]
        event = self._append(
            context,
            event_type=USER_EVENT,
            payload=desired,
            request_id=(
                f"human-auth-user-{subject}-{base_revision}-{digest[:20]}"
            ),
        )
        self._invalidate_events()
        return event

    def publish_all_users(self) -> None:
        context = self._context()
        if context is None or not self._is_authority_context(context):
            return
        for user in db.session.query(User).order_by(User.email).all():
            self.publish_user(user)

    def latest_user_metadata(
        self, email: str, *, refresh: bool = False
    ) -> dict[str, Any] | None:
        context = self._context()
        if context is None:
            return None
        authority = self._authority_from_events(
            context, self._events(context, refresh=refresh)
        )
        if authority is None:
            return None
        normalized = _normalize_email(email)
        latest = self._latest_user_event(
            self._events(context, refresh=refresh),
            authority_node_id=authority.node_id,
            email=normalized,
        )
        return None if latest is None else dict(latest.payload)

    def sync_current_shadow_user(self) -> bool:
        if not current_user.is_authenticated:
            return False
        user = db.session.get(User, current_user.id)
        if user is None or not str(user.fs_uniquifier).startswith(
            FEDERATED_UNIQUIFIER_PREFIX
        ):
            return False
        mode = self.mode()
        if not mode.is_member:
            return False
        metadata = self.latest_user_metadata(user.email)
        if metadata is None:
            return False
        active = metadata.get("active") is True
        role_names = metadata.get("roles")
        if not isinstance(role_names, list) or any(
            not isinstance(name, str) for name in role_names
        ):
            return False
        roles = (
            db.session.query(Role).filter(Role.name.in_(set(role_names))).all()
            if role_names
            else []
        )
        changed = user.active != active or {r.name for r in user.roles} != {
            r.name for r in roles
        }
        if changed:
            user.active = active
            user.roles = roles
            db.session.commit()
        if not active:
            logout_user()
        return changed

    def issue_assertion(
        self,
        *,
        user: User,
        target_node_id: str,
        state: str,
    ) -> str:
        context = self._context()
        if context is None or not self._is_authority_context(context):
            raise FederationOperationError(
                "human-auth-authority-required",
                "only the Federation creator can issue human login assertions",
            )
        if not user.active:
            raise FederationOperationError(
                "human-auth-user-inactive",
                "inactive users cannot receive Federation login assertions",
            )
        if (
            not isinstance(state, str)
            or not state
            or len(state.encode("utf-8")) > MAX_STATE_BYTES
        ):
            raise FederationOperationError(
                "human-auth-state-invalid",
                "the Federation login state is invalid",
                "state",
            )
        federation_id, session_id, node_id, credentials, coordinator = (
            self._context_parts(context)
        )
        store = getattr(coordinator, "store", None)
        require_membership = getattr(store, "require_membership", None)
        if callable(require_membership):
            require_membership(session_id=session_id, node_id=target_node_id)
        elif self.member_endpoint(target_node_id, refresh=True) is None:
            raise FederationOperationError(
                "human-auth-target-unavailable",
                "the target device has not registered a Federation login endpoint",
                "target_node_id",
            )
        now = self._clock().astimezone(timezone.utc)
        payload = {
            "schema": ASSERTION_SCHEMA,
            "federation_id": federation_id,
            "session_id": session_id,
            "issuer_node_id": node_id,
            "target_node_id": target_node_id,
            "subject": _normalize_email(user.email),
            "active": True,
            "roles": sorted(role.name for role in user.roles),
            "state": state,
            "issued_at": _stamp(now),
            "expires_at": _stamp(now + timedelta(seconds=ASSERTION_TTL_SECONDS)),
        }
        envelope = {
            "payload": payload,
            "signature": credentials.sign(_canonical(payload)),
        }
        encoded = _b64encode(_canonical(envelope))
        if len(encoded.encode("ascii")) > MAX_ASSERTION_BYTES:
            raise FederationOperationError(
                "human-auth-assertion-too-large",
                "the Federation login assertion exceeds its size bound",
            )
        return encoded

    def consume_assertion(
        self,
        encoded: str,
        *,
        expected_state: str,
    ) -> User:
        if (
            not isinstance(encoded, str)
            or not encoded
            or len(encoded.encode("ascii", errors="ignore")) > MAX_ASSERTION_BYTES
        ):
            raise FederationOperationError(
                "human-auth-assertion-invalid",
                "the Federation login assertion is missing or too large",
            )
        context = self._context()
        if context is None:
            raise FederationOperationError(
                "human-auth-federation-required",
                "Federation membership is required for Federation sign-in",
            )
        federation_id, session_id, node_id, _credentials, _coordinator = (
            self._context_parts(context)
        )
        if self._is_authority_context(context):
            raise FederationOperationError(
                "human-auth-member-required",
                "the Federation authority signs in locally",
            )
        authority = self._authority_from_events(
            context, self._events(context, refresh=True)
        )
        if authority is None:
            raise FederationOperationError(
                "human-auth-authority-unavailable",
                "the Federation authentication authority is unavailable",
            )
        try:
            envelope = json.loads(_b64decode(encoded).decode("utf-8"))
            payload = envelope["payload"]
            signature = envelope["signature"]
        except (ValueError, KeyError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationOperationError(
                "human-auth-assertion-invalid",
                "the Federation login assertion is malformed",
            ) from exc
        if (
            not isinstance(payload, dict)
            or payload.get("schema") != ASSERTION_SCHEMA
            or not isinstance(signature, str)
        ):
            raise FederationOperationError(
                "human-auth-assertion-invalid",
                "the Federation login assertion uses an unsupported schema",
            )
        if not verify_signature(authority.public_key, _canonical(payload), signature):
            raise FederationOperationError(
                "human-auth-assertion-signature-invalid",
                "the Federation login assertion signature is invalid",
            )
        if (
            payload.get("federation_id") != federation_id
            or payload.get("session_id") != session_id
            or payload.get("issuer_node_id") != authority.node_id
            or payload.get("target_node_id") != node_id
        ):
            raise FederationOperationError(
                "human-auth-assertion-binding-mismatch",
                "the Federation login assertion is not bound to this device and Federation",
            )
        state = payload.get("state")
        if (
            not isinstance(expected_state, str)
            or not isinstance(state, str)
            or not secrets.compare_digest(state, expected_state)
        ):
            raise FederationOperationError(
                "human-auth-state-mismatch",
                "the Federation login response does not match this browser session",
            )
        try:
            issued_at = _parse_stamp(payload.get("issued_at"), "issued_at")
            expires_at = _parse_stamp(payload.get("expires_at"), "expires_at")
        except (TypeError, ValueError) as exc:
            raise FederationOperationError(
                "human-auth-assertion-time-invalid",
                "the Federation login assertion has invalid timestamps",
            ) from exc
        now = self._clock().astimezone(timezone.utc)
        if (
            issued_at > now + timedelta(seconds=ASSERTION_CLOCK_SKEW_SECONDS)
            or expires_at <= now
            or expires_at <= issued_at
            or expires_at - issued_at
            > timedelta(seconds=ASSERTION_TTL_SECONDS + ASSERTION_CLOCK_SKEW_SECONDS)
        ):
            raise FederationOperationError(
                "human-auth-assertion-expired",
                "the Federation login assertion is expired or outside its allowed lifetime",
            )
        try:
            email = _normalize_email(payload.get("subject"))
        except ValueError as exc:
            raise FederationOperationError(
                "human-auth-subject-invalid",
                "the Federation login assertion contains an invalid user identity",
            ) from exc
        role_names = payload.get("roles")
        if (
            payload.get("active") is not True
            or not isinstance(role_names, list)
            or any(not isinstance(name, str) or not name for name in role_names)
            or len(set(role_names)) != len(role_names)
        ):
            raise FederationOperationError(
                "human-auth-authorization-invalid",
                "the Federation login assertion contains invalid authorization metadata",
            )
        known_roles = {
            role.name
            for role in db.session.query(Role).filter(Role.name.in_(role_names)).all()
        }
        if known_roles != set(role_names):
            raise FederationOperationError(
                "human-auth-role-unknown",
                "the Federation login assertion references an unknown local role",
            )
        roles = (
            db.session.query(Role).filter(Role.name.in_(role_names)).all()
            if role_names
            else []
        )
        user = db.session.query(User).filter_by(email=email).one_or_none()
        uniquifier = (
            FEDERATED_UNIQUIFIER_PREFIX
            + hashlib.sha256(f"{session_id}\0{email}".encode("utf-8")).hexdigest()
        )
        if user is None:
            user = User(
                email=email,
                password="!federated-sso-only",
                active=True,
                fs_uniquifier=uniquifier,
                roles=roles,
            )
            db.session.add(user)
        else:
            user.active = True
            user.fs_uniquifier = uniquifier
            user.roles = roles
        db.session.commit()
        return user


def get_federated_human_auth_service() -> FederationHumanAuthService:
    existing = current_app.extensions.get(_SERVICE_EXTENSION_KEY)
    if isinstance(existing, FederationHumanAuthService):
        return existing
    service = FederationHumanAuthService()
    current_app.extensions[_SERVICE_EXTENSION_KEY] = service
    return service


def install_federated_human_auth_service(
    service: FederationHumanAuthService,
) -> None:
    current_app.extensions[_SERVICE_EXTENSION_KEY] = service


def federation_login_context() -> dict[str, Any]:
    service = get_federated_human_auth_service()
    try:
        base_url = configured_base_url()
        mode = service.mode(refresh=True)
        if mode.is_authority:
            authority = service.ensure_authority(base_url)
            try:
                service.publish_all_users()
            except Exception:  # noqa: BLE001 - sign-in stays available if metadata sync fails
                current_app.logger.warning(
                    "Federation human-user metadata publication is temporarily unavailable"
                )
            return {
                "mode": "authority",
                "authority": authority,
                "local_fallback": True,
            }
        if mode.is_member:
            return {
                "mode": "member",
                "authority": mode.authority,
                "local_fallback": local_fallback_enabled(),
            }
    except Exception:  # noqa: BLE001 - login page must remain a recovery surface
        current_app.logger.info(
            "Federation human sign-in discovery is temporarily unavailable"
        )
    if saved_remote_member():
        return {
            "mode": "member-unavailable",
            "authority": None,
            "local_fallback": local_fallback_enabled(),
        }
    return {"mode": "local", "authority": None, "local_fallback": True}


@federated_human_auth.post("/start")
def start():
    service = get_federated_human_auth_service()
    mode = service.mode(refresh=True)
    if not mode.is_member:
        if mode.is_authority:
            return redirect(url_for("security.login"))
        abort(503)
    try:
        service.ensure_member_endpoint(configured_base_url())
        authority = service.authority(refresh=True)
    except Exception as exc:  # noqa: BLE001 - show safe operator-level failure
        current_app.logger.warning(
            "Federation sign-in start failed (%s)", type(exc).__name__
        )
        flash("Federation sign-in is temporarily unavailable.", "error")
        return redirect(url_for("security.login"))
    if authority is None or mode.local_node_id is None:
        flash(
            "The Federation leader has not advertised its human sign-in endpoint yet.",
            "error",
        )
        return redirect(url_for("security.login"))

    state = secrets.token_urlsafe(32)
    session[_SESSION_STATE_KEY] = state
    session[_SESSION_NEXT_KEY] = _safe_next(request.form.get("next"))
    session[_SESSION_ISSUED_KEY] = _stamp(_utc_now())
    query = urlencode(
        {
            "target_node_id": mode.local_node_id,
            "state": state,
        }
    )
    return redirect(f"{authority.base_url}/federation-auth/authorize?{query}")


@federated_human_auth.get("/authorize")
def authorize():
    if not current_user.is_authenticated or not current_user.is_active:
        return redirect(
            url_for(
                "security.login",
                next=request.full_path,
            )
        )
    target_node_id = request.args.get("target_node_id", "")
    state = request.args.get("state", "")
    if (
        not target_node_id
        or not state
        or len(state.encode("utf-8")) > MAX_STATE_BYTES
    ):
        abort(400)

    service = get_federated_human_auth_service()
    mode = service.mode(refresh=True)
    if not mode.is_authority:
        abort(403)
    try:
        service.ensure_authority(configured_base_url())
        member_url = service.member_endpoint(target_node_id, refresh=True)
        if member_url is None:
            raise FederationOperationError(
                "human-auth-target-unavailable",
                "the target Federation member has not registered a web endpoint",
            )
        user = db.session.get(User, current_user.id)
        if user is None or not user.active:
            abort(403)
        service.publish_user(user)
        assertion = service.issue_assertion(
            user=user,
            target_node_id=target_node_id,
            state=state,
        )
    except FederationOperationError as exc:
        current_app.logger.warning("Federation sign-in authorization failed: %s", exc.code)
        abort(403)

    query = urlencode({"assertion": assertion})
    return redirect(f"{member_url}/federation-auth/callback?{query}")


@federated_human_auth.get("/callback")
def callback():
    expected_state = session.get(_SESSION_STATE_KEY)
    issued = session.get(_SESSION_ISSUED_KEY)
    if not isinstance(expected_state, str) or not isinstance(issued, str):
        flash("Federation sign-in request is missing or expired.", "error")
        return redirect(url_for("security.login"))
    try:
        requested_at = _parse_stamp(issued, "requested_at")
    except ValueError:
        requested_at = _utc_now() - timedelta(hours=1)
    if _utc_now() - requested_at > timedelta(minutes=10):
        session.pop(_SESSION_STATE_KEY, None)
        session.pop(_SESSION_ISSUED_KEY, None)
        session.pop(_SESSION_NEXT_KEY, None)
        flash("Federation sign-in request expired. Try again.", "error")
        return redirect(url_for("security.login"))

    encoded = request.args.get("assertion", "")
    service = get_federated_human_auth_service()
    try:
        user = service.consume_assertion(encoded, expected_state=expected_state)
    except FederationOperationError as exc:
        current_app.logger.warning("Federation sign-in callback failed: %s", exc.code)
        flash("Federation sign-in could not be verified.", "error")
        return redirect(url_for("security.login"))

    next_url = _safe_next(session.pop(_SESSION_NEXT_KEY, "/"))
    session.pop(_SESSION_STATE_KEY, None)
    session.pop(_SESSION_ISSUED_KEY, None)
    login_user(user, remember=False, fresh=True)
    return redirect(next_url)


def guard_member_local_password_login():
    """Prevent a member-local password database from bypassing Federation SSO."""

    if request.endpoint != "security.login" or request.method != "POST":
        return None
    service = get_federated_human_auth_service()
    mode = service.mode()
    if (mode.is_member or saved_remote_member()) and not local_fallback_enabled():
        flash(
            "This device uses the Federation leader for human sign-in.",
            "info",
        )
        return redirect(url_for("security.login"))
    return None


def redirect_member_password_change():
    """Keep password changes on the authority that actually stores passwords."""

    if request.endpoint != "security.change_password":
        return None
    service = get_federated_human_auth_service()
    mode = service.mode()
    if not mode.is_member and not saved_remote_member():
        return None
    if request.method != "GET":
        abort(403)
    authority = mode.authority or service.authority(refresh=True)
    if authority is None:
        abort(503)
    return redirect(f"{authority.base_url}{url_for('security.change_password')}")


def sync_federated_human_user():
    """Refresh member-side roles/active state before authorization is evaluated."""

    try:
        get_federated_human_auth_service().sync_current_shadow_user()
    except Exception as exc:  # noqa: BLE001 - federation outage must not corrupt auth state
        current_app.logger.info(
            "Federated human authorization refresh unavailable (%s)",
            type(exc).__name__,
        )
    return None


__all__ = [
    "ASSERTION_SCHEMA",
    "AUTHORITY_EVENT",
    "AUTHORITY_SCHEMA",
    "FEDERATED_UNIQUIFIER_PREFIX",
    "MEMBER_ENDPOINT_EVENT",
    "MEMBER_ENDPOINT_SCHEMA",
    "USER_EVENT",
    "USER_SCHEMA",
    "FederationHumanAuthService",
    "federated_human_auth",
    "federation_login_context",
    "get_federated_human_auth_service",
    "guard_member_local_password_login",
    "redirect_member_password_change",
    "saved_remote_member",
    "sync_federated_human_user",
]
