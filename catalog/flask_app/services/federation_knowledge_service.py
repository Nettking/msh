"""Federation-scoped knowledge documents over the authenticated session log.

Small operator and production knowledge records belong to the Federation, not to
one Flask checkout.  The authoritative ordering is the existing session event
log.  Legacy JSON files remain local compatibility caches and migration inputs;
they are never used to grant Federation or storage authority.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from flask import current_app, has_app_context

from catalog.federation.errors import FederationOperationError
from catalog.federation.models import SessionEvent

from .capability_onboarding_service import get_capability_onboarding_service

UPSERT_EVENT = "knowledge.document.upserted"
DELETE_EVENT = "knowledge.document.deleted"
CHANGE_SCHEMA = "fcp.federation.knowledge-change.v1"
MAX_DOCUMENT_BYTES = 32_000


class FederationKnowledgeError(FederationOperationError):
    """A shared knowledge operation could not be completed safely."""


@dataclass(frozen=True)
class KnowledgeContext:
    session_id: str
    actor_node_id: str
    coordinator: object


@dataclass(frozen=True)
class ReducedCollection:
    documents: dict[str, dict[str, Any]]
    seen_document_ids: frozenset[str]
    latest_revision: int
    latest_at: datetime | None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FederationKnowledgeError(
            "invalid-knowledge-document",
            "knowledge document must contain JSON-compatible values",
            "document",
        ) from exc


def _document_id(document: object) -> str | None:
    if not isinstance(document, dict):
        return None
    value = document.get("id")
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value or len(value) > 512 or any(ord(char) < 32 for char in value):
        return None
    return value


def _documents(payload: object, items_key: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    value = payload.get(items_key)
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if _document_id(item) is not None]


def _document_time(document: dict[str, Any]) -> datetime | None:
    for field in (
        "updated_at",
        "structured_at",
        "recorded_at",
        "signed_at",
        "confirmed_at",
        "created_at",
        "captured_at",
        "decision_time",
    ):
        value = document.get(field)
        if not isinstance(value, str) or not value.strip():
            continue
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is not None and parsed.utcoffset() is not None:
            return parsed.astimezone(timezone.utc)
    return None


def _content_hash(document: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical(document)).hexdigest()


def _request_id(
    *,
    collection: str,
    document_id: str,
    operation: str,
    content_hash: str,
) -> str:
    identity = hashlib.sha256(
        f"{collection}\0{document_id}".encode("utf-8")
    ).hexdigest()[:16]
    return f"knowledge-{operation}-{identity}-{content_hash[:20]}"


class FederationKnowledgeRepository:
    """Project and mutate shared documents through one Federation session."""

    def __init__(
        self,
        *,
        context_provider: Callable[[], KnowledgeContext | None] | None = None,
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._context_provider = context_provider or self._default_context
        self._now = now

    @staticmethod
    def _default_context() -> KnowledgeContext | None:
        if not has_app_context():
            return None
        try:
            context = get_capability_onboarding_service().authorized_context()
        except Exception:  # noqa: BLE001 - disconnected devices retain local cache
            return None
        if context is None:
            return None
        return KnowledgeContext(
            session_id=context.binding.internal_session_id,
            actor_node_id=context.credentials.identity.node_id,
            coordinator=context.coordinator,
        )

    def is_shared(self) -> bool:
        return self._context_provider() is not None

    @staticmethod
    def _events(context: KnowledgeContext) -> tuple[SessionEvent, ...]:
        coordinator = context.coordinator
        runtime = getattr(coordinator, "runtime", None)
        remote_state = getattr(coordinator, "state", None)
        if runtime is not None and remote_state is not None:
            reader = getattr(runtime, "session_events", None)
            if callable(reader):
                return tuple(
                    reader(
                        remote_state,
                        session_id=context.session_id,
                        after_revision=0,
                    )
                )
        replay = getattr(coordinator, "replay", None)
        if not callable(replay):
            raise FederationKnowledgeError(
                "federation-knowledge-read-unavailable",
                "the connected Federation does not expose its durable event log",
            )
        return tuple(
            replay(
                session_id=context.session_id,
                actor_node_id=context.actor_node_id,
                last_applied_revision=0,
            )
        )

    @staticmethod
    def _append(
        context: KnowledgeContext,
        *,
        event_type: str,
        payload: dict[str, Any],
        request_id: str,
    ) -> SessionEvent:
        coordinator = context.coordinator
        runtime = getattr(coordinator, "runtime", None)
        remote_state = getattr(coordinator, "state", None)
        if runtime is not None and remote_state is not None:
            writer = getattr(runtime, "append_session_event", None)
            if callable(writer):
                return writer(
                    remote_state,
                    session_id=context.session_id,
                    event_type=event_type,
                    payload=payload,
                    request_id=request_id,
                )
        append = getattr(coordinator, "append_event", None)
        if not callable(append):
            raise FederationKnowledgeError(
                "federation-knowledge-write-unavailable",
                "the connected Federation does not accept shared knowledge changes",
            )
        event, _created = append(
            session_id=context.session_id,
            actor_node_id=context.actor_node_id,
            request_id=request_id,
            event_type=event_type,
            payload=payload,
        )
        return event

    @staticmethod
    def _reduce(
        events: Iterable[SessionEvent],
        *,
        collection: str,
    ) -> ReducedCollection:
        documents: dict[str, dict[str, Any]] = {}
        seen: set[str] = set()
        latest_revision = 0
        latest_at: datetime | None = None
        for event in sorted(events, key=lambda item: item.revision):
            if event.event_type not in {UPSERT_EVENT, DELETE_EVENT}:
                continue
            payload = event.payload
            if (
                not isinstance(payload, dict)
                or payload.get("schema") != CHANGE_SCHEMA
                or payload.get("collection") != collection
            ):
                continue
            document_id = payload.get("document_id")
            if not isinstance(document_id, str) or not document_id:
                continue
            seen.add(document_id)
            latest_revision = max(latest_revision, event.revision)
            latest_at = event.occurred_at
            if event.event_type == DELETE_EVENT:
                documents.pop(document_id, None)
                continue
            document = payload.get("document")
            if not isinstance(document, dict):
                continue
            if _document_id(document) != document_id:
                continue
            documents[document_id] = dict(document)
        return ReducedCollection(
            documents=documents,
            seen_document_ids=frozenset(seen),
            latest_revision=latest_revision,
            latest_at=latest_at,
        )

    @staticmethod
    def _payload(
        *,
        schema: str,
        items_key: str,
        reduced: ReducedCollection,
    ) -> dict[str, Any]:
        documents = sorted(
            reduced.documents.values(),
            key=lambda item: str(item.get("id") or ""),
        )
        return {
            "schema": schema,
            "updated_at": _stamp(reduced.latest_at),
            items_key: documents,
        }

    def _upsert(
        self,
        context: KnowledgeContext,
        *,
        collection: str,
        document_schema: str,
        document: dict[str, Any],
    ) -> SessionEvent:
        document_id = _document_id(document)
        if document_id is None:
            raise FederationKnowledgeError(
                "invalid-knowledge-document-id",
                "shared knowledge documents require a bounded id",
                "id",
            )
        encoded = _canonical(document)
        if len(encoded) > MAX_DOCUMENT_BYTES:
            raise FederationKnowledgeError(
                "knowledge-document-too-large",
                f"shared knowledge documents must not exceed {MAX_DOCUMENT_BYTES} bytes",
                "document",
            )
        content_hash = hashlib.sha256(encoded).hexdigest()
        payload = {
            "schema": CHANGE_SCHEMA,
            "collection": collection,
            "document_schema": document_schema,
            "document_id": document_id,
            "document_hash": f"sha256:{content_hash}",
            "document": document,
            "changed_at": _stamp(self._now()),
        }
        return self._append(
            context,
            event_type=UPSERT_EVENT,
            payload=payload,
            request_id=_request_id(
                collection=collection,
                document_id=document_id,
                operation="upsert",
                content_hash=content_hash,
            ),
        )

    def load_payload(
        self,
        *,
        collection: str,
        document_schema: str,
        items_key: str,
        local_payload: dict[str, Any],
    ) -> dict[str, Any]:
        context = self._context_provider()
        if context is None:
            return local_payload
        reduced = self._reduce(self._events(context), collection=collection)
        changed = False
        for local_document in _documents(local_payload, items_key):
            document_id = _document_id(local_document)
            assert document_id is not None
            shared_document = reduced.documents.get(document_id)
            should_import = document_id not in reduced.seen_document_ids
            if shared_document is not None:
                local_time = _document_time(local_document)
                shared_time = _document_time(shared_document)
                should_import = (
                    local_time is not None
                    and shared_time is not None
                    and local_time > shared_time
                    and _content_hash(local_document) != _content_hash(shared_document)
                )
            if not should_import:
                continue
            event = self._upsert(
                context,
                collection=collection,
                document_schema=document_schema,
                document=local_document,
            )
            reduced.documents[document_id] = dict(local_document)
            reduced = ReducedCollection(
                documents=reduced.documents,
                seen_document_ids=frozenset(
                    set(reduced.seen_document_ids) | {document_id}
                ),
                latest_revision=max(reduced.latest_revision, event.revision),
                latest_at=event.occurred_at,
            )
            changed = True
        if changed:
            # Re-read to include concurrent changes accepted while migration ran.
            reduced = self._reduce(self._events(context), collection=collection)
        return self._payload(
            schema=document_schema,
            items_key=items_key,
            reduced=reduced,
        )

    def write_payload(
        self,
        *,
        collection: str,
        document_schema: str,
        items_key: str,
        desired_payload: dict[str, Any],
    ) -> dict[str, Any]:
        context = self._context_provider()
        if context is None:
            return desired_payload
        reduced = self._reduce(self._events(context), collection=collection)
        for document in _documents(desired_payload, items_key):
            document_id = _document_id(document)
            assert document_id is not None
            current = reduced.documents.get(document_id)
            if current is not None and _content_hash(current) == _content_hash(document):
                continue
            event = self._upsert(
                context,
                collection=collection,
                document_schema=document_schema,
                document=document,
            )
            reduced.documents[document_id] = dict(document)
            reduced = ReducedCollection(
                documents=reduced.documents,
                seen_document_ids=frozenset(
                    set(reduced.seen_document_ids) | {document_id}
                ),
                latest_revision=max(reduced.latest_revision, event.revision),
                latest_at=event.occurred_at,
            )
        # Include concurrent documents instead of treating a stale page submit as
        # an instruction to replace the whole Federation collection.
        reduced = self._reduce(self._events(context), collection=collection)
        return self._payload(
            schema=document_schema,
            items_key=items_key,
            reduced=reduced,
        )

    def delete_document(
        self,
        *,
        collection: str,
        document_id: str,
    ) -> bool:
        context = self._context_provider()
        if context is None:
            return False
        reduced = self._reduce(self._events(context), collection=collection)
        if document_id not in reduced.documents:
            return False
        prior_hash = _content_hash(reduced.documents[document_id])
        payload = {
            "schema": CHANGE_SCHEMA,
            "collection": collection,
            "document_id": document_id,
            "previous_hash": f"sha256:{prior_hash}",
            "changed_at": _stamp(self._now()),
        }
        self._append(
            context,
            event_type=DELETE_EVENT,
            payload=payload,
            request_id=_request_id(
                collection=collection,
                document_id=document_id,
                operation="delete",
                content_hash=prior_hash,
            ),
        )
        return True


def get_federation_knowledge_repository() -> FederationKnowledgeRepository:
    if has_app_context():
        configured = current_app.config.get("FEDERATION_KNOWLEDGE_REPOSITORY")
        if isinstance(configured, FederationKnowledgeRepository):
            return configured
        repository = current_app.extensions.get("federation_knowledge_repository")
        if isinstance(repository, FederationKnowledgeRepository):
            return repository
        repository = FederationKnowledgeRepository()
        current_app.extensions["federation_knowledge_repository"] = repository
        return repository
    return FederationKnowledgeRepository()


__all__ = [
    "CHANGE_SCHEMA",
    "DELETE_EVENT",
    "FederationKnowledgeError",
    "FederationKnowledgeRepository",
    "KnowledgeContext",
    "UPSERT_EVENT",
    "get_federation_knowledge_repository",
]
