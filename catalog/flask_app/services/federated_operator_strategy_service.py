"""Federation-backed compatibility service for operator strategy knowledge."""

from __future__ import annotations

from typing import Any

from .federation_knowledge_service import get_federation_knowledge_repository
from .operator_strategy_service import (
    SCHEMA_VERSION,
    OperatorStrategyService,
    _normalize_record,
)

COLLECTION = "operator-strategies"
ITEMS_KEY = "records"


class FederatedOperatorStrategyService(OperatorStrategyService):
    """Retain the legacy API while making the Federation authoritative."""

    @property
    def storage_scope(self) -> str:
        repository = get_federation_knowledge_repository()
        return "Federation shared" if repository.is_shared() else "Local cache"

    def load_records(self) -> list[dict[str, Any]]:
        local_records = super().load_records()
        payload = get_federation_knowledge_repository().load_payload(
            collection=COLLECTION,
            document_schema=SCHEMA_VERSION,
            items_key=ITEMS_KEY,
            local_payload={
                "schema": SCHEMA_VERSION,
                "updated_at": "",
                ITEMS_KEY: local_records,
            },
        )
        records = [
            _normalize_record(item)
            for item in payload.get(ITEMS_KEY, [])
            if isinstance(item, dict)
        ]
        if records != local_records:
            super()._write_records(records)
        return records

    def _write_records(self, records: list[dict[str, Any]]) -> None:
        desired = [_normalize_record(record) for record in records]
        payload = get_federation_knowledge_repository().write_payload(
            collection=COLLECTION,
            document_schema=SCHEMA_VERSION,
            items_key=ITEMS_KEY,
            desired_payload={
                "schema": SCHEMA_VERSION,
                "updated_at": "",
                ITEMS_KEY: desired,
            },
        )
        authoritative = [
            _normalize_record(item)
            for item in payload.get(ITEMS_KEY, [])
            if isinstance(item, dict)
        ]
        super()._write_records(authoritative)

    def delete(self, record_id: str) -> bool:
        record_id = record_id.strip()
        records = self.load_records()
        kept = [
            record
            for record in records
            if str(record.get("id") or "") != record_id
        ]
        if len(kept) == len(records):
            return False
        get_federation_knowledge_repository().delete_document(
            collection=COLLECTION,
            document_id=record_id,
        )
        # A recorded Federation tombstone prevents the local compatibility
        # cache from re-importing the deleted document on the next read.
        super()._write_records(kept)
        return True


__all__ = ["FederatedOperatorStrategyService"]
