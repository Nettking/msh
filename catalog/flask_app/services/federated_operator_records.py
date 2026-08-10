"""Federation-backed compatibility wrappers for operator-support records."""

from __future__ import annotations

from typing import Any

from .federation_knowledge_service import get_federation_knowledge_repository
from .first_part_service import FirstPartService
from .machine_notes_service import MachineNotesService
from .operator_confirmation_service import OperatorConfirmationService
from .quality_outcome_service import QualityOutcomeService


class _FederatedCollectionMixin:
    COLLECTION: str
    DOCUMENT_SCHEMA: str
    ITEMS_KEY: str

    @property
    def storage_scope(self) -> str:
        repository = get_federation_knowledge_repository()
        return "Federation shared" if repository.is_shared() else "Local cache"

    def _load(self) -> dict[str, Any]:
        local_payload = super()._load()  # type: ignore[misc]
        payload = get_federation_knowledge_repository().load_payload(
            collection=self.COLLECTION,
            document_schema=self.DOCUMENT_SCHEMA,
            items_key=self.ITEMS_KEY,
            local_payload=local_payload,
        )
        if payload != local_payload:
            super()._write(payload)  # type: ignore[misc]
        return payload

    def _write(self, payload: dict[str, Any]) -> None:
        authoritative = get_federation_knowledge_repository().write_payload(
            collection=self.COLLECTION,
            document_schema=self.DOCUMENT_SCHEMA,
            items_key=self.ITEMS_KEY,
            desired_payload=payload,
        )
        super()._write(authoritative)  # type: ignore[misc]


class FederatedOperatorConfirmationService(
    _FederatedCollectionMixin,
    OperatorConfirmationService,
):
    COLLECTION = "operator-confirmations"
    DOCUMENT_SCHEMA = "fcp.operator_confirmations.v1"
    ITEMS_KEY = "confirmations"


class FederatedFirstPartService(_FederatedCollectionMixin, FirstPartService):
    COLLECTION = "first-part-checks"
    DOCUMENT_SCHEMA = "fcp.first_part_checks.v1"
    ITEMS_KEY = "checks"


class FederatedQualityOutcomeService(
    _FederatedCollectionMixin,
    QualityOutcomeService,
):
    COLLECTION = "quality-outcomes"
    DOCUMENT_SCHEMA = "fcp.quality_outcomes.v1"
    ITEMS_KEY = "outcomes"


class FederatedMachineNotesService(_FederatedCollectionMixin, MachineNotesService):
    COLLECTION = "machine-notes"
    DOCUMENT_SCHEMA = "fcp.machine_notes.v1"
    ITEMS_KEY = "notes"


__all__ = [
    "FederatedFirstPartService",
    "FederatedMachineNotesService",
    "FederatedOperatorConfirmationService",
    "FederatedQualityOutcomeService",
]
