"""Backend-neutral, synchronous local provider contracts."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol

from .models import CapabilityAnnouncement, StorageBatch


class ProviderErrorCode(str, Enum):
    NOT_FOUND = "not-found"
    CONFLICT = "conflict"
    INVALID = "invalid"
    CORRUPT = "corrupt"
    IO_ERROR = "io-error"
    RECONCILIATION_REQUIRED = "reconciliation-required"


@dataclass(frozen=True)
class ProviderError:
    code: ProviderErrorCode
    message: str
    field: str | None = None


@dataclass(frozen=True)
class StorageWriteOutcome:
    batch: StorageBatch
    created: bool
    durable: bool
    error: ProviderError | None = None


@dataclass(frozen=True)
class StorageReadOutcome:
    batch: StorageBatch | None
    payload: Any | None
    error: ProviderError | None = None


class StorageProvider(Protocol):
    """A synchronous contract which exposes domain identities, not backend locators."""

    def write(self, batch: StorageBatch, payload: Any) -> StorageWriteOutcome: ...

    def retrieve(self, session_id: str, batch_id: str) -> StorageReadOutcome: ...


class CapabilityProvider(Protocol):
    def register(self, capability: CapabilityAnnouncement) -> CapabilityAnnouncement: ...
    def update(self, capability: CapabilityAnnouncement) -> CapabilityAnnouncement: ...
    def get(self, session_id: str, capability_id: str) -> CapabilityAnnouncement | None: ...
    def list(self, *, session_id: str | None = None, capability_type: str | None = None,
             node_id: str | None = None) -> tuple[CapabilityAnnouncement, ...]: ...
    def remove(self, session_id: str, capability_id: str) -> CapabilityAnnouncement | None: ...
