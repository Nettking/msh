"""Structured errors returned by the federation domain layer."""

from __future__ import annotations


class FederationValidationError(ValueError):
    """A validation failure with a stable machine-readable code and field."""

    def __init__(self, code: str, field: str, message: str) -> None:
        self.code = code
        self.field = field
        self.message = message
        super().__init__(f"{field}: {message} ({code})")

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "field": self.field, "message": self.message}


class ProtocolCompatibilityError(FederationValidationError):
    """An object uses an unsupported protocol schema."""
