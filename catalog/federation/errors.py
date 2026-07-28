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


class FederationOperationError(RuntimeError):
    """A rejected federation operation with a stable machine-readable reason."""

    def __init__(self, code: str, message: str, field: str | None = None) -> None:
        self.code = code
        self.field = field
        self.message = message
        super().__init__(f"{field + ': ' if field else ''}{message} ({code})")

    def to_dict(self) -> dict[str, str]:
        value = {"code": self.code, "message": self.message}
        if self.field is not None:
            value["field"] = self.field
        return value


class AuthenticationError(FederationOperationError):
    """The remote node did not prove an enrolled, active identity."""


class AuthorizationError(FederationOperationError):
    """An authenticated actor is not authorized for the requested scope."""


class RevisionGapError(FederationOperationError):
    """An ordered session event cannot be applied because a revision is missing."""
