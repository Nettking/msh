"""Errors raised by the local capability benchmark framework."""

from __future__ import annotations


class BenchmarkingError(RuntimeError):
    """Base class for benchmark registry, execution, and persistence errors."""


class UnknownBenchmarkError(BenchmarkingError):
    """Raised when a benchmark ID is not registered."""


class DuplicateBenchmarkError(BenchmarkingError):
    """Raised when a benchmark ID is registered more than once."""


class DuplicateRunIdError(BenchmarkingError):
    """Raised when a run ID is already bound to another or active request."""


class MissingDependencyInputError(BenchmarkingError):
    """Raised when a required invalidation input is absent."""


class InvalidBenchmarkOutputError(BenchmarkingError):
    """Raised when a benchmark probe returns undeclared or unsafe output."""
