"""Local device inspection and bounded capability benchmark framework."""

from .errors import (
    BenchmarkingError,
    DuplicateBenchmarkError,
    DuplicateRunIdError,
    InvalidBenchmarkOutputError,
    MissingDependencyInputError,
    UnknownBenchmarkError,
)
from .fingerprints import dependency_fingerprint, request_fingerprint
from .inspection import (
    DeviceInspector,
    InspectionContext,
    InspectionFinding,
    InspectionProbe,
)
from .registry import (
    BenchmarkProbe,
    BenchmarkRegistry,
    RegisteredBenchmark,
)
from .runner import (
    BenchmarkCancelled,
    BenchmarkExecutionContext,
    BenchmarkObservation,
    BenchmarkRunner,
    BenchmarkRunRequest,
    CancellationToken,
)
from .safety import (
    bounded_public_mapping,
    safe_diagnostics,
    safe_text,
    sanitize_public_value,
)
from .store import SQLiteBenchmarkResultStore
from .validity import (
    BenchmarkValidity,
    BenchmarkValidityReason,
    evaluate_result,
)

__all__ = [
    "BenchmarkCancelled",
    "BenchmarkExecutionContext",
    "BenchmarkObservation",
    "BenchmarkProbe",
    "BenchmarkRegistry",
    "BenchmarkRunRequest",
    "BenchmarkRunner",
    "BenchmarkValidity",
    "BenchmarkValidityReason",
    "BenchmarkingError",
    "CancellationToken",
    "DeviceInspector",
    "DuplicateBenchmarkError",
    "DuplicateRunIdError",
    "InspectionContext",
    "InspectionFinding",
    "InspectionProbe",
    "InvalidBenchmarkOutputError",
    "MissingDependencyInputError",
    "RegisteredBenchmark",
    "SQLiteBenchmarkResultStore",
    "UnknownBenchmarkError",
    "bounded_public_mapping",
    "dependency_fingerprint",
    "evaluate_result",
    "request_fingerprint",
    "safe_diagnostics",
    "safe_text",
    "sanitize_public_value",
]
