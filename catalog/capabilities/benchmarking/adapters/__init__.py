"""Concrete, authority-free inspection and benchmark adapters."""

from .compute import (
    COMPUTE_BENCHMARK_ID,
    COMPUTE_PREREQUISITE,
    RegisteredComputeHandlerAdapter,
)
from .mtconnect import (
    MTCONNECT_BENCHMARK_ID,
    MTCONNECT_PREREQUISITE,
    MtconnectSourceAdapter,
)
from .network import (
    NETWORK_BENCHMARK_ID,
    NETWORK_PREREQUISITE,
    AuthenticatedNetworkPathAdapter,
    AuthenticatedPathSample,
    AuthenticatedPathTarget,
)
from .ollama import (
    OLLAMA_BENCHMARK_ID,
    OLLAMA_PREREQUISITE,
    OllamaBenchmarkAdapter,
    OllamaProbeError,
    OllamaProbeTarget,
)
from .registration import ConcreteAdapterRegistration, register_concrete_adapters
from .storage import (
    STORAGE_BENCHMARK_ID,
    STORAGE_PREREQUISITE,
    StorageCandidateAdapter,
    StorageCandidateTarget,
)

__all__ = [
    "COMPUTE_BENCHMARK_ID",
    "COMPUTE_PREREQUISITE",
    "MTCONNECT_BENCHMARK_ID",
    "MTCONNECT_PREREQUISITE",
    "NETWORK_BENCHMARK_ID",
    "NETWORK_PREREQUISITE",
    "OLLAMA_BENCHMARK_ID",
    "OLLAMA_PREREQUISITE",
    "STORAGE_BENCHMARK_ID",
    "STORAGE_PREREQUISITE",
    "AuthenticatedNetworkPathAdapter",
    "AuthenticatedPathSample",
    "AuthenticatedPathTarget",
    "ConcreteAdapterRegistration",
    "MtconnectSourceAdapter",
    "OllamaBenchmarkAdapter",
    "OllamaProbeError",
    "OllamaProbeTarget",
    "RegisteredComputeHandlerAdapter",
    "StorageCandidateAdapter",
    "StorageCandidateTarget",
    "register_concrete_adapters",
]
