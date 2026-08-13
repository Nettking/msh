"""Isolate the process-wide analysis runtime bindings between tests."""

from __future__ import annotations

import pytest

from catalog.orchestrator import analysis_runtime


@pytest.fixture(autouse=True)
def isolated_analysis_runtime(monkeypatch):
    """Reset the product suppliers and cached runtime for each test.

    The suppliers and runtime are deliberately process-wide in production
    (one device, one federation binding). Tests must not inherit a binding
    registered by another module's Flask application factory.
    """

    monkeypatch.setattr(analysis_runtime, "_IDENTITY_SUPPLIER", None)
    monkeypatch.setattr(analysis_runtime, "_FEDERATION_SUPPLIER", None)
    monkeypatch.setattr(analysis_runtime, "_FEDERATION_GENERATION_SUPPLIER", None)
    monkeypatch.setattr(analysis_runtime, "_RUNTIME", None)
    yield
    analysis_runtime.reset_analysis_runtime()
