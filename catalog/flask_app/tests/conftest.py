"""Compatibility settings for route-unit tests that predate human accounts."""

import pytest


@pytest.fixture(autouse=True)
def explicit_legacy_route_test_auth_bypass(monkeypatch, tmp_path):
    """Keep legacy isolated route tests focused on their existing boundary.

    Authentication behavior is exercised without this bypass in test_human_auth.py.
    """
    monkeypatch.setenv("FCP_DEVELOPMENT", "1")
    monkeypatch.setenv("FCP_AUTH_DISABLED", "1")
    monkeypatch.setenv("FCP_AUTH_DATABASE", str(tmp_path / "human-auth.sqlite3"))
