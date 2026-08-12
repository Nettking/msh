"""Repository-wide test defaults for the new human-auth boundary.

Existing tests exercise Federation, capability, recorder, storage, and UI contracts
that predate human accounts. Keep those tests focused by opting them into the
explicit development-only auth bypass. Human-auth tests remove these environment
variables themselves and exercise the real authenticated application boundary.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def explicit_legacy_test_auth_bypass(monkeypatch, tmp_path):
    monkeypatch.setenv("FCP_DEVELOPMENT", "1")
    monkeypatch.setenv("FCP_AUTH_DISABLED", "1")
    monkeypatch.setenv("FCP_AUTH_DATABASE", str(tmp_path / "human-auth.sqlite3"))
