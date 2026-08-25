from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolate_cached_recorder_storage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep standalone recorder tests isolated if runtime was imported earlier.

    The compatibility tests intentionally set FCP_RECORDER_DATA_DIR before loading
    the standalone entry point.  In a shuffled full suite another test module may
    already have imported catalog.mtconnect_recorder.runtime, whose DATA_DIR is an
    import-time setting.  Bind that cached runtime to this test's temporary data
    directory as well so recovery-frontier state cannot leak across tests.
    """

    from catalog.mtconnect_recorder import runtime as recorder_runtime

    monkeypatch.setattr(recorder_runtime, "DATA_DIR", tmp_path / "data")
