from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import pytest

from catalog.federation import image_retirement


class _CompletedListingProcess:
    def __init__(self, args: list[str], **kwargs: Any) -> None:
        completed = image_retirement.subprocess.run(
            args,
            cwd=kwargs.get("cwd"),
            env=kwargs.get("env"),
            shell=False,
            check=False,
            capture_output=True,
            text=True,
            timeout=1.0,
        )
        self.stdout = io.StringIO(completed.stdout)
        self.returncode = completed.returncode

    def poll(self) -> int:
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return self.returncode

    def terminate(self) -> None:
        return None

    def kill(self) -> None:
        return None


@pytest.fixture(autouse=True)
def _legacy_image_retirement_fake_adapter(request: pytest.FixtureRequest, monkeypatch):
    """Keep the pre-streaming fake daemon tests focused on their original claims."""

    path = Path(str(request.node.path))
    if path.name != "test_image_retirement.py":
        return
    monkeypatch.setattr(image_retirement, "_listing_popen", _CompletedListingProcess)
