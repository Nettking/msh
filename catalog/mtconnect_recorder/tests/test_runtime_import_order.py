from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_start_recorder_does_not_freeze_runtime_environment_before_main() -> None:
    code = r'''
import os
import sys
from pathlib import Path

os.environ.pop("FCP_RECORDER_MANAGED", None)
os.environ.pop("FCP_RECORDER_DATA_DIR", None)

import start_recorder

assert "catalog.mtconnect_recorder.runtime" not in sys.modules

os.environ["FCP_RECORDER_MANAGED"] = "true"
os.environ["FCP_RECORDER_DATA_DIR"] = "configured-recorder-data"

from catalog.mtconnect_recorder import runtime

assert runtime.MANAGED_MODE is True
assert runtime.DATA_DIR == Path("configured-recorder-data")
'''
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
