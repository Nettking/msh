"""Compatibility entry point for the active MSH MTConnect recorder."""
from pathlib import Path
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from catalog.mtconnect_recorder import *  # noqa: E402,F401,F403
from catalog.mtconnect_recorder.config_bootstrap import (  # noqa: E402
    ensure_recorder_capability_config,
)


if __name__ == "__main__":
    ensure_recorder_capability_config()
    run()
