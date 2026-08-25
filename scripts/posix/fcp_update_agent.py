#!/usr/bin/env python3
"""Public POSIX FCP update-agent entrypoint.

The request engine is preserved in ``fcp_update_engine.py``. Keeping this path as
a tiny exec shim lets an agent from the previous release reload directly into
the serialized B04 runner after it finishes the update that installs this code.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def main() -> int:
    runner = Path(__file__).resolve().with_name("fcp_update_agent_runner.py")
    if not runner.is_file():
        print("FCP serialized update runner is unavailable.", file=sys.stderr)
        return 1
    os.execv(sys.executable, [sys.executable, str(runner), *sys.argv[1:]])
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
