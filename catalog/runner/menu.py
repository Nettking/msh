"""Deprecated interactive menu runner.

This entrypoint is retained only for backward compatibility and now defers to
non-interactive orchestration by default.
"""

from __future__ import annotations

import os
import sys

from catalog.orchestrator.pipeline import run_orchestration


def main() -> int:
    print("[deprecated] catalog/runner/menu.py is deprecated as an operational workflow.", flush=True)
    print("[deprecated] Use automatic orchestration + Flask startup instead.", flush=True)
    if os.getenv("MSH_LEGACY_MENU_ENABLED", "0") == "1":
        print("[deprecated] Legacy menu mode is no longer available in this build.", flush=True)
        return 1

    result = run_orchestration()
    print(
        "[deprecated] Completed orchestration fallback. "
        "Launch Flask with: python -m catalog.flask_app.app",
        flush=True,
    )
    return 1 if result.failed_scripts else 0


if __name__ == "__main__":
    sys.exit(main())
