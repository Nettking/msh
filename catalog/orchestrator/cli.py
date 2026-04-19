from __future__ import annotations

from catalog.orchestrator.pipeline import run_orchestration


def main() -> int:
    result = run_orchestration()
    return 1 if result.failed_scripts else 0


if __name__ == "__main__":
    raise SystemExit(main())
