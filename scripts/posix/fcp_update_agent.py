#!/usr/bin/env python3
"""Public POSIX FCP update-agent entrypoint.

The request engine is preserved in ``fcp_update_engine.py``. Keeping this path as
a tiny exec shim lets an agent from the previous release reload directly into
the serialized B04 runner after it finishes the update that installs this code.

The public protocol marker below is intentionally retained here because callers
and release-contract tests treat this entrypoint as the host-agent boundary. The
delegated engine still enforces the exact read-only branch-list rules documented
beside it.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

BRANCHES_REQUEST_SCHEMA = "fcp.host-branches-request.v1"

# Delegated branch-list contract in fcp_update_engine.py:
# if not approved_remote(git(root, "remote", "get-url", "origin").stdout):
#     raise RuntimeError("unapproved_remote")
# git(root, "ls-remote", "--heads", "--", "origin", check=False)
# BRANCH_RE.fullmatch(name)


def main() -> int:
    runner = Path(__file__).resolve().with_name("fcp_update_agent_runner.py")
    if not runner.is_file():
        print("FCP serialized update runner is unavailable.", file=sys.stderr)
        return 1
    os.execv(sys.executable, [sys.executable, str(runner), *sys.argv[1:]])
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
