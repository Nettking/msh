"""Load host-only Federation helpers without importing ``catalog.federation``.

``catalog.federation.__init__`` intentionally exposes server-side storage
providers, including optional PostgreSQL support.  Tailscale discovery/joining
must remain standard-library-only on the host.  This tiny loader gives the
host-only modules a private package namespace so their relative sibling imports
work without executing the server package initializer.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]
FEDERATION_DIRECTORY = ROOT / "catalog" / "federation"
_HOST_PACKAGE = "_fcp_host_federation"
_ALLOWED_MODULES = frozenset(
    {
        "tailnet_join_bridge",
        "tailnet_join_client",
        "tailnet_join_responder",
        "tailscale_host_discovery",
        "tailscale_peer_identity",
    }
)


def _package() -> ModuleType:
    existing = sys.modules.get(_HOST_PACKAGE)
    if isinstance(existing, ModuleType):
        return existing
    package = types.ModuleType(_HOST_PACKAGE)
    package.__path__ = [str(FEDERATION_DIRECTORY)]  # type: ignore[attr-defined]
    package.__package__ = _HOST_PACKAGE
    sys.modules[_HOST_PACKAGE] = package
    return package


def load_host_module(name: str) -> ModuleType:
    """Load one reviewed host-only module in the private package namespace."""

    if name not in _ALLOWED_MODULES:
        raise ValueError(f"unsupported host Federation module: {name}")
    _package()
    qualified = f"{_HOST_PACKAGE}.{name}"
    existing = sys.modules.get(qualified)
    if isinstance(existing, ModuleType):
        return existing
    path = FEDERATION_DIRECTORY / f"{name}.py"
    spec = importlib.util.spec_from_file_location(qualified, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load host Federation module: {name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[qualified] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(qualified, None)
        raise
    return module


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments:
        print("usage: federation_host_runner.py MODULE [ARGS...]", file=sys.stderr)
        return 2
    name = arguments.pop(0)
    try:
        module = load_host_module(name)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"FCP host helper unavailable: {exc}", file=sys.stderr)
        return 2
    entrypoint = getattr(module, "main", None)
    if not callable(entrypoint):
        print(f"FCP host helper has no CLI entrypoint: {name}", file=sys.stderr)
        return 2
    return int(entrypoint(arguments))


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["load_host_module", "main"]
