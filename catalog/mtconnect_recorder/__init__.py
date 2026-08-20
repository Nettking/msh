"""Loss-aware MTConnect recorder package."""
from __future__ import annotations

import importlib as _importlib
from typing import Any

from .model import *
from .parsing import *
from .storage import *
from . import parsing as _parsing


_original_parse_streams = _parsing.parse_streams


def parse_streams(
    xml_text: str,
    *,
    source_name: str,
    probe: ProbeModel | None,
    received_at: str | None = None,
) -> ParsedBatch:
    """Parse one stream document and return observations in sequence order.

    MTConnect XML groups observations by component and data item, not necessarily
    by global sequence. Durable snapshots must therefore reorder the parsed
    observations before carrying machine state forward. The original XML and
    source record IDs remain unchanged.
    """

    batch = _original_parse_streams(
        xml_text,
        source_name=source_name,
        probe=probe,
        received_at=received_at,
    )
    batch.observations.sort(
        key=lambda record: (
            record.get("sequence") is None,
            int(record.get("sequence") or 0),
            str(record.get("source_record_id") or ""),
        )
    )
    return batch


# Patch parsing immediately, but do not import the runtime here.  The standalone
# launcher configures FCP_RECORDER_* after importing Federation support modules;
# importing runtime from the package initializer would freeze those settings
# before the launcher has supplied its managed control/configuration paths.
_parsing.parse_streams = parse_streams


def _runtime_module():
    runtime_module = _importlib.import_module(f"{__name__}.runtime")
    # Keep direct runtime imports and the package facade on the same canonical
    # chronological parser even when the runtime was already present in
    # sys.modules for an embedding process.
    runtime_module.parse_streams = parse_streams
    return runtime_module


def run() -> None:
    """Run the recorder after runtime configuration has been established."""

    _runtime_module().run()


def __getattr__(name: str) -> Any:
    if name == "runtime":
        return _runtime_module()
    if name in {"RecorderRuntime", "MtconnectClient"}:
        return getattr(_runtime_module(), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# Preserve the package's historical star-import surface used by the standalone
# compatibility entry point while keeping runtime itself lazy.
__all__ = [name for name in globals() if not name.startswith("_")]
__all__.extend(["runtime", "RecorderRuntime", "MtconnectClient"])
