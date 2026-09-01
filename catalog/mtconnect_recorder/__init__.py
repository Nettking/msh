"""Loss-aware MTConnect recorder package."""
from __future__ import annotations

import importlib as _importlib
from typing import Any

from . import parsing as _parsing
from . import storage as _storage
from .bounded_storage import BoundedDurableRecorderStore
from .model import *
from .parsing import *
from .publication_frontier_runtime import install_publication_frontier_runtime
from .resource_pressure import install_runtime_resource_pressure
from .storage import *
from .xml_budget import validate_xml_budget

# Install the finite derived writer at the package boundary before runtime or any
# normal package consumer resolves DurableRecorderStore. The original storage
# module keeps historical scanning/recovery behavior; only the two derived batch
# writers are overridden by the bounded subclass.
_storage.DurableRecorderStore = BoundedDurableRecorderStore
DurableRecorderStore = BoundedDurableRecorderStore

_original_parse_stream_header = _parsing.parse_stream_header
_original_parse_probe = _parsing.parse_probe
_original_parse_streams = _parsing.parse_streams


def parse_stream_header(xml_text: str) -> StreamHeader:
    """Parse a stream header only after the XML shape fits the finite budget."""

    validate_xml_budget(xml_text)
    return _original_parse_stream_header(xml_text)


def parse_probe(xml_text: str) -> ProbeModel:
    """Parse a probe only after the XML shape fits the finite budget."""

    validate_xml_budget(xml_text)
    return _original_parse_probe(xml_text)


def parse_streams(
    xml_text: str,
    *,
    source_name: str,
    probe: ProbeModel | None,
    received_at: str | None = None,
    max_observations: int = MAX_OBSERVATIONS_PER_BATCH,
    max_sequence_span: int = MAX_SEQUENCE_SPAN,
) -> ParsedBatch:
    """Parse one stream document and return observations in sequence order.

    ``_original_parse_streams`` resolves ``parse_stream_header`` from its module
    at call time. The package installs the budgeted header parser below, so this
    path validates the complete XML shape before ElementTree retains the stream
    tree. MTConnect groups observations by component/data item rather than by
    global sequence; the parsed observations are then put into canonical order
    before durable state is carried forward.
    """

    batch = _original_parse_streams(
        xml_text,
        source_name=source_name,
        probe=probe,
        received_at=received_at,
        max_observations=max_observations,
        max_sequence_span=max_sequence_span,
    )
    batch.observations.sort(
        key=lambda record: (
            record.get("sequence") is None,
            int(record.get("sequence") or 0),
            str(record.get("source_record_id") or ""),
        )
    )
    return batch


# Patch parser entry points immediately, but do not import runtime here. The
# standalone launcher configures FCP_RECORDER_* after importing Federation
# support modules; importing runtime from the package initializer would freeze
# those settings before the launcher has supplied its managed paths.
_parsing.parse_stream_header = parse_stream_header
_parsing.parse_probe = parse_probe
_parsing.parse_streams = parse_streams


def _runtime_module():
    runtime_module = _importlib.import_module(f"{__name__}.runtime")
    # Keep direct runtime imports and the package facade on the same bounded,
    # chronological parser even when runtime was already present in sys.modules.
    runtime_module.parse_stream_header = parse_stream_header
    runtime_module.parse_probe = parse_probe
    runtime_module.parse_streams = parse_streams
    # Resource admission is also installed here, after launcher environment
    # configuration has established DATA_DIR/STATE_FILE. This preserves the
    # lazy-import contract while making normal package startup resource-aware.
    install_runtime_resource_pressure(runtime_module)
    # B03 adds only bounded publication-discovery metadata. It is composed after
    # B01 so the record participates in the same transaction reservation rather
    # than opening a nested process-resource reservation of its own.
    install_publication_frontier_runtime(runtime_module)
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
__all__.extend(["MtconnectClient", "RecorderRuntime", "runtime"])
