"""Loss-aware MTConnect recorder package."""
from __future__ import annotations

from typing import Any

from .model import *
from .parsing import *
from .storage import *
from . import parsing as _parsing
from . import runtime


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


# Runtime imported parse_streams when its module was loaded. Replace that bound
# reference as well so direct runs, Docker runs, crash recovery, and tests all use
# identical chronological ordering.
_parsing.parse_streams = parse_streams
runtime.parse_streams = parse_streams

from .runtime import RecorderRuntime, MtconnectClient, run
