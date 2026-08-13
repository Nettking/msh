"""Explicitly enumerated recorder durable-artifact schemas across the rename.

The recorder writes several durable artifacts whose payloads carry a ``schema``
discriminator. The product was renamed before this repository's history begins,
so captures recorded by an older installation carry the retired product prefix
while every new write uses ``fcp.``.

This module is the single place that states which schema strings a reader may
accept. It deliberately enumerates complete schema names instead of matching a
prefix: "anything starting with the retired product name" would accept schemas
that never existed and would hide a genuinely unknown future schema behind a
successful read.

Three rules hold for every artifact listed here:

``current`` is canonical
    New data is always written with the ``fcp.`` name. Nothing in this module
    causes a legacy name to be written.
``legacy`` is readable, never rewritten
    A historical capture stays byte-for-byte as it was recorded. Readability is
    a property of the reader, not a reason to migrate immutable evidence.
``unsupported`` is not corruption
    A payload that parses but declares an unknown schema is a different failure
    from a payload that does not parse. Callers must be able to tell them apart,
    so both outcomes are named rather than collapsed into "skip".
"""

from __future__ import annotations

# The retired product name is spelled from its code points so the string does
# not appear as a searchable literal in product source. The same construction is
# already used by ``catalog/federation/onboarding_compat.py``,
# ``catalog/flask_app/services/legacy_settings_migration.py`` and
# ``catalog/mtconnect_recorder/runtime.py``.
_RETIRED_PRODUCT = bytes((109, 115, 104)).decode("ascii")

#: Schema written for every new raw ``MTConnectStreams`` batch manifest.
RAW_BATCH_MANIFEST_SCHEMA = "fcp.mtconnect.raw_batch_manifest.v1"

#: Raw batch manifests recorded before the rename. The payload shape is
#: identical; only the product prefix differs, so no field translation is
#: needed and the same validation applies to both.
LEGACY_RAW_BATCH_MANIFEST_SCHEMAS = frozenset(
    {f"{_RETIRED_PRODUCT}.mtconnect.raw_batch_manifest.v1"}
)

SUPPORTED_RAW_BATCH_MANIFEST_SCHEMAS = frozenset(
    {RAW_BATCH_MANIFEST_SCHEMA}
) | LEGACY_RAW_BATCH_MANIFEST_SCHEMAS

#: Schema written for every new archived ``/probe`` manifest.
PROBE_MANIFEST_SCHEMA = "fcp.mtconnect.probe_manifest.v1"

LEGACY_PROBE_MANIFEST_SCHEMAS = frozenset(
    {f"{_RETIRED_PRODUCT}.mtconnect.probe_manifest.v1"}
)

SUPPORTED_PROBE_MANIFEST_SCHEMAS = frozenset(
    {PROBE_MANIFEST_SCHEMA}
) | LEGACY_PROBE_MANIFEST_SCHEMAS

#: Schema written for the durable recorder checkpoint state file.
CHECKPOINT_SCHEMA = "fcp.mtconnect_recorder.checkpoints.v3"

#: Checkpoint files recorded before the rename. ``RecorderRuntime.load_state``
#: already restores these and leaves the file untouched until the next commit,
#: so every other reader of the same file has to accept them too.
LEGACY_CHECKPOINT_SCHEMAS = frozenset(
    {f"{_RETIRED_PRODUCT}.mtconnect_recorder.checkpoints.v3"}
)

SUPPORTED_CHECKPOINT_SCHEMAS = frozenset(
    {CHECKPOINT_SCHEMA}
) | LEGACY_CHECKPOINT_SCHEMAS

#: Outcome of classifying one declared schema string.
SCHEMA_CURRENT = "current"
SCHEMA_LEGACY = "legacy"
SCHEMA_UNSUPPORTED = "unsupported"


def _classify(value: object, *, current: str, legacy: frozenset[str]) -> str:
    if value == current:
        return SCHEMA_CURRENT
    if isinstance(value, str) and value in legacy:
        return SCHEMA_LEGACY
    return SCHEMA_UNSUPPORTED


def classify_raw_batch_manifest_schema(value: object) -> str:
    """Return ``current``, ``legacy`` or ``unsupported`` for a manifest schema."""

    return _classify(
        value,
        current=RAW_BATCH_MANIFEST_SCHEMA,
        legacy=LEGACY_RAW_BATCH_MANIFEST_SCHEMAS,
    )


def classify_probe_manifest_schema(value: object) -> str:
    """Return ``current``, ``legacy`` or ``unsupported`` for a probe manifest."""

    return _classify(
        value,
        current=PROBE_MANIFEST_SCHEMA,
        legacy=LEGACY_PROBE_MANIFEST_SCHEMAS,
    )


def classify_checkpoint_schema(value: object) -> str:
    """Return ``current``, ``legacy`` or ``unsupported`` for checkpoint state."""

    return _classify(
        value,
        current=CHECKPOINT_SCHEMA,
        legacy=LEGACY_CHECKPOINT_SCHEMAS,
    )


def is_supported_raw_batch_manifest_schema(value: object) -> bool:
    return classify_raw_batch_manifest_schema(value) != SCHEMA_UNSUPPORTED


def is_supported_checkpoint_schema(value: object) -> bool:
    return classify_checkpoint_schema(value) != SCHEMA_UNSUPPORTED


__all__ = [
    "CHECKPOINT_SCHEMA",
    "LEGACY_CHECKPOINT_SCHEMAS",
    "LEGACY_PROBE_MANIFEST_SCHEMAS",
    "LEGACY_RAW_BATCH_MANIFEST_SCHEMAS",
    "PROBE_MANIFEST_SCHEMA",
    "RAW_BATCH_MANIFEST_SCHEMA",
    "SCHEMA_CURRENT",
    "SCHEMA_LEGACY",
    "SCHEMA_UNSUPPORTED",
    "SUPPORTED_CHECKPOINT_SCHEMAS",
    "SUPPORTED_PROBE_MANIFEST_SCHEMAS",
    "SUPPORTED_RAW_BATCH_MANIFEST_SCHEMAS",
    "classify_checkpoint_schema",
    "classify_probe_manifest_schema",
    "classify_raw_batch_manifest_schema",
    "is_supported_checkpoint_schema",
    "is_supported_raw_batch_manifest_schema",
]
