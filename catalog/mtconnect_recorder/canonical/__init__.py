"""Canonical MTConnect observations projected from raw recorder batches.

The recorder's compressed XML and its manifests are the source of truth. This
package is the derived, queryable view of them:

.. code-block:: text

    MTConnect raw recorder batches      (immutable evidence, never modified)
            |
            v
    verified compatible batch reader    (DurableRecorderStore.scan_raw_batches)
            |
            v
    canonical observations              (CanonicalObservation)
            |
            v
    durable observation projection      (CanonicalObservationStore)

Nothing above that boundary lives here. There is no segmentation, no baseline,
no anomaly model and no episode concept yet — the observation model is meant to
be inspected first, and those semantics designed against it afterwards.
"""

from .observation import (
    CATEGORY_CONDITION,
    CATEGORY_EVENT,
    CATEGORY_SAMPLE,
    TIMESTAMP_FROM_BATCH,
    TIMESTAMP_FROM_OBSERVATION,
    VALUE_EMPTY,
    VALUE_NUMBER,
    VALUE_TEXT,
    VALUE_UNAVAILABLE,
    CanonicalObservation,
    CanonicalObservationError,
    epoch_micros,
    parse_timestamp,
    source_key,
)
from .projection import (
    NOTE_COUNT_MISMATCH,
    REJECT_DIGEST_MISMATCH,
    REJECT_DUPLICATE_SEQUENCE,
    REJECT_INSTANCE_MISMATCH,
    REJECT_MISSING_SOURCE_NAME,
    REJECT_NOT_CANONICALISABLE,
    REJECT_OVERLAP_CONFLICT,
    REJECT_PAYLOAD_UNREADABLE,
    REJECT_SOURCE_KEY_MISMATCH,
    REJECT_XML_INVALID,
    BatchRejection,
    CanonicalObservationProjection,
    ObservedRange,
    ProjectionReport,
    rebuild_from_recorder_archive,
)
from .store import (
    BATCH_STATUS_PROJECTED,
    BATCH_STATUS_REJECTED,
    CANONICAL_SCHEMA_NAME,
    CANONICAL_SCHEMA_VERSION,
    CanonicalObservationStore,
    ProjectedBatchRecord,
)

__all__ = [
    "BATCH_STATUS_PROJECTED",
    "BATCH_STATUS_REJECTED",
    "CANONICAL_SCHEMA_NAME",
    "CANONICAL_SCHEMA_VERSION",
    "CATEGORY_CONDITION",
    "CATEGORY_EVENT",
    "CATEGORY_SAMPLE",
    "NOTE_COUNT_MISMATCH",
    "REJECT_DIGEST_MISMATCH",
    "REJECT_DUPLICATE_SEQUENCE",
    "REJECT_INSTANCE_MISMATCH",
    "REJECT_MISSING_SOURCE_NAME",
    "REJECT_NOT_CANONICALISABLE",
    "REJECT_OVERLAP_CONFLICT",
    "REJECT_PAYLOAD_UNREADABLE",
    "REJECT_SOURCE_KEY_MISMATCH",
    "REJECT_XML_INVALID",
    "TIMESTAMP_FROM_BATCH",
    "TIMESTAMP_FROM_OBSERVATION",
    "VALUE_EMPTY",
    "VALUE_NUMBER",
    "VALUE_TEXT",
    "VALUE_UNAVAILABLE",
    "BatchRejection",
    "CanonicalObservation",
    "CanonicalObservationError",
    "CanonicalObservationProjection",
    "CanonicalObservationStore",
    "ObservedRange",
    "ProjectedBatchRecord",
    "ProjectionReport",
    "epoch_micros",
    "parse_timestamp",
    "rebuild_from_recorder_archive",
    "source_key",
]
