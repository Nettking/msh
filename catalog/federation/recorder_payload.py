"""Pure, shared validation for federated MTConnect recorder payloads.

Recorder ingress and product materialization must accept exactly the same
payload language.  Keeping this validator independent of storage, relay, and
filesystem state prevents a manifest-authoritative batch from becoming a
permanent poison pill when another node tries to mirror it.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .errors import FederationValidationError
from .storage_protocol import BatchIngestRequest

RECORDER_DATASET_SCHEMA_NAME = "fcp.mtconnect.observations"
RECORDER_DATASET_SCHEMA_VERSION = 1
RECORDER_CONTENT_SCHEMA = "fcp.mtconnect.observations.v1"
RECORDER_OBSERVATION_SCHEMA = "fcp.mtconnect.observation.v2"

DEFAULT_MAX_BATCH_BYTES = 1_048_576
DEFAULT_MAX_RECORDS_PER_BATCH = 10_000

_SHA256_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")
_DATASET_PATTERN = re.compile(
    r"mtconnect:(?P<node>[A-Za-z0-9][A-Za-z0-9._-]{0,127}):"
    r"(?P<source>[A-Za-z0-9][A-Za-z0-9._-]{0,127})"
)
_CONTENT_KEYS = frozenset(
    {
        "schema",
        "source_name",
        "machine_id",
        "agent_instance_id",
        "first_sequence",
        "last_sequence",
        "raw_batch_first_sequence",
        "raw_batch_last_sequence",
        "raw_sha256",
        "probe_sha256",
        "observation_count",
        "observations",
    }
)


def _validation(code: str, field: str, message: str) -> FederationValidationError:
    return FederationValidationError(code, field, message)


def _positive_bound(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise _validation(
            "invalid-recorder-validation-bound",
            field,
            "must be a positive integer",
        )
    return value


def _required_text(value: Any, field: str, *, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _validation(
            "invalid-telemetry-field",
            field,
            f"must be non-empty printable text no longer than {maximum} bytes",
        )
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise _validation(
            "invalid-telemetry-field",
            field,
            "must contain valid Unicode text",
        ) from exc
    if (
        len(encoded) > maximum
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise _validation(
            "invalid-telemetry-field",
            field,
            f"must be non-empty printable text no longer than {maximum} bytes",
        )
    return value.strip()


def _non_negative_integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _validation(
            "invalid-telemetry-field",
            field,
            "must be a non-negative integer",
        )
    return value


def _sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise _validation(
            "invalid-telemetry-hash",
            field,
            "must be a lowercase sha256 digest",
        )
    return value


def _utc_text(value: Any, field: str) -> str:
    value = _required_text(value, field)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _validation(
            "invalid-telemetry-timestamp",
            field,
            "must be an RFC 3339 timestamp",
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _validation(
            "invalid-telemetry-timestamp",
            field,
            "must include a timezone",
        )
    return value


def _canonical_bytes(value: Any, field: str) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError, RecursionError) as exc:
        raise _validation(
            "invalid-telemetry-json",
            field,
            "must contain only canonical JSON-compatible values",
        ) from exc


def _detached_json(encoded: bytes, field: str) -> Any:
    def build_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key: {key}")
            result[key] = value
        return result

    try:
        return json.loads(encoded.decode("utf-8"), object_pairs_hook=build_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise _validation(
            "invalid-telemetry-json",
            field,
            "must be unambiguous UTF-8 JSON",
        ) from exc


def recorder_source_slug(value: str) -> str:
    """Return the recorder's canonical portable source component."""

    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return cleaned or "unknown"


@dataclass(frozen=True)
class ValidatedRecorderPayload:
    content: dict[str, Any]
    encoded: bytes
    content_hash: str
    recorder_node_id: str
    source_name: str
    source_slug: str
    machine_id: str
    instance_id: int
    first_sequence: int
    last_sequence: int
    raw_first_sequence: int
    raw_last_sequence: int
    raw_hash: str
    observations: tuple[dict[str, Any], ...]


def _validate_observation(
    value: Any,
    *,
    index: int,
    source_name: str,
    instance_id: int,
) -> dict[str, Any]:
    field = f"content.observations[{index}]"
    if not isinstance(value, dict):
        raise _validation("invalid-telemetry-observation", field, "must be an object")
    if value.get("schema") != RECORDER_OBSERVATION_SCHEMA:
        raise _validation(
            "unsupported-telemetry-observation-schema",
            f"{field}.schema",
            f"must be {RECORDER_OBSERVATION_SCHEMA}",
        )
    if value.get("source") != "mtconnect_recorder":
        raise _validation(
            "invalid-telemetry-observation",
            f"{field}.source",
            "must identify the MTConnect recorder",
        )
    if value.get("source_name") != source_name:
        raise _validation(
            "telemetry-source-identity-mismatch",
            f"{field}.source_name",
            "must match the enclosing recorder batch",
        )
    observation_instance = _non_negative_integer(
        value.get("agent_instance_id"),
        f"{field}.agent_instance_id",
    )
    if observation_instance != instance_id:
        raise _validation(
            "telemetry-instance-mismatch",
            f"{field}.agent_instance_id",
            "must match the enclosing recorder batch",
        )
    sequence = _non_negative_integer(value.get("sequence"), f"{field}.sequence")
    _required_text(value.get("source_record_id"), f"{field}.source_record_id")
    _required_text(value.get("machine_id"), f"{field}.machine_id")
    _utc_text(value.get("timestamp"), f"{field}.timestamp")
    _utc_text(value.get("received_at"), f"{field}.received_at")
    category = _required_text(value.get("category"), f"{field}.category")
    if category not in {"SAMPLE", "EVENT", "CONDITION"}:
        raise _validation(
            "invalid-telemetry-observation",
            f"{field}.category",
            "must be SAMPLE, EVENT, or CONDITION",
        )
    signal_identity = (
        value.get("name")
        or value.get("data_item_id")
        or value.get("observation_type")
    )
    _required_text(signal_identity, f"{field}.signal_identity", maximum=256)
    result = dict(value)
    result["sequence"] = sequence
    return result


def validate_recorder_payload(
    *,
    session_id: str,
    dataset_id: str,
    batch_id: str,
    idempotency_key: str,
    dataset_schema_name: str,
    dataset_schema_version: int,
    content: object,
    expected_recorder_node_id: str | None = None,
    expected_source_id: str | None = None,
    expected_first_sequence: int | None = None,
    expected_last_sequence: int | None = None,
    expected_content_hash: str | None = None,
    expected_size_bytes: int | None = None,
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES,
    max_records_per_batch: int = DEFAULT_MAX_RECORDS_PER_BATCH,
) -> ValidatedRecorderPayload:
    """Validate one immutable recorder payload without touching external state."""

    max_batch_bytes = _positive_bound(max_batch_bytes, "max_batch_bytes")
    max_records_per_batch = _positive_bound(
        max_records_per_batch,
        "max_records_per_batch",
    )
    session_id = _required_text(session_id, "session_id", maximum=4096)
    if (
        dataset_schema_name != RECORDER_DATASET_SCHEMA_NAME
        or dataset_schema_version != RECORDER_DATASET_SCHEMA_VERSION
    ):
        raise _validation(
            "unsupported-telemetry-dataset-schema",
            "dataset_schema_name",
            "only the MTConnect observations dataset schema v1 is accepted",
        )
    dataset_match = _DATASET_PATTERN.fullmatch(dataset_id)
    if dataset_match is None:
        raise _validation(
            "unsupported-telemetry-dataset",
            "dataset_id",
            "must use the allowlisted mtconnect:<node>:<source> namespace",
        )
    recorder_node_id = dataset_match.group("node")
    source_slug = dataset_match.group("source")
    if (
        expected_recorder_node_id is not None
        and recorder_node_id != expected_recorder_node_id
    ):
        raise _validation(
            "recorder-dataset-actor-mismatch",
            "dataset_id",
            "recorder dataset namespace must match the authenticated producer",
        )
    if expected_source_id not in {None, dataset_id}:
        raise _validation(
            "telemetry-source-identity-mismatch",
            "source_id",
            "manifest source identity, when present, must match the recorder dataset",
        )
    if not isinstance(content, dict):
        raise _validation("invalid-telemetry-content", "content", "must be an object")
    actual_keys = set(content)
    if actual_keys != _CONTENT_KEYS:
        missing = sorted(_CONTENT_KEYS - actual_keys)
        unknown = sorted(actual_keys - _CONTENT_KEYS)
        field = missing[0] if missing else unknown[0]
        message = "is required" if missing else "is not part of schema v1"
        raise _validation("invalid-telemetry-content", f"content.{field}", message)

    encoded = _canonical_bytes(content, "content")
    if len(encoded) > max_batch_bytes:
        raise _validation(
            "telemetry-batch-too-large",
            "content",
            f"canonical content exceeds {max_batch_bytes} bytes",
        )
    detached = _detached_json(encoded, "content")
    assert isinstance(detached, dict)
    content = detached
    if expected_size_bytes is not None and expected_size_bytes != len(encoded):
        raise _validation(
            "telemetry-size-mismatch",
            "size_bytes",
            "manifest size does not match canonical content",
        )
    content_hash = BatchIngestRequest.calculate_content_hash(content)
    if expected_content_hash is not None and expected_content_hash != content_hash:
        raise _validation(
            "telemetry-content-hash-mismatch",
            "content_hash",
            "manifest hash does not match canonical content",
        )
    if content.get("schema") != RECORDER_CONTENT_SCHEMA:
        raise _validation(
            "unsupported-telemetry-content-schema",
            "content.schema",
            f"must be {RECORDER_CONTENT_SCHEMA}",
        )

    source_name = _required_text(content.get("source_name"), "content.source_name")
    if recorder_source_slug(source_name) != source_slug:
        raise _validation(
            "telemetry-source-identity-mismatch",
            "content.source_name",
            "normalized source name must match the recorder dataset namespace",
        )
    machine_id = _required_text(content.get("machine_id"), "content.machine_id")
    instance_id = _non_negative_integer(
        content.get("agent_instance_id"),
        "content.agent_instance_id",
    )
    first_sequence = _non_negative_integer(
        content.get("first_sequence"),
        "content.first_sequence",
    )
    last_sequence = _non_negative_integer(
        content.get("last_sequence"),
        "content.last_sequence",
    )
    if last_sequence < first_sequence:
        raise _validation(
            "invalid-telemetry-sequence-range",
            "content.last_sequence",
            "must not precede first_sequence",
        )
    raw_first = _non_negative_integer(
        content.get("raw_batch_first_sequence"),
        "content.raw_batch_first_sequence",
    )
    raw_last = _non_negative_integer(
        content.get("raw_batch_last_sequence"),
        "content.raw_batch_last_sequence",
    )
    if raw_first > first_sequence or raw_last < last_sequence or raw_last < raw_first:
        raise _validation(
            "invalid-telemetry-raw-range",
            "content.raw_batch_first_sequence",
            "raw sequence bounds must contain the published sequence range",
        )
    raw_hash = _sha256(content.get("raw_sha256"), "content.raw_sha256")
    probe_hash = content.get("probe_sha256")
    if probe_hash is not None:
        _sha256(probe_hash, "content.probe_sha256")

    observations_value = content.get("observations")
    count = content.get("observation_count")
    if not isinstance(observations_value, list) or not observations_value:
        raise _validation(
            "invalid-telemetry-observations",
            "content.observations",
            "must be a non-empty array",
        )
    if len(observations_value) > max_records_per_batch:
        raise _validation(
            "telemetry-record-limit-exceeded",
            "content.observations",
            f"must contain at most {max_records_per_batch} observations",
        )
    if (
        isinstance(count, bool)
        or not isinstance(count, int)
        or count != len(observations_value)
    ):
        raise _validation(
            "telemetry-observation-count-mismatch",
            "content.observation_count",
            "must equal the number of observations",
        )

    observations: list[dict[str, Any]] = []
    sequences: list[int] = []
    for index, value in enumerate(observations_value):
        observation = _validate_observation(
            value,
            index=index,
            source_name=source_name,
            instance_id=instance_id,
        )
        observations.append(observation)
        sequences.append(observation["sequence"])
    expected_sequences = list(range(first_sequence, last_sequence + 1))
    if sequences != expected_sequences or count != len(expected_sequences):
        raise _validation(
            "invalid-telemetry-sequence-range",
            "content.observations",
            "sequences must be contiguous, ordered, and match declared bounds",
        )
    if (expected_first_sequence is None) != (expected_last_sequence is None):
        raise _validation(
            "invalid-telemetry-sequence-range",
            "first_sequence",
            "expected sequence bounds must be supplied together",
        )
    if expected_first_sequence is not None and (
        expected_first_sequence != first_sequence
        or expected_last_sequence != last_sequence
    ):
        raise _validation(
            "telemetry-manifest-sequence-mismatch",
            "first_sequence",
            "manifest sequence evidence does not match content",
        )

    expected_batch_id = (
        f"{source_slug}:{instance_id}:{first_sequence}:{last_sequence}:{raw_hash[7:]}"
    )
    if batch_id != expected_batch_id:
        raise _validation(
            "telemetry-batch-identity-mismatch",
            "batch_id",
            "batch identity does not match validated recorder content",
        )
    expected_idempotency = f"{session_id}:{dataset_id}:{batch_id}"
    if idempotency_key != expected_idempotency:
        raise _validation(
            "telemetry-idempotency-mismatch",
            "idempotency_key",
            "idempotency key does not match the immutable recorder batch",
        )
    return ValidatedRecorderPayload(
        content=content,
        encoded=encoded,
        content_hash=content_hash,
        recorder_node_id=recorder_node_id,
        source_name=source_name,
        source_slug=source_slug,
        machine_id=machine_id,
        instance_id=instance_id,
        first_sequence=first_sequence,
        last_sequence=last_sequence,
        raw_first_sequence=raw_first,
        raw_last_sequence=raw_last,
        raw_hash=raw_hash,
        observations=tuple(observations),
    )


__all__ = [
    "DEFAULT_MAX_BATCH_BYTES",
    "DEFAULT_MAX_RECORDS_PER_BATCH",
    "RECORDER_CONTENT_SCHEMA",
    "RECORDER_DATASET_SCHEMA_NAME",
    "RECORDER_DATASET_SCHEMA_VERSION",
    "RECORDER_OBSERVATION_SCHEMA",
    "ValidatedRecorderPayload",
    "recorder_source_slug",
    "validate_recorder_payload",
]
