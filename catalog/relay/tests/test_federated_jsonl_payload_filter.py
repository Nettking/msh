"""The relay admits one validated JSONL chunk path and nothing else.

The generic transport filter redacts every key ending in ``_path``. A federated
JSONL chunk cannot be routed without naming the file it carries, so the relay
compares against an expectation in which only that one already-validated schema
field is masked. These tests pin both halves of that bargain: the chunk passes,
and every neighbouring shape a caller might reach for still fails.
"""

from __future__ import annotations

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.shared_file_storage import (
    FEDERATED_JSONL_CONTENT_SCHEMA,
    encode_federated_jsonl_chunk,
    mask_public_jsonl_chunk_paths,
    normalize_jsonl_relative_path,
)
from catalog.relay.service import _ensure_bounded_json

RELATIVE_PATH = "uploads/machine4/2026-08-07.jsonl"


def _content(**overrides: object) -> dict[str, object]:
    content: dict[str, object] = {
        "schema": FEDERATED_JSONL_CONTENT_SCHEMA,
        "producer_node_id": "node-machine-4",
        "relative_path": RELATIVE_PATH,
        "encoding": "gzip",
        "file_size": 4096,
        "file_sha256": f"sha256:{'a' * 64}",
        "encoded_size": 1024,
        "encoded_sha256": f"sha256:{'b' * 64}",
        "source_mtime_ns": 1_786_000_000_000_000_000,
        "chunk_index": 0,
        "chunk_count": 1,
        "chunk_offset": 0,
        "chunk_sha256": f"sha256:{'c' * 64}",
        "data": encode_federated_jsonl_chunk(b'{"machine":"alpha"}\n' * 64),
    }
    content.update(overrides)
    return content


def _payload(content: dict[str, object]) -> dict[str, object]:
    return {
        "group_id": "fcp-local-storage",
        "dataset_id": "federated-jsonl:" + "d" * 64,
        "batch_id": "federated-jsonl-batch",
        "idempotency_key": "federated-jsonl-key",
        "content": content,
    }


def test_a_validated_jsonl_chunk_is_routable():
    _ensure_bounded_json(_payload(_content()), field="payload")


def test_the_encoded_chunk_body_does_not_trip_the_location_filter():
    # Base64 carries "/" but never ":", so no drive, URL, or address form can
    # appear in a chunk body. Pin it so a future encoding change is caught here.
    payload = _payload(_content(data=encode_federated_jsonl_chunk(bytes(range(256)))))

    _ensure_bounded_json(payload, field="payload")


@pytest.mark.parametrize(
    "relative_path",
    [
        "/etc/fcp/secrets.jsonl",
        "C:/fcp/data/machine.jsonl",
        "..\\..\\windows\\system32\\config.jsonl",
        "../../../etc/shadow.jsonl",
        "uploads/./machine.jsonl",
        "https://leader.example.com/data/machine.jsonl",
        "file:///var/lib/fcp/machine.jsonl",
        "100.64.0.4/machine.jsonl",
        "uploads/machine4/notes.txt",
        "",
    ],
)
def test_an_unsafe_or_unvalidated_chunk_path_is_still_rejected(relative_path):
    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(
            _payload(_content(relative_path=relative_path)), field="payload"
        )

    assert caught.value.code == "nonpublic-payload"


def test_the_allowance_does_not_extend_to_other_path_keys():
    payload = _payload(_content(cache_path="/var/lib/fcp/cache/machine.jsonl"))

    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(payload, field="payload")

    assert caught.value.code == "nonpublic-payload"


def test_the_allowance_does_not_extend_to_other_schemas():
    payload = _payload(
        _content(schema="fcp.federation.jsonl-file-chunk.v2")
    )

    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(payload, field="payload")

    assert caught.value.code == "nonpublic-payload"


def test_a_bare_relative_path_outside_the_chunk_schema_is_rejected():
    payload = {
        "group_id": "fcp-local-storage",
        "relative_path": RELATIVE_PATH,
    }

    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(payload, field="payload")

    assert caught.value.code == "nonpublic-payload"


def test_credentials_inside_a_chunk_are_still_rejected():
    payload = _payload(_content(token="fcp_join_" + "e" * 40))

    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(payload, field="payload")

    assert caught.value.code == "nonpublic-payload"


def test_masking_leaves_every_other_field_untouched():
    content = _content()

    masked = mask_public_jsonl_chunk_paths(_payload(content))["content"]

    assert masked["relative_path"] != content["relative_path"]
    assert {key: masked[key] for key in masked if key != "relative_path"} == {
        key: content[key] for key in content if key != "relative_path"
    }


def test_normalization_rejects_address_bearing_paths_before_masking():
    # The suffix and traversal rules alone would accept this name; the location
    # rule is what keeps a tailnet address out of a routed payload.
    with pytest.raises(FederationValidationError) as caught:
        normalize_jsonl_relative_path("100.64.0.4:8765/machine.jsonl")

    assert caught.value.code == "invalid-federated-jsonl-path"
