"""Opaque manifest IDs retain their exact text and control-character rules."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.manifest import (
    AuthoritativeStorageManifest,
    DatasetManifest,
    ManifestItem,
    ManifestItemKind,
    _required_text,
)
from catalog.federation.models import CommitState


NOW = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)


def dataset(value):
    return DatasetManifest(
        dataset_id=value, schema_name="fcp.test", schema_version=1,
        required=True, source_id=None, last_contiguous_sequence=None,
        missing_ranges=(),
    )


def item(value):
    return ManifestItem(
        item_id=value, kind=ManifestItemKind.BATCH, dataset_id="dataset",
        idempotency_key="request", content_hash="sha256:" + "a" * 64,
        size_bytes=1, schema_name="fcp.test", schema_version=1,
        source_id=None, first_sequence=None, last_sequence=None,
        commit_state=CommitState.COMMITTED,
        acknowledged_provider_ids=("provider",), committed_at=NOW,
    )


def manifest(value):
    return AuthoritativeStorageManifest.build(
        session_id=value, group_id="group", revision=0, term=0,
        previous_manifest_hash=None, datasets=(), items=(), updated_at=NOW,
    )


CONSTRUCTORS = [(dataset, "dataset_id"), (item, "item_id"), (manifest, "session_id")]


@pytest.mark.parametrize("construct,field", CONSTRUCTORS)
def test_public_manifest_contracts_reject_every_ascii_control(construct, field):
    for codepoint in range(32):
        control = chr(codepoint)
        for value in (control + "opaque", "opaque" + control, "op" + control + "aque"):
            with pytest.raises(FederationValidationError) as failure:
                construct(value)
            assert failure.value.to_dict() == {
                "code": "invalid-id", "field": field,
                "message": "must be non-empty opaque text",
            }


@pytest.mark.parametrize("construct,field", CONSTRUCTORS)
def test_public_manifest_contracts_preserve_printable_c1_and_unicode(construct, field):
    # DEL and C1 were never forbidden by the ASCII C0 check. Surround whitespace
    # with text so this tests the control boundary separately from blank input.
    values = ["left" + chr(codepoint) + "right" for codepoint in range(32, 256)]
    values += ["  unchanged  ", "æøå", "日本語", "e\u0301", "\u200d", "🔧", "\U0010ffff"]
    for value in values:
        result = construct(value)
        assert getattr(result, field) == value
        assert type(result).from_dict(result.to_dict()) == result


def test_nontext_and_blank_inputs_keep_the_same_error_contract():
    for value in (None, False, 0, 1.5, b"opaque", [], {}, object(), "", "   ",
                  "\t\n", "\x85", "\xa0", "\u2000", "\u3000"):
        with pytest.raises(FederationValidationError) as failure:
            _required_text(value, "opaque_field")
        assert failure.value.to_dict() == {
            "code": "invalid-id", "field": "opaque_field",
            "message": "must be non-empty opaque text",
        }


def test_nonempty_text_is_returned_without_normalization_or_copying():
    class OpaqueId(str):
        pass

    for value in (" leading and trailing ", "\x7f\x80\x9f", "e\u0301", OpaqueId("opaque")):
        assert _required_text(value, "identity") is value


def test_optional_source_and_acknowledgement_ids_keep_control_validation():
    from dataclasses import replace

    for field, value in (
        ("idempotency_key", "request\x00"),
        ("source_id", "source\x1f"),
        ("acknowledged_provider_ids", ("provider\n",)),
    ):
        with pytest.raises(FederationValidationError) as failure:
            replace(item("batch"), **{field: value})
        assert failure.value.code == "invalid-id"
        assert failure.value.field == field
    with pytest.raises(FederationValidationError) as failure:
        replace(dataset("dataset"), source_id="source\r")
    assert failure.value.code == "invalid-id"
    assert failure.value.field == "source_id"
