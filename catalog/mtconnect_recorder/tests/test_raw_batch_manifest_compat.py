"""Reading recorder captures written before and after the product rename."""

from __future__ import annotations

import gzip
import json
import sys
from hashlib import sha256

import pytest

from catalog.mtconnect_recorder.model import (
    RAW_BATCH_ISSUE_MALFORMED,
    RAW_BATCH_ISSUE_RAW_MISSING,
    RAW_BATCH_ISSUE_UNSUPPORTED_SCHEMA,
    MtconnectProtocolError,
)
from catalog.mtconnect_recorder.schema_compat import (
    LEGACY_CHECKPOINT_SCHEMAS,
    LEGACY_RAW_BATCH_MANIFEST_SCHEMAS,
    RAW_BATCH_MANIFEST_SCHEMA,
    SCHEMA_CURRENT,
    SCHEMA_LEGACY,
    SCHEMA_UNSUPPORTED,
    classify_checkpoint_schema,
    classify_raw_batch_manifest_schema,
)

from .conftest import LEGACY_RAW_BATCH_MANIFEST_SCHEMA, machine_batch

SOURCE = "MACHINE-ALPHA-0001"
INSTANCE = 1786093233


def _write(archive, **kwargs):
    xml_text, _ = machine_batch(instance_id=INSTANCE, start_sequence=1)
    manifest = archive.write_batch(source_name=SOURCE, xml_text=xml_text, **kwargs)
    return xml_text, manifest


# ---------------------------------------------------------------------------
# Explicit enumeration
# ---------------------------------------------------------------------------


def test_supported_schemas_are_enumerated_not_prefix_matched():
    assert classify_raw_batch_manifest_schema(RAW_BATCH_MANIFEST_SCHEMA) == SCHEMA_CURRENT
    for legacy in LEGACY_RAW_BATCH_MANIFEST_SCHEMAS:
        assert classify_raw_batch_manifest_schema(legacy) == SCHEMA_LEGACY

    retired = bytes((109, 115, 104)).decode("ascii")
    # A name that merely starts with a known product prefix must not be
    # accepted: "anything beginning with either product prefix" would swallow
    # schemas that never existed and hide genuinely unknown future versions.
    for invented in (
        f"{retired}.mtconnect.raw_batch_manifest.v2",
        f"{retired}.mtconnect.raw_batch_manifest",
        "fcp.mtconnect.raw_batch_manifest.v2",
        "fcp.mtconnect.raw_batch_manifest",
        "fcp.mtconnect.probe_manifest.v1",
        "",
        None,
        123,
    ):
        assert classify_raw_batch_manifest_schema(invented) == SCHEMA_UNSUPPORTED


def test_checkpoint_schema_enumeration_matches_the_runtime():
    from catalog.mtconnect_recorder import runtime

    assert classify_checkpoint_schema(runtime._CHECKPOINT_SCHEMA) == SCHEMA_CURRENT
    assert runtime._LEGACY_CHECKPOINT_SCHEMAS == LEGACY_CHECKPOINT_SCHEMAS
    for legacy in LEGACY_CHECKPOINT_SCHEMAS:
        assert classify_checkpoint_schema(legacy) == SCHEMA_LEGACY


# ---------------------------------------------------------------------------
# Reading both generations
# ---------------------------------------------------------------------------


def test_current_manifest_is_read(recorder_archive):
    _write(recorder_archive)

    scan = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    )

    assert len(scan.refs) == 1
    assert scan.issues == ()
    assert scan.refs[0].manifest_schema == RAW_BATCH_MANIFEST_SCHEMA
    assert scan.refs[0].observation_count == 6


def test_legacy_manifest_is_read(recorder_archive):
    _write(recorder_archive, legacy_manifest=True)

    scan = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    )

    assert len(scan.refs) == 1, "a pre-rename capture must not be silently skipped"
    assert scan.issues == ()
    assert scan.refs[0].manifest_schema == LEGACY_RAW_BATCH_MANIFEST_SCHEMA
    assert scan.refs[0].observation_count == 6


def test_legacy_and_current_manifests_produce_identical_references(recorder_archive):
    xml_text, current_manifest = _write(recorder_archive)
    current = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    ).refs[0]

    recorder_archive.rewrite_manifest(
        current_manifest, schema=LEGACY_RAW_BATCH_MANIFEST_SCHEMA
    )
    legacy = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    ).refs[0]

    # Only the declared schema differs; every integrity-relevant field is equal.
    assert legacy.manifest_schema != current.manifest_schema
    for attribute in (
        "raw_path",
        "raw_sha256",
        "requested_from",
        "first_sequence",
        "last_sequence",
        "next_sequence",
        "observation_count",
        "received_at",
    ):
        assert getattr(legacy, attribute) == getattr(current, attribute)
    assert recorder_archive.store.read_raw_batch(legacy) == xml_text


def test_iter_raw_batches_still_returns_a_plain_list(recorder_archive):
    _write(recorder_archive, legacy_manifest=True)

    refs = recorder_archive.store.iter_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    )

    assert isinstance(refs, list)
    assert len(refs) == 1


# ---------------------------------------------------------------------------
# Unsupported is not corruption
# ---------------------------------------------------------------------------


def test_unsupported_schema_is_reported_as_unsupported(recorder_archive):
    _write(recorder_archive, manifest_schema="fcp.mtconnect.raw_batch_manifest.v9")

    scan = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    )

    assert scan.refs == ()
    assert len(scan.issues) == 1
    issue = scan.issues[0]
    assert issue.reason == RAW_BATCH_ISSUE_UNSUPPORTED_SCHEMA
    assert issue.schema == "fcp.mtconnect.raw_batch_manifest.v9"
    assert RAW_BATCH_MANIFEST_SCHEMA in issue.detail


def test_corrupt_manifest_is_reported_as_malformed_not_unsupported(recorder_archive):
    _, manifest = _write(recorder_archive)
    manifest.write_text("{not json at all", encoding="utf-8")

    scan = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    )

    assert scan.refs == ()
    assert [issue.reason for issue in scan.issues] == [RAW_BATCH_ISSUE_MALFORMED]
    assert scan.issues[0].reason != RAW_BATCH_ISSUE_UNSUPPORTED_SCHEMA


def test_manifest_that_is_not_an_object_is_malformed(recorder_archive):
    _, manifest = _write(recorder_archive)
    manifest.write_text(json.dumps([1, 2, 3]), encoding="utf-8")

    scan = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    )

    assert [issue.reason for issue in scan.issues] == [RAW_BATCH_ISSUE_MALFORMED]


@pytest.mark.parametrize("legacy", [False, True])
def test_missing_required_field_is_malformed_for_both_generations(
    recorder_archive, legacy
):
    _, manifest = _write(recorder_archive, legacy_manifest=legacy)
    payload = recorder_archive.read_manifest(manifest)
    del payload["raw_sha256"]
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    scan = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    )

    # Both generations go through the same structural validation, so a legacy
    # manifest is not trusted more just because it is old.
    assert [issue.reason for issue in scan.issues] == [RAW_BATCH_ISSUE_MALFORMED]


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize(
    ("mutation", "label"),
    [
        ({}, "missing"),
        ({"source_name": ""}, "blank"),
        ({"source_name": "   "}, "whitespace"),
        ({"source_name": 42}, "not-text"),
    ],
)
def test_a_manifest_without_a_usable_source_name_is_malformed(
    recorder_archive, legacy, mutation, label
):
    # source_name is required in every supported version of this manifest, and
    # the canonical projection depends on it as the only surviving evidence of
    # the original identity: the directory is a lossy slug. Accepting a blank
    # would let a slug-derived identity back in through the reader.
    _, manifest = _write(recorder_archive, legacy_manifest=legacy)
    payload = recorder_archive.read_manifest(manifest)
    if mutation:
        payload.update(mutation)
    else:
        del payload["source_name"]
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    scan = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    )

    assert scan.refs == (), f"{label} source_name must not produce a reference"
    assert [issue.reason for issue in scan.issues] == [RAW_BATCH_ISSUE_MALFORMED]
    # A required field being absent is corruption, not a version difference.
    assert scan.issues[0].reason != RAW_BATCH_ISSUE_UNSUPPORTED_SCHEMA


@pytest.mark.parametrize("legacy", [False, True])
def test_a_readable_manifest_always_carries_its_source_name(recorder_archive, legacy):
    _write(recorder_archive, legacy_manifest=legacy)

    scan = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    )

    assert scan.refs
    assert all(ref.source_name == SOURCE for ref in scan.refs)


def test_missing_payload_is_distinct_from_a_bad_manifest(recorder_archive):
    _, manifest = _write(recorder_archive)
    recorder_archive.raw_payload_path(manifest).unlink()
    recorder_archive.rewrite_manifest(manifest, raw_file="/nowhere/at/all.xml.gz")

    scan = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    )

    assert scan.refs == ()
    assert [issue.reason for issue in scan.issues] == [RAW_BATCH_ISSUE_RAW_MISSING]


# ---------------------------------------------------------------------------
# Digest semantics and relocation
# ---------------------------------------------------------------------------


def test_raw_sha256_is_over_the_uncompressed_payload(recorder_archive):
    xml_text, manifest = _write(recorder_archive)
    payload = recorder_archive.raw_payload_path(manifest)

    declared = recorder_archive.read_manifest(manifest)["raw_sha256"]

    assert declared == sha256(xml_text.encode("utf-8")).hexdigest()
    assert declared == sha256(gzip.decompress(payload.read_bytes())).hexdigest()
    assert declared != sha256(payload.read_bytes()).hexdigest()


@pytest.mark.parametrize("legacy", [False, True])
def test_digest_verification_still_rejects_a_tampered_payload(recorder_archive, legacy):
    xml_text, manifest = _write(recorder_archive, legacy_manifest=legacy)
    payload = recorder_archive.raw_payload_path(manifest)
    payload.write_bytes(gzip.compress(xml_text.replace("ACTIVE", "READY").encode()))

    ref = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    ).refs[0]

    with pytest.raises(MtconnectProtocolError, match="checksum mismatch"):
        recorder_archive.store.read_raw_batch(ref)


def test_relocated_capture_resolves_its_payload_beside_the_manifest(recorder_archive):
    xml_text, manifest = _write(recorder_archive, legacy_manifest=True)
    # A capture copied off the machine that recorded it keeps the absolute path
    # of the original host in its manifest, which resolves to nothing here.
    recorder_archive.rewrite_manifest(
        manifest, raw_file="C:/recorder/data/sources/does/not/exist.xml.gz"
    )

    scan = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    )

    assert scan.issues == ()
    assert len(scan.refs) == 1
    assert scan.refs[0].raw_path == recorder_archive.raw_payload_path(manifest)
    assert recorder_archive.store.read_raw_batch(scan.refs[0]) == xml_text


def _load_profile_script():
    import importlib.util
    from pathlib import Path

    script = Path("scripts/profile_mtconnect_raw_batches.py")
    spec = importlib.util.spec_from_file_location("_profile_script", script)
    module = importlib.util.module_from_spec(spec)
    # The script defines dataclasses under postponed annotations, which resolve
    # through sys.modules, so it has to be registered before execution.
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


def test_profiler_scopes_sequence_continuity_to_one_agent_buffer(recorder_archive):
    # Two Agent instances both numbering from 1. Chaining every batch through a
    # single cursor would report a reverse gap such as (7, 1) that does not
    # exist, and could hide a real gap when sources interleave.
    for instance in (INSTANCE, 1786099999):
        xml_text, _ = machine_batch(instance_id=instance, start_sequence=1)
        recorder_archive.write_batch(source_name=SOURCE, xml_text=xml_text)
    other, _ = machine_batch(instance_id=INSTANCE, start_sequence=1)
    recorder_archive.write_batch(source_name="MACHINE-BETA-0002", xml_text=other)

    module = _load_profile_script()
    profile = module.profile_directory(recorder_archive.store.raw_root)

    assert profile.batch_count == 3
    assert profile.sequence_gaps == []


def test_profiler_still_reports_a_real_gap_within_one_buffer(recorder_archive):
    first, _ = machine_batch(instance_id=INSTANCE, start_sequence=1)
    # Sequences 7-12 are never recorded, so 13 leaves a genuine hole.
    third, _ = machine_batch(instance_id=INSTANCE, start_sequence=13)
    recorder_archive.write_batch(source_name=SOURCE, xml_text=first)
    recorder_archive.write_batch(source_name=SOURCE, xml_text=third)

    module = _load_profile_script()
    profile = module.profile_directory(recorder_archive.store.raw_root)

    assert profile.sequence_gaps == [(7, 13)]


def test_profiler_uses_mtconnect_types_for_state_channels():
    module = _load_profile_script()
    profile = module.Profile(
        observations=[
            module.Observation(
                sequence=1,
                timestamp="2026-08-07T09:00:00Z",
                data_item_id="controller-status-custom",
                name=None,
                observation_type="Execution",
                component="Controller/controller",
                value="ACTIVE",
            ),
            module.Observation(
                sequence=2,
                timestamp="2026-08-07T09:00:05Z",
                data_item_id="controller-mode-custom",
                name=None,
                observation_type="ControllerMode",
                component="Controller/controller",
                value="AUTOMATIC",
            ),
            module.Observation(
                sequence=3,
                timestamp="2026-08-07T09:00:06Z",
                data_item_id="program-custom",
                name=None,
                observation_type="Program",
                component="Path/path",
                value="ANONYMISED-PROGRAM",
            ),
            module.Observation(
                sequence=4,
                timestamp="2026-08-07T09:00:07Z",
                data_item_id="availability-custom",
                name=None,
                observation_type="Availability",
                component="Device/MachineAlpha",
                value="AVAILABLE",
            ),
            module.Observation(
                sequence=5,
                timestamp="2026-08-07T09:00:08Z",
                data_item_id="emergency-stop-custom",
                name=None,
                observation_type="EmergencyStop",
                component="Device/MachineAlpha",
                value="ARMED",
            ),
            module.Observation(
                sequence=6,
                timestamp="2026-08-07T09:00:10Z",
                data_item_id="controller-status-custom",
                name=None,
                observation_type="Execution",
                component="Controller/controller",
                value="READY",
            ),
        ]
    )

    summary = module.summarize(profile)

    assert summary["state_timeline"]["exec"] == [
        {"timestamp": "2026-08-07T09:00:00Z", "value": "ACTIVE"},
        {"timestamp": "2026-08-07T09:00:10Z", "value": "READY"},
    ]
    assert summary["state_timeline"]["mode"] == [
        {"timestamp": "2026-08-07T09:00:05Z", "value": "AUTOMATIC"}
    ]
    assert summary["state_timeline"]["pgm"] == [
        {"timestamp": "2026-08-07T09:00:06Z", "value": "ANONYMISED-PROGRAM"}
    ]
    assert summary["state_timeline"]["avail"] == [
        {"timestamp": "2026-08-07T09:00:07Z", "value": "AVAILABLE"}
    ]
    assert summary["state_timeline"]["estop"] == [
        {"timestamp": "2026-08-07T09:00:08Z", "value": "ARMED"}
    ]
    assert summary["execution_dwell_seconds"] == {"ACTIVE": 10.0}


def test_profiler_records_a_truncated_gzip_and_continues(recorder_archive):
    first_xml, _ = machine_batch(instance_id=INSTANCE, start_sequence=1)
    first_manifest = recorder_archive.write_batch(
        source_name=SOURCE, xml_text=first_xml
    )
    second_xml, _ = machine_batch(instance_id=INSTANCE, start_sequence=7)
    recorder_archive.write_batch(source_name=SOURCE, xml_text=second_xml)
    first_payload = recorder_archive.raw_payload_path(first_manifest)
    first_payload.write_bytes(first_payload.read_bytes()[:-8])

    module = _load_profile_script()
    profile = module.profile_directory(recorder_archive.store.raw_root)

    assert profile.batch_count == 1
    assert [item.sequence for item in profile.observations] == list(range(7, 13))
    assert profile.unreadable == [str(first_payload)]


def test_the_profiling_script_accepts_the_same_schemas():
    import importlib.util
    from pathlib import Path

    # The standalone profiler repeats the enumeration so it can run on a machine
    # with no FCP installation. Repetition is only safe while it stays in step.
    script = Path("scripts/profile_mtconnect_raw_batches.py")
    spec = importlib.util.spec_from_file_location("_profile_script", script)
    module = importlib.util.module_from_spec(spec)
    # The script defines dataclasses under postponed annotations, which resolve
    # through sys.modules, so it has to be registered before execution.
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)

    assert module.ACCEPTED_MANIFEST_SCHEMAS == frozenset(
        {RAW_BATCH_MANIFEST_SCHEMA}
    ) | set(LEGACY_RAW_BATCH_MANIFEST_SCHEMAS)


def test_the_skip_policy_is_documented(recorder_archive):
    from pathlib import Path

    policy = Path("docs/implementation/canonical_mtconnect_observations.md").read_text(
        encoding="utf-8"
    )

    # The reasons a batch can be skipped are a documented contract, not an
    # implementation detail a caller has to read the source to discover.
    for reason in (
        RAW_BATCH_ISSUE_UNSUPPORTED_SCHEMA,
        RAW_BATCH_ISSUE_MALFORMED,
        RAW_BATCH_ISSUE_RAW_MISSING,
    ):
        assert reason in policy, f"{reason} is not documented"
    assert "enumerated, never prefix-matched" in policy


def test_one_bad_manifest_does_not_hide_its_healthy_neighbours(recorder_archive):
    first, _ = machine_batch(instance_id=INSTANCE, start_sequence=1)
    second, _ = machine_batch(instance_id=INSTANCE, start_sequence=7)
    third, _ = machine_batch(instance_id=INSTANCE, start_sequence=13)
    recorder_archive.write_batch(source_name=SOURCE, xml_text=first)
    broken = recorder_archive.write_batch(source_name=SOURCE, xml_text=second)
    recorder_archive.write_batch(
        source_name=SOURCE, xml_text=third, legacy_manifest=True
    )
    broken.write_text("{", encoding="utf-8")

    scan = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    )

    assert [ref.first_sequence for ref in scan.refs] == [1, 13]
    assert [issue.reason for issue in scan.issues] == [RAW_BATCH_ISSUE_MALFORMED]
