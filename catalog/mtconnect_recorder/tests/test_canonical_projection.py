"""The canonical observation projection over verified raw recorder batches."""

from __future__ import annotations

import gzip
from dataclasses import replace
from pathlib import Path

import pytest

from catalog.mtconnect_recorder.canonical import (
    BATCH_STATUS_PROJECTED,
    BATCH_STATUS_REJECTED,
    CATEGORY_CONDITION,
    CATEGORY_EVENT,
    CATEGORY_SAMPLE,
    NOTE_COUNT_MISMATCH,
    REJECT_DIGEST_MISMATCH,
    REJECT_DUPLICATE_SEQUENCE,
    REJECT_INSTANCE_MISMATCH,
    REJECT_MISSING_SOURCE_NAME,
    REJECT_OVERLAP_CONFLICT,
    REJECT_SOURCE_KEY_MISMATCH,
    REJECT_XML_INVALID,
    TIMESTAMP_FROM_BATCH,
    TIMESTAMP_FROM_OBSERVATION,
    VALUE_EMPTY,
    VALUE_NUMBER,
    VALUE_TEXT,
    VALUE_UNAVAILABLE,
    CanonicalObservationProjection,
    CanonicalObservationStore,
    epoch_micros,
    parse_timestamp,
    rebuild_from_recorder_archive,
)
from catalog.mtconnect_recorder.model import _slug

from .conftest import (
    Observation,
    machine_batch,
    observation_run,
    stamp,
    streams_document,
)

SOURCE = "MACHINE-ALPHA-0001"
OTHER_SOURCE = "MACHINE-BETA-0002"
INSTANCE = 1786093233
RESTARTED_INSTANCE = 1786099999


@pytest.fixture
def projection(recorder_archive, tmp_path):
    database = CanonicalObservationStore(tmp_path / "canonical.sqlite3")
    return CanonicalObservationProjection(
        store=recorder_archive.store, database=database
    )


def _project(projection, *, source=SOURCE, instance=INSTANCE):
    return projection.project_instance(
        source_name=source, agent_instance_id=instance
    )


def _write_run(archive, *, source=SOURCE, instance=INSTANCE, start, count, **kwargs):
    xml_text, _ = machine_batch(
        instance_id=instance, start_sequence=start, count=count, **kwargs
    )
    return archive.write_batch(source_name=source, xml_text=xml_text)


# ---------------------------------------------------------------------------
# Value and category handling
# ---------------------------------------------------------------------------


def test_observation_values_are_typed_not_flattened(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)

    report = _project(projection)
    rows = projection.database.observations_for_agent_instance(
        source_key=SOURCE, agent_instance_id=INSTANCE
    )

    assert report.observations_written == 6
    by_item = {row.data_item_id: row for row in rows}

    integer_load = by_item["xl"]
    assert integer_load.value_kind == VALUE_NUMBER
    assert integer_load.value_number == 3.0
    assert integer_load.native_value == "3"

    decimal_load = by_item["yl"]
    assert decimal_load.value_kind == VALUE_NUMBER
    assert decimal_load.value_number == pytest.approx(39.5)

    # The MTConnect sentinel is a distinct kind, not the string "UNAVAILABLE"
    # and not a silently-null number.
    unavailable = by_item["pf"]
    assert unavailable.value_kind == VALUE_UNAVAILABLE
    assert unavailable.value_number is None
    assert unavailable.value is None
    assert unavailable.native_value == "UNAVAILABLE"

    execution = by_item["exec"]
    assert execution.value_kind == VALUE_TEXT
    assert execution.value_text == "ACTIVE"


def test_all_three_categories_are_preserved(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    _project(projection)

    rows = projection.database.observations_for_agent_instance(
        source_key=SOURCE, agent_instance_id=INSTANCE
    )
    by_item = {row.data_item_id: row for row in rows}

    assert by_item["xl"].category == CATEGORY_SAMPLE
    assert by_item["exec"].category == CATEGORY_EVENT

    condition = by_item["cond_sys"]
    assert condition.category == CATEGORY_CONDITION
    # For a condition the element name is the state and the text is the message.
    assert condition.observation_type == "Warning"
    assert condition.condition_state == "WARNING"
    assert condition.condition_native_code == "1234"
    assert condition.condition_native_severity == "2"
    assert condition.condition_qualifier == "HIGH"
    assert condition.value_text == "ANONYMISED CONTROLLER MESSAGE"


def test_component_identity_survives_without_a_probe(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    report = _project(projection)

    rows = projection.database.observations_for_agent_instance(
        source_key=SOURCE, agent_instance_id=INSTANCE
    )
    load = next(row for row in rows if row.data_item_id == "xl")

    assert report.probe_available is False
    assert load.component_type == "Linear"
    assert load.component_name == "X"
    assert load.component_id == "x1"
    assert load.component_path == "Linear/X"
    assert load.device_uuid == "MACHINE-ALPHA-0001"
    assert load.device_name == "MachineAlpha"


def test_unpromoted_attributes_are_retained(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    _project(projection)

    row = next(
        item
        for item in projection.database.observations_for_agent_instance(
            source_key=SOURCE, agent_instance_id=INSTANCE
        )
        if item.data_item_id == "xl"
    )

    # Promoted attributes do not linger in the residual map, and a promoted
    # attribute must actually land in its field rather than vanishing.
    assert "sequence" not in row.attributes
    assert "timestamp" not in row.attributes
    assert "dataItemId" not in row.attributes
    assert "subType" not in row.attributes
    assert row.sub_type == "ACTUAL"


def test_unknown_attributes_are_kept_verbatim(recorder_archive, projection):
    document = streams_document(
        instance_id=INSTANCE,
        observations=[
            Observation(
                sequence=1,
                component="Rotary",
                component_id="c1",
                component_name="C",
                element="RotaryVelocity",
                data_item_id="srpm",
                value="1200",
                timestamp=stamp(0),
                attributes={"compositionId": "spindle-1", "statistic": "AVERAGE"},
            )
        ],
    )
    recorder_archive.write_batch(source_name=SOURCE, xml_text=document)
    _project(projection)

    row = projection.database.observations_for_agent_instance(
        source_key=SOURCE, agent_instance_id=INSTANCE
    )[0]

    # Attributes with no dedicated field survive rather than being discarded,
    # so the model does not need a column per MTConnect concept.
    assert row.attributes["compositionId"] == "spindle-1"
    assert row.attributes["statistic"] == "AVERAGE"


def test_empty_value_is_distinct_from_unavailable(recorder_archive, projection):
    document = streams_document(
        instance_id=INSTANCE,
        observations=[
            Observation(
                sequence=1,
                component="Path",
                component_id="p1",
                component_name="path",
                category="Events",
                element="ProgramComment",
                data_item_id="cmt",
                value="",
                timestamp=stamp(0),
            )
        ],
    )
    recorder_archive.write_batch(source_name=SOURCE, xml_text=document)

    _project(projection)
    row = projection.database.observations_for_agent_instance(
        source_key=SOURCE, agent_instance_id=INSTANCE
    )[0]

    assert row.value_kind == VALUE_EMPTY
    assert row.value is None


def test_timestamp_falls_back_to_the_manifest_not_the_wall_clock(
    recorder_archive, projection
):
    document = streams_document(
        instance_id=INSTANCE,
        observations=[
            Observation(
                sequence=1,
                component="Controller",
                component_id="c1",
                category="Events",
                element="Execution",
                data_item_id="exec",
                value="READY",
                timestamp=stamp(0),
            )
        ],
    )
    # Strip the timestamp attribute the way a non-compliant adapter would.
    document = document.replace(f' timestamp="{stamp(0)}"', "")
    manifest = recorder_archive.write_batch(source_name=SOURCE, xml_text=document)
    recorder_archive.rewrite_manifest(manifest, received_at="2026-08-07T10:11:12.500Z")

    _project(projection)
    row = projection.database.observations_for_agent_instance(
        source_key=SOURCE, agent_instance_id=INSTANCE
    )[0]

    assert row.timestamp_source == TIMESTAMP_FROM_BATCH
    # Canonicalised to one fixed-width UTC spelling.
    assert row.observed_at == "2026-08-07T10:11:12.500000Z"


def test_canonical_timestamps_have_one_fixed_width(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    _project(projection)

    rows = projection.database.query_observations(source_key=SOURCE)

    # A whole second must not be spelled differently from a fractional one, or
    # text ordering would disagree with time ordering.
    assert {len(row.observed_at) for row in rows} == {len("2026-08-07T09:00:33.000000Z")}
    assert all(row.observed_at.endswith("Z") for row in rows)
    assert [row.observed_at for row in rows] == sorted(row.observed_at for row in rows)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1970-01-01T00:00:00.000000Z", 0),
        ("1970-01-01T00:00:00.000001Z", 1),
        ("1969-12-31T23:59:59.999999Z", -1),
        ("2026-08-07T09:00:33.123456Z", 1786093233123456),
        # Far beyond any machine data, but the point is that the conversion is
        # exact arithmetic rather than exact-for-the-values-we-tried.
        ("2262-04-11T23:47:16.854775Z", 9223372036854775),
    ],
)
def test_epoch_microseconds_are_exact_integers(text, expected):
    assert epoch_micros(parse_timestamp(text)) == expected


def test_epoch_conversion_does_not_go_through_a_float():
    # datetime.timestamp() is a float; at large magnitudes neighbouring
    # microseconds stop being separately representable and rounding decides the
    # result. The canonical conversion must not inherit that.
    moment = parse_timestamp("2262-04-11T23:47:16.854775Z")

    assert epoch_micros(moment) == 9223372036854775
    assert round(moment.timestamp() * 1_000_000) != epoch_micros(moment)


def test_query_bounds_use_the_same_conversion_as_stored_rows(
    recorder_archive, projection
):
    _write_run(recorder_archive, start=1, count=6)
    _project(projection)

    rows = projection.database.query_observations(source_key=SOURCE)
    first = rows[0]

    # A bound written as the row's own timestamp must include that row: if the
    # two conversions ever diverged, an inclusive bound would drop its own value.
    exactly_at = projection.database.query_observations(
        source_key=SOURCE, start=first.observed_at
    )
    assert exactly_at and exactly_at[0].sequence == first.sequence
    assert _as_micros_equivalent(first)


def _as_micros_equivalent(observation) -> bool:
    return epoch_micros(parse_timestamp(observation.observed_at)) == (
        observation.observed_at_us
    )


def test_normal_observations_report_their_own_timestamp(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    _project(projection)

    rows = projection.database.observations_for_agent_instance(
        source_key=SOURCE, agent_instance_id=INSTANCE
    )

    assert {row.timestamp_source for row in rows} == {TIMESTAMP_FROM_OBSERVATION}


# ---------------------------------------------------------------------------
# Identity: replay, overlap, restart, multiple machines
# ---------------------------------------------------------------------------


def test_replaying_a_complete_batch_creates_no_duplicates(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)

    first = _project(projection)
    second = _project(projection)

    assert first.observations_written == 6
    assert first.projected_batches == 1
    # The second run recognises the batch by digest and does no work at all.
    assert second.observations_written == 0
    assert second.projected_batches == 0
    assert second.already_projected_batches == 1
    assert projection.database.count_observations() == 6


def test_a_byte_identical_batch_stored_twice_is_one_logical_batch(
    recorder_archive, projection
):
    xml_text, _ = machine_batch(instance_id=INSTANCE, start_sequence=1)
    recorder_archive.write_batch(source_name=SOURCE, xml_text=xml_text)
    # Storing the same document again is idempotent on disk: the filename
    # carries the digest, so the recorder overwrites rather than duplicates.
    recorder_archive.write_batch(source_name=SOURCE, xml_text=xml_text)

    report = _project(projection)

    assert report.scanned_batches == 1
    assert projection.database.count_observations() == 6


def test_overlapping_batches_do_not_create_duplicates(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    # A re-fetch after a restart legitimately re-delivers sequences 4-6.
    _write_run(recorder_archive, start=4, count=6)

    report = _project(projection)
    rows = projection.database.observations_for_agent_instance(
        source_key=SOURCE, agent_instance_id=INSTANCE
    )

    assert report.projected_batches == 2
    assert report.observations_written == 9
    assert report.observations_already_present == 3
    assert [row.sequence for row in rows] == list(range(1, 10))


def test_an_overlap_that_disagrees_is_rejected_not_merged(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    _project(projection)

    # Same identities, different content: one of these batches is lying.
    tampered = observation_run(start_sequence=4, count=3)
    tampered[0].value = "999"
    document = streams_document(instance_id=INSTANCE, observations=tampered)
    recorder_archive.write_batch(source_name=SOURCE, xml_text=document)

    report = _project(projection)

    assert [item.reason for item in report.rejections] == [REJECT_OVERLAP_CONFLICT]
    # The already-correct rows are untouched.
    assert projection.database.count_observations() == 6
    row = next(
        item
        for item in projection.database.observations_for_agent_instance(
            source_key=SOURCE, agent_instance_id=INSTANCE
        )
        if item.sequence == 4
    )
    assert row.value_text == "ACTIVE"


def test_agent_restart_coexists_with_earlier_observations(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    # After a restart the Agent reports a new instanceId and sequences from 1.
    _write_run(recorder_archive, instance=RESTARTED_INSTANCE, start=1, count=6)

    before = _project(projection)
    after = _project(projection, instance=RESTARTED_INSTANCE)

    assert before.observations_written == 6
    assert after.observations_written == 6
    assert projection.database.count_observations() == 12
    assert (
        len(
            projection.database.observations_for_agent_instance(
                source_key=SOURCE, agent_instance_id=INSTANCE
            )
        )
        == 6
    )
    assert (
        len(
            projection.database.observations_for_agent_instance(
                source_key=SOURCE, agent_instance_id=RESTARTED_INSTANCE
            )
        )
        == 6
    )


def test_two_machines_sharing_an_instance_id_cannot_collide(
    recorder_archive, projection
):
    # instanceId is conventionally the Agent's start time in seconds, so two
    # machines whose Agents started in the same second share it exactly.
    _write_run(recorder_archive, start=1, count=6)
    _write_run(
        recorder_archive,
        source=OTHER_SOURCE,
        start=1,
        count=6,
        device_name="MachineBeta",
        device_uuid="MACHINE-BETA-0002",
    )

    alpha = _project(projection)
    beta = _project(projection, source=OTHER_SOURCE)

    assert alpha.observations_written == 6
    assert beta.observations_written == 6
    assert projection.database.count_observations() == 12
    assert projection.database.count_observations(source_key=SOURCE) == 6
    assert projection.database.count_observations(source_key=OTHER_SOURCE) == 6

    alpha_rows = projection.database.observations_for_agent_instance(
        source_key=SOURCE, agent_instance_id=INSTANCE
    )
    beta_rows = projection.database.observations_for_agent_instance(
        source_key=OTHER_SOURCE, agent_instance_id=INSTANCE
    )
    assert [row.sequence for row in alpha_rows] == [row.sequence for row in beta_rows]
    assert {row.machine_id for row in alpha_rows} == {"MACHINE-ALPHA-0001"}
    assert {row.machine_id for row in beta_rows} == {"MACHINE-BETA-0002"}


def test_repeated_sequence_inside_one_batch_is_rejected_loudly(
    recorder_archive, projection
):
    duplicated = observation_run(start_sequence=1, count=3)
    duplicated[2].sequence = 2
    document = streams_document(instance_id=INSTANCE, observations=duplicated)
    recorder_archive.write_batch(source_name=SOURCE, xml_text=document)

    report = _project(projection)

    assert [item.reason for item in report.rejections] == [REJECT_DUPLICATE_SEQUENCE]
    assert projection.database.count_observations() == 0


# ---------------------------------------------------------------------------
# Failure containment
# ---------------------------------------------------------------------------


def test_a_digest_mismatch_never_enters_the_projection(recorder_archive, projection):
    good = _write_run(recorder_archive, start=1, count=6)
    bad = _write_run(recorder_archive, start=7, count=6)
    payload = recorder_archive.raw_payload_path(bad)
    payload.write_bytes(gzip.compress(b"<MTConnectStreams/>"))

    report = _project(projection)

    assert report.projected_batches == 1
    assert [item.reason for item in report.rejections] == [REJECT_DIGEST_MISMATCH]
    assert projection.database.count_observations() == 6
    assert {row.raw_batch_sha256 for row in projection.database.query_observations()} == {
        recorder_archive.read_manifest(good)["raw_sha256"]
    }


def test_invalid_xml_is_rejected_and_recorded(recorder_archive, projection):
    manifest = _write_run(recorder_archive, start=1, count=6)
    broken = b"<MTConnectStreams><Header truncated"
    recorder_archive.raw_payload_path(manifest).write_bytes(gzip.compress(broken))
    from hashlib import sha256

    recorder_archive.rewrite_manifest(
        manifest, raw_sha256=sha256(broken).hexdigest()
    )

    report = _project(projection)

    assert [item.reason for item in report.rejections] == [REJECT_XML_INVALID]
    assert projection.database.count_observations() == 0
    ledger = projection.database.batches(status=BATCH_STATUS_REJECTED)
    assert len(ledger) == 1
    assert ledger[0].reason == REJECT_XML_INVALID


def test_instance_mismatch_between_document_and_archive_is_rejected(
    recorder_archive, projection
):
    xml_text, _ = machine_batch(instance_id=INSTANCE, start_sequence=1)
    manifest = recorder_archive.write_batch(source_name=SOURCE, xml_text=xml_text)
    # Claim a different Agent instance than the document declares.
    recorder_archive.rewrite_manifest(manifest, agent_instance_id=RESTARTED_INSTANCE)
    moved = manifest.parent.parent.parent / str(RESTARTED_INSTANCE)
    (moved / manifest.parent.name).mkdir(parents=True)
    for source_file in manifest.parent.iterdir():
        source_file.rename(moved / manifest.parent.name / source_file.name)

    report = _project(projection, instance=RESTARTED_INSTANCE)

    assert [item.reason for item in report.rejections] == [REJECT_INSTANCE_MISMATCH]
    assert projection.database.count_observations() == 0


def test_a_rejected_batch_is_retried_once_its_payload_is_restored(
    recorder_archive, projection
):
    manifest = _write_run(recorder_archive, start=1, count=6)
    payload = recorder_archive.raw_payload_path(manifest)
    intact = payload.read_bytes()
    payload.write_bytes(gzip.compress(b"<MTConnectStreams/>"))

    first = _project(projection)
    payload.write_bytes(intact)
    second = _project(projection)

    assert first.rejected_batches == 1
    # Rejection is recorded for visibility but never suppresses a later retry,
    # so restoring evidence from backup is enough to bring the batch in.
    assert second.projected_batches == 1
    assert projection.database.count_observations() == 6
    assert projection.database.batches(status=BATCH_STATUS_REJECTED) == ()


def test_unsupported_manifest_is_reported_and_contributes_nothing(
    recorder_archive, projection
):
    _write_run(recorder_archive, start=1, count=6)
    unsupported = _write_run(recorder_archive, start=7, count=6)
    recorder_archive.rewrite_manifest(unsupported, schema="fcp.mtconnect.raw_batch.v7")

    report = _project(projection)

    assert report.projected_batches == 1
    assert len(report.unsupported_manifests) == 1
    assert report.malformed_manifests == ()
    assert report.ok is False
    assert projection.database.count_observations() == 6


def test_legacy_manifests_project_exactly_like_current_ones(recorder_archive, projection):
    xml_text, _ = machine_batch(instance_id=INSTANCE, start_sequence=1)
    manifest = recorder_archive.write_batch(source_name=SOURCE, xml_text=xml_text)
    current_fingerprint = None

    _project(projection)
    current_fingerprint = projection.database.content_fingerprint()

    # Re-read the same evidence as a pre-rename capture in a fresh database.
    from .conftest import LEGACY_RAW_BATCH_MANIFEST_SCHEMA

    recorder_archive.rewrite_manifest(
        manifest, schema=LEGACY_RAW_BATCH_MANIFEST_SCHEMA
    )
    fresh = CanonicalObservationStore(
        projection.database.database_path.parent / "legacy.sqlite3"
    )
    legacy_projection = CanonicalObservationProjection(
        store=recorder_archive.store, database=fresh
    )
    report = legacy_projection.project_instance(
        source_name=SOURCE, agent_instance_id=INSTANCE
    )

    assert report.observations_written == 6
    assert fresh.content_fingerprint() == current_fingerprint


# ---------------------------------------------------------------------------
# Determinism, rebuild, incremental
# ---------------------------------------------------------------------------


def test_rebuild_from_scratch_reproduces_the_projection(recorder_archive, tmp_path):
    _write_run(recorder_archive, start=1, count=6)
    _write_run(recorder_archive, start=7, count=6)
    _write_run(recorder_archive, instance=RESTARTED_INSTANCE, start=1, count=6)

    first_path = tmp_path / "first.sqlite3"
    second_path = tmp_path / "second.sqlite3"
    original = rebuild_from_recorder_archive(
        data_dir=recorder_archive.data_dir, database_path=first_path
    )
    rebuilt = rebuild_from_recorder_archive(
        data_dir=recorder_archive.data_dir, database_path=second_path
    )

    assert sum(report.observations_written for report in original) == 18
    assert sum(report.observations_written for report in rebuilt) == 18
    assert (
        CanonicalObservationStore(first_path).content_fingerprint()
        == CanonicalObservationStore(second_path).content_fingerprint()
    )


@pytest.mark.parametrize("legacy", [False, True])
def test_archive_only_rebuild_preserves_the_recorded_name_for_both_schemas(
    recorder_archive, tmp_path, legacy
):
    # The same guarantee has to hold for a pre-rename capture: those are the
    # archives most likely to be rebuilt from disk alone, with no configured
    # source list to supply the original name.
    original = "Machine A 01"
    assert _slug(original) != original

    xml_text, _ = machine_batch(
        instance_id=INSTANCE, start_sequence=1, device_uuid=""
    )
    recorder_archive.write_batch(
        source_name=original, xml_text=xml_text, legacy_manifest=legacy
    )

    configured = CanonicalObservationStore(tmp_path / f"configured-{legacy}.sqlite3")
    CanonicalObservationProjection(
        store=recorder_archive.store, database=configured
    ).project_instance(source_name=original, agent_instance_id=INSTANCE)

    discovered_path = tmp_path / f"discovered-{legacy}.sqlite3"
    rebuild_from_recorder_archive(
        data_dir=recorder_archive.data_dir, database_path=discovered_path
    )
    discovered = CanonicalObservationStore(discovered_path)

    assert configured.count_observations() == 6
    assert configured.content_fingerprint() == discovered.content_fingerprint()
    assert {row.source_name for row in discovered.query_observations()} == {original}


def test_a_reference_without_a_recorded_name_is_rejected_not_substituted(
    recorder_archive, projection
):
    # scan_raw_batches never emits such a reference, so this covers a directly
    # constructed one: the projection must refuse rather than substitute the
    # caller's name, which would be the fallback this design removes.
    _write_run(recorder_archive, start=1, count=6)
    scanned = recorder_archive.store.scan_raw_batches(
        source_name=SOURCE, instance_id=INSTANCE
    ).refs[0]
    nameless = replace(scanned, source_name=None)

    outcome = projection._project_batch(
        ref=nameless,
        source_name=nameless.source_name,
        source_key=SOURCE,
        agent_instance_id=INSTANCE,
        probe=None,
    )

    assert outcome.reason == REJECT_MISSING_SOURCE_NAME
    assert projection.database.count_observations() == 0


def test_archive_only_rebuild_preserves_a_source_name_the_slug_rewrites(
    recorder_archive, tmp_path
):
    # The storage partition is _slug(source_name), which is lossy: "Machine A"
    # and "Machine-A" share a directory. The recorded name must therefore come
    # from the manifest, never from the directory it was found in.
    original = "Machine A 01"
    assert _slug(original) != original

    xml_text, _ = machine_batch(
        instance_id=INSTANCE, start_sequence=1, device_uuid=""
    )
    recorder_archive.write_batch(source_name=original, xml_text=xml_text)

    configured_path = tmp_path / "configured.sqlite3"
    configured = CanonicalObservationStore(configured_path)
    CanonicalObservationProjection(
        store=recorder_archive.store, database=configured
    ).project_instance(source_name=original, agent_instance_id=INSTANCE)

    # Archive-only: no configured source list, discovery walks directories.
    discovered_path = tmp_path / "discovered.sqlite3"
    rebuild_from_recorder_archive(
        data_dir=recorder_archive.data_dir, database_path=discovered_path
    )
    discovered = CanonicalObservationStore(discovered_path)

    assert configured.content_fingerprint() == discovered.content_fingerprint()
    for store in (configured, discovered):
        rows = store.query_observations()
        assert rows, "expected the capture to project"
        assert {row.source_name for row in rows} == {original}
        assert {row.source_key for row in rows} == {_slug(original)}
        # A stream with no device identity falls back to the source name, so a
        # slugged name would have changed machine_id too.
        assert {row.machine_id for row in rows} == {original}


def test_a_manifest_naming_another_partition_is_rejected(recorder_archive, projection):
    manifest = _write_run(recorder_archive, start=1, count=6)
    recorder_archive.rewrite_manifest(manifest, source_name="A Completely Other Name")

    report = _project(projection)

    assert [item.reason for item in report.rejections] == [REJECT_SOURCE_KEY_MISMATCH]
    assert projection.database.count_observations() == 0


def test_deleting_the_projection_loses_nothing_recoverable(recorder_archive, tmp_path):
    _write_run(recorder_archive, start=1, count=6)
    database_path = tmp_path / "canonical.sqlite3"
    rebuild_from_recorder_archive(
        data_dir=recorder_archive.data_dir, database_path=database_path
    )
    before = CanonicalObservationStore(database_path).content_fingerprint()

    for stale in database_path.parent.glob("canonical.sqlite3*"):
        stale.unlink()
    rebuild_from_recorder_archive(
        data_dir=recorder_archive.data_dir, database_path=database_path
    )

    assert CanonicalObservationStore(database_path).content_fingerprint() == before


def test_the_raw_capture_is_never_modified(recorder_archive, projection):
    manifest = _write_run(recorder_archive, start=1, count=6)
    payload = recorder_archive.raw_payload_path(manifest)
    manifest_before = manifest.read_bytes()
    payload_before = payload.read_bytes()

    _project(projection)
    _project(projection)

    assert manifest.read_bytes() == manifest_before
    assert payload.read_bytes() == payload_before


def test_a_new_batch_does_not_reparse_history(recorder_archive, projection, monkeypatch):
    _write_run(recorder_archive, start=1, count=6)
    _write_run(recorder_archive, start=7, count=6)
    _project(projection)

    _write_run(recorder_archive, start=13, count=6)
    reads: list[str] = []
    original = recorder_archive.store.read_raw_batch

    def counting_read(ref):
        reads.append(ref.raw_sha256)
        return original(ref)

    monkeypatch.setattr(recorder_archive.store, "read_raw_batch", counting_read)
    report = _project(projection)

    assert report.already_projected_batches == 2
    # Only the new batch was decompressed; the two already-projected batches
    # were skipped on their digest alone.
    assert len(reads) == 1
    assert projection.database.count_observations() == 18


# ---------------------------------------------------------------------------
# Query surface
# ---------------------------------------------------------------------------


def test_observations_are_returned_in_event_order(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    _write_run(recorder_archive, start=7, count=6)
    _project(projection)

    rows = projection.database.query_observations(source_key=SOURCE)

    assert [row.sequence for row in rows] == list(range(1, 13))
    assert [row.observed_at_us for row in rows] == sorted(
        row.observed_at_us for row in rows
    )


def test_ordering_is_total_across_agent_restarts(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    _write_run(recorder_archive, instance=RESTARTED_INSTANCE, start=1, count=6)
    _project(projection)
    _project(projection, instance=RESTARTED_INSTANCE)

    rows = projection.database.query_observations(source_key=SOURCE)

    # Sequence numbers repeat after a restart, so time is the outer ordering and
    # the identity columns make it a total order rather than an arbitrary one.
    assert len(rows) == 12
    keys = [(row.observed_at_us, row.agent_instance_id, row.sequence) for row in rows]
    assert keys == sorted(keys)


def test_time_window_is_inclusive_start_exclusive_end(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    _project(projection)

    window = projection.database.query_observations(
        source_key=SOURCE, start=stamp(0.5), end=stamp(2.0)
    )

    assert [row.sequence for row in window] == [2, 3, 4]


def test_query_by_machine_and_data_item(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    _write_run(recorder_archive, start=7, count=6)
    _project(projection)

    machine = projection.database.observations_for_machine("MACHINE-ALPHA-0001")
    loads = projection.database.observations_for_data_item("xl")

    assert len(machine) == 12
    assert [row.data_item_id for row in loads] == ["xl", "xl"]
    assert [row.sequence for row in loads] == [1, 7]


def test_latest_observation_for_a_data_item(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    _write_run(recorder_archive, start=7, count=6)
    _project(projection)

    latest = projection.database.latest_observation("exec", source_key=SOURCE)

    assert latest is not None
    assert latest.sequence == 10
    assert projection.database.latest_observation("nope") is None


def test_query_for_one_agent_instance_only(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    _write_run(recorder_archive, instance=RESTARTED_INSTANCE, start=1, count=6)
    _project(projection)
    _project(projection, instance=RESTARTED_INSTANCE)

    rows = projection.database.query_observations(
        source_key=SOURCE, agent_instance_id=RESTARTED_INSTANCE
    )

    assert len(rows) == 6
    assert {row.agent_instance_id for row in rows} == {RESTARTED_INSTANCE}


# ---------------------------------------------------------------------------
# Traceability
# ---------------------------------------------------------------------------


def test_every_observation_traces_back_to_its_raw_batch(recorder_archive, projection):
    first = _write_run(recorder_archive, start=1, count=6)
    second = _write_run(recorder_archive, start=7, count=6)
    _project(projection)

    rows = projection.database.query_observations(source_key=SOURCE)
    row = rows[-1]
    batch = projection.database.batch_for_observation(row)

    assert batch is not None
    assert batch.status == BATCH_STATUS_PROJECTED
    assert Path(batch.manifest_path) == second
    assert Path(batch.raw_path).exists()
    assert batch.first_sequence == 7

    # And the digest recorded on the row really is the digest of that evidence.
    from hashlib import sha256

    payload = recorder_archive.raw_payload_path(second)
    assert row.raw_batch_sha256 == sha256(gzip.decompress(payload.read_bytes())).hexdigest()
    assert (
        projection.database.batch_for_digest(
            source_key=SOURCE,
            raw_sha256=recorder_archive.read_manifest(first)["raw_sha256"],
        ).first_sequence
        == 1
    )


def test_identical_content_under_two_sources_traces_to_the_right_one(
    recorder_archive, projection
):
    # Two identically configured machines can record byte-identical payloads,
    # so raw_sha256 alone is not a provenance key.
    xml_text, _ = machine_batch(instance_id=INSTANCE, start_sequence=1)
    alpha_manifest = recorder_archive.write_batch(
        source_name=SOURCE, xml_text=xml_text
    )
    beta_manifest = recorder_archive.write_batch(
        source_name=OTHER_SOURCE, xml_text=xml_text
    )
    _project(projection)
    _project(projection, source=OTHER_SOURCE)

    digest = recorder_archive.read_manifest(alpha_manifest)["raw_sha256"]
    assert digest == recorder_archive.read_manifest(beta_manifest)["raw_sha256"]

    alpha = projection.database.batch_for_digest(source_key=SOURCE, raw_sha256=digest)
    beta = projection.database.batch_for_digest(
        source_key=OTHER_SOURCE, raw_sha256=digest
    )

    assert Path(alpha.manifest_path) == alpha_manifest
    assert Path(beta.manifest_path) == beta_manifest
    assert alpha.source_key != beta.source_key

    for observation in projection.database.query_observations():
        traced = projection.database.batch_for_observation(observation)
        assert traced is not None
        assert traced.source_key == observation.source_key


def test_the_ledger_records_which_manifest_generation_was_read(
    recorder_archive, projection
):
    manifest = _write_run(recorder_archive, start=1, count=6)
    from .conftest import LEGACY_RAW_BATCH_MANIFEST_SCHEMA

    recorder_archive.rewrite_manifest(
        manifest, schema=LEGACY_RAW_BATCH_MANIFEST_SCHEMA
    )
    _project(projection)

    ledger = projection.database.batches(source_key=SOURCE)

    assert len(ledger) == 1
    assert ledger[0].manifest_schema == LEGACY_RAW_BATCH_MANIFEST_SCHEMA
    assert ledger[0].manifest_observation_count == 6
    assert ledger[0].projected_count == 6


def test_ledger_range_comes_from_the_verified_payload(recorder_archive, projection):
    manifest = _write_run(recorder_archive, start=1, count=6)
    # The digest covers the XML, not the sidecar, so a corrupted range survives
    # verification. The ledger must not repeat it as though it were checked.
    recorder_archive.rewrite_manifest(
        manifest, first_observation_sequence=999, last_observation_sequence=4242
    )

    report = _project(projection)
    ledger = projection.database.batches(source_key=SOURCE)

    assert report.projected_batches == 1
    assert ledger[0].status == BATCH_STATUS_PROJECTED
    assert (ledger[0].first_sequence, ledger[0].last_sequence) == (1, 6)


def test_a_declared_count_that_disagrees_is_recorded_not_hidden(
    recorder_archive, projection
):
    manifest = _write_run(recorder_archive, start=1, count=6)
    recorder_archive.rewrite_manifest(manifest, observation_count=99)

    report = _project(projection)
    ledger = projection.database.batches(source_key=SOURCE)[0]

    # Advisory rather than fatal: rejecting here would make a legacy capture
    # whose recorder counted differently unreadable, which is the exact failure
    # this work exists to remove. The disagreement is visible instead.
    assert report.projected_batches == 1
    assert ledger.status == BATCH_STATUS_PROJECTED
    assert ledger.manifest_observation_count == 99
    assert ledger.projected_count == 6
    assert NOTE_COUNT_MISMATCH in (ledger.detail or "")


def test_a_payload_with_no_observations_is_rejected(recorder_archive, projection):
    manifest = _write_run(recorder_archive, start=1, count=6)
    empty = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:1.3">'
        f'<Header creationTime="2026-08-07T09:00:33Z" sender="a" instanceId="{INSTANCE}"'
        ' bufferSize="1" firstSequence="1" lastSequence="1" nextSequence="2"/>'
        "<Streams/></MTConnectStreams>"
    ).encode()
    from hashlib import sha256

    recorder_archive.raw_payload_path(manifest).write_bytes(gzip.compress(empty))
    recorder_archive.rewrite_manifest(manifest, raw_sha256=sha256(empty).hexdigest())

    report = _project(projection)

    assert report.rejected_batches == 1
    assert projection.database.count_observations() == 0


def test_describe_reports_projection_state(recorder_archive, projection):
    _write_run(recorder_archive, start=1, count=6)
    _project(projection)

    described = projection.database.describe()

    assert described["observations"] == 6
    assert described["projected_batches"] == 1
    assert described["rejected_batches"] == 0
    assert described["schema_version"] == 1
