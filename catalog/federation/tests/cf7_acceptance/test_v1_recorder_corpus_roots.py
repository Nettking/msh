"""Offline layout regressions; these fixtures are never physical evidence."""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

from catalog.mtconnect_recorder.parsing import parse_streams
from catalog.mtconnect_recorder.storage import DurableRecorderStore
from scripts.acceptance import v1_physical_probes as probes
from scripts.acceptance.v1_physical_runtime_binding import RuntimeBinding


def _archive(data: Path, *, batches: int = 34, value_size: int = 32768,
             legacy: bool = False) -> tuple[list[Path], list[Path]]:
    """Write distinct test batches through the production parser and store."""
    store = DurableRecorderStore(data)
    if legacy:
        store.raw_root = data / "raw"
        store.observation_root = data / "observations"
    raw_files: list[Path] = []
    observation_files: list[Path] = []
    for sequence in range(1, batches + 1):
        xml = (
            '<MTConnectStreams><Header instanceId="1" firstSequence="1" '
            f'lastSequence="{sequence}" nextSequence="{sequence + 1}"/>'
            '<Streams><DeviceStream name="offline-machine" uuid="offline-machine">'
            '<ComponentStream component="Controller" componentId="controller"><Events>'
            f'<Message dataItemId="message" sequence="{sequence}" '
            'timestamp="2026-09-28T00:00:00Z">'
            + "x" * value_size
            + '</Message></Events></ComponentStream></DeviceStream></Streams>'
            '</MTConnectStreams>'
        )
        batch = parse_streams(xml, source_name="offline-machine", probe=None,
                              received_at="2026-09-28T00:00:01Z")
        raw = store.store_raw_batch(source_name="offline-machine",
                                   requested_from=sequence, xml_text=xml, batch=batch)
        observations = store.store_observation_batch(
            source_name="offline-machine", batch=batch, raw_sha256=raw.raw_sha256,
        )
        raw_files.extend((raw.raw_path, raw.manifest_path))
        observation_files.append(observations)
    return raw_files, observation_files


def _context(tmp_path: Path) -> probes.ProbeContext:
    runtime = RuntimeBinding(
        host_id="offline-recorder", target_candidate_sha="a" * 40,
        acceptance_harness_sha="a" * 40, harness_checkout=tmp_path / "harness",
        kind="compose", data_root=tmp_path / "runtime-data",
        results_root=tmp_path / "runtime-results",
    )
    return probes.ProbeContext(
        checkout=runtime.harness_checkout, evidence_root=tmp_path / "evidence",
        commit="a" * 40, host_id=runtime.host_id, os_category="posix",
        profile="cnc-recorder", scenario="P07", assertion="aged-corpus",
        runtime_binding=runtime,
    )


@pytest.mark.parametrize("legacy", [False, True])
def test_corpus_counts_recorder_store_artifacts_with_external_data_binding(
    tmp_path: Path, legacy: bool,
) -> None:
    context = _context(tmp_path)
    raw, observations = _archive(context.data_dir, legacy=legacy)
    federation = context.data_dir / "federation"
    federation.mkdir()
    with sqlite3.connect(federation / "offline-history.sqlite3") as database:
        database.execute("CREATE TABLE offline_history (id INTEGER PRIMARY KEY)")

    outcome = probes.PROBES["corpus-size"].run(context)

    assert outcome.status == probes.PASS
    assert outcome.detail["files"] == len(raw) + len(observations) == 102
    assert outcome.detail["bytes"] == sum(p.stat().st_size for p in raw + observations)
    assert outcome.detail["min_files"] == 100
    assert outcome.detail["min_bytes"] == 1024 * 1024
    # Resolving Recorder archives must not rebase the Federation/history root.
    assert outcome.detail["database_count"] == 1
    assert context.data_dir == tmp_path / "runtime-data"
    extras = probes.collect_sample_extras(context.checkout, context.runtime_binding)
    assert extras["recorder"]["raw_files"] == len(raw)
    assert extras["recorder"]["raw_bytes"] == sum(p.stat().st_size for p in raw)
    assert extras["history"]["database_count"] == 1


@pytest.mark.parametrize(("batches", "value_size", "missing"), [
    (33, 32768, "files"),
    (34, 1, "bytes"),
])
def test_canonical_corpus_still_requires_both_existing_minimums(
    tmp_path: Path, batches: int, value_size: int, missing: str,
) -> None:
    context = _context(tmp_path)
    _archive(context.data_dir, batches=batches, value_size=value_size)

    outcome = probes.PROBES["corpus-size"].run(context)

    assert outcome.status == probes.FAIL
    other = "bytes" if missing == "files" else "files"
    assert outcome.detail[missing] < outcome.detail["min_" + missing]
    assert outcome.detail[other] >= outcome.detail["min_" + other]


def test_canonical_layout_does_not_double_count_legacy_mirrors(tmp_path: Path) -> None:
    context = _context(tmp_path)
    raw, observations = _archive(context.data_dir, batches=33)
    canonical = context.data_dir / "sources" / "mtconnect_recorder"
    for name in ("raw", "observations"):
        shutil.copytree(canonical / name, context.data_dir / name)

    outcome = probes.PROBES["corpus-size"].run(context)
    extras = probes.collect_sample_extras(context.checkout, context.runtime_binding)

    assert outcome.status == probes.FAIL
    assert outcome.detail["files"] == len(raw) + len(observations) == 99
    assert extras["recorder"]["raw_files"] == len(raw)
    assert extras["recorder"]["raw_bytes"] == sum(p.stat().st_size for p in raw)
