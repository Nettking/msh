"""Behaviour tests for the checked-in P01-P12 physical probes.

These exercise real probe bodies rather than stubs. Every probe must be safe to
run on an ordinary developer machine and on a CI runner: read-only, bounded, and
honest about what it could not observe.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
from pathlib import Path

import pytest

from scripts.acceptance import v1_physical_probes as probes
from scripts.acceptance.v1_physical_automation import PLANS

COMMIT = "a" * 40


def context(
    tmp_path: Path,
    *,
    scenario: str = "P01",
    assertion: str = "posix-runtime-state",
    options: dict[str, str] | None = None,
    evidence_root: Path | None = None,
) -> probes.ProbeContext:
    return probes.ProbeContext(
        checkout=tmp_path,
        evidence_root=evidence_root or (tmp_path / "evidence"),
        commit=COMMIT,
        host_id="nitro",
        os_category="posix",
        profile="school-control",
        scenario=scenario,
        assertion=assertion,
        options=options or {},
    )


def test_every_registered_probe_is_reachable_from_the_classification() -> None:
    referenced = {
        probe_id
        for plans in PLANS.values()
        for plan in plans.values()
        for probe_id in plan.all_probe_ids()
    }
    assert referenced <= set(probes.PROBES)
    unused = sorted(set(probes.PROBES) - referenced)
    assert not unused, f"probes are checked in but never used: {unused}"


def test_probe_ids_are_stable_and_self_describing() -> None:
    for probe_id, spec in probes.PROBES.items():
        assert spec.probe_id == probe_id
        assert spec.title.strip()
        assert spec.os_category in {None, "windows", "posix"}


def test_recorder_path_containment_refuses_every_hostile_component(
    tmp_path: Path,
) -> None:
    outcome = probes.PROBES["recorder-path-containment"].run(context(tmp_path))
    assert outcome.status == probes.PASS
    assert outcome.detail["escaped"] == []
    assert outcome.detail["hostile_cases"] >= 3


def test_recorder_limits_reports_finite_declared_bounds(tmp_path: Path) -> None:
    outcome = probes.PROBES["recorder-limits"].run(context(tmp_path))
    assert outcome.status == probes.PASS
    assert outcome.detail["unbounded"] == []
    assert outcome.detail["declared_limits"]["MAX_SEQUENCE_SPAN"] > 0


def test_safe_storage_refusal_writes_nothing(tmp_path: Path) -> None:
    before = sorted(path.name for path in tmp_path.iterdir())
    outcome = probes.PROBES["safe-storage-refusal"].run(context(tmp_path))
    assert outcome.status in {probes.PASS, probes.UNAVAILABLE}
    assert outcome.detail.get("bytes_written") == 0
    if outcome.status == probes.PASS:
        assert outcome.detail["impossible_reservation_refused"] is True
    assert sorted(path.name for path in tmp_path.iterdir()) == before


def test_sqlite_integrity_is_unavailable_without_a_database(tmp_path: Path) -> None:
    outcome = probes.PROBES["sqlite-integrity"].run(context(tmp_path))
    assert outcome.status == probes.UNAVAILABLE
    assert outcome.detail["database_count"] == 0


def test_sqlite_integrity_checks_a_real_database(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    database = data / "control.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE example (id INTEGER PRIMARY KEY)")
        connection.execute("INSERT INTO example (id) VALUES (1)")
    outcome = probes.PROBES["sqlite-integrity"].run(context(tmp_path))
    assert outcome.status == probes.PASS
    assert outcome.detail["database_count"] == 1
    assert outcome.detail["failed"] == 0
    assert "control" not in json.dumps(outcome.detail)


def test_publication_idempotency_detects_duplicate_durable_identities(
    tmp_path: Path,
) -> None:
    data = tmp_path / "data"
    data.mkdir()
    database = data / "outbox.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE outbox ("
            " id INTEGER PRIMARY KEY,"
            " session_id TEXT NOT NULL,"
            " destination_id TEXT NOT NULL,"
            " idempotency_key TEXT NOT NULL,"
            " state TEXT NOT NULL)"
        )
        connection.executemany(
            "INSERT INTO outbox"
            " (session_id, destination_id, idempotency_key, state)"
            " VALUES (?, ?, ?, ?)",
            [
                ("s", "d", "one", "pending"),
                ("s", "d", "one", "pending"),
            ],
        )
    outcome = probes.PROBES["publication-idempotency"].run(context(tmp_path))
    assert outcome.status == probes.FAIL
    assert outcome.detail["outboxes"][0]["duplicates"] == 1


def test_publication_backlog_reports_state_counts(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    database = data / "outbox.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE outbox ("
            " id INTEGER PRIMARY KEY,"
            " session_id TEXT NOT NULL,"
            " destination_id TEXT NOT NULL,"
            " idempotency_key TEXT NOT NULL,"
            " state TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO outbox"
            " (session_id, destination_id, idempotency_key, state)"
            " VALUES ('s', 'd', 'one', 'pending')"
        )
    outcome = probes.PROBES["publication-backlog"].run(context(tmp_path))
    assert outcome.status == probes.PASS
    assert outcome.detail["total_entries"] == 1


def test_service_health_is_unavailable_on_a_host_with_no_product(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(probes, "_docker_available", lambda: False)
    outcome = probes.PROBES["service-health"].run(context(tmp_path))
    assert outcome.status == probes.UNAVAILABLE


def test_host_mutation_serialization_refuses_a_second_actor(
    tmp_path: Path,
) -> None:
    completed = subprocess.run(
        ["git", "init", "--quiet", str(tmp_path)],
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        pytest.skip("git is required to resolve the host mutation lock path")
    outcome = probes.PROBES["host-mutation-serialization"].run(context(tmp_path))
    if outcome.status == probes.UNAVAILABLE:
        pytest.skip("this platform does not expose the host mutation lock here")
    assert outcome.status == probes.PASS
    assert outcome.detail["second_actor_refused"] is True
    assert outcome.detail["lock_released_after_use"] is True


def test_update_entrypoint_disposition_reads_the_checked_in_contract(
    tmp_path: Path,
) -> None:
    (tmp_path / "update.cmd").write_text(
        "echo update.cmd is retired\nexit /b 2\n",
        encoding="utf-8",
    )
    outcome = probes.PROBES["update-entrypoint-disposition"].run(context(tmp_path))
    assert outcome.status == probes.PASS
    assert outcome.detail["declares_retired"] is True
    assert outcome.detail["refuses_with_exit_code_2"] is True


def test_update_entrypoint_disposition_fails_a_bypassing_script(
    tmp_path: Path,
) -> None:
    (tmp_path / "update.cmd").write_text("git pull\nexit /b 0\n", encoding="utf-8")
    outcome = probes.PROBES["update-entrypoint-disposition"].run(context(tmp_path))
    assert outcome.status == probes.FAIL


def test_repository_update_entrypoint_still_matches_its_disposition() -> None:
    outcome = probes.PROBES["update-entrypoint-disposition"].run(
        probes.ProbeContext(
            checkout=Path.cwd(),
            evidence_root=Path.cwd() / "evidence",
            commit=COMMIT,
            host_id="ci",
            os_category="posix",
            profile="school-control",
            scenario="P03",
            assertion="update-cmd-disposition",
        )
    )
    assert outcome.status == probes.PASS


def test_backup_preflight_requires_a_declared_independent_destination(
    tmp_path: Path,
) -> None:
    outcome = probes.PROBES["backup-preflight"].run(context(tmp_path))
    assert outcome.status == probes.UNAVAILABLE
    assert outcome.detail["destination_declared"] is False

    destination = tmp_path / "backup"
    destination.mkdir()
    outcome = probes.PROBES["backup-preflight"].run(
        context(tmp_path, options={"destination": str(destination)})
    )
    assert outcome.status == probes.FAIL
    assert outcome.detail["destination_is_independent"] is False


def test_corpus_size_fails_an_empty_installation(tmp_path: Path) -> None:
    data = tmp_path / "data" / "raw"
    data.mkdir(parents=True)
    (data / "one.json").write_text("{}", encoding="utf-8")
    outcome = probes.PROBES["corpus-size"].run(
        context(tmp_path, options={"subject": "recorder"})
    )
    assert outcome.status == probes.FAIL
    assert outcome.detail["files"] == 1


def test_collect_sample_extras_returns_every_required_series(tmp_path: Path) -> None:
    extras = probes.collect_sample_extras(tmp_path)
    assert set(extras) == {
        "recorder",
        "publication",
        "history",
        "orphan",
        "cpu_ram",
    }
    assert extras["recorder"]["source_count"] == 0


def test_probe_detail_never_carries_a_raw_source_name(tmp_path: Path) -> None:
    state = tmp_path / "data" / "source_state"
    state.mkdir(parents=True)
    (state / "mtconnect_recorder_state.json").write_text(
        json.dumps(
            {
                "schema": "fcp.recorder.checkpoints.v1",
                "sources": {
                    "private-machine-name": {
                        "agent_instance_id": 1,
                        "next_sequence": 42,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    outcome = probes.PROBES["recorder-continuity"].run(context(tmp_path))
    assert outcome.status == probes.PASS
    assert "private-machine-name" not in json.dumps(outcome.detail)
    assert outcome.detail["source_count"] == 1
