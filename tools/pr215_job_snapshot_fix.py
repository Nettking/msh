from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file_path = Path(path)
    text = file_path.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one match in {path}, found {count}: {old!r}")
    file_path.write_text(text.replace(old, new, 1), encoding="utf-8")


def append_once(path: str, marker: str, addition: str) -> None:
    file_path = Path(path)
    text = file_path.read_text(encoding="utf-8")
    if marker in text:
        return
    file_path.write_text(text.rstrip() + "\n\n\n" + addition.rstrip() + "\n", encoding="utf-8")


replace_once(
    "catalog/capabilities/job_store.py",
    '''    def snapshot(self, job_id: str) -> DurableJobSnapshot:\n        job_id = _text(job_id, "job_id")\n        with self._connect() as connection:\n            return self._snapshot_from_row(\n                connection,\n                self._row(connection, job_id),\n            )\n''',
    '''    def snapshot(self, job_id: str) -> DurableJobSnapshot:\n        job_id = _text(job_id, "job_id")\n        with self._connect() as connection:\n            # Pin the canonical job row and its normalized attempt rows to one\n            # SQLite read snapshot. Without an explicit read transaction, a\n            # concurrent completion can commit between the two SELECTs and make\n            # a healthy job appear internally inconsistent for one read.\n            connection.execute("BEGIN")\n            return self._snapshot_from_row(\n                connection,\n                self._row(connection, job_id),\n            )\n''',
)

replace_once(
    "catalog/flask_app/tests/test_upload_analysis_jobs.py",
    "import time\n",
    "import threading\nimport time\n",
)
replace_once(
    "catalog/flask_app/tests/test_upload_analysis_jobs.py",
    "from catalog.capabilities.jobs import AttemptStatus, JobStatus\n",
    "from catalog.capabilities.job_store import SQLiteJobStore\nfrom catalog.capabilities.jobs import AttemptStatus, JobStatus\n",
)
append_once(
    "catalog/flask_app/tests/test_upload_analysis_jobs.py",
    "test_job_snapshot_uses_one_read_transaction_during_concurrent_completion",
    '''def test_job_snapshot_uses_one_read_transaction_during_concurrent_completion(\n    tmp_path: Path, monkeypatch\n) -> None:\n    runtime = _Runtime()\n    service = _service(tmp_path, runtime)\n    job_id = service.submit_batch(_batch("upload-concurrent-snapshot"))\n    active = service._activate(job_id)\n    ownership = active.ownership\n    assert ownership is not None\n\n    row_loaded = threading.Event()\n    release_reader = threading.Event()\n    original_snapshot_from_row = service.store._snapshot_from_row\n\n    def paused_snapshot_from_row(connection, row):\n        row_loaded.set()\n        assert release_reader.wait(timeout=5)\n        return original_snapshot_from_row(connection, row)\n\n    monkeypatch.setattr(service.store, "_snapshot_from_row", paused_snapshot_from_row)\n    reader_result: list[object] = []\n    reader_errors: list[BaseException] = []\n\n    def read_snapshot() -> None:\n        try:\n            reader_result.append(service.store.snapshot(job_id))\n        except BaseException as exc:  # noqa: BLE001 - test records cross-thread failure\n            reader_errors.append(exc)\n\n    reader = threading.Thread(target=read_snapshot)\n    reader.start()\n    assert row_loaded.wait(timeout=5)\n\n    writer = SQLiteJobStore(service.database)\n    writer.complete(\n        job_id,\n        coordinator_id="node-upload-tests",\n        owner_provider_id=ownership.owner_provider_id,\n        attempt_id=ownership.attempt_id,\n        lease_id=ownership.lease_id,\n        command_id="complete-concurrent-snapshot",\n        expected_revision=active.revision,\n        terminal_status=AttemptStatus.SUCCEEDED,\n        error_code=None,\n        now=service.clock(),\n    )\n    release_reader.set()\n    reader.join(timeout=5)\n\n    assert not reader.is_alive()\n    assert reader_errors == []\n    assert len(reader_result) == 1\n    observed = reader_result[0]\n    assert observed.job.status is JobStatus.ACTIVE\n    assert observed.job.attempts[-1].status is AttemptStatus.RUNNING\n    assert writer.snapshot(job_id).job.status is JobStatus.SUCCEEDED\n''',
)
