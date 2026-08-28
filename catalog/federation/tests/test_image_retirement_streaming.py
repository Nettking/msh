from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from catalog.federation import image_retirement


class _CountingStream:
    def __init__(self, rows: int) -> None:
        self.rows = rows
        self.read = 0
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self) -> str:
        if self.closed or self.read >= self.rows:
            raise StopIteration
        index = self.read
        self.read += 1
        return json.dumps({"ID": f"sha256:{index:08d}"}) + "\n"

    def close(self) -> None:
        self.closed = True


class _StreamingProcess:
    def __init__(self, rows: int) -> None:
        self.stdout = _CountingStream(rows)
        self.returncode: int | None = None
        self.terminated = False

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = -15

    def kill(self) -> None:
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        if self.returncode is None:
            self.returncode = 0
        return self.returncode


class _BlockedStream:
    def __init__(self) -> None:
        self.closed = threading.Event()

    def __iter__(self):
        return self

    def __next__(self) -> str:
        self.closed.wait(5.0)
        raise StopIteration

    def close(self) -> None:
        self.closed.set()


class _BlockedProcess(_StreamingProcess):
    def __init__(self) -> None:
        super().__init__(0)
        self.stdout = _BlockedStream()


def test_listing_stops_consuming_at_the_row_budget(monkeypatch, tmp_path: Path) -> None:
    process = _StreamingProcess(5000)
    monkeypatch.setattr(image_retirement, "_listing_popen", lambda *_a, **_k: process)

    listed = image_retirement._listed_dangling_fcp_image_ids(
        tmp_path,
        {},
        limit=64,
        deadline=time.monotonic() + 2.0,
    )

    assert listed is not None
    assert len(listed) == 64
    assert process.terminated is True
    # One row may already be queued and one may be in flight when cancellation
    # wins. The lifetime-sized remainder must never be consumed.
    assert process.stdout.read <= 66


def test_stalled_listing_is_cut_off_by_the_pass_deadline(
    monkeypatch, tmp_path: Path
) -> None:
    process = _BlockedProcess()
    monkeypatch.setattr(image_retirement, "_listing_popen", lambda *_a, **_k: process)

    listed = image_retirement._listed_dangling_fcp_image_ids(
        tmp_path,
        {},
        limit=64,
        deadline=time.monotonic() + 0.05,
    )

    assert listed is None
    assert process.terminated is True
