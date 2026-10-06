from __future__ import annotations

import contextlib
import gzip
import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from catalog.mtconnect_recorder import runtime as recorder_runtime
from catalog.mtconnect_recorder.model import MtconnectProtocolError, SourceCheckpoint
from catalog.mtconnect_recorder.storage import DurableRecorderStore

_INSTANCE_ID = 77
_SOURCE = "synthetic"
_PROBE = (
    '<MTConnectDevices><Header instanceId="77"/><Devices>'
    '<Device id="synthetic" name="Synthetic" uuid="SYNTHETIC-001"/>'
    "</Devices></MTConnectDevices>"
)


def _streams(
    first: int,
    *,
    instance_id: int = _INSTANCE_ID,
    discontinuous: bool = False,
    last_sequence: int | None = None,
    buffer_first_sequence: int | None = None,
) -> str:
    # /sample can report the Agent's latest lastSequence while nextSequence
    # identifies the next cursor after just the returned observations.
    last = first + 2 if last_sequence is None else last_sequence
    buffer_first = first if buffer_first_sequence is None else buffer_first_sequence
    next_sequence = first + 3
    sequences = [first, first + 2] if discontinuous else range(first, first + 3)
    observations = "".join(
        f'<Position dataItemId="x" sequence="{sequence}" '
        f'timestamp="2026-10-06T00:00:00Z">{sequence}</Position>'
        for sequence in sequences
    )
    return (
        "<MTConnectStreams>"
        f'<Header instanceId="{instance_id}" firstSequence="{buffer_first}" '
        f'lastSequence="{last}" nextSequence="{next_sequence}"/>'
        '<Streams><DeviceStream name="Synthetic" uuid="SYNTHETIC-001">'
        '<ComponentStream component="Linear"><Samples>'
        f"{observations}</Samples></ComponentStream></DeviceStream></Streams>"
        "</MTConnectStreams>"
    )


class _Server(ThreadingHTTPServer):
    daemon_threads = True


@contextlib.contextmanager
def _agent(mode: str) -> Iterator[tuple[str, dict[str, object]]]:
    state: dict[str, object] = {
        "current_calls": 0,
        "front": 10,
        "sample_requests": [],
        "responses": [],
    }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            params = parse_qs(parsed.query)
            status_code = 200
            if parsed.path == "/probe":
                body = _PROBE
            elif parsed.path == "/current":
                index = int(state["current_calls"])
                state["current_calls"] = index + 1
                state["front"] = 10 + index * 6
                body = _streams(int(state["front"]))
            elif parsed.path == "/sample":
                requests = state["sample_requests"]
                assert isinstance(requests, list)
                requests.append(params)
                if mode == "http_404_rolling_frontier":
                    requested = int(params["from"][0]) if "from" in params else None
                    if requested == 10:
                        status_code = 404
                        state["front"] = 109
                        body = (
                            "<MTConnectError><Errors>"
                            '<Error errorCode="OUT_OF_RANGE">expired cursor</Error>'
                            "</Errors></MTConnectError>"
                        )
                    elif requested is not None:
                        body = _streams(
                            requested, last_sequence=109, buffer_first_sequence=101
                        )
                    else:
                        body = _streams(101, last_sequence=109)
                    if status_code == 200:
                        responses = state["responses"]
                        assert isinstance(responses, list)
                        responses.append(body)
                elif "from" in params and mode in {
                    "http_404_out_of_range",
                    "http_404_invalid_request",
                    "http_404_malformed",
                    "http_500_out_of_range",
                }:
                    status_code = 500 if mode == "http_500_out_of_range" else 404
                    if mode == "http_404_malformed":
                        body = "not an MTConnect XML error document"
                    else:
                        code = (
                            "OUT_OF_RANGE"
                            if mode != "http_404_invalid_request"
                            else "INVALID_REQUEST"
                        )
                        body = (
                            "<MTConnectError><Errors>"
                            f'<Error errorCode="{code}">synthetic expired sequence</Error>'
                            "</Errors></MTConnectError>"
                        )
                elif mode == "permissive_roll":
                    assert "from" in params
                    body = _streams(int(params["from"][0]) + 3)
                elif "from" in params or mode in {
                    "repeat_out_of_range",
                    "other_error",
                }:
                    code = (
                        "INVALID_REQUEST" if mode == "other_error" else "OUT_OF_RANGE"
                    )
                    body = (
                        "<MTConnectError><Errors>"
                        f'<Error errorCode="{code}">synthetic expired sequence</Error>'
                        "</Errors></MTConnectError>"
                    )
                    state["front"] = int(state["front"]) + 3
                else:
                    body = _streams(
                        int(state["front"]),
                        instance_id=78 if mode == "new_instance" else _INSTANCE_ID,
                        discontinuous=mode == "discontinuous",
                    )
                    responses = state["responses"]
                    assert isinstance(responses, list)
                    responses.append(body)
            else:
                self.send_error(404)
                return

            payload = body.encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/xml; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format: str, *args: object) -> None:
            del args

    server = _Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _runtime(
    tmp_path: Path,
    endpoint: str,
    monkeypatch: pytest.MonkeyPatch,
) -> recorder_runtime.RecorderRuntime:
    monkeypatch.setattr(recorder_runtime, "DATA_DIR", tmp_path / "default-data")
    monkeypatch.setattr(recorder_runtime, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(recorder_runtime, "REQUEST_TIMEOUT", 2)
    worker = recorder_runtime.RecorderRuntime()
    worker.store = DurableRecorderStore(tmp_path / "data")
    worker.checkpoints[_SOURCE] = SourceCheckpoint(
        source_name=_SOURCE,
        base_url=endpoint,
        machine_id="SYNTHETIC-001",
        agent_instance_id=_INSTANCE_ID,
        next_sequence=10,
        probe_sha256="",
    )
    return worker


def _close(worker: recorder_runtime.RecorderRuntime) -> None:
    worker.executor.shutdown(wait=True, cancel_futures=True)
    recorder_runtime.unregister_stop_target(worker)


def test_out_of_range_fallback_archives_available_sample_and_persists_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _agent("out_of_range") as (endpoint, state):
        worker = _runtime(tmp_path, endpoint, monkeypatch)
        try:
            result = worker.capture_source(_SOURCE, endpoint)

            assert result.success and result.transaction_complete
            assert worker.checkpoints[_SOURCE].next_sequence == 16
            assert worker.raw_batches_written == 1
            assert worker.observations_written == 3
            raw_paths = list(worker.store.raw_root.rglob("*.xml.gz"))
            assert len(raw_paths) == 1
            raw = gzip.decompress(raw_paths[0].read_bytes()).decode("utf-8")
            assert raw == state["responses"][0]
            gap = json.loads(
                (
                    worker.store.gap_root
                    / _SOURCE
                    / str(_INSTANCE_ID)
                    / "gap-10-12.json"
                ).read_text(encoding="utf-8")
            )
            assert (gap["missing_from"], gap["missing_to"], gap["reason"]) == (
                10,
                12,
                "agent_buffer_overflow",
            )
            persisted = json.loads(
                recorder_runtime.STATE_FILE.read_text(encoding="utf-8")
            )
            assert persisted["sources"][_SOURCE]["next_sequence"] == 16
            assert worker.checkpoints[_SOURCE].last_raw_file == str(raw_paths[0])
            assert state["sample_requests"] == [
                {"from": ["10"], "count": [str(recorder_runtime.BATCH_SIZE)]},
                {"count": [str(recorder_runtime.BATCH_SIZE)]},
            ]
        finally:
            _close(worker)


def test_out_of_range_fallback_drains_against_advanced_sample_frontier(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _agent("http_404_rolling_frontier") as (endpoint, state):
        worker = _runtime(tmp_path, endpoint, monkeypatch)
        try:
            result = worker.capture_source(_SOURCE, endpoint)

            assert result.success and result.transaction_complete
            assert worker.raw_batches_written == 3
            assert worker.observations_written == 9
            assert worker.checkpoints[_SOURCE].next_sequence == 110
            source = worker.source_status[_SOURCE]
            assert source["agent_last_sequence"] == 109
            assert source["caught_up"] is True
            assert len(list(worker.store.raw_root.rglob("*.xml.gz"))) == 3
            gap = json.loads(
                (
                    worker.store.gap_root
                    / _SOURCE
                    / str(_INSTANCE_ID)
                    / "gap-10-100.json"
                ).read_text(encoding="utf-8")
            )
            assert (gap["missing_from"], gap["missing_to"], gap["reason"]) == (
                10,
                100,
                "agent_buffer_overflow",
            )
            requests = state["sample_requests"]
            assert requests == [
                {"from": ["10"], "count": [str(recorder_runtime.BATCH_SIZE)]},
                {"count": [str(recorder_runtime.BATCH_SIZE)]},
                {"from": ["104"], "count": [str(recorder_runtime.BATCH_SIZE)]},
                {"from": ["107"], "count": [str(recorder_runtime.BATCH_SIZE)]},
            ]
        finally:
            _close(worker)


def test_advancing_sample_window_is_committed_on_successive_capture_cycles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _agent("permissive_roll") as (endpoint, state):
        worker = _runtime(tmp_path, endpoint, monkeypatch)
        try:
            results = [
                worker.capture_source(_SOURCE, endpoint),
                worker.capture_source(_SOURCE, endpoint),
            ]

            assert all(item.success and item.transaction_complete for item in results)
            assert worker.raw_batches_written == 2
            assert worker.checkpoints[_SOURCE].next_sequence == 22
            assert len(list(worker.store.raw_root.rglob("*.xml.gz"))) == 2
            assert (
                worker.store.gap_root / _SOURCE / str(_INSTANCE_ID) / "gap-10-12.json"
            ).is_file()
            assert (
                worker.store.gap_root / _SOURCE / str(_INSTANCE_ID) / "gap-16-18.json"
            ).is_file()
            assert len(state["sample_requests"]) == 2
        finally:
            _close(worker)


@pytest.mark.parametrize(
    ("mode", "error", "expected_requests"),
    [
        ("other_error", "INVALID_REQUEST", 1),
        ("repeat_out_of_range", "OUT_OF_RANGE", 2),
        ("new_instance", "instance changed", 2),
        ("discontinuous", "missing 14", 2),
    ],
)
def test_fallback_remains_bounded_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    error: str,
    expected_requests: int,
) -> None:
    with _agent(mode) as (endpoint, state):
        worker = _runtime(tmp_path, endpoint, monkeypatch)
        try:
            result = worker.capture_source(_SOURCE, endpoint)

            assert not result.success
            assert not result.transaction_complete
            assert error in result.error
            assert worker.checkpoints[_SOURCE].next_sequence == 10
            assert worker.raw_batches_written == 0
            assert not list(worker.store.raw_root.rglob("*.xml.gz"))
            assert len(state["sample_requests"]) == expected_requests
            assert worker.next_attempt_at[_SOURCE] > 0
            assert worker.backoff[_SOURCE] > 0
        finally:
            _close(worker)


def test_mtconnect_other_errors_do_not_trigger_fallback() -> None:
    with _agent("other_error") as (endpoint, state):
        client = recorder_runtime.MtconnectClient(endpoint, timeout=2)
        with pytest.raises(MtconnectProtocolError, match="INVALID_REQUEST"):
            client.fetch_sample(from_sequence=10, count=recorder_runtime.BATCH_SIZE)
        assert state["sample_requests"] == [
            {"from": ["10"], "count": [str(recorder_runtime.BATCH_SIZE)]}
        ]


def test_standard_http_404_out_of_range_retries_without_expired_cursor() -> None:
    with _agent("http_404_out_of_range") as (endpoint, state):
        client = recorder_runtime.MtconnectClient(endpoint, timeout=2)

        body = client.fetch_sample(from_sequence=10, count=recorder_runtime.BATCH_SIZE)

        assert recorder_runtime.parse_stream_header(body).first_sequence == 10
        assert state["sample_requests"] == [
            {"from": ["10"], "count": [str(recorder_runtime.BATCH_SIZE)]},
            {"count": [str(recorder_runtime.BATCH_SIZE)]},
        ]


@pytest.mark.parametrize(
    "mode",
    ["http_404_invalid_request", "http_404_malformed", "http_500_out_of_range"],
)
def test_non_out_of_range_http_errors_do_not_trigger_fallback(mode: str) -> None:
    with _agent(mode) as (endpoint, state):
        client = recorder_runtime.MtconnectClient(endpoint, timeout=2)

        with pytest.raises(requests.HTTPError):
            client.fetch_sample(from_sequence=10, count=recorder_runtime.BATCH_SIZE)

        assert state["sample_requests"] == [
            {"from": ["10"], "count": [str(recorder_runtime.BATCH_SIZE)]}
        ]
