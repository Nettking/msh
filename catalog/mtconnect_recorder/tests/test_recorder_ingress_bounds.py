from __future__ import annotations

import contextlib
import math
import threading
import time
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from catalog.mtconnect_recorder import parsing
from catalog.mtconnect_recorder import runtime as recorder_runtime
from catalog.mtconnect_recorder.model import MtconnectProtocolError
from catalog.mtconnect_recorder.storage import DurableRecorderStore


def _streams_xml(sequences: list[int]) -> str:
    observations = "".join(
        f'<Position dataItemId="x" sequence="{sequence}" '
        f'timestamp="2026-08-24T00:00:{index:02d}Z">{sequence}</Position>'
        for index, sequence in enumerate(sequences)
    )
    first = min(sequences)
    last = max(sequences)
    return (
        '<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:1.7">'
        f'<Header instanceId="7" firstSequence="{first}" '
        f'lastSequence="{last}" nextSequence="{last + 1}"/>'
        '<Streams><DeviceStream name="machine" uuid="machine-1">'
        '<ComponentStream component="Linear" componentId="x">'
        f"<Samples>{observations}</Samples>"
        "</ComponentStream></DeviceStream></Streams></MTConnectStreams>"
    )


PROBE_XML = (
    '<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.7">'
    '<Header instanceId="7"/>'
    '<Devices><Device id="machine" name="machine" uuid="machine-1"/>'
    "</Devices></MTConnectDevices>"
)


class _TestServer(ThreadingHTTPServer):
    daemon_threads = True


@contextlib.contextmanager
def _http_server(
    responder: Callable[[BaseHTTPRequestHandler], None],
) -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            responder(self)

        def log_message(self, _format: str, *args: object) -> None:
            del args

    server = _TestServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize(
    ("endpoint", "fetch"),
    [
        ("current", lambda client: client.fetch_current()),
        ("probe", lambda client: client.fetch_probe()),
        (
            "sample",
            lambda client: client.fetch_sample(from_sequence=1, count=1),
        ),
    ],
)
def test_each_mtconnect_endpoint_rejects_declared_oversized_response(
    endpoint: str,
    fetch: Callable[[recorder_runtime.MtconnectClient], str],
) -> None:
    limit = recorder_runtime.RESPONSE_BYTE_LIMITS[endpoint]

    def respond(handler: BaseHTTPRequestHandler) -> None:
        handler.send_response(200)
        handler.send_header("Content-Type", "application/xml; charset=utf-8")
        handler.send_header("Content-Length", str(limit + 1))
        handler.end_headers()

    with _http_server(respond) as base_url:
        client = recorder_runtime.MtconnectClient(base_url, timeout=1)
        with pytest.raises(
            MtconnectProtocolError,
            match=rf"/{endpoint}.*maximum.*bytes",
        ):
            fetch(client)


def test_streaming_body_limit_does_not_trust_content_length(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        recorder_runtime,
        "RESPONSE_BYTE_LIMITS",
        {"current": 64, "probe": 64, "sample": 64},
        raising=False,
    )
    body = ("<MTConnectStreams>" + (" " * 128) + "</MTConnectStreams>").encode()

    def respond(handler: BaseHTTPRequestHandler) -> None:
        handler.send_response(200)
        handler.send_header("Content-Type", "application/xml; charset=utf-8")
        handler.end_headers()
        handler.wfile.write(body)

    with _http_server(respond) as base_url:
        client = recorder_runtime.MtconnectClient(base_url, timeout=1)
        with pytest.raises(MtconnectProtocolError, match="/current.*maximum.*bytes"):
            client.fetch_current()


def test_request_deadline_stops_a_slow_trickle_before_socket_inactivity_timeout() -> None:
    body = _streams_xml([1]).encode()

    def respond(handler: BaseHTTPRequestHandler) -> None:
        handler.send_response(200)
        handler.send_header("Content-Type", "application/xml; charset=utf-8")
        handler.end_headers()
        try:
            for byte in body:
                handler.wfile.write(bytes((byte,)))
                handler.wfile.flush()
                time.sleep(0.02)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    with _http_server(respond) as base_url:
        client = recorder_runtime.MtconnectClient(base_url, timeout=0.15)
        started = time.monotonic()
        with pytest.raises(MtconnectProtocolError, match="/current.*deadline"):
            client.fetch_current()
        elapsed = time.monotonic() - started

    assert elapsed < 0.75


@pytest.mark.parametrize("timeout", [0.0, -1.0, math.nan, math.inf, 61.0])
def test_client_refuses_non_finite_or_unbounded_request_deadlines(timeout: float) -> None:
    with pytest.raises(ValueError, match="deadline"):
        recorder_runtime.MtconnectClient("http://127.0.0.1:5000", timeout=timeout)


def test_parser_rejects_more_observations_than_the_finite_batch_limit() -> None:
    with pytest.raises(MtconnectProtocolError, match="more than 3 observations"):
        parsing.parse_streams(
            _streams_xml([1, 2, 3, 4]),
            source_name="machine",
            probe=None,
            max_observations=3,
            max_sequence_span=10,
        )


def test_parser_rejects_sequence_span_independently_of_observation_count() -> None:
    with pytest.raises(MtconnectProtocolError, match="sequence span.*100"):
        parsing.parse_streams(
            _streams_xml([1, 101]),
            source_name="machine",
            probe=None,
            max_observations=10,
            max_sequence_span=100,
        )


def test_continuity_validation_never_materializes_the_expected_integer_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch = parsing.parse_streams(
        _streams_xml([1, 3]),
        source_name="machine",
        probe=None,
        max_observations=10,
        max_sequence_span=10,
    )

    def forbidden_range(*args: object) -> object:
        raise AssertionError(f"continuity validation materialized range{args!r}")

    monkeypatch.setattr(parsing, "range", forbidden_range, raising=False)
    with pytest.raises(MtconnectProtocolError, match="missing 2"):
        parsing.validate_batch_continuity(batch, 1)


def test_agent_ignoring_requested_count_cannot_write_raw_or_advance_checkpoint(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sample_xml = _streams_xml([1, 2, 3])
    requested_counts: list[int] = []

    class IgnoringClient:
        def __init__(self, base_url: str, *, timeout: float) -> None:
            del base_url, timeout

        def fetch_current(self) -> str:
            return sample_xml

        def fetch_probe(self) -> str:
            return PROBE_XML

        def fetch_sample(self, *, from_sequence: int, count: int) -> str:
            assert from_sequence == 1
            requested_counts.append(count)
            return sample_xml

    monkeypatch.setattr(recorder_runtime, "MtconnectClient", IgnoringClient)
    monkeypatch.setattr(
        recorder_runtime,
        "MAX_OBSERVATIONS_PER_BATCH",
        2,
        raising=False,
    )
    monkeypatch.setattr(recorder_runtime, "MAX_SEQUENCE_SPAN", 2, raising=False)
    monkeypatch.setattr(recorder_runtime, "BATCH_SIZE", 2)

    runtime = recorder_runtime.RecorderRuntime()
    runtime.store = DurableRecorderStore(tmp_path / "data")
    try:
        _, success, error = runtime.capture_source("machine", "http://agent:5000")
    finally:
        runtime.executor.shutdown(wait=True, cancel_futures=True)
        recorder_runtime.unregister_stop_target(runtime)

    assert requested_counts == [2]
    assert success is False
    assert "more than 2 observations" in error
    assert runtime.checkpoints == {}
    assert runtime.raw_batches_written == 0
    assert not list(runtime.store.raw_root.rglob("*.xml.gz"))
