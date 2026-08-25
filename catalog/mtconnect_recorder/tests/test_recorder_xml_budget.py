from __future__ import annotations

import pytest

from catalog.mtconnect_recorder import xml_budget
from catalog.mtconnect_recorder.model import MtconnectProtocolError
from catalog.mtconnect_recorder.parsing import parse_stream_header


def _header(inner: str = "") -> str:
    return (
        '<MTConnectStreams><Header instanceId="1" firstSequence="1" '
        'lastSequence="1" nextSequence="2"/>'
        f"{inner}</MTConnectStreams>"
    )


def test_xml_element_count_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(xml_budget, "MAX_XML_ELEMENTS", 3)

    with pytest.raises(MtconnectProtocolError, match="element-count"):
        parse_stream_header(_header("<A/><B/>"))


def test_xml_depth_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(xml_budget, "MAX_XML_DEPTH", 2)

    with pytest.raises(MtconnectProtocolError, match="nesting-depth"):
        parse_stream_header(_header("<A><B/></A>"))


def test_xml_attributes_per_element_are_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(xml_budget, "MAX_XML_ATTRIBUTES_PER_ELEMENT", 4)

    with pytest.raises(MtconnectProtocolError, match="attribute-count"):
        parse_stream_header(
            '<MTConnectStreams a="1" b="2" c="3" d="4" e="5">'
            '<Header instanceId="1" firstSequence="1" lastSequence="1" '
            'nextSequence="2"/></MTConnectStreams>'
        )


def test_xml_text_content_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(xml_budget, "MAX_XML_TEXT_BYTES", 4)

    with pytest.raises(MtconnectProtocolError, match="text-content"):
        parse_stream_header(_header("<A>12345</A>"))


def test_malformed_xml_uses_recorder_protocol_error() -> None:
    with pytest.raises(MtconnectProtocolError, match="well-formed XML"):
        parse_stream_header("<MTConnectStreams>")


def test_budgeted_header_parser_preserves_normal_header_semantics() -> None:
    header = parse_stream_header(_header())

    assert header.instance_id == 1
    assert header.first_sequence == 1
    assert header.last_sequence == 1
    assert header.next_sequence == 2
