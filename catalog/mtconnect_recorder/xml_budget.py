"""Structural budget validation for bounded MTConnect XML documents."""
from __future__ import annotations

import io
from xml.etree import ElementTree as ET

from .limits import (
    MAX_XML_ATTRIBUTES_PER_ELEMENT,
    MAX_XML_DEPTH,
    MAX_XML_ELEMENTS,
    MAX_XML_TEXT_BYTES,
)
from .model import MtconnectProtocolError


def validate_xml_budget(xml_text: str) -> None:
    """Reject XML whose object graph/text budget exceeds the recorder envelope.

    The HTTP client already bounds encoded response bytes. This streaming pass
    limits the ElementTree shape before production parsers build a retained tree,
    preventing many tiny tags/attributes from amplifying one accepted response
    into an uncontrolled in-memory object graph.
    """

    element_count = 0
    depth = 0
    text_bytes = 0
    try:
        for event, element in ET.iterparse(
            io.StringIO(xml_text),
            events=("start", "end"),
        ):
            if event == "start":
                element_count += 1
                depth += 1
                if element_count > MAX_XML_ELEMENTS:
                    raise MtconnectProtocolError(
                        "MTConnect XML exceeds the finite element-count limit "
                        f"of {MAX_XML_ELEMENTS}."
                    )
                if depth > MAX_XML_DEPTH:
                    raise MtconnectProtocolError(
                        "MTConnect XML exceeds the finite nesting-depth limit "
                        f"of {MAX_XML_DEPTH}."
                    )
                if len(element.attrib) > MAX_XML_ATTRIBUTES_PER_ELEMENT:
                    raise MtconnectProtocolError(
                        "MTConnect XML element exceeds the finite attribute-count "
                        f"limit of {MAX_XML_ATTRIBUTES_PER_ELEMENT}."
                    )
                continue

            text_bytes += len((element.text or "").encode("utf-8"))
            text_bytes += len((element.tail or "").encode("utf-8"))
            if text_bytes > MAX_XML_TEXT_BYTES:
                raise MtconnectProtocolError(
                    "MTConnect XML exceeds the finite text-content limit "
                    f"of {MAX_XML_TEXT_BYTES} bytes."
                )
            element.clear()
            depth -= 1
    except ET.ParseError as exc:
        raise MtconnectProtocolError("MTConnect response was not well-formed XML.") from exc


__all__ = ["validate_xml_budget"]
