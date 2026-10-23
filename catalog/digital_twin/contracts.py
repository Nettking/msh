"""Version-one process-twin boundaries; no source or runtime side effects."""

from __future__ import annotations

import ctypes
import json
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from catalog.mtconnect_recorder.operational.model import canonical_sha256

MAX_REQUEST = 65_536
MAX_INPUT = 2_147_483_648
MAX_RECORDS = 2_000_000
MAX_ARTIFACT = 536_870_912
MIN_FREE = 603_979_776
BUILD_SECONDS = 120
LEASE_SECONDS = 150
VIEWS = ("episodes", "execution", "gaps", "operator", "legacy_daily")
GOALS = {"PROCESS_HISTORY", "PROVENANCE", "DATA_QUALITY", "OPERATOR_CONTEXT"}
PROPERTIES = {
    "EXECUTION_HISTORY",
    "OPERATIONAL_EPISODES",
    "OPERATOR_RECORDS",
    "EVIDENCE",
}
ROLES = {
    "observer": "SYSTEM",
    "analyst": "SYSTEM_DESCRIPTIVE",
    "decision_maker": "HUMAN",
    "executor": "HUMAN",
}
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class TwinError(Exception):
    """Only a public-safe code crosses the HTTP boundary."""

    def __init__(self, code: str, status: int = 400, field: str | None = None):
        super().__init__(code)
        self.code, self.status, self.field = code, status, field
        self.command_id = None


def require(condition, code="INVALID_REQUEST", status=400, field=None):
    if not condition:
        raise TwinError(code, status, field)


def encode(value) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def digest(value) -> str:
    # Preserve the recorder's canonical serialization; reject non-finite values first.
    encode(value)
    return canonical_sha256(value)


def revision(value) -> str:
    return "rev_" + digest(value)


def identifier(prefix: str, value=None) -> str:
    return prefix + "_" + (uuid4().hex if value is None else digest(value)[:32])


def check_id(value, prefix):
    size = 64 if prefix == "rev" else 32
    require(
        isinstance(value, str)
        and re.fullmatch(prefix + r"_[0-9a-f]{" + str(size) + "}", value) is not None
    )
    return value


def fields(value, names, *, prefix=None):
    require(isinstance(value, dict), field=prefix)
    for name in names:
        require(name in value, field=f"{prefix}.{name}" if prefix else name)
    require(set(value) == set(names))
    return value


def text(value, maximum=4096, name=None):
    require(
        isinstance(value, str)
        and bool(value.strip())
        and len(value.encode("utf-8")) <= maximum,
        field=name,
    )
    return value


def codes(value, allowed, maximum=4):
    require(isinstance(value, list) and 1 <= len(value) <= maximum)
    require(all(isinstance(v, str) and v in allowed for v in value))
    require(len(set(value)) == len(value))
    return sorted(value)


def micros(value):
    require(
        isinstance(value, str)
        and re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z", value)
        is not None
    )
    try:
        delta = (
            datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                tzinfo=timezone.utc
            )
            - EPOCH
        )
    except ValueError:
        raise TwinError("INVALID_REQUEST") from None
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def timestamp(value):
    return (EPOCH + timedelta(microseconds=value)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def strict_json(raw):
    def pairs(items):
        result = {}
        for k, v in items:
            require(k not in result, "INVALID_JSON")
            result[k] = v
        return result

    def constant(_value):
        raise TwinError("INVALID_JSON")

    try:
        value = json.loads(
            raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant
        )
        require(isinstance(value, dict), "INVALID_JSON")
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise TwinError("INVALID_JSON") from None


def validate(operation, payload):
    if operation == "PREPARE_DATASET":
        fields(
            payload,
            (
                "schema",
                "input_id",
                "expected_binding_revision",
                "operator_input_id",
                "expected_operator_binding_revision",
                "time_policy",
            ),
        )
        require(payload["schema"] == "fcp.process_twin.prepare.v1")
        check_id(payload["input_id"], "in")
        check_id(payload["expected_binding_revision"], "rev")
        if payload["operator_input_id"] is not None:
            check_id(payload["operator_input_id"], "in")
            check_id(payload["expected_operator_binding_revision"], "rev")
        else:
            require(payload["expected_operator_binding_revision"] is None)
        require(payload["time_policy"] in ("SOURCE_UTC", "ASSUME_UTC", "UNRESOLVED"))
    elif operation in ("CREATE_TWIN", "REVISE_TWIN"):
        if operation == "REVISE_TWIN":
            fields(payload, ("expected_revision_id", "definition"))
            check_id(payload["expected_revision_id"], "rev")
            definition = payload["definition"]
        else:
            definition = payload
        fields(
            definition,
            (
                "schema",
                "display_name",
                "counterpart_kind",
                "counterpart_description",
                "process_boundary",
                "dataset_revision_id",
                "goals",
                "properties",
                "role_profile",
                "validity_scope",
            ),
            prefix="definition" if operation == "REVISE_TWIN" else None,
        )
        require(definition["schema"] == "fcp.process_twin.definition.v1")
        require(definition["counterpart_kind"] == "PRODUCTION_PROCESS")
        text(definition["display_name"], 480)
        require(len(definition["display_name"]) <= 120)
        for name in ("counterpart_description", "process_boundary", "validity_scope"):
            text(definition[name], name=name)
        check_id(definition["dataset_revision_id"], "rev")
        definition["goals"] = codes(definition["goals"], GOALS)
        definition["properties"] = codes(definition["properties"], PROPERTIES)
        fields(
            definition["role_profile"],
            ROLES,
            prefix="definition.role_profile"
            if operation == "REVISE_TWIN"
            else "role_profile",
        )
        require(definition["role_profile"] == ROLES)
    elif operation == "BUILD_SNAPSHOT":
        fields(
            payload,
            (
                "schema",
                "definition_revision_id",
                "dataset_revision_ids",
                "window",
                "data_mode",
            ),
        )
        require(payload["schema"] == "fcp.process_twin.snapshot_request.v1")
        check_id(payload["definition_revision_id"], "rev")
        refs = payload["dataset_revision_ids"]
        require(isinstance(refs, list) and bool(refs))
        for ref in refs:
            check_id(ref, "rev")
        require(payload["data_mode"] in ("HISTORICAL", "LIVE", "REPLAY"))
        fields(payload["window"], ("start_at", "end_at"), prefix="window")
        a, b = (micros(payload["window"][k]) for k in ("start_at", "end_at"))
        require(0 < b - a <= 2_678_400_000_000, "INVALID_TIME_WINDOW", 422)
    elif operation == "REBUILD_INDEX":
        fields(payload, ("schema", "expected_content_revision"))
        require(payload["schema"] == "fcp.process_twin.rebuild.v1")
        check_id(payload["expected_content_revision"], "rev")
    else:
        raise TwinError("INVALID_REQUEST")


def rss_bytes():
    if os.name == "nt":
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("faults", wintypes.DWORD)] + [
                (n, ctypes.c_size_t)
                for n in (
                    "peak",
                    "working",
                    "pool_peak",
                    "pool",
                    "nonpool_peak",
                    "nonpool",
                    "page",
                    "page_peak",
                )
            ]

        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        get_process = ctypes.windll.kernel32.GetCurrentProcess
        get_process.restype = wintypes.HANDLE
        memory_info = ctypes.windll.psapi.GetProcessMemoryInfo
        memory_info.argtypes = (
            wintypes.HANDLE,
            ctypes.POINTER(Counters),
            wintypes.DWORD,
        )
        memory_info.restype = wintypes.BOOL
        require(
            bool(memory_info(get_process(), ctypes.byref(counters), counters.cb)),
            "MEMORY_LIMIT_EXCEEDED",
            422,
        )
        return counters.working
    try:
        return int(Path("/proc/self/statm").read_text().split()[1]) * os.sysconf(
            "SC_PAGE_SIZE"
        )
    except (OSError, ValueError, IndexError):
        raise TwinError("MEMORY_LIMIT_EXCEEDED", 422) from None


class Budget:
    def __init__(self):
        self.started = time.monotonic()
        self.rss = rss_bytes()
        self.last = self.started
        self.records = self.source_bytes = 0

    def checkpoint(self):
        """Cheap callback for iterative owner code; RSS is sampled every 100 ms."""
        self.check(force=False)

    def iterate(self, values):
        for value in values:
            self.checkpoint()
            yield value

    def check(self, *, force=True, records=0, source_bytes=0):
        self.records += records
        self.source_bytes += source_bytes
        require(self.records <= MAX_RECORDS, "RECORD_LIMIT_EXCEEDED", 422)
        require(self.source_bytes <= MAX_INPUT, "INPUT_LIMIT_EXCEEDED", 422)
        elapsed = time.monotonic() - self.started
        require(math.isfinite(elapsed) and elapsed >= 0, "CLOCK_UNSAFE", 503)
        require(elapsed <= BUILD_SECONDS, "BUILD_DEADLINE_EXCEEDED", 504)
        if force or time.monotonic() - self.last >= 0.1:
            require(
                rss_bytes() - self.rss <= MAX_ARTIFACT, "MEMORY_LIMIT_EXCEEDED", 422
            )
            self.last = time.monotonic()

    def encoded(self, value):
        self.check()
        result = encode(value)
        self.check()
        require(
            len(result.encode("utf-8")) <= MAX_ARTIFACT, "ARTIFACT_LIMIT_EXCEEDED", 422
        )
        return result
