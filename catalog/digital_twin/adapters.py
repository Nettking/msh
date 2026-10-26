"""Bounded reads of owner schemas. Registry bindings come from the server only.

Authorization callbacks must use current local owner authority; they must raise
AUTHORIZATION_UNAVAILABLE when that authority cannot be checked locally. No
copied manifest, administrator role, or browser-provided path supplies access.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path

from catalog.federation.errors import FederationValidationError
from catalog.mtconnect_recorder.canonical import store as canonical
from catalog.mtconnect_recorder.operational import store as operational
from catalog.mtconnect_recorder.operational.policy import (
    SEGMENTATION_POLICY,
    device_key,
)
from catalog.mtconnect_recorder.operational.projection import _input_fingerprint

from .contracts import Budget, TwinError, check_id, digest, require, revision, timestamp


@dataclass(frozen=True)
class InputSource:
    input_id: str
    binding_revision: str
    kind: str
    display_name: str
    root: Path
    paths: tuple[Path, ...]
    binding: dict
    authorize: Callable[[str, str | None], bool]
    utc_verified: bool = False
    origin_status: str = "UNKNOWN"
    local_authoritative: bool = True

    def __post_init__(self):
        check_id(self.input_id, "in")
        check_id(self.binding_revision, "rev")
        require(self.kind in ("CANONICAL_DEVICE", "LEGACY_JSONL", "OPERATOR_RECORDS"))
        require(self.origin_status in ("KNOWN", "UNKNOWN"))

    def allowed(self, actor, record=None):
        try:
            return self.authorize(actor, record) is True
        except TwinError:
            raise
        except Exception:  # noqa: BLE001 - owner authorization must fail closed
            raise TwinError("AUTHORIZATION_UNAVAILABLE", 503) from None

    def checked_paths(self):
        root = self.root.resolve(strict=True)
        result = []
        for path in self.paths:
            resolved = path.resolve(strict=True)
            require(
                resolved.is_relative_to(root) and resolved.is_file(),
                "SOURCE_BINDING_UNAVAILABLE",
                503,
            )
            result.append(resolved)
        require(bool(result), "SOURCE_BINDING_UNAVAILABLE", 503)
        require(len(set(result)) == len(result), "OVERLAP_UNRESOLVED", 422)
        return tuple(result)


class InputRegistry:
    def __init__(self, sources=()):
        self.sources = {s.input_id: s for s in sources}
        require(len(self.sources) == len(sources))

    def get(self, input_id, actor, record=None):
        source = self.sources.get(input_id)
        require(
            source is not None and source.allowed(actor, record),
            "OBJECT_NOT_FOUND",
            404,
        )
        return source

    def check_scopes(self, scopes, actor):
        for scope in scopes:
            self.get(scope["input_id"], actor, scope.get("record"))

    def list(self, actor):
        items = []
        for source in sorted(self.sources.values(), key=lambda s: s.input_id):
            if not source.allowed(actor):
                continue
            reasons = []
            if not source.local_authoritative:
                reasons = ["LOCAL_REVISION_NOT_VERIFIABLE"]
            else:
                try:
                    source.checked_paths()
                except (OSError, TwinError):
                    reasons = ["SOURCE_UNAVAILABLE"]
            items.append(
                {
                    "input_id": source.input_id,
                    "binding_revision": source.binding_revision,
                    "kind": source.kind,
                    "display_name": source.display_name,
                    "can_prepare": not reasons,
                    "reason_codes": reasons,
                }
            )
        return items


@contextmanager
def readonly_database(path, schema_name, validate, budget):
    connection = None
    try:
        connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=1)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        budget.check()
        row = connection.execute(
            "SELECT version FROM fcp_schema_versions WHERE schema_name=?",
            (schema_name,),
        ).fetchone()
        require(row is not None and row[0] == 1, "INPUT_SCHEMA_UNSUPPORTED", 422)
        validate(connection)
        yield connection
        budget.check()
    except FederationValidationError:
        raise TwinError("INPUT_SCHEMA_UNSUPPORTED", 422) from None
    except sqlite3.Error:
        raise TwinError("INPUT_SCHEMA_UNSUPPORTED", 422) from None
    finally:
        if connection is not None:
            connection.close()


def _rows(connection, sql, values, budget):
    budget.check()
    result = []
    for row in connection.execute(sql, values):
        budget.check(force=False, records=1)
        result.append(row)
    budget.check()
    return result


def read_canonical(source: InputSource, budget: Budget):
    paths = source.checked_paths()
    require(len(paths) == 2, "SOURCE_BINDING_UNAVAILABLE", 503)
    for path in paths:
        size = path.stat().st_size
        wal = path.with_name(path.name + "-wal")
        if wal.exists():
            size += wal.stat().st_size
        budget.check(source_bytes=size)
    binding = source.binding
    require(
        set(binding) >= {"source_key", "agent_instance_id", "device_key"},
        "SOURCE_BINDING_UNAVAILABLE",
        503,
    )
    values = (binding["source_key"], binding["agent_instance_id"])
    with readonly_database(
        paths[0], canonical.CANONICAL_SCHEMA_NAME, canonical._validate, budget
    ) as c:
        observations = tuple(
            canonical._row_to_observation(r)
            for r in budget.iterate(
                _rows(
                    c,
                    "SELECT * FROM canonical_observations WHERE source_key=? AND agent_instance_id=? ORDER BY sequence",
                    values,
                    budget,
                )
            )
        )
        batches = [
            dict(r)
            for r in _rows(
                c,
                "SELECT * FROM projected_batches WHERE source_key=? AND agent_instance_id=? ORDER BY first_sequence,raw_sha256",
                values,
                budget,
            )
        ]
        with readonly_database(
            paths[1],
            operational.SEGMENTATION_SCHEMA_NAME,
            operational._validate,
            budget,
        ) as op:
            projections = _rows(
                op,
                "SELECT * FROM segmentation_projection_runs WHERE source_key=? AND agent_instance_id=? AND segmentation_policy=?",
                (*values, SEGMENTATION_POLICY),
                budget,
            )
            require(len(projections) == 1, "PROJECTION_REVISION_MISMATCH", 409)
            projection = dict(projections[0])
            require(
                projection["input_fingerprint"]
                == _input_fingerprint(observations, check=budget.checkpoint),
                "PROJECTION_REVISION_MISMATCH",
                409,
            )
            known_batches = {
                item["raw_sha256"]
                for item in batches
                if item["status"] == canonical.BATCH_STATUS_PROJECTED
            }
            require(
                all(o.raw_batch_sha256 in known_batches for o in observations),
                "SOURCE_INTEGRITY_FAILED",
                422,
            )
            selected = {
                "source_key": values[0],
                "agent_instance_id": values[1],
                "device_key": binding["device_key"],
            }
            contents = {}
            for name, table in (
                ("runs", "segmentation_runs"),
                ("cycles", "segmentation_cycles"),
                ("episodes", "segmentation_episodes"),
                ("boundaries", "segmentation_boundaries"),
                ("device_status", "segmentation_device_status"),
            ):
                contents[name] = [
                    dict(r)
                    for r in _rows(
                        op,
                        f"SELECT * FROM {table} WHERE source_key=? AND agent_instance_id=? AND device_key=? AND segmentation_policy=? ORDER BY rowid",
                        (*selected.values(), SEGMENTATION_POLICY),
                        budget,
                    )
                ]
            require(bool(contents["device_status"]), "SOURCE_BINDING_UNAVAILABLE", 503)
    times = [
        o.observed_at_us
        for o in budget.iterate(observations)
        if device_key(o) == binding["device_key"]
    ]
    return dict(
        kind="CANONICAL_DEVICE",
        observed_bounds={
            binding["device_key"]: [timestamp(min(times)), timestamp(max(times))]
        }
        if times
        else {},
        observations=[asdict(o) for o in budget.iterate(observations)],
        batches=batches,
        projection=projection,
        **contents,
    )


def file_digest(path, budget, *, count_bytes=True):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            budget.check(force=False, source_bytes=len(chunk) if count_bytes else 0)
            h.update(chunk)
    return h.hexdigest()


def read_legacy(source: InputSource, budget: Budget):
    from catalog.flask_app.services.digital_twin_service import DigitalTwinService

    all_paths = source.checked_paths()
    hashes = {
        str(p.relative_to(source.root.resolve())): file_digest(p, budget)
        for p in sorted(all_paths)
    }
    unique = {}
    for p in sorted(all_paths):
        value = hashes[str(p.relative_to(source.root.resolve()))]
        if value in unique:
            require(source.origin_status == "KNOWN", "OVERLAP_UNRESOLVED", 422)
        else:
            unique[value] = p
    paths = tuple(unique.values())
    # Multiple legacy files require documented recording identities; otherwise
    # shared timestamps are not sufficient evidence of disjoint recordings.
    require(
        len(paths) == 1 or bool(source.binding.get("disjoint_recordings")),
        "OVERLAP_UNRESOLVED",
        422,
    )

    class BoundLegacyService(DigitalTwinService):
        def _source_fingerprint(self):
            return tuple(
                (
                    str(p.relative_to(self.data_dir)),
                    p.stat().st_size,
                    p.stat().st_mtime_ns,
                )
                for p in sorted(paths)
            )

        def _observe_record(self):
            budget.check(force=False, records=1)

        def _observe_frame(self, frame):
            budget.check()
            self.observed_bounds = {
                str(machine): [
                    timestamp(int(group.timestamp.min().value) // 1000),
                    timestamp(int(group.timestamp.max().value) // 1000),
                ]
                for machine, group in frame.groupby("machine_id", sort=False)
            }
            self.observed_daily_bounds = (
                {
                    str(machine): {
                        str(day): [
                            timestamp(int(group.timestamp.min().value) // 1000),
                            timestamp(int(group.timestamp.max().value) // 1000),
                        ]
                        for day, group in machine_rows.groupby("utc_date", sort=False)
                    }
                    for machine, machine_rows in frame.groupby("machine_id", sort=False)
                }
                if not frame.empty
                else {}
            )

    reader = BoundLegacyService(source.root.resolve())
    snapshot = reader._load_snapshot()
    budget.check()
    require(
        hashes
        == {
            str(p.relative_to(source.root.resolve())): file_digest(
                p, budget, count_bytes=False
            )
            for p in sorted(source.checked_paths())
        },
        "SOURCE_CHANGED",
        409,
    )

    def records(frame):
        return json.loads(frame.to_json(orient="records"))

    return {
        "kind": "LEGACY_JSONL",
        "source_digests": hashes,
        "observed_bounds": reader.observed_bounds,
        "observed_daily_bounds": reader.observed_daily_bounds,
        "record_count": snapshot.record_count,
        "machine_ranges": records(snapshot.machine_ranges),
        "daily_quality": records(snapshot.daily_quality),
        "daily_states": records(snapshot.daily_states),
    }


def read_operator(source, actor, budget, machine_input_id):
    require(source.local_authoritative, "SOURCE_BINDING_UNAVAILABLE", 503)
    paths = source.checked_paths()
    require(len(paths) == 1, "SOURCE_BINDING_UNAVAILABLE", 503)
    before = file_digest(paths[0], budget)
    require(paths[0].stat().st_size <= 536_870_912, "ARTIFACT_LIMIT_EXCEEDED", 422)
    raw = json.loads(paths[0].read_text(encoding="utf-8"))
    budget.check()
    # Existing OperatorStrategyService stores a versioned dictionary; older
    # owner versions also wrote a plain record list. Do not manufacture data.
    if isinstance(raw, dict):
        require(
            raw.get("schema")
            in (
                "fcp.operator_strategy_records.v3",
                "fcp.process_twin.operator_read.v1",
            ),
            "INPUT_SCHEMA_UNSUPPORTED",
            422,
        )
        rows, links = raw.get("records"), raw.get("links", [])
    else:
        rows, links = raw, []
    require(
        isinstance(rows, list) and isinstance(links, list),
        "SOURCE_CONTENT_INVALID",
        422,
    )
    visible = []
    seen = set()
    for row in rows:
        budget.check(force=False, records=1)
        require(
            isinstance(row, dict) and isinstance(row.get("id"), str),
            "SOURCE_CONTENT_INVALID",
            422,
        )
        require(row["id"] not in seen, "SOURCE_CONTENT_INVALID", 422)
        seen.add(row["id"])
        if source.allowed(actor, row["id"]):
            visible.append(dict(row, _source_revision=revision(row)))
    require(
        before == file_digest(paths[0], budget, count_bytes=False),
        "SOURCE_CHANGED",
        409,
    )
    visible_ids = {row["id"] for row in visible}
    return {
        "records": visible,
        "links": [
            link
            for link in links
            if isinstance(link, dict)
            and link.get("record_id") in visible_ids
            and link.get("input_id") == machine_input_id
        ],
        "content_digest": digest(visible),
        "utc_verified": source.utc_verified,
        "record_bindings": source.binding.get("record_bindings", {}),
    }


def prepare(source, operator, actor, policy, budget):
    require(source.kind != "OPERATOR_RECORDS", "BINDING_MISMATCH", 422)
    require(source.local_authoritative, "SOURCE_BINDING_UNAVAILABLE", 503)
    require(
        policy != "ASSUME_UTC" or source.kind == "LEGACY_JSONL",
        "TIME_POLICY_NOT_SUPPORTED",
        422,
    )
    require(
        policy != "SOURCE_UTC" or source.utc_verified, "TIME_POLICY_NOT_SUPPORTED", 422
    )
    try:
        payload = (
            read_canonical(source, budget)
            if source.kind == "CANONICAL_DEVICE"
            else read_legacy(source, budget)
        )
        human = (
            read_operator(operator, actor, budget, source.input_id)
            if operator
            else None
        )
    except FileNotFoundError:
        raise TwinError("SOURCE_UNAVAILABLE", 503) from None
    except (PermissionError, OSError):
        raise TwinError("SOURCE_STORE_UNAVAILABLE", 503) from None
    except (ValueError, TypeError, KeyError):
        raise TwinError("SOURCE_CONTENT_INVALID", 422) from None
    payload["operator"] = human
    payload["time_policy"] = policy
    payload["input_binding"] = {
        "input_id": source.input_id,
        "binding_revision": source.binding_revision,
    }
    payload["operator_binding"] = (
        {"input_id": operator.input_id, "binding_revision": operator.binding_revision}
        if operator
        else None
    )
    payload["machine_binding"] = source.binding
    payload["origin_status"] = source.origin_status
    budget.encoded(payload)
    return payload
