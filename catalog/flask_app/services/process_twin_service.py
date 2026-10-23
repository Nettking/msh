"""Explicit commands and authorized reads for the historical process twin."""

from __future__ import annotations

import base64
import json
import os

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from catalog.digital_twin import projection
from catalog.digital_twin.adapters import prepare
from catalog.digital_twin.contracts import (
    PROPERTIES,
    Budget,
    TwinError,
    check_id,
    digest,
    encode,
    identifier,
    now,
    require,
    revision,
    validate,
)
from catalog.digital_twin.records import TwinStore

API = "/api/process-twins/v1"
WORKFLOW = frozenset(("CREATE_TWIN", "REVISE_TWIN"))
OPERATIONS = WORKFLOW | {"PREPARE_DATASET", "BUILD_SNAPSHOT", "REBUILD_INDEX"}


class ProcessTwinService:
    def __init__(self, directory, registry):
        self.store = TwinStore(directory)
        self.registry = registry

    def _registry(self):
        require(self.registry is not None, "INPUT_REGISTRY_UNAVAILABLE", 503)
        return self.registry

    def authorize(self, scopes, actor):
        if scopes:
            self._registry().check_scopes(scopes, actor)

    def get(self, object_id, actor, kind=None):
        obj, scopes = self.store.object(object_id, kind)
        self.authorize(scopes, actor)
        return obj, scopes

    def inputs(self, actor):
        return self._registry().list(actor)

    def dataset(self, ref, actor):
        check_id(ref, "rev")
        return self.get(ref, actor, "dataset")[0]["manifest"]

    def _dataset_full(self, ref, actor):
        data, scopes = self.get(ref, actor, "dataset")
        content = self.store.object(data["content_ref"], "dataset_content")[0]
        require(digest(content) == data["content_digest"], "SOURCE_CHANGED", 409)
        return dict(data, content=content), scopes

    def definition(self, twin, ref, actor):
        check_id(twin, "tw")
        check_id(ref, "rev")
        value, _ = self.get(ref, actor, "definition")
        require(value["twin_id"] == twin, "OBJECT_NOT_FOUND", 404)
        return value

    def snapshot(self, ref, actor):
        check_id(ref, "sn")
        return self.get(ref, actor, "snapshot")[0]["manifest"]

    def command(self, actor, *, operation=None, key=None, command_id=None):
        if command_id:
            check_id(command_id, "cmd")
        else:
            require(operation in OPERATIONS)
        result = self.store.command(
            actor=actor, operation=operation, key=key, command_id=command_id
        )
        require(result is not None, "OBJECT_NOT_FOUND", 404)
        self.authorize(result["scopes"], actor)
        return result

    def _references(self, operation, payload, target, actor):
        if operation == "PREPARE_DATASET":
            registry = self._registry()
            source = registry.get(payload["input_id"], actor)
            require(source.kind != "OPERATOR_RECORDS", "BINDING_MISMATCH", 422)
            operator = (
                registry.get(payload["operator_input_id"], actor)
                if payload["operator_input_id"]
                else None
            )
            require(
                operator is None or operator.kind == "OPERATOR_RECORDS",
                "BINDING_MISMATCH",
                422,
            )
            require(
                payload["time_policy"] != "SOURCE_UTC" or source.utc_verified,
                "TIME_POLICY_NOT_SUPPORTED",
                422,
            )
            require(
                payload["time_policy"] != "ASSUME_UTC" or source.kind == "LEGACY_JSONL",
                "TIME_POLICY_NOT_SUPPORTED",
                422,
            )
            return [{"input_id": s.input_id} for s in (source, operator) if s]
        if operation in WORKFLOW:
            definition = (
                payload["definition"] if operation == "REVISE_TWIN" else payload
            )
            data, scopes = self.get(definition["dataset_revision_id"], actor, "dataset")
            expected = (
                PROPERTIES
                if data["kind"] == "CANONICAL_DEVICE"
                else PROPERTIES - {"OPERATIONAL_EPISODES"}
            )
            require(
                set(definition["properties"]) == expected, "PROPERTY_NOT_SUPPORTED", 422
            )
            if operation == "REVISE_TWIN":
                check_id(target, "tw")
                self.definition(target, payload["expected_revision_id"], actor)
            return scopes
        if operation == "BUILD_SNAPSHOT":
            check_id(target, "tw")
            definition = self.definition(
                target, payload["definition_revision_id"], actor
            )
            all_data = [
                self.get(ref, actor, "dataset")
                for ref in payload["dataset_revision_ids"]
            ]
            require(len(all_data) == 1, "MULTI_DATASET_NOT_SUPPORTED", 422)
            require(
                payload["data_mode"] == "HISTORICAL", "DATA_MODE_NOT_SUPPORTED", 422
            )
            data, scopes = all_data[0]
            require(
                data["input_binding"] == definition["input_binding"]
                and data["operator_binding"] == definition["operator_binding"],
                "BINDING_MISMATCH",
                422,
            )
            if data["kind"] == "LEGACY_JSONL":
                require(
                    all(
                        v.endswith("T00:00:00.000000Z")
                        for v in payload["window"].values()
                    ),
                    "LEGACY_WINDOW_NOT_DAY_ALIGNED",
                    422,
                )
            return scopes
        check_id(target, "sn")
        snapshot, scopes = self.get(target, actor, "snapshot")
        require(
            payload["expected_content_revision"]
            == snapshot["manifest"]["content_revision"],
            "REBUILD_DIFFERENT_RESULT",
            409,
        )
        return scopes

    def execute(
        self, actor, operation, payload, key, *, target=None, authorize_operation=None
    ):
        require(operation in OPERATIONS)
        validate(operation, payload)
        scopes = self._references(operation, payload, target, actor)
        body_digest = digest({"target": target, "payload": payload})
        previous = self.store.command(actor=actor, operation=operation, key=key)
        if previous:
            self.authorize(previous["scopes"], actor)
            if authorize_operation:
                authorize_operation()
            require(previous["digest"] == body_digest, "IDEMPOTENCY_CONFLICT", 409)
            if previous["status"] == "RUNNING":
                raise TwinError("COMMAND_RUNNING", 409)
            if previous["status"] == "FAILED":
                error = TwinError(
                    previous["response"]["code"],
                    previous["http"],
                    previous["response"].get("field"),
                )
                error.command_id = previous["id"]
                raise error
            return previous["response"], previous["http"], previous["location"]
        budget = Budget()
        command_id = self.store.reserve(
            actor, operation, key, body_digest, scopes, operation not in WORKFLOW
        )
        try:
            # The deadline starts at the durable reservation, including any
            # elapsed commit time before this process receives control again.
            reserved = self.store.command(actor=actor, command_id=command_id)
            budget.started = reserved["mono"]
            result, location, objects, views, scopes, expected_latest = self._perform(
                actor, operation, payload, target, command_id, budget, scopes
            )

            def check_current():
                self.authorize(scopes, actor)
                if authorize_operation:
                    authorize_operation()
                if operation == "PREPARE_DATASET":
                    for id_field, rev_field in (
                        ("input_id", "expected_binding_revision"),
                        ("operator_input_id", "expected_operator_binding_revision"),
                    ):
                        if payload[id_field]:
                            source = self._registry().get(payload[id_field], actor)
                            require(
                                source.binding_revision == payload[rev_field],
                                "SOURCE_CHANGED",
                                409,
                            )

            self.store.finish(
                command_id,
                actor,
                result,
                200 if operation == "REBUILD_INDEX" else 201,
                location,
                objects,
                views,
                budget=budget,
                before_commit=check_current,
                scopes=scopes,
                rebuild=operation == "REBUILD_INDEX",
                expected_latest=expected_latest,
            )
            return result, 200 if operation == "REBUILD_INDEX" else 201, location
        except Exception as exc:  # noqa: BLE001 - persist safe terminal failure, never publish partial data
            error = (
                exc if isinstance(exc, TwinError) else TwinError("INTERNAL_ERROR", 500)
            )
            error.command_id = command_id
            try:
                self.store.fail(command_id, actor, error)
            except TwinError:
                pass  # A full journal remains RUNNING until fenced lease expiry.
            raise error from None

    def _perform(self, actor, operation, payload, target, command_id, budget, scopes):
        expected_latest = None
        if operation == "PREPARE_DATASET":
            registry = self._registry()
            source = registry.get(payload["input_id"], actor)
            operator = (
                registry.get(payload["operator_input_id"], actor)
                if payload["operator_input_id"]
                else None
            )
            require(
                source.binding_revision == payload["expected_binding_revision"],
                "SOURCE_CHANGED",
                409,
            )
            require(
                operator is None
                or operator.binding_revision
                == payload["expected_operator_binding_revision"],
                "SOURCE_CHANGED",
                409,
            )
            content = prepare(source, operator, actor, payload["time_policy"], budget)
            bounds = (
                content.get("observed_bounds", {}).get(
                    source.binding.get("device_key")
                    if source.kind == "CANONICAL_DEVICE"
                    else source.binding.get("machine_id")
                )
                if payload["time_policy"] != "UNRESOLVED"
                else None
            )
            if operator:
                scopes += [
                    {"input_id": operator.input_id, "record": r["id"]}
                    for r in content["operator"]["records"]
                ]
            content_digest = digest(content)
            ref = revision(
                {
                    "content_digest": content_digest,
                    "input_binding": content["input_binding"],
                    "operator_binding": content["operator_binding"],
                }
            )
            manifest = {
                "dataset_id": identifier("ds"),
                "dataset_revision_id": ref,
                "input_binding": content["input_binding"],
                "operator_binding": content["operator_binding"],
                "kind": source.kind,
                "time_policy": payload["time_policy"],
                "created_at": now(),
                "created_by": actor,
                "first_observed_at": bounds[0] if bounds else None,
                "last_observed_at": bounds[1] if bounds else None,
                "source_references": [
                    identifier("ev", [source.input_id, content_digest])
                ],
                "operator_record_count": len(content["operator"]["records"])
                if operator
                else None,
                "integrity_status": "VERIFIED",
                "origin_status": source.origin_status,
                "reason_codes": [],
                "command_id": command_id,
            }
            content_ref = identifier("ds", [ref, "preserved-content"])
            data = {
                "kind": source.kind,
                "time_policy": payload["time_policy"],
                "input_binding": content["input_binding"],
                "operator_binding": content["operator_binding"],
                "machine_binding": content["machine_binding"],
                "content_digest": content_digest,
                "content_ref": content_ref,
                "manifest": manifest,
            }
            # An existing identical prepared revision is immutable; the command
            # can refer to it without replacing its original actor or timestamp.
            try:
                existing, existing_scopes = self.store.object(ref, "dataset")
            except TwinError as exc:
                if exc.code != "OBJECT_NOT_FOUND":
                    raise
                objects = [
                    (ref, "dataset", ref, None, data, scopes),
                    (content_ref, "dataset_content", ref, ref, content, []),
                ]
            else:
                self.authorize(existing_scopes, actor)
                manifest = existing["manifest"]
                objects = []
            return (
                manifest,
                API + "/dataset-revisions/" + ref,
                objects,
                None,
                scopes,
                None,
            )
        if operation in WORKFLOW:
            definition = (
                payload["definition"] if operation == "REVISE_TWIN" else payload
            )
            dataset, scopes = self.get(
                definition["dataset_revision_id"], actor, "dataset"
            )
            twin = target or identifier("tw")
            if operation == "REVISE_TWIN":
                previous = self.store.latest_revision(twin)
                require(
                    previous == payload["expected_revision_id"],
                    "DEFINITION_CHANGED",
                    409,
                )
                old, _ = self.get(previous, actor, "definition")
                require(old["definition"] != definition, "NO_DEFINITION_CHANGE", 409)
                expected_latest = (twin, previous)
            else:
                previous = None
            ref = revision([twin, definition])
            result = dict(
                twin_id=twin,
                definition_revision_id=ref,
                previous_revision_id=previous,
                **definition,
                definition=definition,
                input_binding=dataset["input_binding"],
                operator_binding=dataset["operator_binding"],
                allowed_services=["history", "operator_knowledge", "evidence"],
                created_at=now(),
                created_by=actor,
                command_id=command_id,
            )
            return (
                result,
                API + f"/twins/{twin}/revisions/{ref}",
                [(ref, "definition", ref, twin, result, scopes)],
                None,
                scopes,
                expected_latest,
            )
        if operation == "BUILD_SNAPSHOT":
            definition = self.definition(
                target, payload["definition_revision_id"], actor
            )
            budget.check()
            dataset, scopes = self._dataset_full(
                payload["dataset_revision_ids"][0], actor
            )
            budget.check()
            built = projection.build(dataset, definition, payload["window"], budget)
            snapshot = identifier("sn")
            manifest = dict(
                snapshot_id=snapshot,
                twin_id=target,
                definition_revision_id=definition["definition_revision_id"],
                dataset_revision_ids=payload["dataset_revision_ids"],
                data_mode="HISTORICAL",
                source_kind=dataset["kind"],
                time_policy=dataset["time_policy"],
                window=payload["window"],
                captured_at=now(),
                created_by=actor,
                command_id=command_id,
                model_validity="NOT_ASSESSED",
                **{
                    k: built[k]
                    for k in (
                        "first_observed_at",
                        "last_observed_at",
                        "content_revision",
                        "services",
                        "summary",
                        "counts",
                        "available_views",
                    )
                },
            )
            durable = {
                "manifest": manifest,
                "recipe": payload,
                "definition_ref": definition["definition_revision_id"],
            }
            objects = [
                (
                    snapshot,
                    "snapshot",
                    built["content_revision"],
                    target,
                    durable,
                    scopes,
                )
            ]
            for kind, values in (
                ("record", built["records"].items()),
                ("evidence", built["evidence"].items()),
                ("link", ((item["link_id"], item) for item in built["links"])),
            ):
                for ref, value in values:
                    objects.append(
                        (
                            snapshot + ":" + ref,
                            kind,
                            revision(value),
                            snapshot,
                            value,
                            [],
                        )
                    )
            return (
                manifest,
                API + "/snapshots/" + snapshot,
                objects,
                {snapshot: built["views"]},
                scopes,
                None,
            )
        stored, scopes = self.get(target, actor, "snapshot")
        recipe = stored["recipe"]
        definition = self.get(stored["definition_ref"], actor, "definition")[0]
        budget.check()
        dataset = self._dataset_full(recipe["dataset_revision_ids"][0], actor)[0]
        budget.check()
        built = projection.build(dataset, definition, recipe["window"], budget)
        require(
            built["content_revision"] == stored["manifest"]["content_revision"],
            "REBUILD_DIFFERENT_RESULT",
            409,
        )
        return (
            stored["manifest"],
            API + "/snapshots/" + target,
            [],
            {target: built["views"]},
            scopes,
            None,
        )

    def cursor(self, value=None, *, claims=None):
        key = bytes.fromhex(self.store.cursor_key())
        cipher = AESGCM(key)
        if claims is not None:
            nonce = os.urandom(12)
            return base64.urlsafe_b64encode(
                nonce
                + cipher.encrypt(
                    nonce, encode(claims).encode(), b"process-twin.cursor.v1"
                )
            ).decode()
        try:
            raw = base64.b64decode(value, altchars=b"-_", validate=True)
            return json.loads(
                cipher.decrypt(raw[:12], raw[12:], b"process-twin.cursor.v1")
            )
        except (ValueError, TypeError, InvalidTag, UnicodeError):
            raise TwinError("INVALID_CURSOR") from None

    def page(
        self,
        items,
        actor,
        scope,
        *,
        limit=100,
        cursor=None,
        view=None,
        snapshot=None,
        selection=None,
        availability=None,
    ):
        selection = selection or revision(items)
        offset = 0
        if cursor:
            prior = self.cursor(cursor)
            require(
                prior["actor"] == actor
                and prior["scope"] == scope
                and prior["view"] == view
                and prior["limit"] == limit,
                "INVALID_CURSOR",
            )
            require(prior["selection"] == selection, "LIST_CHANGED", 409)
            offset = prior["offset"]
            require(
                isinstance(offset, int) and 0 <= offset <= len(items), "INVALID_CURSOR"
            )
        more = offset + limit < len(items)
        token = (
            self.cursor(
                claims={
                    "actor": actor,
                    "scope": scope,
                    "view": view,
                    "limit": limit,
                    "selection": selection,
                    "offset": offset + limit,
                }
            )
            if more
            else None
        )
        return {
            "items": items[offset : offset + limit],
            "total": len(items),
            "limit": limit,
            "has_more": more,
            "next_cursor": token,
            "snapshot_id": snapshot,
            "view": view,
            "availability": availability or projection.service("AVAILABLE"),
        }

    def list_objects(self, kind, actor, parent=None, **options):
        items = []
        if parent is not None:
            check_id(parent, "tw")
            self.definition(parent, self.store.latest_revision(parent), actor)
        for value, scopes in self.store.objects(kind, parent):
            try:
                self.authorize(scopes, actor)
            except TwinError as exc:
                if exc.code == "OBJECT_NOT_FOUND":
                    continue
                raise
            items.append(
                value["manifest"]
                if kind == "snapshot"
                else {k: v for k, v in value.items() if k != "definition"}
            )
        if kind == "definition":
            latest = {v["twin_id"]: v for v in items}
            items = [latest[k] for k in sorted(latest)]
        return self.page(items, actor, kind + ":" + str(parent), **options)

    def timeline(self, snapshot, actor, *, view=None, limit=100, cursor=None):
        manifest = self.snapshot(snapshot, actor)
        require(view is None or view in projection.VIEWS)
        view = view or (
            "episodes"
            if manifest["source_kind"] == "CANONICAL_DEVICE"
            else "legacy_daily"
        )
        require(view in manifest["available_views"], "VIEW_NOT_SUPPORTED", 422)
        available = manifest["services"][
            "operator_knowledge" if view == "operator" else "history"
        ]
        total = manifest["counts"][view]
        offset = 0
        if cursor:
            prior = self.cursor(cursor)
            require(
                prior["actor"] == actor
                and prior["scope"] == snapshot
                and prior["view"] == view
                and prior["limit"] == limit
                and prior["selection"] == manifest["content_revision"],
                "INVALID_CURSOR",
            )
            offset = prior["offset"]
            require(
                type(offset) is int and 0 <= offset <= (total or 0), "INVALID_CURSOR"
            )
        rows = self.store.rows(snapshot, view, offset, limit)
        more = total is not None and offset + len(rows) < total
        token = (
            self.cursor(
                claims={
                    "actor": actor,
                    "scope": snapshot,
                    "view": view,
                    "limit": limit,
                    "selection": manifest["content_revision"],
                    "offset": offset + len(rows),
                }
            )
            if more
            else None
        )
        return {
            "items": rows,
            "total": total,
            "limit": limit,
            "has_more": more,
            "next_cursor": token,
            "snapshot_id": snapshot,
            "view": view,
            "availability": available,
        }

    def detail(self, snapshot, actor, kind, ref):
        _stored, _ = self.get(snapshot, actor, "snapshot")
        if kind == "episode":
            check_id(ref, "row")
            # Indexed lookup: no full-timeline scan on each detail request.
            with self.store.connection(index=True) as c:
                value = c.execute(
                    "SELECT payload FROM views.rows WHERE snapshot=? AND view='episodes' AND id=?",
                    (snapshot, ref),
                ).fetchone()
            require(value is not None, "OBJECT_NOT_FOUND", 404)
            return json.loads(value[0])
        prefix = "rec" if kind == "record" else "ev"
        check_id(ref, prefix)
        result = self.store.object(snapshot + ":" + ref, kind)[0]
        if kind == "evidence":
            if (
                result["content"] is not None
                and digest(result["content"]) != result["content_digest"]
            ):
                result = dict(
                    result,
                    state="SOURCE_UNAVAILABLE",
                    content=None,
                    reason_codes=["SOURCE_UNAVAILABLE"],
                )
            require(
                result["content"] is None
                or len(result["content"].encode("utf-8")) <= 65_536,
                "EVIDENCE_TOO_LARGE",
                413,
            )
        return result
