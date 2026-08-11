from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace

from flask import Flask

from catalog.federation.models import CommitState
from catalog.federation.storage_catalog import (
    CommittedBatchPage,
    CommittedBatchReference,
)
from catalog.federation.telemetry_mirror import TelemetryMirrorIngestState
from catalog.flask_app.services.federated_telemetry_product_bridge import (
    FederatedTelemetryProductBridge,
)
from catalog.flask_app.services.federation_pairing_install import (
    install_federation_pairing,
)

_MANIFEST_HASH = "sha256:" + "a" * 64


def _reference(index: int, *, schema_name: str = "fcp.mtconnect.observations"):
    return CommittedBatchReference(
        session_id="session-1",
        group_id="storage-1",
        dataset_id=f"mtconnect:recorder-{index}:source-{index}",
        batch_id=f"batch-{index}",
        idempotency_key=f"idempotency-{index}",
        content_hash="sha256:" + f"{index:064x}",
        size_bytes=100,
        schema_name=schema_name,
        schema_version=1,
        source_id=None,
        first_sequence=None,
        last_sequence=None,
        commit_state=CommitState.COMMITTED,
        acknowledged_provider_ids=("provider-1",),
        committed_at=datetime(2026, 8, 11, tzinfo=timezone.utc),
        manifest_revision=7,
        manifest_hash=_MANIFEST_HASH,
    )


def _page(
    *references: CommittedBatchReference,
    cursor: str | None = None,
) -> CommittedBatchPage:
    return CommittedBatchPage(
        session_id="session-1",
        group_id="storage-1",
        dataset_id=None,
        manifest_revision=7,
        manifest_hash=_MANIFEST_HASH,
        batches=tuple(references),
        next_cursor=cursor,
    )


def _status(*, ready: bool = True) -> dict[str, object]:
    capabilities: list[dict[str, object]] = []
    if ready:
        capabilities.append(
            {
                "session_id": "session-1",
                "node_id": "owner-1",
                "type": "storage-control",
                "protocol": "fcp.storage-control",
                "status": "ready",
                "properties": {"group_ids": ["storage-1"]},
            }
        )
    return {
        "sessions": [
            {
                "session_id": "session-1",
                "created_by_node_id": "owner-1",
            }
        ],
        "capabilities": capabilities,
    }


class _Runtime:
    def __init__(self, pages: dict[str | None, CommittedBatchPage], *, ready=True):
        self.pages = pages
        self.status = _status(ready=ready)
        self.list_calls: list[dict[str, object]] = []
        self.read_calls: list[CommittedBatchReference] = []

    def coordinator_status(self):
        return self.status

    def list_committed_batches(self, state, **kwargs):
        del state
        self.list_calls.append(dict(kwargs))
        return self.pages[kwargs["cursor"]]

    def read_committed_batch(self, state, **kwargs):
        del state
        reference = kwargs["reference"]
        self.read_calls.append(reference)
        return {"batch_id": reference.batch_id}


@dataclass
class _MirrorResult:
    state: TelemetryMirrorIngestState


class _Mirror:
    def __init__(self, *, existing=(), rebuild_count: int = 0):
        self.stored = {reference.batch_id for reference in existing}
        self.rebuild_count = rebuild_count
        self.rebuild_calls = 0
        self.contains_calls: list[CommittedBatchReference] = []
        self.ingest_calls: list[tuple[CommittedBatchReference, object]] = []

    def rebuild_materializations(self):
        self.rebuild_calls += 1
        return self.rebuild_count

    def contains(self, reference):
        self.contains_calls.append(reference)
        return reference.batch_id in self.stored

    def ingest(self, reference, content):
        self.ingest_calls.append((reference, content))
        if reference.batch_id in self.stored:
            return _MirrorResult(TelemetryMirrorIngestState.ALREADY_STORED)
        self.stored.add(reference.batch_id)
        return _MirrorResult(TelemetryMirrorIngestState.STORED)


def _bridge(
    runtime: _Runtime,
    mirror: _Mirror,
    *,
    refreshes: list[str],
    max_pages: int = 5,
    max_batches: int = 100,
    refresh_accepts: list[bool] | None = None,
):
    app = Flask(__name__)
    app.config.update(
        FEDERATED_TELEMETRY_MAX_PAGES_PER_SYNC=max_pages,
        FEDERATED_TELEMETRY_MAX_BATCHES_PER_SYNC=max_batches,
    )
    service = SimpleNamespace(relay_runtime=runtime)

    def refresh(*, reason: str):
        refreshes.append(reason)
        if refresh_accepts:
            return refresh_accepts.pop(0)
        return True

    bridge = FederatedTelemetryProductBridge(
        app,
        service,
        mirror=mirror,  # type: ignore[arg-type]
        refresh_callback=refresh,
        clock=lambda: datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc),
    )
    runtime_state = SimpleNamespace(
        binding=SimpleNamespace(internal_session_id="session-1")
    )
    context = SimpleNamespace(binding=SimpleNamespace(internal_session_id="session-1"))
    return bridge, runtime_state, context


def test_sync_paginates_skips_existing_and_refreshes_only_for_new_files():
    first, second, third, fourth = (_reference(index) for index in range(1, 5))
    runtime = _Runtime(
        {
            None: _page(first, second, cursor="next-page"),
            "next-page": _page(third, fourth),
        }
    )
    mirror = _Mirror(existing=(second,))
    refreshes: list[str] = []
    bridge, state, context = _bridge(runtime, mirror, refreshes=refreshes)

    snapshot = bridge.sync(state, context)

    assert [call["cursor"] for call in runtime.list_calls] == [None, "next-page"]
    assert runtime.read_calls == [first, third, fourth]
    assert [call[0] for call in mirror.ingest_calls] == runtime.read_calls
    assert snapshot == {
        "status": "up-to-date",
        "authority": "owner-1",
        "group": "storage-1",
        "manifest": {"revision": 7, "hash": _MANIFEST_HASH},
        "discovered": 4,
        "downloaded": 3,
        "already": 1,
        "last_error": None,
        "last_sync": "2026-08-11T12:00:00Z",
    }
    assert refreshes == ["federated_telemetry_mirror_updated"]

    runtime.list_calls.clear()
    runtime.read_calls.clear()
    second_snapshot = bridge.sync(state, context)

    assert second_snapshot["downloaded"] == 0
    assert second_snapshot["already"] == 4
    assert runtime.read_calls == []
    assert refreshes == ["federated_telemetry_mirror_updated"]


def test_sync_resumes_from_cursor_when_per_pass_page_bound_is_reached():
    first, second = _reference(1), _reference(2)
    runtime = _Runtime(
        {
            None: _page(first, cursor="next-page"),
            "next-page": _page(second),
        }
    )
    mirror = _Mirror()
    refreshes: list[str] = []
    bridge, state, context = _bridge(
        runtime,
        mirror,
        refreshes=refreshes,
        max_pages=1,
    )

    assert bridge.sync(state, context)["status"] == "syncing"
    assert bridge.sync(state, context)["status"] == "up-to-date"

    assert [call["cursor"] for call in runtime.list_calls] == [None, "next-page"]
    assert runtime.read_calls == [first, second]
    assert len(refreshes) == 2


def test_sync_honors_batch_bound_and_does_not_read_unsupported_schema():
    unsupported = _reference(1, schema_name="fcp.some.other.dataset")
    runtime = _Runtime({None: _page(unsupported, cursor="next-page")})
    mirror = _Mirror()
    refreshes: list[str] = []
    bridge, state, context = _bridge(
        runtime,
        mirror,
        refreshes=refreshes,
        max_batches=1,
    )

    snapshot = bridge.sync(state, context)

    assert snapshot["status"] == "syncing"
    assert snapshot["discovered"] == 1
    assert runtime.list_calls[0]["limit"] == 1
    assert runtime.read_calls == []
    assert mirror.contains_calls == []
    assert refreshes == []


def test_sync_fails_closed_when_storage_authority_is_unavailable():
    runtime = _Runtime({}, ready=False)
    mirror = _Mirror()
    refreshes: list[str] = []
    bridge, state, context = _bridge(runtime, mirror, refreshes=refreshes)

    snapshot = bridge.sync(state, context)

    assert snapshot["status"] == "authority-unavailable"
    assert snapshot["authority"] is None
    assert snapshot["last_error"] == "authority-unavailable"
    assert runtime.list_calls == []
    assert mirror.rebuild_calls == 1
    assert refreshes == []


def test_initial_local_materialization_rebuild_requests_one_refresh():
    runtime = _Runtime({None: _page()})
    mirror = _Mirror(rebuild_count=2)
    refreshes: list[str] = []
    bridge, state, context = _bridge(runtime, mirror, refreshes=refreshes)

    bridge.sync(state, context)
    bridge.sync(state, context)

    assert mirror.rebuild_calls == 1
    assert refreshes == ["federated_telemetry_mirror_updated"]


def test_declined_catalog_refresh_is_retried_after_batch_is_already_local():
    reference = _reference(1)
    runtime = _Runtime({None: _page(reference)})
    mirror = _Mirror()
    refreshes: list[str] = []
    bridge, state, context = _bridge(
        runtime,
        mirror,
        refreshes=refreshes,
        refresh_accepts=[False, True],
    )

    assert bridge.sync(state, context)["downloaded"] == 1
    assert bridge.sync(state, context)["downloaded"] == 0
    assert refreshes == [
        "federated_telemetry_mirror_updated",
        "federated_telemetry_mirror_updated",
    ]


def test_default_refresh_targets_catalog_owned_by_this_app():
    reference = _reference(1)
    runtime = _Runtime({None: _page(reference)})
    mirror = _Mirror()
    reasons: list[str] = []

    class Catalog:
        @staticmethod
        def start_background_rescan_if_idle(*, reason: str) -> bool:
            reasons.append(reason)
            return True

    app = Flask(__name__)
    app.config["ARTIFACT_CATALOG"] = Catalog()
    service = SimpleNamespace(relay_runtime=runtime)
    bridge = FederatedTelemetryProductBridge(app, service, mirror=mirror)  # type: ignore[arg-type]
    state = SimpleNamespace(binding=SimpleNamespace(internal_session_id="session-1"))
    context = SimpleNamespace(binding=SimpleNamespace(internal_session_id="session-1"))

    bridge.sync(state, context)

    assert reasons == ["federated_telemetry_mirror_updated"]


def test_install_exposes_the_same_bridge_owned_by_reconnect_monitor():
    app = Flask(__name__)

    install_federation_pairing(app)

    monitor = app.extensions["federation_saved_membership_reconnect"]
    assert (
        app.extensions["federated_telemetry_product_bridge"] is monitor.telemetry_bridge
    )
