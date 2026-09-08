"""Configured product provider routes use a real authenticated C03 authority."""

from __future__ import annotations

import asyncio
import copy
import json
import multiprocessing
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.capabilities.operator_wire import (
    operator_snapshot_from_dict,
    operator_view_from_dict,
)
from catalog.capabilities.provider_enrollment import _announcement_fingerprint
from catalog.federation.errors import FederationOperationError
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.onboarding_models import FederationSessionBinding
from catalog.federation.tests.test_control_plane_public_journal import (
    SESSION,
    _cluster,
)
from catalog.flask_app.provider_federation_routes import provider_federation_web
from catalog.flask_app.services.c03_pairing_onboarding import (
    C03PairingOnboardingService,
)
from catalog.flask_app.services.federation_pairing_install import _build_service
from catalog.flask_app.tests.test_c03_pairing_onboarding import (
    _app_config,
    _binding,
    _close_runtime,
    _receive,
)
from catalog.node.client import RelayRemoteError

CSRF = "c03-provider-regression-csrf-token-0123456789"
CAPABILITY = "manual-pending-compute"


def _provider_process(connection, root, identity, database, relay_url, binding_value):
    """Use the normal C03 service factory and the actual product HTTP routes."""
    app = _app_config(Path(root), Path(identity), Path(database), relay_url)
    app.secret_key = "disposable-provider-route-test-key"
    service = _build_service(app)
    assert isinstance(service, C03PairingOnboardingService)
    service.binding_store.save(FederationSessionBinding.from_dict(binding_value))
    app.config["CAPABILITY_ONBOARDING_SERVICE"] = service
    app.register_blueprint(provider_federation_web)
    try:
        with app.test_client() as client:
            with client.session_transaction() as session:
                session["provider_federation_csrf_token"] = CSRF
            connection.send({"ready": True})
            while True:
                command = connection.recv()
                if command is None:
                    break
                response = client.open(
                    command["path"],
                    method=command["method"],
                    json=command.get("json"),
                    headers=command.get("headers", {}),
                )
                connection.send({
                    "status": response.status_code,
                    "json": response.get_json(),
                })
    finally:
        _close_runtime(service)
        connection.close()


def test_configured_c03_provider_page_and_manual_pending_approval(tmp_path: Path):
    async def scenario():
        async with _cluster(tmp_path) as cluster:
            clients = [
                cluster.client(runtime.deployment.identity_directory, 0, f"voter-{index}")
                for index, runtime in enumerate(cluster.runtimes[:2])
            ]
            for client in clients:
                await client.connect()
            provider = clients[1]
            announcement = CapabilityAnnouncement(
                capability_id=CAPABILITY,
                node_id=provider.node_id,
                session_id=SESSION,
                type="compute",
                protocol="registered-handler-regression",
                protocol_version="1",
                status=CapabilityStatus.REGISTERING,
                properties={
                    "kind": "capability-first-candidate",
                    "candidate_id": "registered-handler-candidate",
                    "supported_handlers": ["registered-review-reading"],
                    "review_metadata": {"exact": "retained by authority"},
                },
                announced_at=datetime.now(timezone.utc),
            )
            await provider.announce_capability(announcement)
            requested = await provider.request(
                "provider.enrollment.request",
                session_id=SESSION,
                request_id="provider-manual-request",
                payload={"capability_id": CAPABILITY},
            )
            revision = requested["enrollment"]["revision"]
            operator = clients[0]
            with pytest.raises(RelayRemoteError, match="provider-not-ready"):
                await operator.request(
                    "provider.enrollment.approve", session_id=SESSION,
                    request_id="generic-ready-only",
                    payload={"capability_id": CAPABILITY, "expected_revision": revision},
                )
            await operator.disconnect()
            runtime = cluster.runtimes[0]
            ctx = multiprocessing.get_context("spawn")
            parent_pipe, child_pipe = ctx.Pipe()
            process = ctx.Process(target=_provider_process, args=(
                child_pipe,
                str(tmp_path / "flask-provider"),
                str(operator.state_directory),
                str(runtime.deployment.coordinator_database),
                cluster.relays[0].url,
                _binding(runtime, operator.node_id).to_dict(),
            ))
            process.start()
            child_pipe.close()
            try:
                assert await asyncio.to_thread(_receive, parent_pipe) == {"ready": True}

                async def request(method, path, **kwargs):
                    parent_pipe.send({"method": method, "path": path, **kwargs})
                    return await asyncio.to_thread(_receive, parent_pipe)

                view = await request("GET", "/provider-federation/api/providers")
                approved = await request(
                    "POST", f"/provider-federation/api/providers/{CAPABILITY}/approve",
                    json={"expected_revision": revision, "command_id": "manual-approve"},
                    headers={"X-CSRF-Token": CSRF},
                )
                # Both real paths must work; retain their actual failure response
                # in pytest output when proving the unfixed composition defect.
                assert (view["status"], approved["status"]) == (200, 200), {
                    "view": view, "approved": approved,
                }
                pending = next(
                    row for row in view["json"]["view"]["providers"]
                    if row["capability_id"] == CAPABILITY
                )
                assert pending["announcement_status"] == CapabilityStatus.REGISTERING.value
                assert "approve" in pending["allowed_actions"]
                assert "properties" not in pending
                assert "announcement_fingerprint" not in pending
                assert "supported_handlers" not in pending
                assert pending["announced_at"] == announcement.to_dict()["announced_at"]
                assert approved["json"]["provider"]["enrollment_state"] == "approved"
                assert approved["json"]["provider"]["activation_state"] != "eligible"
                store = cluster.relays[0].provider_enrollment.store
                record = store.get(session_id=SESSION, capability_id=CAPABILITY)
                assert record.announcement_fingerprint == _announcement_fingerprint(announcement)
                assert not record.eligible_for_resource_binding
                assert not list((tmp_path / "flask-provider").rglob("provider_*.sqlite3"))

                async def action(name, command_id, expected_revision, **extra):
                    return await request(
                        "POST", f"/provider-federation/api/providers/{CAPABILITY}/{name}",
                        json={"expected_revision": expected_revision, "command_id": command_id, **extra},
                        headers={"X-CSRF-Token": CSRF},
                    )

                # Rendered controls are never authorization. Existing CSRF,
                # bound context, revisions and command identity remain enforced.
                no_csrf = await request(
                    "POST", f"/provider-federation/api/providers/{CAPABILITY}/suspend",
                    json={"expected_revision": record.revision, "command_id": "no-csrf"},
                )
                assert no_csrf["status"] == 403
                override = await action("suspend", "actor-override", record.revision,
                                        actor_node_id=provider.node_id)
                assert override["status"] == 400
                conflict = await action("suspend", "manual-approve", record.revision)
                assert conflict["status"] == 409
                assert conflict["json"]["error"]["code"] == "idempotency-conflict"
                stale = await action("suspend", "stale-revision", record.revision + 10)
                assert stale["status"] == 409
                repeated = await action("approve", "manual-approve", revision)
                assert repeated["status"] == 403  # Existing action matrix rejects already approved.
                assert store.get(session_id=SESSION, capability_id=CAPABILITY) == record

                readonly = await provider.request("provider.operator.view", session_id=SESSION, payload={})
                assert readonly["view"]["is_owner"] is False
                assert all(not item["allowed_actions"] for item in readonly["view"]["providers"])
                with pytest.raises(RelayRemoteError, match="federation-leader-required"):
                    await provider.request(
                        "provider.operator.execute", session_id=SESSION,
                        payload={"action": "suspend", "capability_id": CAPABILITY,
                                 "expected_revision": record.revision},
                    )
                with pytest.raises(RelayRemoteError):
                    await provider.request("provider.operator.view", session_id="foreign-session", payload={})
                with pytest.raises(RelayRemoteError, match="invalid-provider-operator-command"):
                    await provider.request(
                        "provider.operator.view", session_id=SESSION,
                        payload={"actor_node_id": operator.node_id},
                    )

                storage = replace(announcement, capability_id="storage-pending", type="storage")
                await provider.announce_capability(storage)
                refused_storage = await request(
                    "POST", "/provider-federation/api/providers/storage-pending/request",
                    json={"command_id": "storage-never-provider-approval"},
                    headers={"X-CSRF-Token": CSRF},
                )
                assert refused_storage["status"] == 403
                assert store.get(session_id=SESSION, capability_id="storage-pending") is None

                # A READY announcement and explicit reconciliation are separate
                # steps. Neither approval nor this view registers an executor.
                ready = replace(announcement, status=CapabilityStatus.READY,
                                announced_at=datetime.now(timezone.utc))
                await provider.announce_capability(ready)
                assert not cluster.relays[0].provider_enrollment.eligible_records(
                    session_id=SESSION, actor_node_id=operator.node_id,
                )
                reconciled = await action("reconcile", "exact-reconcile", record.revision)
                assert reconciled["status"] == 200
                reconciled_record = store.get(session_id=SESSION, capability_id=CAPABILITY)
                replayed = await action("reconcile", "exact-reconcile", record.revision)
                assert replayed["status"] == 200
                assert store.get(session_id=SESSION, capability_id=CAPABILITY) == reconciled_record
                assert reconciled_record.announcement_fingerprint == _announcement_fingerprint(ready)
                assert reconciled_record.eligible_for_resource_binding
                assert reconciled["json"]["provider"]["activation_state"] != "eligible"
                changed = replace(ready, properties={**ready.properties, "review_metadata": {"exact": "changed"}},
                                  announced_at=datetime.now(timezone.utc))
                await provider.announce_capability(changed)
                assert not cluster.relays[0].provider_enrollment.eligible_records(
                    session_id=SESSION, actor_node_id=operator.node_id,
                )

                await asyncio.to_thread(
                    cluster.relays[0].coordinator.remove_member,
                    session_id=SESSION, actor_node_id=operator.node_id,
                    target_node_id=provider.node_id, request_id="remove-rendered-provider-member",
                    reason="operator-regression-membership-removal",
                )
                with pytest.raises(RelayRemoteError, match="not-session-member"):
                    await provider.request("provider.operator.view", session_id=SESSION, payload={})
                assert store.get(session_id=SESSION, capability_id=CAPABILITY) == reconciled_record

                # Losing both other real voters invalidates the already-rendered
                # action. The last committed provider decision stays untouched.
                assert runtime.ready
                await cluster.stop_runtime(1)
                await cluster.stop_runtime(2)
                no_quorum = await action("suspend", "no-quorum", reconciled_record.revision)
                assert no_quorum["status"] in {403, 409, 503}
                assert store.get(session_id=SESSION, capability_id=CAPABILITY) == reconciled_record
            finally:
                if process.is_alive():
                    parent_pipe.send(None)
                await asyncio.to_thread(process.join, 15)
                if process.is_alive():
                    process.terminate()
                    await asyncio.to_thread(process.join, 5)
                parent_pipe.close()
                assert process.exitcode == 0

    asyncio.run(scenario())


def _public_view():
    """A public wire fixture contains no authority payload or local services."""
    stamp = "2026-09-09T10:00:00Z"
    snapshot = {
        "schema": "fcp.provider-operator-snapshot.v1",
        "session_id": SESSION, "capability_id": CAPABILITY, "node_id": "provider-node",
        "capability_type": "compute", "protocol": "registered-handler-regression",
        "protocol_version": "1", "discovered": True,
        "announcement_status": "registering", "announced_at": stamp,
        "enrollment_state": "pending", "enrollment_revision": 1,
        "enrollment_reason_code": "requested", "health_state": "absent",
        "health_reason_code": "no-report", "provider_status": None,
        "provider_generation": None, "report_revision": None,
        "reported_at": None, "expires_at": None, "max_concurrent_jobs": None,
        "active_jobs": None, "queue_depth": None, "utilization_millis": None,
        "activation_state": "unavailable", "activation_reason_code": "enrollment-pending",
        "inventory_compatible": None, "can_manage": True,
        "allowed_actions": ["approve", "suspend", "revoke", "reconcile"],
    }
    return {
        "schema": "fcp.provider-operator-view.v1", "session_id": SESSION,
        "actor_node_id": "operator-node", "is_owner": True,
        "generated_at": stamp, "providers": [snapshot],
    }


def test_operator_wire_roundtrip_is_typed_and_bound():
    raw = _public_view()
    decoded = operator_view_from_dict(raw, session_id=SESSION, actor_node_id="operator-node")
    assert json.loads(json.dumps(decoded.to_dict())) == raw
    assert decoded.providers[0].announced_at.tzinfo is not None
    with pytest.raises(FederationOperationError, match="invalid-provider-operator-response"):
        operator_snapshot_from_dict(raw["providers"][0], session_id=SESSION, capability_id="foreign")


@pytest.mark.parametrize("scope,field,value", [
    ("view", "actor_node_id", "foreign-actor"),
    ("view", "session_id", "foreign-session"),
    ("view", "schema", "unreviewed-schema"),
    ("view", "is_owner", 1),
    ("view", "extra", True),
    ("view", "generated_at", "2026-09-09T10:00:00"),
    ("snapshot", "session_id", "foreign-session"),
    ("snapshot", "schema", "unreviewed-schema"),
    ("snapshot", "properties", {"private": "not-part-of-this-projection"}),
    ("snapshot", "provider_status", {}),
    ("snapshot", "provider_status", []),
    ("snapshot", "provider_status", "unreviewed-status"),
    ("snapshot", "capability_id", "invalid-\ud800"),
    ("snapshot", "enrollment_revision", True),
    ("snapshot", "enrollment_revision", "1"),
    ("snapshot", "enrollment_revision", 0),
    ("snapshot", "announcement_status", "invented"),
    ("snapshot", "health_state", "invented"),
    ("snapshot", "activation_state", "invented"),
    ("snapshot", "allowed_actions", ["execute-arbitrary-provider"]),
    ("snapshot", "allowed_actions", ["approve", "approve"]),
    ("snapshot", "can_manage", False),
])
def test_operator_wire_rejects_malformed_or_foreign_projection(scope, field, value):
    raw = copy.deepcopy(_public_view())
    selected = raw if scope == "view" else raw["providers"][0]
    selected[field] = value
    with pytest.raises(FederationOperationError, match="invalid-provider-operator-response"):
        operator_view_from_dict(raw, session_id=SESSION, actor_node_id="operator-node")
