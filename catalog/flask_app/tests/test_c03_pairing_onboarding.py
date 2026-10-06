"""Configured Flask writers cross a real relay/replica process boundary."""

from __future__ import annotations

import asyncio
import hashlib
import multiprocessing
import re
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import pytest
from flask import Flask, jsonify, request

from catalog.federation.errors import FederationOperationError
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus, Session
from catalog.federation.onboarding_compat import federation_id_from_session_id
from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationSessionBinding,
)
from catalog.federation.tests.test_control_plane_public_journal import (
    SESSION,
    _cluster,
    _journal,
    _populate,
    _wait,
)
from catalog.flask_app import federation_pairing_routes
from catalog.flask_app.services.c03_pairing_onboarding import (
    C03PairingOnboardingService,
    C03RelayCoordinatorFacade,
)
from catalog.flask_app.services.federation_device_names import (
    FederationDeviceNamingService,
)
from catalog.flask_app.services.federation_leader_authority import (
    resolve_federation_leader,
)
from catalog.flask_app.services.federation_pairing_install import _build_service
from catalog.flask_app.services.federation_pairing_service import (
    PairingAwareCapabilityOnboardingService,
    RemotePairingState,
)


def _app_config(root: Path, identity: Path, database: Path, relay_url: str) -> Flask:
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        FCP_REPLICATED_CONTROL_PLANE_CONFIG="configured-by-test-bootstrap",
        CAPABILITY_ONBOARDING_IDENTITY_DIRECTORY=identity,
        CAPABILITY_ONBOARDING_STATE_DATABASE=root / "onboarding.sqlite3",
        CAPABILITY_ONBOARDING_COORDINATOR_DATABASE=database,
        CAPABILITY_ONBOARDING_DEVICE_NAME="Flask member",
        CAPABILITY_ONBOARDING_LOCAL_RELAY_URL=relay_url,
    )
    return app


def _close_runtime(service) -> None:
    runtime = service.relay_runtime
    if runtime._loop is None:
        return
    try:
        runtime._submit(runtime._disconnect_current())
    finally:
        runtime._loop.call_soon_threadsafe(runtime._loop.stop)
        if runtime._thread is not None:
            runtime._thread.join(timeout=5)


def _flask_process(connection, root, identity, database, relay_url, binding_value):
    """A separate Flask process; no coordinator/replica is constructed here.

    The test-only HTTP endpoints exercise the real application service and
    coordinator API. Existing product-route tests retain their auth/CSRF scope.
    """

    app = _app_config(Path(root), Path(identity), Path(database), relay_url)
    service = _build_service(app)
    assert isinstance(service, C03PairingOnboardingService)
    service.binding_store.save(FederationSessionBinding.from_dict(binding_value))
    app.config["CAPABILITY_ONBOARDING_SERVICE"] = service

    @app.errorhandler(FederationOperationError)
    def unavailable(error):
        return jsonify({"error": error.code}), 409

    @app.post("/test/name")
    def name():
        return jsonify(
            {"name": FederationDeviceNamingService().rename_self(request.json["name"])}
        )

    @app.post("/test/event")
    def event():
        context = service.authorized_context()
        assert context is not None
        assert isinstance(context.coordinator, C03RelayCoordinatorFacade)
        event, created = context.coordinator.append_event(
            session_id=context.binding.internal_session_id,
            actor_node_id=context.credentials.identity.node_id,
            event_type="flask.regression.metadata",
            payload=request.json["payload"],
            request_id=request.json["request_id"],
        )
        return jsonify({"event": event.to_dict(), "created": created})

    @app.post("/test/leadership")
    def leadership():
        context = service.authorized_context()
        assert context is not None
        return jsonify(
            context.coordinator.session_leadership(
                context.binding.internal_session_id
            ).to_dict()
        )

    @app.post("/test/authority")
    def authority():
        context = service.authorized_context()
        assert context is not None
        session = context.coordinator.store.get_session(
            context.binding.internal_session_id
        )
        assert isinstance(session, Session)
        leadership = resolve_federation_leader(context)
        return jsonify({
            "session": session.to_dict(),
            "leader_node_id": leadership.leader_node_id,
            "term": leadership.term,
        })

    try:
        with app.test_client() as client:
            connection.send({"ready": True})
            while True:
                command = connection.recv()
                if command is None:
                    break
                response = client.post(command["path"], json=command["json"])
                connection.send(
                    {"status": response.status_code, "json": response.get_json()}
                )
    finally:
        _close_runtime(service)
        connection.close()


def _receive(connection):
    assert connection.poll(60), "the separate Flask process did not respond"
    return connection.recv()


def _observe_authority_reads(coordinator, monkeypatch):
    """Retain bounded call outcomes without another store/property read."""
    records = deque(maxlen=16)
    original = coordinator.session_authority

    def observed(**kwargs):
        try:
            started = time.monotonic()
        except Exception:  # noqa: BLE001 - unavailable diagnostics do not block the original call
            return original(**kwargs)
        try:
            value = original(**kwargs)
        except BaseException as error:
            try:
                records.append(
                    {
                        "outcome": "ERROR",
                        "duration_seconds": time.monotonic() - started,
                        "error_type": type(error).__name__,
                        "reason_sha256": hashlib.sha256(
                            str(error).encode()
                        ).hexdigest(),
                    }
                )
            except Exception:  # noqa: BLE001, S110 - diagnostics cannot replace the business exception
                pass
            raise
        else:
            try:
                records.append(
                    {
                        "outcome": "RETURNED",
                        "duration_seconds": time.monotonic() - started,
                    }
                )
            except Exception:  # noqa: BLE001, S110 - diagnostics cannot replace the business result
                pass
            return value

    monkeypatch.setattr(coordinator, "session_authority", observed)
    return records


def _authority_failure(response, records):
    """Only public operation codes and timings enter an assertion failure."""
    try:
        body = response.get("json")
        code = body.get("error") if isinstance(body, dict) else None
        return {
            "endpoint": "/test/authority",
            "status": response.get("status"),
            "error_code": code
            if isinstance(code, str) and re.fullmatch(r"[a-z0-9-]{1,80}", code)
            else None,
            "authority_reads": tuple(records),
        }
    except Exception:  # noqa: BLE001 - a lost diagnostic is never an authority result
        return {"endpoint": "/test/authority", "diagnostics_unavailable": True}


def _binding(runtime, node_id: str) -> FederationSessionBinding:
    session = runtime.local.store.get_session(SESSION)
    assert session is not None
    return FederationSessionBinding(
        federation_id=federation_id_from_session_id(SESSION),
        internal_session_id=SESSION,
        device_id=node_id,
        state=FederationConnectionState.CONNECTED,
        revision=session.revision,
        trusted=True,
        created_at=session.created_at,
        last_verified_at=datetime.now(timezone.utc),
    )


def test_configured_flask_process_publishes_through_quorum_and_cannot_fall_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        async with _cluster(tmp_path) as cluster:
            clients = await _populate(cluster, tmp_path)
            member = clients[-1]
            # Transfer this identity/NodeState connection to the Flask process.
            await member.disconnect()
            leader = cluster.runtimes[0]
            authority_reads = _observe_authority_reads(
                cluster.relays[0].coordinator, monkeypatch
            )
            ctx = multiprocessing.get_context("spawn")
            parent_pipe, child_pipe = ctx.Pipe()
            process = ctx.Process(
                target=_flask_process,
                args=(
                    child_pipe,
                    str(tmp_path / "flask"),
                    str(member.state_directory),
                    str(leader.deployment.coordinator_database),
                    cluster.relays[0].url,
                    _binding(leader, member.node_id).to_dict(),
                ),
            )
            process.start()
            child_pipe.close()
            try:
                assert await asyncio.to_thread(_receive, parent_pipe) == {"ready": True}

                async def post(path, body):
                    parent_pipe.send({"path": path, "json": body})
                    return await asyncio.to_thread(_receive, parent_pipe)

                renamed = await post("/test/name", {"name": "Quorum Flask member"})
                assert renamed == {
                    "status": 200,
                    "json": {"name": "Quorum Flask member"},
                }
                command = {"request_id": "flask-exactly-once", "payload": {"value": 7}}
                first = await post("/test/event", command)
                duplicate = await post("/test/event", command)
                assert first["status"] == duplicate["status"] == 200
                assert first["json"]["created"] is True
                assert duplicate["json"]["created"] is False
                assert first["json"]["event"] == duplicate["json"]["event"]
                conflict = await post("/test/event", {**command, "payload": {"value": 8}})
                assert conflict["status"] == 409
                assert conflict["json"]["error"] == "idempotency-conflict"

                leadership = await post("/test/leadership", {})
                assert leadership["status"] == 200
                assert leadership["json"]["leader_node_id"] == leader.node.voter_id
                authority = await post("/test/authority", {})
                assert authority["status"] == 200, _authority_failure(
                    authority, authority_reads
                )
                typed_session = Session.from_dict(authority["json"]["session"])
                assert typed_session.created_at == leader.local.store.get_session(
                    SESSION
                ).created_at
                assert typed_session.created_by_node_id == leader.node.voter_id
                assert authority["json"]["leader_node_id"] == leader.node.voter_id
                assert authority["json"]["term"] == leadership["json"]["term"]
                journal = _journal(leader)
                assert any(
                    event["event_type"] == "node.display-name.changed"
                    and event["actor_node_id"] == member.node_id
                    for event in journal
                )
                await _wait(
                    lambda: all(_journal(runtime) == journal for runtime in cluster.runtimes),
                    "Flask writes did not reach every canonical public journal",
                )

                await cluster.stop_runtime(1)
                await cluster.stop_runtime(2)
                before = _journal(leader)
                denied = await post(
                    "/test/event",
                    {"request_id": "flask-without-quorum", "payload": {"value": 9}},
                )
                assert denied["status"] == 409
                assert isinstance(denied["json"]["error"], str)
                assert _journal(leader) == before
            finally:
                if process.is_alive():
                    parent_pipe.send(None)
                    await asyncio.to_thread(process.join, 10)
                if process.is_alive():
                    process.terminate()
                    await asyncio.to_thread(process.join, 10)
                parent_pipe.close()
                assert process.exitcode == 0

    asyncio.run(scenario())


def test_authority_failure_retains_only_public_code_and_bounded_outcomes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Coordinator:
        @property
        def store(self):
            raise AssertionError("diagnostics must not read the store")

        def session_authority(self, **_kwargs):
            return "original-result"

    coordinator = Coordinator()
    records = _observe_authority_reads(coordinator, monkeypatch)
    for _ in range(20):
        assert coordinator.session_authority() == "original-result"
    assert len(records) == 16
    failed = _authority_failure(
        {
            "status": 409,
            "json": {"error": "pairing-relay-timeout", "private": "excluded"},
        },
        records,
    )
    assert failed["error_code"] == "pairing-relay-timeout"
    assert set(failed) == {"endpoint", "status", "error_code", "authority_reads"}
    assert all(
        set(record) == {"outcome", "duration_seconds"}
        for record in failed["authority_reads"]
    )
    assert (
        _authority_failure(
            {"status": 409, "json": {"error": "unbounded private message"}}, records
        )["error_code"]
        is None
    )


def test_authority_observer_preserves_exact_business_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OriginalFailure(RuntimeError):
        def __str__(self):
            raise ValueError("diagnostic string conversion failed")

    original_error = OriginalFailure()

    class Coordinator:
        def session_authority(self, **_kwargs):
            raise original_error

    coordinator = Coordinator()
    records = _observe_authority_reads(coordinator, monkeypatch)
    with pytest.raises(OriginalFailure) as caught:
        coordinator.session_authority()
    assert caught.value is original_error
    assert not records


def test_authority_observer_error_retains_type_and_hash_without_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_error = RuntimeError("private inner cause")

    class Coordinator:
        def session_authority(self, **_kwargs):
            raise original_error

    coordinator = Coordinator()
    records = _observe_authority_reads(coordinator, monkeypatch)
    with pytest.raises(RuntimeError) as caught:
        coordinator.session_authority()
    assert caught.value is original_error
    assert (
        records[0]["reason_sha256"]
        == hashlib.sha256(str(original_error).encode()).hexdigest()
    )
    assert records[0]["error_type"] == "RuntimeError"
    assert set(records[0]) == {
        "outcome",
        "duration_seconds",
        "error_type",
        "reason_sha256",
    }


def test_authority_observer_lost_clock_does_not_change_original_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    class Coordinator:
        def session_authority(self, **kwargs):
            calls.append(kwargs)
            return "original-result"

    coordinator = Coordinator()
    records = _observe_authority_reads(coordinator, monkeypatch)

    def unavailable():
        raise RuntimeError("diagnostic clock unavailable")

    monkeypatch.setattr(time, "monotonic", unavailable)
    assert (
        coordinator.session_authority(session_id="original-session")
        == "original-result"
    )
    assert calls == [{"session_id": "original-session"}]
    assert not records


def test_pairing_material_uses_authenticated_leader_and_real_quorum(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with _cluster(tmp_path) as cluster:
            clients = await _populate(cluster, tmp_path)
            leader_client = clients[0]
            member_client = clients[-1]
            with pytest.raises(FederationOperationError) as denied:
                await member_client.request(
                    "session.pairing-material",
                    session_id=SESSION,
                    payload={"ttl_seconds": 60, "actor_node_id": leader_client.node_id},
                    request_id="forged-pairing-actor",
                )
            assert denied.value.code == "federation-leader-required"
            material = await leader_client.request(
                "session.pairing-material",
                session_id=SESSION,
                payload={"ttl_seconds": 60},
                request_id="authorized-host-material",
            )
            assert set(material) == {"enrollment", "invitation"}
            joiner = cluster.client(tmp_path / "paired-from-material", 0, "paired member")
            await joiner.connect(enrollment_token=material["enrollment"]["token"])
            # Authentication alone and a forged payload actor cannot grant a
            # session read to this not-yet-joined device.
            with pytest.raises(FederationOperationError) as unauthorized_read:
                await joiner.request(
                    "session.authority",
                    session_id=SESSION,
                    payload={"actor_node_id": leader_client.node_id},
                )
            assert unauthorized_read.value.code == "not-session-member"
            joined = await joiner.join_session(material["invitation"]["token"])
            assert joined["session_id"] == SESSION
            # The real host-code hook must also use the relay; move the leader
            # identity's existing connection to the configured Flask service.
            await leader_client.disconnect()
            leader = cluster.runtimes[0]
            app = _app_config(
                tmp_path / "host-flask",
                leader_client.state_directory,
                leader.deployment.coordinator_database,
                cluster.relays[0].url,
            )
            service = _build_service(app)
            service.binding_store.save(_binding(leader, leader_client.node_id))
            try:
                code = await asyncio.to_thread(
                    service.create_pairing_code,
                    relay_url=cluster.relays[0].url,
                    ttl_seconds=60,
                    remember=False,
                )
                offer = service.pairing_codec.decode(code)
                assert offer.internal_session_id == SESSION
                assert service.last_pairing_code() is None
                paired = cluster.client(tmp_path / "paired-from-flask", 0, "Flask paired")
                await paired.connect(enrollment_token=offer.enrollment_token)
                assert (await paired.join_session(offer.invitation_token))["session_id"] == SESSION

                facade = service.coordinator
                capability = CapabilityAnnouncement(
                    capability_id="flask-owned-declaration",
                    node_id=leader_client.node_id,
                    session_id=SESSION,
                    type="journal-regression",
                    protocol="bounded-public-payload",
                    protocol_version="1",
                    status=CapabilityStatus.READY,
                    properties={"purpose": "Flask coordinator return types"},
                    announced_at=datetime.now(timezone.utc),
                )
                accepted = await asyncio.to_thread(
                    facade.announce_capability,
                    capability,
                    actor_node_id=leader_client.node_id,
                    request_id="flask-capability-idempotency",
                )
                duplicate = await asyncio.to_thread(
                    facade.announce_capability,
                    capability,
                    actor_node_id=leader_client.node_id,
                    request_id="flask-capability-idempotency",
                )
                assert accepted == (capability, True)
                assert duplicate == (capability, True)

                await cluster.stop_runtime(1)
                await cluster.stop_runtime(2)
                before = _journal(leader)
                app.config["CAPABILITY_ONBOARDING_SERVICE"] = service
                with app.test_request_context("/onboarding/federation/discovery.json"):
                    advertised = federation_pairing_routes._discovery_response()
                assert advertised.status_code == 200
                assert advertised.get_json()["pairing_required"] is True
                assert "pairing_code" not in advertised.get_json()
                assert _journal(leader) == before
                with pytest.raises(FederationOperationError):
                    await asyncio.to_thread(
                        service.create_pairing_code,
                        relay_url=cluster.relays[0].url,
                        ttl_seconds=60,
                    )
                assert service.last_pairing_code() is None
                assert _journal(leader) == before
            finally:
                await asyncio.to_thread(_close_runtime, service)

    asyncio.run(scenario())


def test_builder_preserves_default_and_requires_c03_bootstrap_binding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FCP_REPLICATED_CONTROL_PLANE_CONFIG", raising=False)
    app = _app_config(tmp_path, tmp_path / "identity", tmp_path / "control.db", "")
    app.config["FCP_REPLICATED_CONTROL_PLANE_CONFIG"] = ""
    standalone = _build_service(app)
    assert type(standalone) is PairingAwareCapabilityOnboardingService
    app.config["FCP_REPLICATED_CONTROL_PLANE_CONFIG"] = "configured"
    service = _build_service(app)
    assert isinstance(service, C03PairingOnboardingService)
    service.create_identity()
    with pytest.raises(FederationOperationError) as missing:
        service.connect(request_id="must-not-bootstrap-locally")
    assert missing.value.code == "c03-bootstrap-binding-required"
    assert not (tmp_path / "control.db").exists()
    assert service.relay_runtime._loop is None


def test_c03_missing_endpoint_and_retained_binding_never_open_local_authority(
    tmp_path: Path,
) -> None:
    app = _app_config(tmp_path, tmp_path / "identity", tmp_path / "control.db", "")
    service = _build_service(app)
    credentials = service.create_identity()
    now = datetime.now(timezone.utc)
    binding = FederationSessionBinding(
        federation_id=federation_id_from_session_id(SESSION),
        internal_session_id=SESSION,
        device_id=credentials.identity.node_id,
        state=FederationConnectionState.CONNECTED,
        revision=1,
        trusted=True,
        created_at=now,
        last_verified_at=now,
    )
    service.binding_store.save(binding)
    with pytest.raises(FederationOperationError) as missing:
        service.authorized_context()
    assert missing.value.code == "c03-relay-binding-required"
    # A saved explicitly bound remote endpoint is sufficient for projection;
    # projection construction does not attempt a socket or grant live authority.
    service.remote_store.save(RemotePairingState("ws://127.0.0.1:1", binding))
    retained = service.retained_context_for_read_only_projection()
    assert retained is not None
    assert isinstance(retained.coordinator, C03RelayCoordinatorFacade)
    assert service.relay_runtime._loop is None
    assert not (tmp_path / "control.db").exists()
    with pytest.raises(FederationOperationError) as denied:
        retained.coordinator.append_event(
            session_id=SESSION,
            actor_node_id="another-member",
            request_id="wrong-actor",
            event_type="flask.regression.metadata",
            payload={},
        )
    assert denied.value.code == "pairing-actor-mismatch"
    assert service.relay_runtime._loop is None
