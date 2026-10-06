"""Real shared application relay stays responsive during cold manifest work."""
from __future__ import annotations

import asyncio
import copy
import json
import threading
from dataclasses import replace
from datetime import datetime, timezone

from catalog.ai.relay_remote import RelayRemoteAIEndpoint
from catalog.capabilities.relay_lifecycle import RelayLifecycleEndpoint
from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.manifest import ManifestItemKind
from catalog.federation.manifest_store import (
    ManifestItemProposal,
    _VerifiedRevisionCache,
)
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.recorder_storage_relay import (
    RecorderLogicalStorageAuthority,
    RelayRecorderStorageClient,
)
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.federation.tests.test_recorder_storage_relay import _request_payload
from catalog.flask_app.services import (
    trusted_storage_authority_runtime as authority_runtime,
)
from catalog.flask_app.services.federation_storage_authority_install import (
    _StorageAwareRelayView,
)
from catalog.flask_app.tests.test_storage_authority_local_provider_runtime import (
    _register_primary,
    _settings,
)
from catalog.node.client import RelayNodeClient
from catalog.relay.service import RelayServer


def test_real_shared_wire_cold_validation_heartbeat_and_two_distinct_commits(tmp_path, monkeypatch):
    async def scenario():
        relay = RelayServer(SessionCoordinator(tmp_path / "relay.sqlite3"), host="127.0.0.1", port=0)
        await relay.start()
        clients = [RelayNodeClient(
            state_directory=tmp_path / name, relay_url=relay.url,
            display_name=f"Isolated {name}", allow_insecure_local=True, request_timeout=2,
        ) for name in ("creator", "recorder", "peer")]
        creator, recorder, peer = clients
        stages, sender, task = [], None, None
        entered, release = threading.Event(), threading.Event()
        try:
            for client in clients:
                enrollment = relay.coordinator.create_enrollment_token(ttl_seconds=30, max_uses=1)
                await client.connect(enrollment_token=enrollment["token"])
            session_id = (await creator.create_session("Isolated shared storage cold phases"))["session_id"]
            invite = await creator.create_invitation(session_id, ttl_seconds=30, max_uses=2)
            for client in (recorder, peer):
                await client.join_session(invite["token"])
            settings = replace(_settings(tmp_path), session_id=session_id, relay=relay.url,
                relay_control_database=str(tmp_path / "relay.sqlite3"))
            control = PhaseDControlPlane(settings.storage_control_database)
            provider_id = authority_runtime._builtin_local_provider_id(creator.node_id)
            now = datetime.now(timezone.utc)
            group = "fcp-local-storage"
            _register_primary(control, session_id=session_id, provider_id=provider_id, node_id=creator.node_id, now=now)
            revision = control.snapshot(session_id).revision
            # Tiny, explicitly synthetic complete history, not manufactured
            # Recorder/acceptance evidence or a prediction of 721-row timings.
            for index in range(8):
                control.manifests.commit(ManifestItemProposal(
                    session_id=session_id, group_id=group, item_id=f"synthetic-{index}",
                    kind=ManifestItemKind.BATCH, dataset_id="synthetic-history",
                    idempotency_key=f"synthetic:{index}", content_hash="sha256:" + "a" * 64,
                    size_bytes=1, schema_name="synthetic.history", schema_version=1,
                    source_id=None, first_sequence=None, last_sequence=None, dataset_required=True,
                    acknowledged_provider_ids=(provider_id,), term=1,
                    expected_control_revision=revision, committed_at=now,
                ))
            control.manifests._verified_revisions = _VerifiedRevisionCache()
            original, owner_thread, paused = control.manifests._decode_revision, threading.get_ident(), False

            def cold(row, **kwargs):
                nonlocal paused
                if not paused:
                    paused = True
                    assert threading.get_ident() != owner_thread
                    entered.set()
                    assert release.wait(3), "owner loop did not run during cold validation"
                return original(row, **kwargs)

            monkeypatch.setattr(control.manifests, "_decode_revision", cold)
            ai = RelayRemoteAIEndpoint(creator, request_timeout=2)
            await ai.start()
            stages.append(ai)
            endpoint = RelayStorageEndpoint(creator, message_source=ai, request_timeout=2)
            service = authority_runtime._ensure_builtin_local_storage_service(
                endpoint=endpoint, control=control, client=creator, settings=settings,
            )
            assert service is not None
            await endpoint.start()
            stages.append(endpoint)
            channel = authority_runtime.SharedRecorderAwareStorageControlRelayChannel(creator, endpoint, timeout=2)
            logical = PhaseDLogicalStorageClient(
                session_id=session_id, actor_node_id=creator.node_id, control_plane=control,
                transport=endpoint, acknowledgements=DurableAcknowledgementStore(settings.acknowledgements_database),
            )
            authority = RecorderLogicalStorageAuthority(client=creator, logical_client=logical, session_id=session_id)
            channel.set_recorder_ingest_handler(authority.handle_request)
            await channel.start()
            stages.append(channel)
            lifecycle = RelayLifecycleEndpoint(creator, message_source=_StorageAwareRelayView(ai, channel))
            await lifecycle.start()
            stages.append(lifecycle)
            sender = RelayRecorderStorageClient(recorder, session_id=session_id, authority_node_id=creator.node_id, request_timeout=3)
            await sender.start()
            original_payload = _request_payload()
            dataset = "mtconnect:" + recorder.node_id + ":Mazak"

            def values(first):
                content = copy.deepcopy(original_payload["content"])
                content.update(first_sequence=first, last_sequence=first + 1,
                    raw_batch_first_sequence=first, raw_batch_last_sequence=first + 1)
                for index, item in enumerate(content["observations"]):
                    item["sequence"] = first + index
                batch = f"Mazak:1:{first}:{first + 1}:{content['raw_sha256'][7:]}"
                return {"group_id": group, "dataset_id": dataset, "batch_id": batch,
                    "idempotency_key": f"{session_id}:{dataset}:{batch}", "content": content,
                    "dataset_schema_name": original_payload["dataset_schema_name"],
                    "dataset_schema_version": 1, "created_at": datetime.now(timezone.utc)}

            first = values(1)
            task = asyncio.create_task(sender.ingest_batch(**first))
            assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 3), 4)
            # A real authenticated relay round trip, while cold decoder is held.
            heartbeat = await asyncio.wait_for(peer.request("heartbeat", payload={}), 1)
            assert heartbeat["connection_state"] == "connected"
            assert not task.done()
            release.set()
            assert (await asyncio.wait_for(task, 5)).committed is True
            first_head = control.manifests.head(session_id, group)
            await asyncio.sleep(.01)
            second = values(3)
            assert (await asyncio.wait_for(sender.ingest_batch(**second), 5)).committed is True
            second_head = control.manifests.head(session_id, group)
            assert second_head.revision == first_head.revision + 1
            assert first["batch_id"] != second["batch_id"]
            for request in (first, second):
                assert logical.acknowledgements.status(session_id, group, request["batch_id"]).committed
                assert service.provider.read(session_id=session_id, group_id=group, batch_id=request["batch_id"]) == request["content"]
            assert not ai._reader_task.done() and not endpoint._reader_task.done()
            (tmp_path / "isolated-wire-proof.json").write_text(json.dumps({
                "physical_acceptance": False, "synthetic_history_items": 8,
                "real_authenticated_heartbeat_while_cold_phase_held": True,
                "first_head_revision": first_head.revision, "second_head_revision": second_head.revision,
                "distinct_committed_batches": [first["batch_id"], second["batch_id"]],
            }), encoding="utf-8")
        finally:
            release.set()
            if task is not None:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            if sender is not None:
                await sender.close()
            for stage in reversed(stages):
                await stage.close()
            for client in reversed(clients):
                await client.disconnect()
            await relay.stop()

    asyncio.run(asyncio.wait_for(scenario(), 25))
