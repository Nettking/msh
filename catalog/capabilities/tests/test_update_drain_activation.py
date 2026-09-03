from __future__ import annotations

from catalog.capabilities.provider_reports import ProviderStatus
from catalog.capabilities.tests.analysis_harness import build_stack
from catalog.capabilities.update_drain import (
    NodeUpdateDrainTarget,
    SQLiteNodeUpdateDrainStore,
)


def test_provider_refresh_projects_durable_drain_and_retains_owned_worker(tmp_path) -> None:
    stack = build_stack(tmp_path)
    provisioner = stack.provisioner
    old_worker = provisioner._worker
    assert old_worker is not None

    drains = SQLiteNodeUpdateDrainStore(stack.store)
    provisioner.drain_store = drains
    mutation = drains.request_drain(
        session_id=stack.session_id,
        target=NodeUpdateDrainTarget(stack.node_id, (stack.provider_id,)),
        command_id="update-drain-refresh",
        now=stack.clock.now,
    )

    assert provisioner.refresh() is True
    report = stack.federation.health.store.get(
        session_id=stack.session_id,
        capability_id=stack.provider_id,
    )
    assert report is not None
    assert report.report.status is ProviderStatus.DRAINING
    assert provisioner._worker is old_worker

    # A provider republication may roll its generation while the node is
    # draining. The durable admission fence follows the exact session/node/
    # provider identity and must not disappear with the old report row.
    provisioner.provider_generation = 2
    assert provisioner.refresh() is True
    rolled = stack.federation.health.store.get(
        session_id=stack.session_id,
        capability_id=stack.provider_id,
    )
    assert rolled is not None
    assert rolled.provider_generation == 2
    assert rolled.report.status is ProviderStatus.DRAINING
    assert provisioner._worker is old_worker

    drains.clear_drain(
        session_id=stack.session_id,
        node_id=stack.node_id,
        expected_revision=mutation.record.revision,
        now=stack.clock.advance(seconds=1),
    )
    assert provisioner.refresh() is True
    ready = stack.federation.health.store.get(
        session_id=stack.session_id,
        capability_id=stack.provider_id,
    )
    assert ready is not None
    assert ready.report.status is ProviderStatus.READY
    assert provisioner._worker is not None
