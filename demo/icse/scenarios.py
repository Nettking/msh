"""Claim-aligned ICSE demonstration scenarios E1 and E2."""

from __future__ import annotations

from pathlib import Path

from catalog.federation.onboarding_models import ContributionActivationState
from demo.icse.harness import (
    AI_ID,
    NOW,
    STORAGE_ID,
    SyntheticAuthorityHarness,
    build_stack,
    candidate_for_existing_federation,
    candidate_ids,
    configured_discovery,
    connect_local,
    inspect_and_benchmark,
    join_candidate,
    save_choices,
)


def _intent_snapshot(stack, ids: dict[str, str]) -> dict[str, dict[str, str]]:
    with stack.app.app_context():
        by_id = {intent.candidate_id: intent for intent in stack.contributions.intents()}
    return {
        capability: {
            "desired": by_id[candidate_id].desired_state.value,
            "activation": by_id[candidate_id].activation_state.value,
        }
        for capability, candidate_id in sorted(ids.items())
    }


def selective_contribution(root: Path) -> dict[str, object]:
    """E1: one member can independently govern several local functions."""

    current = [NOW]
    harness = SyntheticAuthorityHarness()
    stack = build_stack(root / "member", current=current, harness=harness)
    try:
        client = stack.app.test_client()
        csrf, command_id = connect_local(client)
        inspect_and_benchmark(client, csrf)
        ids = candidate_ids(stack)
        save_choices(
            client,
            csrf=csrf,
            command_id=command_id,
            ids=ids,
            recorder="enabled",
            language_model="enabled",
            compute="disabled",
            storage="ask-later",
        )
        states = _intent_snapshot(stack, ids)

        if not harness.recorder.enabled:
            raise AssertionError("recorder contribution did not activate")
        if harness.ai_active != {AI_ID}:
            raise AssertionError("language-model contribution did not activate")
        if harness.compute_active:
            raise AssertionError("disabled compute contribution activated")
        if states["storage"]["desired"] != "ask-later":
            raise AssertionError("storage contribution intent was not preserved")

        return {
            "claim": "one member can govern several local functions independently",
            "member": "member",
            "contributions": states,
            "observed_runtime": {
                "recorder_active": harness.recorder.enabled,
                "language_model_active": sorted(harness.ai_active),
                "compute_active": bool(harness.compute_active),
            },
        }
    finally:
        stack.close()


def authority_boundary(root: Path) -> dict[str, object]:
    """E2: contribution intent is insufficient without provider authority."""

    current = [NOW]
    owner_harness = SyntheticAuthorityHarness()
    owner = build_stack(root / "owner", current=current, harness=owner_harness)
    compute = None
    storage = None
    try:
        owner_client = owner.app.test_client()
        owner_csrf, owner_command = connect_local(owner_client)
        context = owner.onboarding.authorized_context()
        if context is None:
            raise AssertionError("owner has no authorized federation context")
        session_id = context.binding.internal_session_id
        actor_node_id = context.credentials.identity.node_id

        compute_candidate = candidate_for_existing_federation(
            owner,
            actor_node_id=actor_node_id,
            session_id=session_id,
            suffix="compute",
        )
        storage_candidate = candidate_for_existing_federation(
            owner,
            actor_node_id=actor_node_id,
            session_id=session_id,
            suffix="storage",
        )

        compute_harness = SyntheticAuthorityHarness()
        storage_harness = SyntheticAuthorityHarness()
        compute = build_stack(
            root / "compute",
            current=current,
            harness=compute_harness,
            coordinator=owner.coordinator,
            discovery_sources=(configured_discovery(compute_candidate),),
        )
        storage = build_stack(
            root / "storage",
            current=current,
            harness=storage_harness,
            coordinator=owner.coordinator,
            discovery_sources=(configured_discovery(storage_candidate),),
        )
        compute_client = compute.app.test_client()
        storage_client = storage.app.test_client()
        compute_csrf, compute_command = join_candidate(compute_client, compute_candidate)
        storage_csrf, storage_command = join_candidate(storage_client, storage_candidate)

        for member in (owner, compute, storage):
            member_context = member.onboarding.authorized_context()
            if member_context is None or member_context.binding.internal_session_id != session_id:
                raise AssertionError("members did not join one federation session")

        inspect_and_benchmark(owner_client, owner_csrf)
        owner_ids = candidate_ids(owner)
        save_choices(
            owner_client,
            csrf=owner_csrf,
            command_id=owner_command,
            ids=owner_ids,
            language_model="enabled",
        )

        inspect_and_benchmark(compute_client, compute_csrf)
        compute_ids = candidate_ids(compute)
        save_choices(
            compute_client,
            csrf=compute_csrf,
            command_id=compute_command,
            ids=compute_ids,
            compute="enabled",
        )

        inspect_and_benchmark(storage_client, storage_csrf)
        storage_ids = candidate_ids(storage)
        save_choices(
            storage_client,
            csrf=storage_csrf,
            command_id=storage_command,
            ids=storage_ids,
            storage="enabled",
        )
        before = _intent_snapshot(storage, storage_ids)["storage"]
        if before["activation"] != ContributionActivationState.PENDING.value:
            raise AssertionError(f"storage should be pending before authority, got {before}")
        if storage_harness.storage_assigned:
            raise AssertionError("storage became assigned before authority was granted")

        storage_harness.storage_assigned.add(STORAGE_ID)
        response = storage_client.post(
            "/onboarding/contributions/reconcile",
            data={
                "_csrf_token": storage_csrf,
                "command_id": storage_command,
            },
        )
        if response.status_code != 303:
            raise RuntimeError(f"storage reconcile returned {response.status_code}")
        after = _intent_snapshot(storage, storage_ids)["storage"]
        if after["activation"] != ContributionActivationState.ACTIVE.value:
            raise AssertionError(f"storage should activate after authority, got {after}")

        return {
            "claim": "contribution intent does not itself grant provider authority",
            "federation_session_shared": True,
            "member_contributions": {
                "owner": _intent_snapshot(owner, owner_ids),
                "compute": _intent_snapshot(compute, compute_ids),
                "storage": _intent_snapshot(storage, storage_ids),
            },
            "storage_transition": {
                "before_authority": before,
                "after_authority": after,
            },
        }
    finally:
        if storage is not None:
            storage.close()
        if compute is not None:
            compute.close()
        owner.close()
