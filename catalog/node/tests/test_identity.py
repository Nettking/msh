from __future__ import annotations

import json
import multiprocessing
import os
import stat
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from catalog.node.identity import (
    PRIVATE_KEY_FILENAME,
    PUBLIC_IDENTITY_FILENAME,
    IdentityStore,
    NodeIdentityStateError,
    verify_signature,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=timezone.utc)


def _identity_process_worker(
    state_directory: str,
    operation: str,
    start_event: Any,
    result_queue: Any,
) -> None:
    if not start_event.wait(timeout=30):
        raise RuntimeError("identity concurrency test start barrier timed out")
    store = IdentityStore(Path(state_directory), display_name="Concurrent node")
    try:
        credentials = (
            store.load_or_create(now=NOW)
            if operation == "load_or_create"
            else store.create(now=NOW)
        )
    except NodeIdentityStateError as error:
        result_queue.put(("error", error.code, error.field))
    else:
        result_queue.put(
            (
                "ok",
                credentials.identity.node_id,
                credentials.identity.public_key,
            )
        )


def _run_concurrent_identity_workers(
    state_directory: Path, *, operation: str
) -> list[tuple[str, str, str | None]]:
    context = multiprocessing.get_context("spawn")
    start_event = context.Event()
    result_queue = context.Queue()
    processes = [
        context.Process(
            target=_identity_process_worker,
            args=(
                str(state_directory),
                operation,
                start_event,
                result_queue,
            ),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    start_event.set()
    try:
        outcomes = [result_queue.get(timeout=60) for _ in processes]
    finally:
        for process in processes:
            process.join(timeout=30)
            if process.is_alive():
                process.terminate()
                process.join(timeout=10)
        result_queue.close()
        result_queue.join_thread()
    assert [process.exitcode for process in processes] == [0, 0]
    return outcomes


def test_persistent_identity_survives_restart_and_authenticates(tmp_path: Path) -> None:
    state_directory = tmp_path / "node"
    first = IdentityStore(state_directory, display_name="Recorder").load_or_create(
        now=NOW
    )
    signature = first.sign(b"relay-auth-challenge")

    restarted = IdentityStore(
        state_directory, display_name="Ignored after creation"
    ).load_or_create()

    assert restarted.identity == first.identity
    assert restarted.verify(b"relay-auth-challenge", signature)
    assert verify_signature(
        restarted.identity.public_key, b"relay-auth-challenge", signature
    )
    assert not restarted.verify(b"different-challenge", signature)


def test_concurrent_load_or_create_returns_one_persistent_identity(
    tmp_path: Path,
) -> None:
    state_directory = tmp_path / "concurrent-load-or-create"

    outcomes = _run_concurrent_identity_workers(
        state_directory, operation="load_or_create"
    )

    assert [outcome[0] for outcome in outcomes] == ["ok", "ok"]
    identities = {(outcome[1], outcome[2]) for outcome in outcomes}
    assert len(identities) == 1
    persisted = IdentityStore(
        state_directory, display_name="Concurrent node"
    ).load()
    assert (
        persisted.identity.node_id,
        persisted.identity.public_key,
    ) == next(iter(identities))


def test_concurrent_explicit_create_has_one_clear_loser(
    tmp_path: Path,
) -> None:
    state_directory = tmp_path / "concurrent-create"

    outcomes = _run_concurrent_identity_workers(
        state_directory, operation="create"
    )

    successes = [outcome for outcome in outcomes if outcome[0] == "ok"]
    failures = [outcome for outcome in outcomes if outcome[0] == "error"]
    assert len(successes) == 1
    assert failures == [("error", "identity-state-exists", "identity")]
    persisted = IdentityStore(
        state_directory, display_name="Concurrent node"
    ).load()
    assert persisted.identity.node_id == successes[0][1]
    assert persisted.identity.public_key == successes[0][2]


def test_private_key_is_separate_and_never_in_public_metadata_or_repr(
    tmp_path: Path,
) -> None:
    credentials = IdentityStore(
        tmp_path / "node", display_name="Node"
    ).load_or_create(now=NOW)
    public_path = tmp_path / "node" / PUBLIC_IDENTITY_FILENAME
    private_path = tmp_path / "node" / PRIVATE_KEY_FILENAME
    public_text = public_path.read_text(encoding="utf-8")

    assert private_path.is_file()
    assert "PRIVATE KEY" not in public_text
    assert "PRIVATE KEY" not in repr(credentials)
    assert "_private_key" not in repr(credentials)
    assert credentials.identity.public_key in public_text
    protection = json.loads(public_text)["private_key_protection"]
    if os.name == "nt":
        assert protection == "windows-dpapi-current-user"
        assert b"PRIVATE KEY" not in private_path.read_bytes()
    else:
        assert protection == "filesystem-permissions"


@pytest.mark.skipif(os.name == "nt", reason="Windows does not expose POSIX mode bits")
def test_identity_files_have_restrictive_permissions(tmp_path: Path) -> None:
    state_directory = tmp_path / "node"
    IdentityStore(state_directory, display_name="Node").load_or_create(now=NOW)

    assert stat.S_IMODE(state_directory.stat().st_mode) == 0o700
    assert stat.S_IMODE(
        (state_directory / PRIVATE_KEY_FILENAME).stat().st_mode
    ) == 0o600
    assert stat.S_IMODE(
        (state_directory / PUBLIC_IDENTITY_FILENAME).stat().st_mode
    ) == 0o600


def test_load_reports_missing_and_incomplete_identity_state(tmp_path: Path) -> None:
    store = IdentityStore(tmp_path / "node", display_name="Node")
    with pytest.raises(NodeIdentityStateError) as missing:
        store.load()
    assert missing.value.code == "identity-state-missing"

    (tmp_path / "node").mkdir()
    (tmp_path / "node" / PUBLIC_IDENTITY_FILENAME).write_text(
        "{}", encoding="utf-8"
    )
    with pytest.raises(NodeIdentityStateError) as incomplete:
        store.load_or_create()
    assert incomplete.value.code == "identity-state-incomplete"


@pytest.mark.parametrize("filename", [PRIVATE_KEY_FILENAME, PUBLIC_IDENTITY_FILENAME])
def test_malformed_identity_state_fails_clearly(
    tmp_path: Path, filename: str
) -> None:
    state_directory = tmp_path / "node"
    IdentityStore(state_directory, display_name="Node").load_or_create(now=NOW)
    (state_directory / filename).write_bytes(b"not-valid-identity-state")

    with pytest.raises(NodeIdentityStateError) as raised:
        IdentityStore(state_directory, display_name="Node").load()

    assert raised.value.code.startswith("malformed-")


def test_replaced_private_identity_is_detected(tmp_path: Path) -> None:
    original_directory = tmp_path / "original"
    replacement_directory = tmp_path / "replacement"
    IdentityStore(original_directory, display_name="Original").load_or_create(now=NOW)
    IdentityStore(replacement_directory, display_name="Replacement").load_or_create(
        now=NOW
    )
    (original_directory / PRIVATE_KEY_FILENAME).write_bytes(
        (replacement_directory / PRIVATE_KEY_FILENAME).read_bytes()
    )

    with pytest.raises(NodeIdentityStateError) as raised:
        IdentityStore(original_directory, display_name="Original").load()

    assert raised.value.code == "identity-state-replaced"


def test_replaced_node_id_is_detected(tmp_path: Path) -> None:
    state_directory = tmp_path / "node"
    credentials = IdentityStore(
        state_directory, display_name="Node"
    ).load_or_create(now=NOW)
    public_path = state_directory / PUBLIC_IDENTITY_FILENAME
    metadata = json.loads(public_path.read_text(encoding="utf-8"))
    metadata.update(replace(credentials.identity, node_id="node-replaced").to_dict())
    public_path.write_text(json.dumps(metadata), encoding="utf-8")

    with pytest.raises(NodeIdentityStateError) as raised:
        IdentityStore(state_directory, display_name="Node").load()

    assert raised.value.code == "identity-state-replaced"


def test_wrong_private_key_cannot_authenticate(tmp_path: Path) -> None:
    first = IdentityStore(
        tmp_path / "first", display_name="First"
    ).load_or_create(now=NOW)
    second = IdentityStore(
        tmp_path / "second", display_name="Second"
    ).load_or_create(now=NOW)

    signature = second.sign(b"challenge")

    assert not verify_signature(first.identity.public_key, b"challenge", signature)
    assert first.identity.node_id != second.identity.node_id
