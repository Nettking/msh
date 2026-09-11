"""Atomic artifact publication must coexist with readers on every platform."""

from __future__ import annotations

import os
from contextlib import closing

import pytest

from catalog.capabilities.analysis.content_store import LocalArtifactContentStore
from catalog.federation import stable_filesystem
from catalog.federation.errors import FederationValidationError


@pytest.mark.parametrize("same_content", [True, False])
@pytest.mark.parametrize("key", ["analysis/session/work/plan.json", "x"])
def test_publication_preserves_open_reader_snapshot(tmp_path, same_content, key):
    store = LocalArtifactContentStore(tmp_path / "artifacts", chunk_size=1024)
    original = b"original" * 4096
    replacement = original if same_content else b"replacement" * 8192
    old_identity = store.write_bytes(key, original)

    with closing(store.stream(key, **vars(old_identity))) as reader:
        first = next(reader)
        # A suspended public reader owns a real open file; no sleep or injected
        # sharing error is needed to reproduce the Windows replacement failure.
        new_identity = store.write_bytes(key, replacement)
        assert first + b"".join(reader) == original
        assert store.read_bytes(key, **vars(new_identity)) == replacement

    assert store.identity(key) == new_identity
    assert (
        store.read_range(key, offset=1, length=17, **vars(new_identity))
        == replacement[1:18]
    )


def test_replacement_does_not_relax_registered_identity(tmp_path):
    store = LocalArtifactContentStore(tmp_path / "artifacts", chunk_size=4)
    key = "analysis/session/work/plan.json"
    identity = store.write_bytes(key, b"original")
    store.write_bytes(key, b"modified")

    with pytest.raises(FederationValidationError) as error:
        store.read_bytes(key, **vars(identity))
    assert error.value.code == "analysis-artifact-integrity-mismatch"

    store.write_bytes(key, b"changed size")
    for read in (
        lambda: store.read_bytes(key, **vars(identity)),
        lambda: store.read_range(key, offset=0, length=4, **vars(identity)),
    ):
        with pytest.raises(FederationValidationError) as error:
            read()
        assert error.value.code == "analysis-artifact-integrity-mismatch"


@pytest.mark.skipif(os.name != "nt", reason="Windows native publication failure")
def test_unsupported_native_publication_preserves_existing_artifact(
    tmp_path, monkeypatch
):
    store = LocalArtifactContentStore(tmp_path / "artifacts")
    key = "analysis/session/work/plan.json"
    original = b"original"
    identity = store.write_bytes(key, original)
    calls = []

    def unsupported(_source, _iosb, buffer, _length, information_class):
        info = stable_filesystem._FileRenameInformationEx.from_buffer(buffer)
        calls.append((information_class, info.Flags))
        return -1073741637  # STATUS_NOT_SUPPORTED: do not retry or unlink first.

    monkeypatch.setattr(stable_filesystem, "_NtSetInformationFile", unsupported)
    with pytest.raises(stable_filesystem.StableFilesystemError):
        store.write_bytes(key, b"replacement")

    assert calls == [(65, 3)]
    assert store.read_bytes(key, **vars(identity)) == original
    assert not list((store.root / ".fcp-analysis-content-tmp").glob("*.partial"))
