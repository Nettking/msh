from __future__ import annotations

import builtins
import errno
import os
import stat
import subprocess
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.common import data_loading
from catalog.flask_app.tests.test_federated_jsonl_product_bridge import _bridge


def _file(root: Path, relative: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"value":1}\n', encoding="utf-8")
    return path


@pytest.mark.parametrize("publish_uploads", [True, False])
def test_excluded_candidates_skip_ancestor_marker_checks(tmp_path, monkeypatch, publish_uploads):
    bridge = _bridge(tmp_path, "candidate-filter")
    bridge.app.config["FEDERATED_JSONL_PUBLISH_UPLOADS"] = publish_uploads
    root = bridge.data_root
    _file(root, "federation/shared/deep/mirrored.jsonl")
    _file(root, "sources/mtconnect_recorder/jsonl/machine/1/day/recorded.jsonl")
    upload = _file(root, "uploads/batch/upload.jsonl")
    local = _file(root, "exports/local.jsonl")
    checked = []
    original = data_loading._inside_incomplete_import

    def inspect_marker(path, directory):
        checked.append(path)
        return original(path, directory)

    monkeypatch.setattr(data_loading, "_inside_incomplete_import", inspect_marker)
    expected = [local, upload] if publish_uploads else [local]
    assert list(bridge._local_candidates()) == [
        (path.relative_to(root).as_posix(), path) for path in expected
    ]
    assert checked == expected


def test_optional_file_filter_preserves_sorted_default_discovery(tmp_path):
    root = tmp_path / "data"
    files = [_file(root, relative) for relative in ("z.jsonl", "nested/b.jsonl", "a.jsonl")]
    (root / "directory.jsonl").mkdir()
    _file(root, "ignored.txt")
    seen = []

    def select(path):
        seen.append(path)
        return path.name != "z.jsonl"

    assert list(data_loading.iter_jsonl_files(root)) == sorted(files)
    assert list(data_loading.iter_jsonl_files(root, file_filter=select)) == sorted(files)[0:2]
    assert seen == sorted(files), "The filter must run only after a match is a file"
    assert list(data_loading.iter_jsonl_files(root, recursive=False)) == sorted([files[0], files[2]])


@pytest.mark.parametrize("recursive", [True, False])
def test_entry_metadata_traversal_keeps_native_glob_selection(tmp_path, recursive):
    root = tmp_path / "entry-default-equivalence"
    for relative in ("z.jsonl", "A.JSONL", "nested/b.jsonl", "nested/deep/a.jsonl", "ignored.txt"):
        _file(root, relative)
    (root / "directory.jsonl").mkdir()
    hidden = _file(root, "hidden/import.jsonl")
    (hidden.parent / ".fcp-importing").touch()
    assert list(data_loading.iter_jsonl_files(root, recursive=recursive, entry_filter=lambda entry: True)) == list(
        data_loading.iter_jsonl_files(root, recursive=recursive)
    )


def test_candidate_marker_errors_still_fail_closed(tmp_path, monkeypatch):
    bridge = _bridge(tmp_path, "marker-error")
    path = _file(bridge.data_root, "uploads/batch/data.jsonl")
    marker = path.parent / ".fcp-importing"
    original = Path.lstat

    def deny_marker(self):
        if self == marker:
            raise OSError(errno.EACCES, "marker state unavailable")
        return original(self)

    monkeypatch.setattr(Path, "lstat", deny_marker)
    assert list(bridge._local_candidates()) == []


def test_candidates_keep_prefix_boundaries_and_hidden_imports(tmp_path):
    bridge = _bridge(tmp_path, "selection-order")
    root = bridge.data_root
    visible = [_file(root, name) for name in (
        "z.jsonl", "federation-peer/a.jsonl", "sources/mtconnect_recorder/jsonl-other/a.jsonl",
        "uploads-ready/a.jsonl", "a.jsonl",
    )]
    hidden = _file(root, "uploads/batch/hidden.jsonl")
    marker = hidden.parent / ".fcp-importing"
    marker.mkdir()
    assert list(bridge._local_candidates()) == [
        (p.relative_to(root).as_posix(), p) for p in sorted(visible)
    ]
    marker.rmdir()
    assert list(bridge._local_candidates()) == [
        (p.relative_to(root).as_posix(), p) for p in sorted([*visible, hidden])
    ]


def test_resolved_file_alias_selection_and_lexical_markers_are_preserved(tmp_path):
    bridge = _bridge(tmp_path, "file-alias")
    root = bridge.data_root
    allowed = _file(root, "exports/target.jsonl")
    excluded = _file(root, "federation/mirror.jsonl")
    outside = _file(tmp_path, "outside.jsonl")
    alias = root / "federation/allowed-alias.jsonl"
    try:
        alias.symlink_to(allowed)
        (root / "exports/excluded-alias.jsonl").symlink_to(excluded)
        (root / "exports/outside-alias.jsonl").symlink_to(outside)
        (root / "exports/broken.jsonl").symlink_to(root / "missing.jsonl")
    except OSError as exc:
        pytest.skip(f"file symlink creation unavailable: {exc}")
    expected = [("exports/target.jsonl", path) for path in sorted([allowed, alias])]
    assert list(bridge._local_candidates()) == expected
    (alias.parent / ".fcp-importing").write_text("publishing", encoding="utf-8")
    assert list(bridge._local_candidates()) == [("exports/target.jsonl", allowed)]


@pytest.mark.skipif(os.name != "nt", reason="Windows directory junction semantics")
def test_windows_junction_alias_keeps_current_glob_selection(tmp_path):
    bridge = _bridge(tmp_path, "directory-alias")
    root = bridge.data_root
    allowed = _file(root, "exports/target.jsonl")
    link = root / "federation/alias"
    link.parent.mkdir(exist_ok=True)
    assert link.resolve().is_relative_to(tmp_path.resolve())
    result = subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J", str(link), str(allowed.parent)],
        capture_output=True, timeout=10,
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    assert link.is_junction()
    # Use unchanged default discovery as the native pathlib traversal oracle.
    # The bridge must keep aliases even when their lexical parent is excluded.
    expected = [
        (path.resolve().relative_to(root).as_posix(), path)
        for path in data_loading.iter_jsonl_files(root)
    ]
    assert list(bridge._local_candidates()) == expected


def test_excluded_regular_siblings_resolve_parent_once(tmp_path, monkeypatch):
    bridge = _bridge(tmp_path, "parent-cost")
    files = [_file(bridge.data_root, f"federation/deep/{i}.jsonl") for i in range(8)]
    resolved = []
    original = Path.resolve

    def resolve(self, *args, **kwargs):
        resolved.append(self)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve)
    assert list(bridge._local_candidates()) == []
    assert resolved == [files[0].parent]


def test_large_excluded_corpus_skips_path_metadata_before_sorting(tmp_path, monkeypatch):
    bridge = _bridge(tmp_path, "excluded-metadata-cost")
    root = bridge.data_root
    excluded = {
        _file(root, f"sources/mtconnect_recorder/jsonl/machine/1/day/{i}.jsonl")
        for i in range(32)
    }
    visible = [_file(root, name) for name in ("z.jsonl", "exports/a.jsonl")]
    calls = []
    sorted_candidates = []
    original_stat = Path.stat

    def stat_path(self, *args, **kwargs):
        if self in excluded:
            calls.append(self)
        return original_stat(self, *args, **kwargs)

    def sort_candidates(paths):
        paths = list(paths)
        sorted_candidates.extend(paths)
        return builtins.sorted(paths)

    monkeypatch.setattr(Path, "stat", stat_path)
    monkeypatch.setattr(data_loading, "sorted", sort_candidates, raising=False)
    assert list(bridge._local_candidates()) == [
        (path.relative_to(root).as_posix(), path) for path in sorted(visible)
    ]
    assert calls == [], "Excluded ordinary leaves must not incur Path stat/lstat RPCs"
    assert set(sorted_candidates) == set(visible), "Excluded leaves must not accumulate in the global sort"


@pytest.mark.parametrize("entry_kind", ["symlink", "windows-reparse"])
def test_reparse_leaf_falls_back_to_full_resolution(tmp_path, monkeypatch, entry_kind):
    if entry_kind == "windows-reparse" and os.name != "nt":
        pytest.skip("Windows reparse attributes")
    bridge = _bridge(tmp_path, "reparse-leaf")
    leaf = _file(bridge.data_root, "federation/reparse.jsonl")
    target = bridge.data_root / "exports/target.jsonl"
    original_stat, original_resolve, original_scandir = Path.lstat, Path.resolve, os.scandir
    resolved = []

    def lstat(self):
        metadata = original_stat(self)
        if self == leaf:
            return SimpleNamespace(st_mode=metadata.st_mode,
                                   st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT)
        return metadata

    def resolve(self, *args, **kwargs):
        resolved.append(self)
        return target if self == leaf else original_resolve(self, *args, **kwargs)

    class ReparseEntry:
        def __init__(self, entry):
            self.entry = entry

        def __getattr__(self, name):
            return getattr(self.entry, name)

        def is_symlink(self):
            # Model the same nonordinary leaf at both metadata boundaries.
            return entry_kind == "symlink"

        def stat(self, **kwargs):
            metadata = self.entry.stat(**kwargs)
            return SimpleNamespace(st_mode=metadata.st_mode,
                                   st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT)

    @contextmanager
    def scandir(path):
        with original_scandir(path) as entries:
            yield (ReparseEntry(entry) if Path(entry.path) == leaf else entry for entry in entries)

    monkeypatch.setattr(Path, "lstat", lstat)
    monkeypatch.setattr(Path, "resolve", resolve)
    monkeypatch.setattr(os, "scandir", scandir)
    assert list(bridge._local_candidates()) == [("exports/target.jsonl", leaf)]
    assert resolved.count(leaf) == 2, "Reparse leaf and final guard both resolve the full path"


def test_parent_resolution_is_refreshed_for_each_pass(tmp_path, monkeypatch):
    bridge = _bridge(tmp_path, "parent-refresh")
    leaf = _file(bridge.data_root, "federation/source/record.jsonl")
    resolved_parent = leaf.parent
    original = Path.resolve

    def resolve(self, *args, **kwargs):
        if self == leaf.parent:
            return resolved_parent
        if self == leaf:
            return resolved_parent / self.name
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve)
    assert list(bridge._local_candidates()) == []
    resolved_parent = bridge.data_root / "exports"
    assert list(bridge._local_candidates()) == [("exports/record.jsonl", leaf)]


@pytest.mark.skipif(os.name != "nt", reason="Windows directory junction rebinding")
def test_final_resolution_rejects_junction_rebound_after_prefilter(tmp_path, monkeypatch):
    bridge = _bridge(tmp_path, "rebound-final-check")
    root = bridge.data_root
    allowed = _file(root, "exports/record.jsonl")
    excluded = _file(root, "federation/target/record.jsonl")
    link = root / "alias"

    def junction(target):
        assert link.parent.resolve() == root and target.resolve().is_relative_to(root)
        result = subprocess.run(
            ["cmd.exe", "/d", "/c", "mklink", "/J", str(link), str(target)],
            capture_output=True, timeout=10,
        )
        assert result.returncode == 0, result.stderr.decode(errors="replace")
        assert link.is_junction()

    junction(allowed.parent)
    alias = link / "record.jsonl"
    original = data_loading._inside_incomplete_import
    rebound = False

    def inspect_marker(path, directory):
        nonlocal rebound
        if path == alias:
            assert link.is_junction()
            link.rmdir()  # Remove only this owned junction, never its target.
            junction(excluded.parent)
            rebound = True
        return original(path, directory)

    monkeypatch.setattr(data_loading, "_inside_incomplete_import", inspect_marker)
    selected = list(bridge._local_candidates())
    assert rebound
    assert selected == [("exports/record.jsonl", allowed)]
    assert allowed.is_file() and excluded.is_file()
