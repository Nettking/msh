"""Exercise shard coverage, fixture ownership and fail-closed aggregation."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.ci_pytest_shards import partition, verify

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "ci_pytest_shards.py"


def _git(source: Path, *args: str) -> str:
    result = subprocess.run(
        [
            "git",
            "-C",
            str(source),
            "-c",
            "core.autocrlf=false",
            "-c",
            "commit.gpgsign=false",
            "-c",
            f"core.hooksPath={source / '.git/no-hooks'}",
            "-c",
            "user.name=Shard fixture",
            "-c",
            "user.email=shard@example.invalid",
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=20,
        check=True,
    )
    return result.stdout.strip()


def _commit_fixture(source: Path) -> str:
    if not (source / ".git").exists():
        _git(source, "init")
        (source / ".gitignore").write_text("__pycache__/\n.pytest_cache/\n")
    _git(source, "add", "--all")
    _git(source, "commit", "-m", "Synthetic shard fixture")
    return _git(source, "rev-parse", "HEAD")


def test_partition_is_order_independent_and_keeps_files_together():
    nodeids = [
        f"test_{file}.py::test_value[{value}]"
        for file in range(7)
        for value in range(file + 1)
    ]
    shards = partition(nodeids, 3)
    assert shards == partition(list(reversed(nodeids)), 3)
    assert sorted(item for shard in shards for item in shard) == sorted(nodeids)
    assert max(map(len, shards)) - min(map(len, shards)) <= 1
    for file in range(7):
        assert (
            sum(
                any(item.startswith(f"test_{file}.py::") for item in shard)
                for shard in shards
            )
            == 1
        )


@pytest.mark.parametrize(
    "nodeids,count", [([], 1), (["a::x"], 0), (["a::x"], 2), (["a::x", "a::x"], 1)]
)
def test_invalid_collection_is_rejected(nodeids, count):
    with pytest.raises(ValueError):
        partition(nodeids, count)


def test_real_pytest_shards_cover_new_files_and_preserve_failure(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    # No repository fixtures or third-party plugins enter these subprocesses.
    (source / "pytest.ini").write_text("[pytest]\n")
    for name, count in [("first", 4), ("second", 2), ("new_directory/test_third", 3)]:
        path = source / (name + ".py" if "/" in name else "test_" + name + ".py")
        path.parent.mkdir(exist_ok=True)
        path.write_text(
            "import pytest\n"
            "@pytest.fixture(scope='module')\n"
            "def resource():\n    return object()\n"
            f"@pytest.mark.parametrize('value', range({count}))\n"
            "def test_value(value, resource):\n    assert resource is not None\n"
        )
    source_sha = _commit_fixture(source)
    environment = {
        **os.environ,
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "GITHUB_SHA": source_sha,
    }
    reports = tmp_path / "reports"
    for index in range(2):
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "run",
                "--index",
                str(index),
                "--count",
                "2",
                "--manifest",
                str(reports / f"shard-{index}.json"),
                "--",
                "-q",
            ],
            cwd=source,
            env=environment,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
    assert verify(reports, 2, source_sha) == 9
    manifest_path = reports / "shard-1.json"
    original = json.loads(manifest_path.read_text())
    for field, replacement in [
        ("source_sha", "0" * 40),
        ("source_identity_before", {"sha": source_sha, "tree": "0" * 40}),
        ("source_identity_after", {"sha": source_sha, "tree": "0" * 40}),
        ("collected", []),
        ("selected", []),
        ("executed", []),
        ("exit_code", 1),
        ("collection_complete", False),
        ("shard_index", 0),
    ]:
        manifest_path.write_text(json.dumps({**original, field: replacement}))
        with pytest.raises(ValueError):
            verify(reports, 2, source_sha)
    manifest_path.unlink()
    with pytest.raises(ValueError):
        verify(reports, 2, source_sha)
    (source / "test_first.py").write_text("def test_failure():\n    assert False\n")
    environment["GITHUB_SHA"] = _commit_fixture(source)
    failed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "run",
            "--index",
            "0",
            "--count",
            "1",
            "--manifest",
            str(manifest_path),
            "--",
            "-q",
        ],
        cwd=source,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert failed.returncode == 1
    assert json.loads(manifest_path.read_text())["exit_code"] == 1
    collect_only = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "run",
            "--index",
            "0",
            "--count",
            "1",
            "--manifest",
            str(manifest_path),
            "--",
            "--collect-only",
        ],
        cwd=source,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert collect_only.returncode != 0
    assert json.loads(manifest_path.read_text())["collection_complete"] is False
    setup_only = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "run",
            "--index",
            "0",
            "--count",
            "1",
            "--manifest",
            str(manifest_path),
            "--",
            "--setup-only",
        ],
        cwd=source,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert setup_only.returncode != 0
    assert json.loads(manifest_path.read_text())["executed"] == []


@pytest.mark.parametrize(
    "fault", ["wrong-sha", "dirty-before", "dirty-during", "commit-during"]
)
def test_real_checkout_identity_refuses_mislabelling_and_mutation(tmp_path, fault):
    source = tmp_path / "source"
    source.mkdir()
    (source / "pytest.ini").write_text("[pytest]\n")
    marker = tmp_path / "executed"
    mutation = ""
    if fault == "dirty-during":
        mutation = "    Path('tracked.txt').write_text('changed')\n"
    elif fault == "commit-during":
        mutation = (
            "    Path('tracked.txt').write_text('changed')\n"
            "    import subprocess\n"
            "    subprocess.run(['git', '-c', 'core.autocrlf=false', 'add', '--all'], check=True)\n"
            "    subprocess.run(['git', '-c', 'commit.gpgsign=false', "
            "'-c', 'core.hooksPath=.git/no-hooks', '-c', 'user.name=Shard fixture', "
            "'-c', 'user.email=shard@example.invalid', 'commit', '-m', 'Synthetic change'], check=True)\n"
        )
    (source / "tracked.txt").write_text("original")
    (source / "test_owned.py").write_text(
        "from pathlib import Path\n"
        "def test_runs():\n"
        f"    Path({str(marker)!r}).write_text('executed')\n" + mutation
    )
    source_sha = _commit_fixture(source)
    if fault == "dirty-before":
        (source / "tracked.txt").write_text("changed")
    output = tmp_path / "reports" / "shard-0.json"
    environment = {
        **os.environ,
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "GITHUB_SHA": "0" * 40 if fault == "wrong-sha" else source_sha,
    }
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "run",
            "--index",
            "0",
            "--count",
            "1",
            "--manifest",
            str(output),
            "--",
            "-q",
        ],
        cwd=source,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode != 0, result.stdout + result.stderr
    if fault in {"wrong-sha", "dirty-before"}:
        assert not marker.exists()
        assert not output.exists()
    else:
        assert marker.read_text() == "executed"
        manifest = json.loads(output.read_text())
        assert manifest["executed"] == ["test_owned.py::test_runs"]
        assert manifest["exit_code"] == 2
        assert manifest["source_error"]
        if fault == "commit-during":
            assert manifest["source_identity_after"]["sha"] != source_sha
        with pytest.raises(ValueError):
            verify(output.parent, 1, source_sha)
