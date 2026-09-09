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
    # No repository fixtures or third-party plugins enter these subprocesses.
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    for name, count in [("first", 4), ("second", 2), ("new_directory/test_third", 3)]:
        path = tmp_path / (name + ".py" if "/" in name else "test_" + name + ".py")
        path.parent.mkdir(exist_ok=True)
        path.write_text(
            "import pytest\n"
            "@pytest.fixture(scope='module')\n"
            "def resource():\n    return object()\n"
            f"@pytest.mark.parametrize('value', range({count}))\n"
            "def test_value(value, resource):\n    assert resource is not None\n"
        )
    environment = {
        **os.environ,
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "GITHUB_SHA": "candidate",
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
            cwd=tmp_path,
            env=environment,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
    assert verify(reports, 2, "candidate") == 9
    manifest_path = reports / "shard-1.json"
    original = json.loads(manifest_path.read_text())
    for field, replacement in [
        ("source_sha", "different-candidate"),
        ("collected", []),
        ("selected", []),
        ("exit_code", 1),
        ("collection_complete", False),
        ("shard_index", 0),
    ]:
        manifest_path.write_text(json.dumps({**original, field: replacement}))
        with pytest.raises(ValueError):
            verify(reports, 2, "candidate")
    manifest_path.unlink()
    with pytest.raises(ValueError):
        verify(reports, 2, "candidate")
    (tmp_path / "test_first.py").write_text("def test_failure():\n    assert False\n")
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
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert failed.returncode == 1
    assert json.loads(manifest_path.read_text())["exit_code"] == 1
