"""Keep consolidated native CI coverage on the changes that require it.

These checks cover this workflow's explicit paths and folded command blocks.
Real GitHub path-trigger execution is separate retirement evidence.
"""

from __future__ import annotations

import fnmatch
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/phase-f7-closeout.yml"
REQUIRED_PATHS = (
    "catalog/capabilities/analysis/content_store.py",
    "catalog/ai/runtime.py",
    "catalog/relay/tests/test_phase_f74_capability_dispatch.py",
    "catalog/relay/tests/test_phase_f75_retry_cancellation.py",
    "catalog/federation/object_transfer.py",
    "catalog/federation/resumable_chunk_transfer.py",
    "catalog/federation/tests/test_phase_f641_chunk_contract.py",
    "catalog/node/tests/test_phase_f642_verified_chunk_transfer.py",
    "catalog/node/tests/test_phase_f65_resumable_chunk_transfer.py",
    "catalog/flask_app/static/js/ai-explainer.js",
    "catalog/flask_app/services/server_setup_service.py",
    "catalog/flask_app/tests/test_ai_explainer_candidate_visibility.py",
    "catalog/common/tests/test_f7_ci_coverage.py",
)
TRANSFER_TESTS = REQUIRED_PATHS[6:9]
RELAY_TESTS = REQUIRED_PATHS[2:4]
WINDOWS_AI_TESTS = (
    "catalog/ai/tests/test_ai_explainer.py",
    "catalog/ai/tests/test_ai_explainer_cache.py",
    "catalog/ai/tests/test_grounding.py",
    "catalog/ai/tests/test_ollama_client.py",
    "catalog/ai/tests/test_runtime.py",
    "catalog/ai/tests/test_runtime_manager.py",
    "catalog/ai/tests/test_runtime_manager_reconciliation.py",
    "catalog/flask_app/tests/test_ai_explainer_chat.py",
    "catalog/flask_app/tests/test_connected_ai_provider.py",
    "catalog/flask_app/tests/test_model_provider_compose.py",
    *RELAY_TESTS,
)


def _workflow():
    return WORKFLOW.read_text(encoding="utf-8")


def _event(text, event):
    trigger = text.split("\non:\n", 1)[1].split("\npermissions:\n", 1)[0]
    matches = re.findall(
        rf"^  {event}:\n(.*?)(?=^  \S|\Z)", trigger, re.MULTILINE | re.DOTALL
    )
    assert len(matches) == 1
    return matches[0]


def _paths(text, event):
    block = _event(text, event)
    assert "paths-ignore:" not in block and "types:" not in block
    paths = re.findall(r'^      - "([^"\n]+)"$', block, re.MULTILINE)
    assert paths and len(paths) == len(set(paths))
    # Deliberately limit the matcher to the exact and directory-prefix patterns
    # used here; do not silently emulate unsupported GitHub pattern semantics.
    assert all(not any(c in p for c in "!?[]{}+") for p in paths)
    assert all("*" not in p or p.endswith("/**") for p in paths)
    return paths


def _commands(text, marker):
    found = []
    for body in re.findall(
        r"^      - name: [^\n]+\n(.*?)(?=^      - |\Z)", text, re.MULTILINE | re.DOTALL
    ):
        run = re.search(r"^        run: ([^\n]+)\n?", body, re.MULTILINE)
        if not run:
            continue
        value = run.group(1)
        if value in (">-", "|", "|-", ">"):
            value = " ".join(line.strip() for line in body[run.end() :].splitlines())
        if marker in value:
            found.append((body, value.split()))
    return found


@pytest.mark.parametrize("event", ["pull_request", "push"])
@pytest.mark.parametrize("changed_path", REQUIRED_PATHS)
def test_relevant_single_file_changes_trigger_native_matrix(event, changed_path):
    assert (ROOT / changed_path).is_file()
    assert any(fnmatch.fnmatchcase(changed_path, p) for p in _paths(_workflow(), event))


def test_trigger_and_runner_boundaries_remain_native():
    text = _workflow()
    assert _paths(text, "pull_request") == _paths(text, "push")
    assert "branches: [main]" in _event(text, "push")
    assert "  workflow_dispatch:" in text
    jobs = text.split("\njobs:\n", 1)[1]
    assert re.findall(r"^  ([\w-]+):$", jobs, re.MULTILINE) == ["capability-closeout"]
    assert "os: Linux\n            runner: fcp-linux-fast" in jobs
    assert "os: Windows\n            runner: fcp-windows" in jobs
    assert 'runs-on: [self-hosted, "${{ matrix.runner }}"]' in jobs
    assert "ubuntu-latest" not in text and "windows-latest" not in text
    assert "timeout-minutes: 30" in jobs and "fail-fast: false" in jobs
    assert "uses: ./.github/actions/self-hosted-python" in jobs
    assert 'python-version: "3.12"' in jobs
    assert "uses: ./.github/actions/ubuntu-storage-precondition" in jobs
    assert "docker compose config --quiet" in jobs
    assert "git diff --check" in jobs


def test_unique_windows_modules_and_transfer_tests_are_unconditional():
    commands = _commands(_workflow(), "python -m pytest")
    assert len(commands) == 1
    body, args = commands[0]
    assert "        if:" not in body
    assert args[:3] == ["python", "-m", "pytest"]
    assert not {"-k", "-m", "--ignore", "--deselect", "--collect-only"} & {
        arg.split("=", 1)[0] for arg in args[3:]
    }
    scopes = [arg for arg in args if arg.startswith("catalog/")]
    for module in (*WINDOWS_AI_TESTS, *TRANSFER_TESTS, REQUIRED_PATHS[-1]):
        assert (ROOT / module).is_file()
        assert any(
            module == p or module.startswith(p.rstrip("/") + "/") for p in scopes
        )
    assert "TEMP:" in body and "TMP:" in body and "fcp-qtmp" in body


def test_unique_lint_rules_and_relay_scopes_are_unconditional():
    commands = _commands(_workflow(), "python -m ruff check")
    guards = [(body, args) for body, args in commands if "--select" in args]
    assert len(guards) == 1
    body, args = guards[0]
    assert "        if:" not in body
    assert args == [
        "python",
        "-m",
        "ruff",
        "check",
        "catalog/capabilities",
        "--select",
        "UP035",
    ]
    relay = [
        (body, args) for body, args in commands if all(p in args for p in RELAY_TESTS)
    ]
    assert len(relay) == 1
    body, args = relay[0]
    assert "        if:" not in body
    assert args == [
        "python",
        "-m",
        "ruff",
        "check",
        *RELAY_TESTS,
        "--ignore",
        "I001,RUF022,B008,C408,PLC0206",
    ]
