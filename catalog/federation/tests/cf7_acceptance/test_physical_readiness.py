from __future__ import annotations

import json
from pathlib import Path

import pytest

from ops import cf7_physical_readiness as readiness

COMMIT = "a" * 40


def test_sanitize_text_removes_endpoints_addresses_paths_and_credentials(
    tmp_path: Path,
) -> None:
    unsafe = (
        f"checkout={tmp_path}\n"
        "endpoint=http://192.168.10.172:11434/api/tags\n"
        "Authorization: Bearer private-token\n"
        "path=C:\\Users\\operator\\secret.txt"
    )

    safe = readiness.sanitize_text(unsafe, cwd=tmp_path)

    assert str(tmp_path) not in safe
    assert "192.168.10.172" not in safe
    assert "http://" not in safe
    assert "private-token" not in safe
    assert "C:\\Users" not in safe
    assert "<redacted-endpoint>" in safe
    assert "<redacted-credential>" in safe


def test_private_mapping_is_environment_only_and_alias_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "MSH_CF7_MTCONNECT_ENDPOINTS",
        json.dumps({"cnc-one": "http://private-one", "cnc-two": "http://private-two"}),
    )

    result = readiness.parse_private_mapping("MSH_CF7_MTCONNECT_ENDPOINTS")

    assert set(result) == {"cnc-one", "cnc-two"}
    assert "private-one" in result["cnc-one"]

    monkeypatch.setenv("MSH_CF7_MTCONNECT_ENDPOINTS", '{"bad alias": "value"}')
    with pytest.raises(readiness.ReadinessError):
        readiness.parse_private_mapping("MSH_CF7_MTCONNECT_ENDPOINTS")


def test_initialize_evidence_uses_template_and_never_embeds_private_endpoints(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout = tmp_path / "checkout"
    template = (
        checkout
        / "catalog"
        / "federation"
        / "tests"
        / "cf7_acceptance"
        / "physical_evidence.template.json"
    )
    template.parent.mkdir(parents=True)
    template.write_text(
        json.dumps(
            {
                "schema": "msh.cf7.physical-evidence.v1",
                "commit_sha": "0" * 40,
                "executed_at": "1970-01-01T00:00:00Z",
                "operator": "pending",
                "environments": {},
                "scenarios": {},
                "privacy_review": {},
                "notes": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        readiness,
        "verify_checkout",
        lambda *_args, **_kwargs: {"commit_sha": COMMIT, "repository_clean": True},
    )
    monkeypatch.setenv("MSH_CF7_OLLAMA_URL", "http://192.168.10.172:11434")

    evidence_root = checkout / "evidence"
    evidence_path = readiness.initialize_evidence(
        checkout,
        evidence_root=evidence_root,
        machine="local-ai",
        operator="Martin",
        commit=COMMIT,
    )

    document = json.loads(evidence_path.read_text(encoding="utf-8"))
    profile = json.loads(
        (evidence_root / "local-ai" / "profile.json").read_text(encoding="utf-8")
    )
    serialized = json.dumps({"document": document, "profile": profile})
    assert document["commit_sha"] == COMMIT
    assert document["operator"] == "Martin"
    assert profile["contains_private_endpoints"] is False
    assert "192.168.10.172" not in serialized
    assert "11434" not in serialized


def test_ollama_probe_reports_inventory_without_persisting_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MSH_CF7_OLLAMA_URL", "http://192.168.10.172:11434")
    monkeypatch.setenv("MSH_CF7_OLLAMA_MODEL", "qwen2.5:7b")
    requested: list[str] = []

    def fake_http_json(url: str, **_kwargs):
        requested.append(url)
        return {"models": [{"name": "qwen2.5:7b"}, {"name": "smollm2:360m"}]}

    monkeypatch.setattr(readiness, "_http_json", fake_http_json)

    result = readiness.probe_ollama()

    assert result["reachable"] is True
    assert result["target_model_present"] is True
    assert result["inventory_count"] == 2
    assert requested and "192.168.10.172" in requested[0]
    assert "192.168.10.172" not in json.dumps(result)
    assert "11434" not in json.dumps(result)


def test_topology_requires_three_profiles_and_both_os_families(tmp_path: Path) -> None:
    topology = tmp_path / "topology.json"
    topology.write_text(
        json.dumps(
            {
                "schema": readiness.TOPOLOGY_SCHEMA,
                "machines": {
                    "local-ai": {"os_family": "windows"},
                    "cnc-recorder": {"os_family": "linux"},
                    "school-control": {"os_family": "windows"},
                },
            }
        ),
        encoding="utf-8",
    )

    summary = readiness.validate_topology(topology)
    assert summary["ready"] is True
    assert summary["machine_count"] == 3
    assert summary["os_families"] == ["linux", "windows"]

    topology.write_text(
        json.dumps(
            {
                "schema": readiness.TOPOLOGY_SCHEMA,
                "machines": {
                    name: {"os_family": "windows"}
                    for name in readiness.MACHINE_PROFILES
                },
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(readiness.ReadinessError):
        readiness.validate_topology(topology)


def test_gate_plan_covers_acceptance_routes_compose_and_hygiene() -> None:
    commands = {name: command for name, command, _timeout in readiness.gate_commands()}

    assert set(commands) == {
        "cf7-acceptance",
        "capability-routes",
        "compose-config",
        "diff-hygiene",
    }
    assert "catalog/federation/tests/cf7_acceptance" in commands["cf7-acceptance"]
    assert "catalog/flask_app/tests/test_capability_contribution_route.py" in commands[
        "capability-routes"
    ]
    assert commands["compose-config"] == ("docker", "compose", "config")


def test_gate_log_is_redacted_and_summary_is_not_acceptance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout = tmp_path / "private-checkout"
    checkout.mkdir()
    evidence_root = checkout / "evidence"
    monkeypatch.setattr(
        readiness,
        "verify_checkout",
        lambda *_args, **_kwargs: {"commit_sha": COMMIT, "repository_clean": True},
    )
    monkeypatch.setattr(readiness, "os_family", lambda: "windows")
    monkeypatch.setattr(
        readiness,
        "gate_commands",
        lambda: (("sample", (str(checkout / "python.exe"), "sample.py"), 10.0),),
    )
    monkeypatch.setattr(
        readiness,
        "_run",
        lambda command, **kwargs: readiness.CommandResult(
            name=kwargs["name"],
            command=tuple(command),
            returncode=0,
            duration_seconds=0.1,
            output_tail=readiness.sanitize_text(
                "connected to http://192.168.1.50:5000 with token=secret",
                cwd=checkout,
            ),
        ),
    )

    summary = readiness.run_gates(
        checkout,
        evidence_root=evidence_root,
        machine="school-control",
        commit=COMMIT,
    )

    log = (evidence_root / "windows" / "commands.txt").read_text(encoding="utf-8")
    assert summary["passed"] is True
    assert summary["accepted"] is False
    assert str(checkout) not in log
    assert "192.168.1.50" not in log
    assert "secret" not in log


def test_preflight_delegates_only_to_selected_physical_role(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        readiness,
        "verify_checkout",
        lambda *_args, **_kwargs: {"commit_sha": COMMIT, "repository_clean": True},
    )
    monkeypatch.setattr(readiness, "os_family", lambda: "windows")
    monkeypatch.setattr(readiness.shutil, "which", lambda _name: "/safe/command")
    monkeypatch.setenv("MSH_CF7_OLLAMA_URL", "http://private-provider")
    monkeypatch.setenv("MSH_CF7_OLLAMA_MODEL", "qwen2.5:7b")
    monkeypatch.setattr(
        readiness,
        "probe_ollama",
        lambda: {"service": "ollama", "reachable": True, "private_endpoint_persisted": False},
    )
    monkeypatch.setattr(
        readiness,
        "probe_mtconnect",
        lambda: pytest.fail("MTConnect must not be probed by the AI profile"),
    )
    monkeypatch.setattr(
        readiness,
        "probe_peers",
        lambda: pytest.fail("Peers must not be probed by the AI profile"),
    )

    result = readiness.preflight(
        tmp_path,
        evidence_root=tmp_path / "evidence",
        machine="local-ai",
        commit=COMMIT,
    )

    assert result["machine"] == "local-ai"
    assert result["accepted"] is False
    assert result["physical_probe"]["service"] == "ollama"
