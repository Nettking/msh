from __future__ import annotations

import pytest

from catalog.mtconnect_recorder.runtime import _managed_configuration


def test_managed_recorder_reads_role_free_capability_config() -> None:
    sources, interval = _managed_configuration(
        {
            "schema": "fcp.capability_config.v1",
            "recorder_sources": "Mazak=http://192.168.200.10:5000",
            "recorder_poll_interval": "0.5",
        }
    )

    assert sources == {"Mazak": "http://192.168.200.10:5000"}
    assert interval == 0.5


def test_legacy_technical_fallback_ignores_deployment_role() -> None:
    sources, interval = _managed_configuration(
        {
            "schema": "fcp.server_setup.v3",
            # Deliberately contradictory: this old role must not fence or grant
            # recorder readiness. Only control.json decides enabled authority.
            "deployment_mode": "web-ui-only",
            "ai_enabled": True,
            "recorder_sources": "Mazak=http://192.168.200.10:5000",
            "recorder_poll_interval": "0.25",
        }
    )

    assert sources == {"Mazak": "http://192.168.200.10:5000"}
    assert interval == 0.25


def test_managed_recorder_empty_config_is_not_ready_input() -> None:
    sources, interval = _managed_configuration({})

    assert sources == {}
    assert interval == 0.2


def test_managed_recorder_rejects_unknown_configuration_schema() -> None:
    with pytest.raises(ValueError, match="Unsupported managed recorder"):
        _managed_configuration(
            {
                "schema": "fcp.unknown.v1",
                "recorder_sources": "Mazak=http://192.168.200.10:5000",
            }
        )
