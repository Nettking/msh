from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from flask import Flask

from catalog.federation.tailscale_host_discovery import (
    ADVERTISEMENT_SCHEMA,
    DISCOVERY_SCHEMA,
    write_snapshot,
)
from catalog.flask_app import capability_startup_transition_routes as transition_routes
from catalog.flask_app import federation_pairing_routes as pairing_routes
from catalog.flask_app.auth.policy import PUBLIC_ENDPOINTS

APP_ROOT = Path(__file__).resolve().parents[1]
_DISCOVERY_ADVERTISEMENT_ENDPOINT = "federation_pairing_web.federation_discovery"


def _snapshot() -> dict[str, object]:
    return {
        "schema": DISCOVERY_SCHEMA,
        "tailscale_available": True,
        "federations": [
            {
                "schema": ADVERTISEMENT_SCHEMA,
                "federation_label": "Workshop Federation",
                "federation_fingerprint": "e" * 32,
                "device_name": "Coordinator PC",
                "relay_port": 8765,
                "pairing_required": True,
                "auto_join_port": 5151,
                "tailscale_ip": "100.90.80.70",
                "tailscale_dns_name": "coordinator.example.ts.net",
                "tailscale_host_name": "coordinator",
                "web_port": 5000,
            }
        ],
    }


def test_transition_view_model_reads_host_snapshot_without_join_authority(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "tailscale_discovery.json"
    payload = _snapshot()
    payload["federations"][0]["pairing_code"] = "FCP1-never-expose-this"  # type: ignore[index]
    write_snapshot(path, payload)
    monkeypatch.setenv("FCP_TAILSCALE_DISCOVERY_FILE", str(path))

    view_model = transition_routes._attach_tailscale_discovery({})

    discovery = view_model["tailscale_discovery"]
    assert discovery["tailscale_available"] is True
    assert discovery["federations"][0]["federation_label"] == "Workshop Federation"
    assert "FCP1-never-expose-this" not in json.dumps(discovery)


def test_discovery_advertisement_is_the_only_public_pairing_exception() -> None:
    # The advertisement remains unauthenticated so trusted peers can discover a
    # coordinator. On a truly fresh local installation the first-user gate still
    # requires human-admin bootstrap before local onboarding can mutate state.
    assert _DISCOVERY_ADVERTISEMENT_ENDPOINT in PUBLIC_ENDPOINTS
    assert "federation_pairing_web.create_pairing_code" not in PUBLIC_ENDPOINTS
    assert "federation_pairing_web.pair_device" not in PUBLIC_ENDPOINTS


def test_discovery_advertisement_contains_no_enrollment_or_pairing_material(
    monkeypatch,
) -> None:
    context = SimpleNamespace(
        binding=SimpleNamespace(
            federation_id="federation-secret-internal-id",
            internal_session_id="internal-session",
        ),
        credentials=SimpleNamespace(
            identity=SimpleNamespace(display_name="Coordinator PC")
        ),
        coordinator=SimpleNamespace(
            store=SimpleNamespace(
                get_session=lambda _session_id: SimpleNamespace(
                    display_name="Workshop Federation"
                )
            )
        ),
    )
    fake_service = SimpleNamespace(
        remote_store=SimpleNamespace(load=lambda: None),
        authorized_context=lambda: context,
    )
    monkeypatch.setattr(pairing_routes, "_pairing_service", lambda: fake_service)

    app = Flask(__name__)
    with app.test_request_context(
        "/onboarding/federation/discovery.json", base_url="http://100.90.80.70:5000"
    ):
        response = pairing_routes._discovery_response()

    assert response.status_code == 200
    payload = response.get_json()
    assert payload == {
        "schema": ADVERTISEMENT_SCHEMA,
        "federation_label": "Workshop Federation",
        "federation_fingerprint": payload["federation_fingerprint"],
        "device_name": "Coordinator PC",
        "relay_port": 8765,
        "pairing_required": True,
        "auto_join_port": 5151,
    }
    assert len(payload["federation_fingerprint"]) == 32
    serialized = json.dumps(payload)
    assert "federation-secret-internal-id" not in serialized
    assert "internal-session" not in serialized
    assert "pairing_code" not in serialized
    assert "enrollment" not in serialized
    assert response.headers["Cache-Control"] == "no-store"


def test_remote_member_does_not_advertise_as_federation_authority(monkeypatch) -> None:
    fake_service = SimpleNamespace(
        remote_store=SimpleNamespace(load=lambda: object()),
    )
    monkeypatch.setattr(pairing_routes, "_pairing_service", lambda: fake_service)

    app = Flask(__name__)
    with app.test_request_context("/onboarding/federation/discovery.json"):
        response = pairing_routes._discovery_response()

    assert response.status_code == 404


def test_onboarding_copy_keeps_tailscale_discovery_separate_from_pairing() -> None:
    template = (
        APP_ROOT / "templates" / "federation" / "onboarding" / "_federation.html"
    ).read_text(encoding="utf-8")

    assert "Tailscale found an FCP Federation" in template
    assert "Discovery proves reachability only" in template
    assert "does not grant Federation membership" in template
    assert "FCP1 code" in template
    assert 'name="pairing_code"' in template
    assert "Open discovered FCP device" in template


def test_discovered_federation_is_never_reported_as_no_federation_found(
    tmp_path, monkeypatch
) -> None:
    """The badge and the panel must never contradict each other.

    Physical acceptance showed "No federation found" in the status badge while
    the panel below it listed a Tailscale-discovered Federation on the same page.
    """

    path = tmp_path / "tailscale_discovery.json"
    write_snapshot(path, _snapshot())
    monkeypatch.setenv("FCP_TAILSCALE_DISCOVERY_FILE", str(path))

    view_model = transition_routes._attach_tailscale_discovery(
        {"federation": {"state": "empty", "state_label": "No federation found"}}
    )

    federation = view_model["federation"]
    assert federation["state"] != "empty"
    assert federation["state_label"] == "Federation found"
    assert view_model["tailscale_discovery"]["federations"]


def test_failed_local_discovery_still_reports_a_discovered_federation(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "tailscale_discovery.json"
    write_snapshot(path, _snapshot())
    monkeypatch.setenv("FCP_TAILSCALE_DISCOVERY_FILE", str(path))

    view_model = transition_routes._attach_tailscale_discovery(
        {"federation": {"state": "unavailable", "state_label": "Discovery unavailable"}}
    )

    assert view_model["federation"]["state_label"] == "Federation found"


def test_connected_and_repair_states_outrank_discovery(tmp_path, monkeypatch) -> None:
    path = tmp_path / "tailscale_discovery.json"
    write_snapshot(path, _snapshot())
    monkeypatch.setenv("FCP_TAILSCALE_DISCOVERY_FILE", str(path))

    for state, label in (("connected", "Connected"), ("repair", "Reconnect required")):
        view_model = transition_routes._attach_tailscale_discovery(
            {"federation": {"state": state, "state_label": label}}
        )
        assert view_model["federation"]["state"] == state
        assert view_model["federation"]["state_label"] == label


def test_no_discovered_federation_keeps_the_empty_state(tmp_path, monkeypatch) -> None:
    path = tmp_path / "tailscale_discovery.json"
    payload = _snapshot()
    payload["federations"] = []
    write_snapshot(path, payload)
    monkeypatch.setenv("FCP_TAILSCALE_DISCOVERY_FILE", str(path))

    view_model = transition_routes._attach_tailscale_discovery(
        {"federation": {"state": "empty", "state_label": "No federation found"}}
    )

    assert view_model["federation"]["state_label"] == "No federation found"


def test_discovered_candidate_card_stays_readable_at_desktop_widths() -> None:
    """The fingerprint must not wrap one character per line.

    The discovered card carries no radio input, so the three-column candidate
    grid collapsed its middle column. It needs a full-width single column.
    """

    template = (
        APP_ROOT / "templates" / "federation" / "onboarding" / "_federation.html"
    ).read_text(encoding="utf-8")
    stylesheet = (APP_ROOT / "static" / "css" / "federation.css").read_text(
        encoding="utf-8"
    )

    assert "federation-candidate--discovered" in template
    assert (
        ".federation-candidate--discovered{grid-template-columns:minmax(0,1fr)}"
        in stylesheet
    )
    assert ".federation-candidate--discovered>*{grid-column:1/-1;min-width:0}" in stylesheet
    # The fingerprint itself must still be allowed to break inside a long hash.
    assert "overflow-wrap:anywhere" in stylesheet
