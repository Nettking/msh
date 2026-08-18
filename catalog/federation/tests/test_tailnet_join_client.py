"""The joining host asks discovered coordinators, and stores nothing on refusal."""

from __future__ import annotations

import json
import urllib.error
from pathlib import Path

from catalog.federation import tailnet_join_client as client
from catalog.federation.tailnet_join_bridge import (
    GRANT_SCHEMA,
    clear_grant,
    load_grant,
    store_grant,
)
from catalog.federation.tailscale_host_discovery import (
    ADVERTISEMENT_SCHEMA,
    DISCOVERY_SCHEMA,
    write_snapshot,
)

GRANT = "FCP1-test-grant-not-real"


def _snapshot(tmp_path: Path, *, address="100.90.80.70", auto_join_port=5151) -> Path:
    path = tmp_path / "tailscale_discovery.json"
    write_snapshot(
        path,
        {
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
                    "auto_join_port": auto_join_port,
                    "tailscale_ip": address,
                    "tailscale_dns_name": "coordinator.example.ts.net",
                    "tailscale_host_name": "coordinator",
                    "web_port": 5000,
                }
            ],
        },
    )
    return path


def _opener(payload, *, status=200):
    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def read(self, _size=-1):
            return json.dumps(payload).encode("utf-8")

    def opener(request, timeout=None):
        if status != 200:
            raise urllib.error.HTTPError(
                request.full_url, status, "refused", None, None
            )
        return _Response()

    return opener


def test_a_granted_join_is_stored_for_the_local_application(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    grant_file = tmp_path / "grant.json"

    joined, reason = client.join_discovered_federation(
        snapshot_path=snapshot,
        grant_file=grant_file,
        opener=_opener({"accepted": True, "pairing_code": GRANT}),
    )

    assert joined is True
    assert reason == "granted"
    assert load_grant(grant_file) == GRANT
    assert json.loads(grant_file.read_text())["schema"] == GRANT_SCHEMA


def test_a_refused_join_stores_nothing(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    grant_file = tmp_path / "grant.json"

    joined, reason = client.join_discovered_federation(
        snapshot_path=snapshot,
        grant_file=grant_file,
        opener=_opener({"accepted": False, "error": "peer-owned-by-another-user"}),
    )

    assert joined is False
    assert reason == "peer-owned-by-another-user"
    assert not grant_file.exists()
    assert load_grant(grant_file) == ""


def test_an_unreachable_responder_stores_nothing(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    grant_file = tmp_path / "grant.json"

    def opener(request, timeout=None):
        raise urllib.error.URLError("connection refused")

    joined, reason = client.join_discovered_federation(
        snapshot_path=snapshot,
        grant_file=grant_file,
        opener=opener,
    )

    assert joined is False
    assert reason == "responder-unreachable"
    assert not grant_file.exists()


def test_no_discovered_federation_is_not_an_error(tmp_path: Path) -> None:
    path = tmp_path / "tailscale_discovery.json"
    write_snapshot(
        path,
        {"schema": DISCOVERY_SCHEMA, "tailscale_available": True, "federations": []},
    )
    grant_file = tmp_path / "grant.json"

    joined, reason = client.join_discovered_federation(
        snapshot_path=path,
        grant_file=grant_file,
        opener=_opener({"accepted": True, "pairing_code": GRANT}),
    )

    assert joined is False
    assert reason == "no-discovered-federation"
    assert not grant_file.exists()


def test_a_non_tailnet_coordinator_address_is_never_contacted(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path, address="203.0.113.9")
    grant_file = tmp_path / "grant.json"

    def opener(request, timeout=None):
        raise AssertionError("a non-tailnet address must not be contacted")

    joined, _reason = client.join_discovered_federation(
        snapshot_path=snapshot,
        grant_file=grant_file,
        opener=opener,
    )

    assert joined is False
    assert not grant_file.exists()


def test_a_grant_is_one_use_and_is_cleared(tmp_path: Path) -> None:
    grant_file = tmp_path / "grant.json"
    store_grant(grant_file, GRANT)
    assert load_grant(grant_file) == GRANT

    clear_grant(grant_file)

    assert load_grant(grant_file) == ""
    assert not grant_file.exists()
    # Clearing an absent grant is safe on restart.
    clear_grant(grant_file)


def test_a_malformed_grant_file_is_ignored(tmp_path: Path) -> None:
    grant_file = tmp_path / "grant.json"
    grant_file.write_text("not json", encoding="utf-8")
    assert load_grant(grant_file) == ""

    grant_file.write_text(json.dumps({"schema": "other", "pairing_code": GRANT}))
    assert load_grant(grant_file) == ""


def test_host_and_container_resolve_the_same_shared_files() -> None:
    """A custom data directory must not split the two sides apart.

    The container never sets FCP_DATA_DIR and resolves against its own working
    directory, where the volume is mounted. The host may point the same volume
    somewhere else, and must follow it or the two would authenticate against
    different files and every automatic join would fail.
    """

    from catalog.federation.tailnet_join_bridge import grant_path, secret_path

    container = secret_path({})
    assert str(container) == "data/federation/onboarding/auto_join_secret"

    host = secret_path({"FCP_DATA_DIR": "/srv/fcp-data"})
    assert str(host) == "/srv/fcp-data/federation/onboarding/auto_join_secret"
    # Same suffix beneath the mounted directory, so both sides meet in the volume.
    assert str(host).endswith(str(container).split("data/", 1)[1])

    assert str(grant_path({"FCP_DATA_DIR": "/srv/fcp-data"})).startswith("/srv/fcp-data/")
    # An explicit override still wins on either side.
    assert str(secret_path({"FCP_AUTO_JOIN_SECRET_FILE": "/x/y"})) == "/x/y"
