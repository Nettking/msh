from __future__ import annotations

from catalog.flask_app.services.source_connection_test_service import SourceConnectionTestService
from catalog.flask_app.services.source_inventory_service import SourceInventoryService


class FakeResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def getcode(self):
        return 200

    def read(self, size: int):
        return b"<MTConnectStreams/>"


class FakeSocket:
    def close(self):
        pass


def test_mtconnect_connection_uses_current_endpoint(tmp_path):
    inventory = SourceInventoryService(tmp_path / "sources.json")
    assert inventory.add_machine_from_form({"name": "Mazak i500", "mtconnect_url": "10.0.0.20:5000"})[0]
    machine_id = inventory.status_model()["machines"][0]["id"]
    seen_urls: list[str] = []

    def fake_urlopen(request, timeout):
        seen_urls.append(request.full_url)
        return FakeResponse()

    result = SourceConnectionTestService(inventory, urlopen_func=fake_urlopen).test_mtconnect(machine_id)

    assert result.ok
    assert seen_urls == ["http://10.0.0.20:5000/current"]
    assert "HTTP 200" in result.detail


def test_vpn_network_test_uses_configured_host_and_port(tmp_path):
    inventory = SourceInventoryService(tmp_path / "sources.json")
    assert inventory.add_machine_from_form({"name": "Mazak i500", "vpn_test_host": "10.0.0.20", "vpn_test_port": "5000"})[0]
    machine_id = inventory.status_model()["machines"][0]["id"]
    seen_targets: list[tuple[str, int]] = []

    def fake_connect(target, timeout):
        seen_targets.append(target)
        return FakeSocket()

    result = SourceConnectionTestService(inventory, socket_connect_func=fake_connect).test_vpn_network(machine_id)

    assert result.ok
    assert seen_targets == [("10.0.0.20", 5000)]
    assert result.target == "10.0.0.20:5000"


def test_vpn_network_test_falls_back_to_mtconnect_host(tmp_path):
    inventory = SourceInventoryService(tmp_path / "sources.json")
    assert inventory.add_machine_from_form({"name": "Mazak i500", "mtconnect_url": "http://10.0.0.30:5000"})[0]
    machine_id = inventory.status_model()["machines"][0]["id"]
    seen_targets: list[tuple[str, int]] = []

    def fake_connect(target, timeout):
        seen_targets.append(target)
        return FakeSocket()

    result = SourceConnectionTestService(inventory, socket_connect_func=fake_connect).test_vpn_network(machine_id)

    assert result.ok
    assert seen_targets == [("10.0.0.30", 5000)]
