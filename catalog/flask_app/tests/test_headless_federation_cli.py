from __future__ import annotations

from types import SimpleNamespace

import pytest

from catalog.federation.errors import FederationOperationError, FederationValidationError
from catalog.flask_app.services import headless_federation_cli as cli


def _identity(node_id: str = "node-a") -> SimpleNamespace:
    return SimpleNamespace(identity=SimpleNamespace(node_id=node_id))


def _binding(
    *,
    federation_id: str = "federation-a",
    session_id: str = "session-a",
    device_id: str = "node-a",
) -> SimpleNamespace:
    return SimpleNamespace(
        federation_id=federation_id,
        internal_session_id=session_id,
        device_id=device_id,
    )


class FakeRemoteStore:
    def __init__(self, value=None) -> None:
        self.value = value

    def load(self):
        return self.value


class FakeService:
    def __init__(self) -> None:
        self.credentials = _identity()
        self.binding = None
        self.remote_store = FakeRemoteStore()
        self.connected = []
        self.redeemed = []
        self.codes = []

    def create_identity(self):
        return self.credentials

    def identity_or_none(self):
        return self.credentials

    def binding_or_none(self):
        return self.binding

    def connect(self, *, request_id: str):
        self.connected.append(request_id)
        self.binding = _binding()
        return self.binding

    def authorized_context(self):
        if self.binding is None:
            return None
        return SimpleNamespace(credentials=self.credentials, binding=self.binding)

    def redeem_pairing_code(self, code: str):
        self.redeemed.append(code)
        self.binding = _binding()
        return self.binding

    def create_pairing_code(self, *, relay_url: str):
        self.codes.append(relay_url)
        return "FCP1-issued"


def test_create_first_federation_uses_existing_onboarding_authority(monkeypatch) -> None:
    service = FakeService()
    monkeypatch.setattr(cli, "_service", lambda: service)

    result = cli.create_first_federation()

    assert result.state == "created"
    assert result.node_id == "node-a"
    assert result.federation_id == "federation-a"
    assert len(service.connected) == 1
    assert service.connected[0].startswith("headless-create-")


def test_create_first_federation_is_idempotent_for_local_authority(monkeypatch) -> None:
    service = FakeService()
    service.binding = _binding()
    monkeypatch.setattr(cli, "_service", lambda: service)

    result = cli.create_first_federation()

    assert result.state == "already-connected"
    assert service.connected == []


def test_create_first_federation_refuses_remote_joined_device(monkeypatch) -> None:
    service = FakeService()
    service.binding = _binding()
    service.remote_store = FakeRemoteStore(object())
    monkeypatch.setattr(cli, "_service", lambda: service)

    with pytest.raises(FederationOperationError) as excinfo:
        cli.create_first_federation()

    assert excinfo.value.code == "headless-create-join-only-device"


def test_join_requires_fcp1_code(monkeypatch) -> None:
    service = FakeService()
    monkeypatch.setattr(cli, "_service", lambda: service)

    with pytest.raises(FederationValidationError) as excinfo:
        cli.join_existing_federation("not-a-pairing-code")

    assert excinfo.value.code == "headless-pairing-code-required"
    assert service.redeemed == []


def test_join_refuses_to_replace_existing_binding(monkeypatch) -> None:
    service = FakeService()
    service.binding = _binding()
    monkeypatch.setattr(cli, "_service", lambda: service)

    with pytest.raises(FederationOperationError) as excinfo:
        cli.join_existing_federation("FCP1-new-code")

    assert excinfo.value.code == "headless-existing-binding-preserved"
    assert service.redeemed == []


def test_join_reuses_authenticated_pairing_service(monkeypatch) -> None:
    service = FakeService()
    monkeypatch.setattr(cli, "_service", lambda: service)

    result = cli.join_existing_federation("FCP1-one-use")

    assert service.redeemed == ["FCP1-one-use"]
    assert result.state == "connected"
    assert result.node_id == "node-a"
    assert "pairing_code" not in result.public_dict()


def test_pairing_code_requires_and_uses_local_authorized_context(monkeypatch) -> None:
    service = FakeService()
    service.binding = _binding()
    monkeypatch.setattr(cli, "_service", lambda: service)

    result = cli.create_pairing_code("ws://100.64.0.10:8765")

    assert service.codes == ["ws://100.64.0.10:8765"]
    assert result.pairing_code == "FCP1-issued"
    assert result.public_dict()["pairing_code"] == "FCP1-issued"


def test_status_never_invents_identity_or_membership(monkeypatch) -> None:
    service = FakeService()
    service.credentials = None
    monkeypatch.setattr(cli, "_service", lambda: service)

    result = cli.federation_status()

    assert result.state == "identity-missing"
    assert result.node_id is None
    assert result.federation_id is None
