from __future__ import annotations

import io
from types import SimpleNamespace

import pytest

from catalog.federation.errors import FederationOperationError
from catalog.flask_app.services import zero_touch_federation_cli as cli


def test_initializer_accepts_email_but_never_a_password_argument() -> None:
    args = cli.build_parser().parse_args(
        ["initialize", "--email", "admin@example.com"]
    )
    assert args.email == "admin@example.com"
    assert not hasattr(args, "password")

    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(
            [
                "initialize",
                "--email",
                "admin@example.com",
                "--password",
                "do-not-accept-this",
            ]
        )


def test_noninteractive_initializer_reads_password_only_from_stdin(monkeypatch) -> None:
    monkeypatch.setattr(cli.sys, "stdin", io.StringIO("private password value\n"))

    assert cli._read_password() == "private password value"


def test_auto_join_clears_one_use_grant_before_redemption(monkeypatch) -> None:
    events: list[str] = []
    identity = SimpleNamespace(identity=SimpleNamespace(node_id="node-b"))
    binding = SimpleNamespace(federation_id="fed-a", internal_session_id="session-a")

    class Service:
        remote_store = SimpleNamespace(load=lambda: None)

        @staticmethod
        def identity_or_none():
            return identity

        @staticmethod
        def redeem_pairing_code(code):
            assert code == "FCP1-private-one-use"
            events.append("redeem")
            return binding

    monkeypatch.setattr(cli, "_service", lambda: Service())
    monkeypatch.setattr(cli, "grant_path", lambda: "grant.json")
    monkeypatch.setattr(cli, "load_grant", lambda _path: "FCP1-private-one-use")
    monkeypatch.setattr(cli, "clear_grant", lambda _path: events.append("clear"))
    monkeypatch.setattr(
        cli, "_publish_human_auth_state", lambda: events.append("publish-sso")
    )

    result = cli.redeem_pending_tailnet_grant()

    assert events == ["clear", "redeem", "publish-sso"]
    assert result.state == "connected"
    assert result.node_id == "node-b"
    assert result.federation_id == "fed-a"


def test_creator_publishes_authority_users_without_waiting_for_browser(monkeypatch) -> None:
    events: list[tuple[str, str | None]] = []

    class HumanAuth:
        @staticmethod
        def mode(*, refresh):
            assert refresh is True
            return SimpleNamespace(is_authority=True, is_member=False)

        @staticmethod
        def ensure_authority(base_url):
            events.append(("authority", base_url))
            return SimpleNamespace(node_id="creator")

        @staticmethod
        def publish_all_users():
            events.append(("users", None))

    monkeypatch.setattr(cli, "configured_base_url", lambda: "http://100.80.0.1:5000")
    monkeypatch.setattr(cli, "get_federated_human_auth_service", lambda: HumanAuth())

    cli._publish_human_auth_state()

    assert events == [
        ("authority", "http://100.80.0.1:5000"),
        ("users", None),
    ]


def test_member_publishes_target_endpoint_without_waiting_for_login(monkeypatch) -> None:
    endpoints: list[str] = []

    class HumanAuth:
        @staticmethod
        def mode(*, refresh):
            assert refresh is True
            return SimpleNamespace(is_authority=False, is_member=True)

        @staticmethod
        def ensure_member_endpoint(base_url):
            endpoints.append(base_url)
            return base_url

    monkeypatch.setattr(cli, "configured_base_url", lambda: "http://100.80.0.2:5000")
    monkeypatch.setattr(cli, "get_federated_human_auth_service", lambda: HumanAuth())

    cli._publish_human_auth_state()

    assert endpoints == ["http://100.80.0.2:5000"]


def test_missing_host_grant_fails_instead_of_creating_membership(monkeypatch) -> None:
    service = SimpleNamespace(
        identity_or_none=lambda: None,
        remote_store=SimpleNamespace(load=lambda: None),
    )
    monkeypatch.setattr(cli, "_service", lambda: service)
    monkeypatch.setattr(cli, "grant_path", lambda: "grant.json")
    monkeypatch.setattr(cli, "load_grant", lambda _path: "")

    with pytest.raises(FederationOperationError) as caught:
        cli.redeem_pending_tailnet_grant()

    assert caught.value.code == "zero-touch-auto-join-grant-required"
