from __future__ import annotations

from catalog.flask_app.services.startup_contribution_reconcile import (
    run_startup_contribution_reconcile,
)


def test_transient_startup_reconcile_failure_releases_claim_for_retry() -> None:
    extensions: dict[str, object] = {}
    calls = 0

    def operation() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("relay is still starting")
        return "restored"

    attempted, result, error = run_startup_contribution_reconcile(
        extensions,
        operation,
    )

    assert attempted is True
    assert result is None
    assert isinstance(error, OSError)
    assert extensions == {}

    attempted, result, error = run_startup_contribution_reconcile(
        extensions,
        operation,
    )

    assert attempted is True
    assert result == "restored"
    assert error is None
    assert calls == 2

    attempted, result, error = run_startup_contribution_reconcile(
        extensions,
        operation,
    )

    assert attempted is False
    assert result is None
    assert error is None
    assert calls == 2


def test_in_progress_reconcile_blocks_duplicate_activation() -> None:
    extensions: dict[str, object] = {}
    nested_attempted: list[bool] = []

    def operation() -> str:
        attempted, _result, _error = run_startup_contribution_reconcile(
            extensions,
            lambda: "duplicate",
        )
        nested_attempted.append(attempted)
        return "single-owner"

    attempted, result, error = run_startup_contribution_reconcile(
        extensions,
        operation,
    )

    assert attempted is True
    assert result == "single-owner"
    assert error is None
    assert nested_attempted == [False]
