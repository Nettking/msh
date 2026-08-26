"""Contract for the bounded authoritative-history reader.

The reader exists to remove one specific failure: an authority projection that
runs out of pages, stops, and reports the prefix it managed to read as the
current state of the Federation. Every exit that is not "the coordinator's own
current revision was reached" must raise.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from catalog.federation.authoritative_replay import (
    AUTHORITATIVE_REPLAY_INCOMPLETE,
    AuthoritativeReplayIncomplete,
    replay_authoritative_history,
)


def _event(revision: int) -> SimpleNamespace:
    return SimpleNamespace(revision=revision)


def _reader(total: int, *, page_size: int, current_revision: int | None = None):
    reported = total if current_revision is None else current_revision

    def read_page(last_revision: int):
        page = tuple(
            _event(revision)
            for revision in range(last_revision + 1, min(last_revision + page_size, total) + 1)
        )
        return page, reported

    return read_page


def test_a_complete_read_returns_the_proven_revision() -> None:
    applied: list[int] = []
    proven = replay_authoritative_history(
        _reader(10, page_size=4),
        apply_page=lambda page: applied.extend(event.revision for event in page),
        max_pages=8,
    )
    assert proven == 10
    assert applied == list(range(1, 11))


def test_an_empty_history_is_complete() -> None:
    applied: list[int] = []
    proven = replay_authoritative_history(
        _reader(0, page_size=4),
        apply_page=lambda page: applied.extend(event.revision for event in page),
        max_pages=8,
    )
    assert proven == 0
    assert applied == []


def test_an_exhausted_page_budget_fails_closed_instead_of_returning_the_prefix() -> None:
    applied: list[int] = []
    with pytest.raises(AuthoritativeReplayIncomplete) as failure:
        replay_authoritative_history(
            _reader(100, page_size=4),
            apply_page=lambda page: applied.extend(event.revision for event in page),
            max_pages=3,
        )
    assert failure.value.code == AUTHORITATIVE_REPLAY_INCOMPLETE
    assert "bounded page budget" in failure.value.message
    # The prefix was folded, but the caller never receives it as an answer.
    assert applied == list(range(1, 13))


def test_a_short_page_before_the_current_revision_is_not_the_end_of_history() -> None:
    def read_page(last_revision: int):
        return (), 42

    with pytest.raises(AuthoritativeReplayIncomplete) as failure:
        replay_authoritative_history(
            read_page,
            apply_page=lambda _page: None,
            max_pages=8,
        )
    assert "before the coordinator's current revision" in failure.value.message


def test_a_non_advancing_revision_cannot_masquerade_as_progress() -> None:
    def read_page(last_revision: int):
        return (_event(1),), 9

    with pytest.raises(AuthoritativeReplayIncomplete) as failure:
        replay_authoritative_history(
            read_page,
            apply_page=lambda _page: None,
            max_pages=8,
        )
    assert "non-advancing" in failure.value.message


def test_a_revision_gap_cannot_masquerade_as_complete_history() -> None:
    with pytest.raises(AuthoritativeReplayIncomplete) as failure:
        replay_authoritative_history(
            lambda _last: ((_event(2),), 2),
            apply_page=lambda _page: None,
            max_pages=8,
        )
    assert "non-contiguous" in failure.value.message


def test_an_event_cannot_run_ahead_of_the_reported_current_revision() -> None:
    with pytest.raises(AuthoritativeReplayIncomplete) as failure:
        replay_authoritative_history(
            lambda _last: ((_event(1),), 0),
            apply_page=lambda _page: None,
            max_pages=8,
        )
    assert "beyond its current revision" in failure.value.message


def test_the_current_revision_cannot_move_behind_applied_history() -> None:
    with pytest.raises(AuthoritativeReplayIncomplete) as failure:
        replay_authoritative_history(
            lambda _last: ((), 2),
            apply_page=lambda _page: None,
            max_pages=8,
            start_revision=3,
        )
    assert "behind applied history" in failure.value.message


def test_a_withdrawn_replay_surface_fails_closed() -> None:
    with pytest.raises(AuthoritativeReplayIncomplete):
        replay_authoritative_history(
            lambda _last: None,
            apply_page=lambda _page: None,
            max_pages=8,
        )


@pytest.mark.parametrize("reported", [None, True, -1, "12", 3.0])
def test_an_unusable_current_revision_is_refused(reported: object) -> None:
    with pytest.raises(AuthoritativeReplayIncomplete):
        replay_authoritative_history(
            lambda _last: ((), reported),
            apply_page=lambda _page: None,
            max_pages=8,
        )


@pytest.mark.parametrize("revision", [None, True, -1, "4"])
def test_an_unusable_event_revision_is_refused(revision: object) -> None:
    with pytest.raises(AuthoritativeReplayIncomplete):
        replay_authoritative_history(
            lambda _last: ((SimpleNamespace(revision=revision),), 9),
            apply_page=lambda _page: None,
            max_pages=8,
        )


@pytest.mark.parametrize("budget", [0, -1, True, 2.0, "8"])
def test_a_page_budget_must_be_a_positive_integer(budget: object) -> None:
    with pytest.raises(ValueError):
        replay_authoritative_history(
            _reader(4, page_size=2),
            apply_page=lambda _page: None,
            max_pages=budget,
        )


def test_history_that_grows_during_the_read_is_still_followed_to_the_end() -> None:
    reported = [6]

    def read_page(last_revision: int):
        page = tuple(
            _event(revision)
            for revision in range(last_revision + 1, min(last_revision + 3, reported[0]) + 1)
        )
        if last_revision == 0:
            reported[0] = 9
        return page, reported[0]

    applied: list[int] = []
    proven = replay_authoritative_history(
        read_page,
        apply_page=lambda page: applied.extend(event.revision for event in page),
        max_pages=8,
    )
    assert proven == 9
    assert applied == list(range(1, 10))
