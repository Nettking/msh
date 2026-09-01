"""Bounded replay of authoritative session history for authority projections.

A page ceiling is a resource bound, not a retention policy. A consumer that
stops at its own ceiling and returns whatever it accumulated is presenting a
bounded prefix of the authoritative log as current truth: a leadership
handover, an authority rotation or a role change recorded past that ceiling
simply does not exist for the projection that reads it.

Until a coordinator-authenticated snapshot/base-revision mechanism exists,
authority and security projections must fail closed instead. This module owns
that single contract. It drives a caller-supplied bounded page reader, folds
each page in revision order, and returns only once the coordinator's own
reported current revision has actually been reached. Every other exit raises
:class:`AuthoritativeReplayIncomplete`.

This is deliberately not a new bound. It does not widen any page ceiling, read
more history than the caller already read, or reinterpret coordinator
authority; it only refuses to let an unfinished read be mistaken for the
current authoritative state.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from .errors import FederationOperationError, RevisionGapError

#: Stable machine-readable reason for an authoritative read that stopped short.
AUTHORITATIVE_REPLAY_INCOMPLETE = "authoritative-replay-incomplete"


class AuthoritativeReplayIncomplete(FederationOperationError):
    """A bounded read could not prove it reached the authoritative revision.

    This is an explicit bounded failure, not an unavailable Federation. The
    coordinator answered; the reader's own page budget was too small to reach
    the end of the authoritative log, so the projection has no current truth to
    report and must refuse rather than answer from the prefix it did read.
    """

    def __init__(self, message: str) -> None:
        super().__init__(AUTHORITATIVE_REPLAY_INCOMPLETE, message)


def _revision(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AuthoritativeReplayIncomplete(
            f"authoritative replay reported a non-numeric {field}"
        )
    return value


def _read_page(
    reader: Callable[[int], tuple[Sequence[Any], Any] | None],
    last_revision: int,
) -> tuple[Sequence[Any], Any] | None:
    """Read one page while preserving revision gaps as fail-closed outcomes.

    A local coordinator reports missing authoritative history with
    :class:`RevisionGapError`. A paired coordinator returns the same condition
    through a structured remote operation error whose stable code is
    ``revision-gap``. Both mean the reader cannot prove current truth and must
    therefore use the same bounded-incomplete contract as page exhaustion.
    Other Federation operation failures retain their original semantics.
    """

    try:
        return reader(last_revision)
    except AuthoritativeReplayIncomplete:
        raise
    except RevisionGapError as exc:
        raise AuthoritativeReplayIncomplete(
            "authoritative replay encountered a revision gap"
        ) from exc
    except FederationOperationError as exc:
        if exc.code == "revision-gap":
            raise AuthoritativeReplayIncomplete(
                "authoritative replay encountered a remote revision gap"
            ) from exc
        raise


def replay_authoritative_history(
    read_page: Callable[[int], tuple[Sequence[Any], Any] | None],
    *,
    apply_page: Callable[[tuple[Any, ...]], None],
    max_pages: int,
    start_revision: int = 0,
) -> int:
    """Fold every authoritative event through ``apply_page`` or fail closed.

    ``read_page`` receives the highest revision applied so far and returns the
    next bounded page together with the coordinator's current revision. Pages
    are applied in exact contiguous revision order; the read succeeds only when
    the last applied revision equals that reported current revision.

    Returns the proven revision. Raises :class:`AuthoritativeReplayIncomplete`
    when the page budget is exhausted first, when a page makes no forward
    progress, skips a revision, contradicts the coordinator's current revision,
    when a coordinator reports a revision gap, or when the reader does not
    report usable revisions at all.
    """

    if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages < 1:
        raise ValueError("max_pages must be a positive integer")
    last_revision = _revision(start_revision, "start revision")
    for _ in range(max_pages):
        result = _read_page(read_page, last_revision)
        if result is None:
            raise AuthoritativeReplayIncomplete(
                "the connected Federation stopped exposing its authoritative event log"
            )
        page, reported_revision = result
        current_revision = _revision(reported_revision, "current revision")
        if current_revision < last_revision:
            raise AuthoritativeReplayIncomplete(
                "authoritative replay reported a current revision behind applied history"
            )
        events = tuple(page)
        for event in events:
            revision = _revision(getattr(event, "revision", None), "event revision")
            if revision <= last_revision:
                raise AuthoritativeReplayIncomplete(
                    "authoritative replay returned a non-advancing event revision"
                )
            if revision != last_revision + 1:
                raise AuthoritativeReplayIncomplete(
                    "authoritative replay returned a non-contiguous event revision"
                )
            if revision > current_revision:
                raise AuthoritativeReplayIncomplete(
                    "authoritative replay returned an event beyond its current revision"
                )
            last_revision = revision
        apply_page(events)
        if last_revision == current_revision:
            return last_revision
        if not events:
            # The coordinator still reports later history than this reader
            # applied. Treating the short page as the end of the log is exactly
            # the bounded-prefix-as-truth failure this primitive exists to stop.
            raise AuthoritativeReplayIncomplete(
                "authoritative replay stopped before the coordinator's current revision"
            )
    raise AuthoritativeReplayIncomplete(
        "authoritative replay exceeded its bounded page budget before reaching "
        "the coordinator's current revision"
    )


__all__ = [
    "AUTHORITATIVE_REPLAY_INCOMPLETE",
    "AuthoritativeReplayIncomplete",
    "replay_authoritative_history",
]
