"""Transaction ownership for the replicated public-journal materialized store.

The controller must acquire its runtime lifecycle lock before entering SQLite.
It stages one complete operation through ``raw_transaction`` and decides whether
that operation may commit. Materialization uses ``projection`` under the same
lock. Neither trusted entry point establishes quorum by itself.

Journal triggers prevent accidental writes through an ordinary CoordinatorStore
or another SQLite connection. They are not a security boundary against a local
administrator who can replace the database schema or register arbitrary SQL
functions. Standalone databases receive no triggers until a controller attaches.
"""

from __future__ import annotations

import secrets
import sqlite3
import threading
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Protocol

from .host_resources import ProcessResourceAdmission
from .persistence import COORDINATOR_ID, CoordinatorStore

JOURNAL_WRITER_FUNCTION = "fcp_c03_journal_writer"
JOURNAL_WRITE_REFUSED = "C03 public journal requires an owned transaction"
ROLLBACK_ONLY_MESSAGE = "C03 journal transaction is rollback-only after a nested failure"
LOCAL_CONNECTIVITY_COLUMNS = frozenset({"state", "connection_id", "disconnected_at", "last_error"})


class JournalTransactionController(Protocol):
    def transaction(
        self, store: JournalCoordinatorStore
    ) -> AbstractContextManager[sqlite3.Connection]:
        """Acquire the lifecycle lock, then stage through store.raw_transaction()."""
        ...


class JournalCoordinatorStore(CoordinatorStore):
    """CoordinatorStore with controller-owned writes and nested stage visibility.

    ``transaction`` keeps the standalone behavior until attachment. Once attached,
    an outer ordinary transaction delegates to ``controller.transaction(self)``.
    The controller must enter ``raw_transaction`` before yielding its connection.
    Nested transactions reuse that owned connection through savepoints. A caught
    nested write failure still makes the complete outer stage rollback-only.

    ``read_transaction`` reuses the stage while an owned operation is active.
    Inherited public read helpers use that scope. Explicitly supplied connections
    retain their caller's ownership and are never committed or closed here.
    """

    def __init__(
        self,
        database: Path | str,
        *,
        coordinator_id: str = COORDINATOR_ID,
        token_factory: Callable[[int], str] = secrets.token_urlsafe,
        id_factory: Callable[[], str] | None = None,
        resource_admission: ProcessResourceAdmission | None = None,
    ) -> None:
        self._journal_context = threading.local()
        self._journal_controller: JournalTransactionController | None = None
        super().__init__(
            database,
            coordinator_id=coordinator_id,
            token_factory=token_factory,
            id_factory=id_factory,
            resource_admission=resource_admission,
        )

    def _active_connection(self) -> sqlite3.Connection | None:
        return getattr(self._journal_context, "connection", None)

    def assert_committable(self) -> None:
        """Refuse a poisoned owned stage before any external durable commit.

        The controller must check this immediately after the complete operation
        returns, before saving or proposing its consensus envelope. The final
        raw_transaction check remains necessary for failures during its unwind.
        """
        if self._active_connection() is None:
            raise RuntimeError("C03 journal commit requires an owned transaction")
        if self._journal_context.rollback_only:
            raise RuntimeError(ROLLBACK_ONLY_MESSAGE)

    def _connect(self) -> sqlite3.Connection:
        connection = super()._connect()
        try:
            # Authorization is connection- and thread-scoped, not a global flag.
            # Another connection opened during a stage must remain unauthorized.
            connection.create_function(
                JOURNAL_WRITER_FUNCTION,
                0,
                lambda: int(self._active_connection() is connection),
            )
        except BaseException:
            connection.close()
            raise
        return connection

    def require_active_node(
        self, node_id: str, *, database: sqlite3.Connection | None = None
    ) -> sqlite3.Row:
        # Preserve the base method's explicit-connection behavior. Its implicit
        # reader can borrow this operation's stage without closing that stage.
        if database is None:
            database = self._active_connection()
        return super().require_active_node(node_id, database=database)

    def attach_controller(self, controller: JournalTransactionController) -> None:
        """Install durable guards, then route ordinary outer transactions.

        Call at runtime setup under the lifecycle lock, outside any transaction.
        Attachment cannot be replaced or detached; reopening a guarded database
        through a plain store deliberately does not restore write permission.
        """
        if not callable(getattr(controller, "transaction", None)):
            raise TypeError("journal controller must provide transaction(store)")
        if self._active_connection() is not None:
            raise RuntimeError("cannot attach a journal controller during a transaction")
        if self._journal_controller is not None:
            if self._journal_controller is controller:
                return
            raise RuntimeError("journal controller is already attached")
        with self.raw_transaction() as database:
            for operation in ("INSERT", "UPDATE", "DELETE"):
                # Identifiers come only from this fixed tuple; never executescript
                # here because it would commit an existing SQLite transaction.
                database.execute(
                    f"""
                    CREATE TRIGGER IF NOT EXISTS fcp_c03_journal_guard_{operation.lower()}
                    BEFORE {operation} ON session_events
                    WHEN {JOURNAL_WRITER_FUNCTION}() IS NOT 1
                    BEGIN
                        SELECT RAISE(ABORT, '{JOURNAL_WRITE_REFUSED}');
                    END
                    """
                )
        self._journal_controller = controller

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        if self._active_connection() is not None:
            with self.raw_transaction() as database:
                yield database
        elif self._journal_controller is not None:
            with self._journal_controller.transaction(self) as database:
                yield database
        else:
            with super().transaction() as database:
                yield database

    @contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        database = self._active_connection()
        if database is not None:
            # Do not BEGIN, commit, roll back or close the owning write stage.
            yield database
        else:
            with super().read_transaction() as database:
                yield database

    @contextmanager
    def raw_transaction(self) -> Iterator[sqlite3.Connection]:
        """Trusted staging entry point; only its outermost context can commit.

        The caller must already hold the lifecycle lock. Any exception escaping
        a nested write context poisons this complete stage, even if subsequently
        caught. A successful inner context only releases its savepoint.
        """
        active = self._active_connection()
        if active is not None:
            self._journal_context.savepoint_number += 1
            savepoint = f"fcp_c03_stage_{self._journal_context.savepoint_number}"
            try:
                active.execute(f"SAVEPOINT {savepoint}")
            except BaseException:
                self._journal_context.rollback_only = True
                raise
            try:
                yield active
            except BaseException:
                self._journal_context.rollback_only = True
                try:
                    active.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                finally:
                    active.execute(f"RELEASE SAVEPOINT {savepoint}")
                raise
            else:
                try:
                    active.execute(f"RELEASE SAVEPOINT {savepoint}")
                except BaseException:
                    self._journal_context.rollback_only = True
                    raise
            return

        with super().transaction() as database:
            self._journal_context.connection = database
            self._journal_context.rollback_only = False
            self._journal_context.savepoint_number = 0
            try:
                yield database
                if self._journal_context.rollback_only:
                    raise RuntimeError(ROLLBACK_ONLY_MESSAGE)
            finally:
                # The superclass performs the one outer commit/rollback and
                # closes the connection after ownership is withdrawn here.
                self._journal_context.connection = None
                self._journal_context.rollback_only = False
                self._journal_context.savepoint_number = 0

    @contextmanager
    def projection(self) -> Iterator[sqlite3.Connection]:
        """Trusted materialization context; bypass controller under lifecycle lock."""
        with self.raw_transaction() as database:
            yield database

    @contextmanager
    def local_connectivity_transaction(self) -> Iterator[sqlite3.Connection]:
        """Update only local liveness, without resolving pending authority work.

        The caller holds the runtime lifecycle lock. This scope cannot inherit
        an authority stage or grant its callbacks general projection permission.
        Existing health methods reuse its connection through nested savepoints.
        Any denied statement poisons the stage even if its exception is caught.
        """
        if self._active_connection() is not None:
            raise RuntimeError("local connectivity cannot inherit an active transaction")

        def authorize(action, table, column, database_name, trigger):
            if action in (sqlite3.SQLITE_READ, sqlite3.SQLITE_SELECT, sqlite3.SQLITE_SAVEPOINT):
                return sqlite3.SQLITE_OK
            if (
                action == sqlite3.SQLITE_UPDATE
                and database_name == "main"
                and table == "node_connectivity"
                and column in LOCAL_CONNECTIVITY_COLUMNS
                and trigger is None
            ):
                return sqlite3.SQLITE_OK
            self._journal_context.rollback_only = True
            return sqlite3.SQLITE_DENY

        with self.raw_transaction() as database:
            database.set_authorizer(authorize)
            try:
                yield database
            finally:
                # Restore before the owned transaction commits/rolls back and
                # closes; the authorizer must not intercept that outer cleanup.
                database.set_authorizer(None)


__all__ = ["JournalCoordinatorStore", "JournalTransactionController"]
