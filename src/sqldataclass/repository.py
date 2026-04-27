"""Base repository classes and transaction handle for database access.

Repositories receive an SA Connection (or a psycopg cursor/connection,
which is auto-wrapped via ``from_psycopg``). Raw SQL goes through
``exec_driver_sql()`` — using the driver's native param style.

Endpoints own the transaction; repos only execute queries.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from sqlalchemy.engine import Connection, RowMapping

if TYPE_CHECKING:
    import psycopg


def _ensure_sa_connection(connection: Connection | psycopg.Cursor[Any] | psycopg.Connection[Any]) -> Connection:
    """Return an SA Connection, wrapping psycopg objects if necessary."""
    if isinstance(connection, Connection):
        return connection
    from sqldataclass.compat import from_psycopg

    return from_psycopg(connection)


class ReadRepository:
    """Base class for read-only database access.

    Accepts an SA Connection or a psycopg cursor/connection::

        repo = MyReadRepo(sa_connection)
        repo = MyReadRepo(psycopg_cursor)
        repo = MyReadRepo(psycopg_connection)
    """

    def __init__(self, connection: Connection | psycopg.Cursor[Any] | psycopg.Connection[Any]) -> None:
        self._connection = _ensure_sa_connection(connection)

    def _fetch_one(self, query: str, params: dict[str, Any] | None = None) -> RowMapping | None:
        """Fetch a single row as a dict-like RowMapping, or None."""
        result = self._connection.exec_driver_sql(query, params or {})
        return result.mappings().fetchone()

    def _fetch_all(self, query: str, params: dict[str, Any] | None = None) -> list[RowMapping]:
        """Fetch all matching rows as dict-like RowMappings."""
        result = self._connection.exec_driver_sql(query, params or {})
        return list(result.mappings().fetchall())

    def _fetch_value(self, query: str, params: dict[str, Any] | None = None) -> Any:
        """Fetch a single scalar value."""
        result = self._connection.exec_driver_sql(query, params or {})
        row = result.fetchone()
        if row is None:
            return None
        return row[0]


class WriteRepository(ReadRepository):
    """Base class for read-write database access."""

    def _execute(self, query: str, params: dict[str, Any] | None = None) -> None:
        """Execute a statement (INSERT, UPDATE, DELETE)."""
        self._connection.exec_driver_sql(query, params or {})

    def commit(self) -> None:
        """Commit the current transaction."""
        self._connection.commit()


class TransactionHandle:
    """Restricted transaction interface for endpoints.

    Wraps an SA Connection but only exposes transaction control — no query
    execution. Repos receive the underlying Connection directly.

    Accepts an SA Connection or a psycopg cursor/connection::

        handle = TransactionHandle(sa_connection)
        handle = TransactionHandle(psycopg_cursor)
    """

    def __init__(self, connection: Connection | psycopg.Cursor[Any] | psycopg.Connection[Any]) -> None:
        self._connection = _ensure_sa_connection(connection)

    @contextmanager
    def savepoint(self) -> Generator[None]:
        """Create a savepoint that rolls back on exception without affecting the outer transaction."""
        with self._connection.begin_nested():
            yield

    def commit(self) -> None:
        """Commit the current transaction."""
        self._connection.commit()
