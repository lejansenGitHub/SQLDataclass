"""psycopg compatibility — wrap a raw psycopg connection or cursor for use with SQLDataclass."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, overload

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection as SAConnection
from sqlalchemy.pool import StaticPool

if TYPE_CHECKING:
    import psycopg


@overload
def from_psycopg(psycopg_conn_or_cursor: psycopg.Cursor[Any]) -> SAConnection: ...


@overload
def from_psycopg(psycopg_conn_or_cursor: psycopg.Connection[Any]) -> SAConnection: ...


def from_psycopg(psycopg_conn_or_cursor: psycopg.Cursor[Any] | psycopg.Connection[Any]) -> SAConnection:
    """Wrap a psycopg3 connection or cursor into an SQLAlchemy Connection.

    The returned SA Connection shares the same underlying psycopg connection
    (and therefore the same transaction) as the original object. This lets
    you mix legacy cursor-based code with SQLDataclass in a single transaction.

    The caller owns the psycopg connection lifecycle — closing the SA
    Connection does **not** close the underlying psycopg connection.

    Usage::

        from sqldataclass import from_psycopg

        sa_conn = from_psycopg(cur)           # from a psycopg cursor
        sa_conn = from_psycopg(psycopg_conn)  # from a psycopg connection

        heroes = Hero.load_all(sa_conn, where=Hero.c.age > 30)
    """
    raw_conn = _extract_connection(psycopg_conn_or_cursor)
    engine = create_engine(
        "postgresql+psycopg://",
        creator=lambda: raw_conn,
        poolclass=StaticPool,
    )
    return engine.connect()


def _extract_connection(
    psycopg_conn_or_cursor: psycopg.Cursor[Any] | psycopg.Connection[Any],
) -> psycopg.Connection[Any]:
    """Extract the psycopg connection from a cursor, or return as-is if already a connection."""
    try:
        import psycopg as _psycopg
    except ImportError as exc:
        msg = "psycopg is required for from_psycopg(). Install with: pip install sqldataclass[postgres]"
        raise ImportError(msg) from exc

    if isinstance(psycopg_conn_or_cursor, _psycopg.Cursor):
        return psycopg_conn_or_cursor.connection

    if isinstance(psycopg_conn_or_cursor, _psycopg.Connection):
        return psycopg_conn_or_cursor

    msg = f"Expected a psycopg Connection or Cursor, got {type(psycopg_conn_or_cursor).__name__}"
    raise TypeError(msg)
