"""psycopg compatibility — wrap a raw psycopg connection or cursor for use with SQLDataclass.

Supports both psycopg (v3) and psycopg2. The driver is detected from the
type of the object passed in; the appropriate SQLAlchemy URL is selected
accordingly.

The wrapped SAConnection shares the caller's psycopg transaction: writes
made through the original cursor before ``from_psycopg(cur)`` is called
remain visible afterwards, and the SAConnection returned by this function
will not commit or rollback the underlying psycopg connection on its own.
The caller owns the connection lifecycle.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, overload

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection as SAConnection
from sqlalchemy.pool import StaticPool

if TYPE_CHECKING:
    import psycopg


_PSYCOPG3_URL = "postgresql+psycopg://"
_PSYCOPG2_URL = "postgresql+psycopg2://"


@overload
def from_psycopg(psycopg_conn_or_cursor: psycopg.Cursor[Any]) -> SAConnection: ...


@overload
def from_psycopg(psycopg_conn_or_cursor: psycopg.Connection[Any]) -> SAConnection: ...


def from_psycopg(psycopg_conn_or_cursor: Any) -> SAConnection:
    """Wrap a psycopg (v3) or psycopg2 connection/cursor into an SQLAlchemy Connection.

    The returned SA Connection shares the same underlying psycopg connection
    — and therefore the same transaction — as the original object. Reads via
    the returned SAConnection see uncommitted writes made through the caller's
    cursor, and SD will neither commit nor roll back the underlying connection
    on the caller's behalf. The caller owns the connection lifecycle; closing
    the SA Connection does **not** close the underlying psycopg connection.

    Usage::

        from sqldataclass import from_psycopg

        sa_conn = from_psycopg(cur)           # from a psycopg / psycopg2 cursor
        sa_conn = from_psycopg(psycopg_conn)  # from a psycopg / psycopg2 connection

        heroes = Hero.load_all(sa_conn, where=Hero.c.age > 30)

    Notes:
    - Supports both psycopg (v3) and psycopg2; the driver is detected from
      the object's type.
    - If the underlying psycopg connection has a custom ``cursor_factory``
      that returns non-tuple rows (e.g. a DictCursor variant), SA's dialect
      introspection will fail. Reset the factory to ``None`` before calling,
      or pass a tuple-cursor connection.
    """
    driver, raw_conn = _detect_driver_and_extract_connection(psycopg_conn_or_cursor)
    url = _PSYCOPG3_URL if driver == "psycopg" else _PSYCOPG2_URL

    engine = create_engine(
        url,
        creator=lambda: raw_conn,
        poolclass=StaticPool,
        # Disable the pool's default rollback-on-return. SA's pool wipes any
        # pending transaction when the SAConnection is returned (e.g. garbage
        # collected); the contract here is that the caller's transaction must
        # survive a round trip through SA.
        pool_reset_on_return=None,
    )

    # SA hardcodes ``dialect.do_rollback(c.connection)`` in the finally block of
    # ``dialect.initialize()`` (see ``sqlalchemy/engine/create.py``). That call
    # would discard the caller's in-flight writes. Patch ``do_rollback`` to a
    # no-op for the duration of the first ``engine.connect()`` (where
    # initialize fires), then restore so subsequent ``SAConnection.rollback()``
    # calls behave normally.
    original_do_rollback = engine.dialect.do_rollback
    engine.dialect.do_rollback = lambda _conn: None  # type: ignore[method-assign, assignment]  # intentional temporary patch of a bound method
    try:
        sa_conn = engine.connect()
    finally:
        engine.dialect.do_rollback = original_do_rollback  # type: ignore[method-assign]  # restoring original bound method
    return sa_conn


def _detect_driver_and_extract_connection(
    psycopg_conn_or_cursor: Any,
) -> tuple[str, Any]:
    """Detect whether the object came from psycopg (v3) or psycopg2 and extract the connection.

    Returns a ``(driver_name, connection)`` tuple. ``driver_name`` is one of
    ``"psycopg"`` (v3) or ``"psycopg2"``. Raises ``ImportError`` if neither
    driver is installed, ``TypeError`` if the object matches neither driver.
    """
    psycopg3_mod = _try_import("psycopg")
    psycopg2_mod = _try_import("psycopg2")

    if psycopg3_mod is None and psycopg2_mod is None:
        msg = (
            "Neither psycopg (v3) nor psycopg2 is installed. Install one with:\n"
            "  pip install sqldataclass[postgres]   # installs psycopg (v3)\n"
            "  pip install psycopg2-binary          # or psycopg2"
        )
        raise ImportError(msg)

    if psycopg3_mod is not None:
        if isinstance(psycopg_conn_or_cursor, psycopg3_mod.Cursor):
            return "psycopg", psycopg_conn_or_cursor.connection
        if isinstance(psycopg_conn_or_cursor, psycopg3_mod.Connection):
            return "psycopg", psycopg_conn_or_cursor

    if psycopg2_mod is not None:
        ext = _try_import("psycopg2.extensions")
        if ext is not None:
            if isinstance(psycopg_conn_or_cursor, ext.cursor):
                return "psycopg2", psycopg_conn_or_cursor.connection
            if isinstance(psycopg_conn_or_cursor, ext.connection):
                return "psycopg2", psycopg_conn_or_cursor

    type_name = type(psycopg_conn_or_cursor).__name__
    available = [name for name, mod in (("psycopg", psycopg3_mod), ("psycopg2", psycopg2_mod)) if mod is not None]
    msg = (
        f"Expected a psycopg or psycopg2 Connection or Cursor, got {type_name}. "
        f"Installed drivers: {', '.join(available) or 'none'}."
    )
    raise TypeError(msg)


def _try_import(name: str) -> Any:
    """Import a module by name and return it, or None if unavailable."""
    try:
        import importlib

        return importlib.import_module(name)
    except ImportError:
        return None


# Backwards-compatibility shim — earlier versions exposed _extract_connection.
def _extract_connection(psycopg_conn_or_cursor: Any) -> Any:
    """Deprecated. Use :func:`from_psycopg` directly.

    Returns just the connection (matching the previous return type).
    """
    _, conn = _detect_driver_and_extract_connection(psycopg_conn_or_cursor)
    return conn
