"""Tests for #3: ``Field(server_managed=True)``.

A ``server_managed`` column is owned by the database (e.g. populated by a
BEFORE INSERT/UPDATE trigger, a computed column, or a server-side calculation).
SD must never write the column on insert or update, but reads it back via
RETURNING after insert.

Semantics:
- Excluded from INSERT regardless of the Python-side value.
- Excluded from ``insert_many`` rows too.
- ``Model.update({"<server_managed>": ...})`` raises ``ValueError``.
- After ``model.insert()``, the field is populated on the instance from RETURNING.
- ``__server_managed_columns__`` is exposed on the class for introspection.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import create_engine, text

import sqldataclass.model as _model
from sqldataclass import Field, SQLDataclass
from sqldataclass.write import flatten_for_table


class WithHash(SQLDataclass, table=True):
    __tablename__ = "smc_with_hash"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    # In a real DB this would be set by a BEFORE INSERT trigger; here we
    # simulate by manually updating the column after insert. A server_default
    # is supplied so the NOT NULL column has a value if no trigger fires
    # (a realistic pairing for trigger-managed columns).
    content_hash: int = Field(default=0, server_managed=True, server_default="0")


@pytest.fixture
def bound_engine() -> Any:
    engine = create_engine("sqlite:///:memory:")
    # Per-table create avoids picking up unrelated ARRAY columns from sibling
    # tests that share SQLDataclass.metadata (SQLite can't compile ARRAY).
    WithHash.__table__.create(engine, checkfirst=True)
    SQLDataclass.bind(engine)
    yield engine
    _model._BOUND_ENGINE = None


def test_server_managed_metadata_recorded_on_class() -> None:
    """The class records its server_managed columns at construction time."""
    # --- Assert ---
    assert "content_hash" in WithHash.__server_managed_columns__


def test_flatten_for_table_drops_server_managed_column() -> None:
    """flatten_for_table excludes server_managed columns regardless of value.

    Even when the user sets a non-zero value, the column is stripped from the
    flat dict so it never makes it into the INSERT statement.
    """
    inst = WithHash(name="hello", content_hash=999)
    flat = flatten_for_table(inst)

    # --- Assert ---
    assert "name" in flat
    assert flat["name"] == "hello"
    assert "content_hash" not in flat


def test_server_managed_column_excluded_from_emitted_insert_sql(bound_engine: Any) -> None:
    """The INSERT SQL SD generates does not reference content_hash —
    the DB's mechanism (trigger, computed col, default) is what populates it."""
    captured: list[str] = []

    from sqlalchemy import event

    @event.listens_for(bound_engine, "before_cursor_execute")
    def _capture(conn: Any, _cur: Any, statement: str, *_args: Any, **_kwargs: Any) -> None:
        captured.append(statement)

    WithHash(name="hello").insert()

    # --- Assert ---
    insert_stmts = [s for s in captured if "INSERT INTO smc_with_hash" in s]
    assert insert_stmts, f"No INSERT captured. Captured: {captured}"
    for stmt in insert_stmts:
        # The column may appear in the RETURNING clause (that's a read path);
        # what must not happen is content_hash appearing in the column list
        # or VALUES placeholders before RETURNING.
        before_returning = stmt.split("RETURNING", 1)[0]
        assert "content_hash" not in before_returning, f"INSERT column-list must not mention content_hash, got: {stmt}"


def test_update_rejects_server_managed_keys(bound_engine: Any) -> None:
    """Attempting Model.update() with a server_managed key in values raises ValueError."""
    inst = WithHash(name="abc")
    inst.insert()

    # --- Assert ---
    with pytest.raises(ValueError, match="server_managed=True"):
        WithHash.update(
            {"name": "renamed", "content_hash": 42},
            where=WithHash.c.id == inst.id,
        )


def test_update_without_server_managed_keys_works(bound_engine: Any) -> None:
    """Update on non-server_managed columns is unaffected by the new check."""
    inst = WithHash(name="orig")
    inst.insert()
    count = WithHash.update({"name": "updated-name"}, where=WithHash.c.id == inst.id)

    # --- Assert ---
    assert count == 1
    refetched = WithHash.load_one(where=WithHash.c.id == inst.id)
    assert refetched is not None
    assert refetched.name == "updated-name"


def test_returning_populates_server_managed_after_db_side_write(bound_engine: Any) -> None:
    """After insert() + a simulated DB-side write of content_hash, a subsequent
    load_one reads the DB value back through the model — confirming the
    column is part of the SELECT path (only excluded from writes)."""
    inst = WithHash(name="abcdef")
    inst.insert()
    # Simulate a trigger by writing content_hash directly via raw SQL.
    with bound_engine.begin() as conn:
        conn.execute(
            text("UPDATE smc_with_hash SET content_hash = :h WHERE id = :id"),
            {"h": len("abcdef"), "id": inst.id},
        )

    refetched = WithHash.load_one(where=WithHash.c.id == inst.id)

    # --- Assert ---
    assert refetched is not None
    assert refetched.content_hash == 6


def test_insert_many_excludes_server_managed_column(bound_engine: Any) -> None:
    """insert_many strips server_managed columns from each row."""
    captured: list[str] = []

    from sqlalchemy import event

    @event.listens_for(bound_engine, "before_cursor_execute")
    def _capture(conn: Any, _cur: Any, statement: str, *_args: Any, **_kwargs: Any) -> None:
        captured.append(statement)

    WithHash.insert_many(
        objects=[
            WithHash(name="a", content_hash=999),
            WithHash(name="abcd", content_hash=999),
        ],
    )

    # --- Assert ---
    insert_stmts = [s for s in captured if "INSERT INTO smc_with_hash" in s]
    assert insert_stmts, "No INSERT captured for insert_many"
    for stmt in insert_stmts:
        assert "content_hash" not in stmt
