"""Tests for ``SQLDataclass.insert_if_absent`` (ON CONFLICT DO NOTHING)."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from sqldataclass import Field, SQLDataclass


class Catalog(SQLDataclass, table=True):
    __tablename__ = "catalog_iia"
    id: int | None = Field(default=None, primary_key=True)
    code: str = Field(unique=True)
    label: str


@pytest.fixture
def conn() -> Generator[Connection]:
    engine: Engine = create_engine("sqlite:///:memory:")
    Catalog.__table__.create(engine, checkfirst=True)
    with engine.begin() as connection:
        yield connection


def test_insert_if_absent_inserts_when_no_conflict(conn: Connection) -> None:
    """No row matches *conflict_columns*, so the INSERT proceeds — the method
    returns True and RETURNING hydrates the autoincrement PK onto the instance.
    """
    # --- Input ---
    item = Catalog(code="A1", label="alpha")
    assert item.id is None

    # --- Execute ---
    inserted = item.insert_if_absent(conn, conflict_columns=["code"])

    # --- Assert ---
    assert inserted is True
    assert item.id is not None  # PK hydrated via RETURNING

    row = conn.execute(
        Catalog.__table__.select().where(Catalog.__table__.c.code == "A1"),
    ).fetchone()
    assert row is not None
    assert row.label == "alpha"


def test_insert_if_absent_returns_false_on_conflict(conn: Connection) -> None:
    """A row already exists at the conflict target, so ON CONFLICT DO NOTHING
    skips the insert: the method returns False, the duplicate instance stays
    un-hydrated (no RETURNING row), and the existing row is untouched.
    """
    # --- Input: pre-existing row at code A1 ---
    Catalog(code="A1", label="alpha").insert_if_absent(
        conn,
        conflict_columns=["code"],
    )

    # --- Execute: try to insert another row with the same code ---
    duplicate = Catalog(code="A1", label="beta")
    inserted = duplicate.insert_if_absent(conn, conflict_columns=["code"])

    # --- Assert ---
    assert inserted is False
    assert duplicate.id is None  # not hydrated because nothing was returned

    # The existing row is untouched (label still "alpha").
    rows = conn.execute(
        Catalog.__table__.select().where(Catalog.__table__.c.code == "A1"),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0].label == "alpha"


def test_insert_if_absent_two_distinct_rows_both_inserted(conn: Connection) -> None:
    """Two rows whose conflict-column values differ do not conflict with each
    other — both insert successfully and receive distinct DB-generated PKs.
    """
    # --- Input ---
    first = Catalog(code="A1", label="alpha")
    second = Catalog(code="B2", label="beta")

    # --- Execute ---
    first_inserted = first.insert_if_absent(conn, conflict_columns=["code"])
    second_inserted = second.insert_if_absent(conn, conflict_columns=["code"])

    # --- Assert ---
    assert first_inserted is True
    assert second_inserted is True
    assert first.id != second.id

    count = conn.execute(Catalog.__table__.select()).fetchall()
    assert len(count) == 2
