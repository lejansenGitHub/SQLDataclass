"""Tests for ``Relationship(join_type=JoinType.INNER)``.

Many-to-one relationships default to ``LEFT OUTER JOIN`` so optional
relationships (``Foo | None``) remain expressible. When the FK column is
NOT NULL and a FK constraint guarantees a match, callers can request
``INNER JOIN`` for stricter semantics — a row with no matching target is
excluded entirely.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import create_engine

import sqldataclass.model as _model
from sqldataclass import Field, JoinType, Relationship, SQLDataclass


class Voltage(SQLDataclass, table=True):
    __tablename__ = "join_test_voltage"
    id: int | None = Field(default=None, primary_key=True)
    label: str


class Cable(SQLDataclass, table=True):
    __tablename__ = "join_test_cable"
    id: int | None = Field(default=None, primary_key=True)
    voltage_id: int = Field(foreign_key="join_test_voltage.id")
    voltage: Voltage = Relationship(
        foreign_key="voltage_id",
        join_type=JoinType.INNER,
    )


class CableOuter(SQLDataclass, table=True):
    """Same shape as Cable but with the default LEFT OUTER JOIN."""

    __tablename__ = "join_test_cable_outer"
    id: int | None = Field(default=None, primary_key=True)
    voltage_id: int | None = Field(default=None, foreign_key="join_test_voltage.id")
    voltage: Voltage | None = Relationship(foreign_key="voltage_id")


@pytest.fixture
def bound_engine() -> Any:
    engine = create_engine("sqlite:///:memory:")
    for cls in (Voltage, Cable, CableOuter):
        cls.__table__.create(engine, checkfirst=True)
    SQLDataclass.bind(engine)
    yield engine
    _model._BOUND_ENGINE = None


def test_inner_join_relationship_emits_inner_join_sql(bound_engine: Any) -> None:
    """The compiled SELECT must use INNER JOIN, not LEFT OUTER JOIN."""
    voltage = Voltage(label="20kV")
    voltage.insert()
    Cable(voltage_id=voltage.id).insert()

    captured: list[str] = []
    from sqlalchemy import event

    @event.listens_for(bound_engine, "before_cursor_execute")
    def _capture(_c: Any, _cur: Any, statement: str, *_a: Any, **_kw: Any) -> None:
        captured.append(statement)

    Cable.load_all()

    # --- Assert ---
    select_statements = [s for s in captured if "FROM join_test_cable" in s and "SELECT" in s]
    assert select_statements, f"No SELECT captured. Captured: {captured}"
    join_sql = select_statements[-1]
    assert " JOIN join_test_voltage" in join_sql, join_sql
    assert "LEFT OUTER JOIN" not in join_sql, join_sql


def test_outer_join_default_emits_left_outer_join_sql(bound_engine: Any) -> None:
    """The default behavior (no join_type passed) stays LEFT OUTER JOIN."""
    voltage = Voltage(label="20kV")
    voltage.insert()
    CableOuter(voltage_id=voltage.id).insert()

    captured: list[str] = []
    from sqlalchemy import event

    @event.listens_for(bound_engine, "before_cursor_execute")
    def _capture(_c: Any, _cur: Any, statement: str, *_a: Any, **_kw: Any) -> None:
        captured.append(statement)

    CableOuter.load_all()

    # --- Assert ---
    select_statements = [s for s in captured if "FROM join_test_cable_outer" in s]
    assert select_statements, f"No SELECT captured. Captured: {captured}"
    join_sql = select_statements[-1]
    assert "LEFT OUTER JOIN join_test_voltage" in join_sql, join_sql


def test_inner_join_excludes_rows_without_matching_target(bound_engine: Any) -> None:
    """If the FK points at a missing target row, INNER JOIN drops the parent.

    Simulated by inserting a Cable, deleting the linked Voltage out from under
    it (bypassing FK enforcement which SQLite doesn't enforce by default).
    """
    voltage = Voltage(label="20kV")
    voltage.insert()
    Cable(voltage_id=voltage.id).insert()
    # Orphan the Cable by deleting Voltage out from under it.
    with bound_engine.begin() as conn:
        from sqlalchemy import text

        conn.execute(text("DELETE FROM join_test_voltage WHERE id = :id"), {"id": voltage.id})

    rows = Cable.load_all()

    # --- Assert ---
    assert rows == []  # INNER JOIN drops orphaned Cable


def test_outer_join_keeps_rows_without_matching_target(bound_engine: Any) -> None:
    """LEFT OUTER JOIN returns the parent row with variant=None when the
    target is missing."""
    voltage = Voltage(label="20kV")
    voltage.insert()
    CableOuter(voltage_id=voltage.id).insert()
    with bound_engine.begin() as conn:
        from sqlalchemy import text

        conn.execute(text("DELETE FROM join_test_voltage WHERE id = :id"), {"id": voltage.id})

    rows = CableOuter.load_all()

    # --- Assert ---
    assert len(rows) == 1
    assert rows[0].voltage is None


def test_invalid_join_type_value_rejected_at_class_construction() -> None:
    """A non-JoinType value passed to Relationship raises TypeError."""
    # --- Assert ---
    with pytest.raises(TypeError, match="join_type must be a JoinType"):
        Relationship(foreign_key="voltage_id", join_type="left")  # type: ignore[arg-type]  # deliberate misuse


def test_resolved_relationship_records_join_type() -> None:
    """The relationship metadata exposes the configured JoinType."""
    # --- Assert ---
    assert Cable.__relationships__["voltage"].join_type is JoinType.INNER
    assert CableOuter.__relationships__["voltage"].join_type is JoinType.OUTER
