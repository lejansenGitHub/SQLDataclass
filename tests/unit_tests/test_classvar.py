"""Tests for B3: ClassVar annotations are skipped by the type-to-column mapper.

A ``ClassVar[X]`` declares a class-level constant — not an instance field.
Standard ``dataclasses`` and pydantic both honor this convention; SQLDataclass
must too. Previously, declaring ``_CONST: ClassVar[int] = 5`` on a
``table=True`` class raised ``TypeError: Cannot map Python type
typing.ClassVar[int] to a SQLAlchemy column type``.
"""

from __future__ import annotations

from typing import ClassVar

from sqldataclass import Field, SQLDataclass


def test_classvar_int_on_table_model() -> None:
    """A ClassVar[int] constant on a table=True class is not mapped to a column —
    matches stdlib dataclasses behavior, allowing class-level magic numbers."""

    class WithConstant(SQLDataclass, table=True):
        __tablename__ = "classvar_int"
        _SCHEMA_NUMBER: ClassVar[int] = 7
        id: int = Field(primary_key=True)
        name: str

    column_names = [col.name for col in WithConstant.__table__.columns]

    # --- Assert ---
    assert "_SCHEMA_NUMBER" not in column_names
    assert WithConstant._SCHEMA_NUMBER == 7


def test_classvar_complex_type_on_table_model() -> None:
    """ClassVar[dict[str, int]] is also skipped — any wrapped type qualifies."""

    class WithMap(SQLDataclass, table=True):
        __tablename__ = "classvar_dict"
        _LOOKUP: ClassVar[dict[str, int]] = {"a": 1, "b": 2}
        id: int = Field(primary_key=True)

    column_names = [col.name for col in WithMap.__table__.columns]

    # --- Assert ---
    assert "_LOOKUP" not in column_names
    assert WithMap._LOOKUP == {"a": 1, "b": 2}


def test_classvar_does_not_block_normal_columns() -> None:
    """A ClassVar alongside normal fields doesn't disrupt the rest of the table."""

    class Mixed(SQLDataclass, table=True):
        __tablename__ = "classvar_mixed"
        _TAG: ClassVar[str] = "mixed-class"
        id: int = Field(primary_key=True)
        value: float
        label: str = "default"

    column_names = [col.name for col in Mixed.__table__.columns]

    # --- Assert ---
    assert set(column_names) == {"id", "value", "label"}
    assert Mixed._TAG == "mixed-class"


def test_classvar_constant_is_accessible_on_instance() -> None:
    """The constant is reachable both as a class attribute and via an instance."""

    class Counter(SQLDataclass, table=True):
        __tablename__ = "classvar_instance"
        _MAX_VALUE: ClassVar[int] = 100
        id: int = Field(primary_key=True)
        value: int = 0

    inst = Counter(id=1, value=42)

    # --- Assert ---
    assert Counter._MAX_VALUE == 100
    assert inst._MAX_VALUE == 100  # standard Python class-attribute lookup
