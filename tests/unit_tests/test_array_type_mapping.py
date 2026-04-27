"""Tests for automatic list[T] → ARRAY(T) type mapping."""

from __future__ import annotations

import pytest
from sqlalchemy import ARRAY, Float, Integer, String

from sqldataclass import Field, SQLDataclass
from sqldataclass.model import _python_type_to_sa

# ---------------------------------------------------------------------------
# _python_type_to_sa unit tests
# ---------------------------------------------------------------------------


def test_list_float_maps_to_array_float() -> None:
    """list[float] maps to ARRAY(Float)."""
    result = _python_type_to_sa(list[float])

    # --- Assert ---
    assert isinstance(result, ARRAY)
    assert isinstance(result.item_type, Float)


def test_list_int_maps_to_array_integer() -> None:
    """list[int] maps to ARRAY(Integer)."""
    result = _python_type_to_sa(list[int])

    # --- Assert ---
    assert isinstance(result, ARRAY)
    assert isinstance(result.item_type, Integer)


def test_list_str_maps_to_array_string() -> None:
    """list[str] maps to ARRAY(String)."""
    result = _python_type_to_sa(list[str])

    # --- Assert ---
    assert isinstance(result, ARRAY)
    assert isinstance(result.item_type, String)


def test_optional_list_float_maps_to_array_float() -> None:
    """list[float] | None unwraps to ARRAY(Float)."""
    result = _python_type_to_sa(list[float] | None)

    # --- Assert ---
    assert isinstance(result, ARRAY)
    assert isinstance(result.item_type, Float)


def test_list_unsupported_element_type_raises() -> None:
    """list[T] where T is not in _TYPE_MAP raises TypeError."""
    with pytest.raises(TypeError, match="ARRAY"):
        _python_type_to_sa(list[complex])


# ---------------------------------------------------------------------------
# Model-level column creation
# ---------------------------------------------------------------------------


def test_model_list_float_creates_array_column() -> None:
    """A list[float] field on a model produces an ARRAY(Float) column."""

    class Measurement(SQLDataclass, table=True):
        __tablename__ = "measurements_array_test"
        id: int = Field(primary_key=True)
        values: list[float] = Field(default_factory=list)

    column = Measurement.__table__.c["values"]

    # --- Assert ---
    assert isinstance(column.type, ARRAY)
    assert isinstance(column.type.item_type, Float)


def test_model_optional_list_int_creates_nullable_array_column() -> None:
    """A list[int] | None field produces a nullable ARRAY(Integer) column."""

    class Sensor(SQLDataclass, table=True):
        __tablename__ = "sensors_array_test"
        id: int = Field(primary_key=True)
        readings: list[int] | None = None

    column = Sensor.__table__.c["readings"]

    # --- Assert ---
    assert isinstance(column.type, ARRAY)
    assert isinstance(column.type.item_type, Integer)
    assert column.nullable is True


def test_model_list_str_creates_array_column() -> None:
    """A list[str] field produces an ARRAY(String) column."""

    class TaggedItem(SQLDataclass, table=True):
        __tablename__ = "tagged_items_array_test"
        id: int = Field(primary_key=True)
        tags: list[str] = Field(default_factory=list)

    column = TaggedItem.__table__.c["tags"]

    # --- Assert ---
    assert isinstance(column.type, ARRAY)
    assert isinstance(column.type.item_type, String)


def test_explicit_sa_type_overrides_auto_mapping() -> None:
    """Field(sa_type=ARRAY(Float, dimensions=2)) takes precedence over auto-mapping."""

    class Matrix(SQLDataclass, table=True):
        __tablename__ = "matrices_array_test"
        id: int = Field(primary_key=True)
        grid: list[list[float]] = Field(default_factory=list, sa_type=ARRAY(Float, dimensions=2))

    column = Matrix.__table__.c["grid"]

    # --- Assert ---
    assert isinstance(column.type, ARRAY)
    assert column.type.dimensions == 2


def test_flatten_for_table_preserves_list_values() -> None:
    """flatten_for_table must not drop list values that are ARRAY columns (regression)."""
    from sqldataclass.write import flatten_for_table

    class Sample(SQLDataclass, table=True):
        __tablename__ = "samples_flatten_test"
        id: int = Field(primary_key=True)
        readings: list[float] = Field(default_factory=list)

    instance = Sample(id=1, readings=[1.0, 2.5, 3.7])
    flat = flatten_for_table(instance)

    # --- Assert ---
    assert flat["readings"] == [1.0, 2.5, 3.7]


def test_bare_list_raises_type_error() -> None:
    """Bare list without type args raises TypeError."""
    with pytest.raises(TypeError, match="Cannot map Python type"):
        _python_type_to_sa(list)


def test_list_bool_maps_to_array_boolean() -> None:
    """list[bool] maps to ARRAY(Boolean)."""
    from sqlalchemy import Boolean

    result = _python_type_to_sa(list[bool])

    # --- Assert ---
    assert isinstance(result, ARRAY)
    assert isinstance(result.item_type, Boolean)
