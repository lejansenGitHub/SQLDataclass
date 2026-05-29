"""Tests for B5: Field(column=False) no longer requires a default.

Pydantic still enforces required-at-construction for column=False fields
without a default; the SD-side TypeError has been removed.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from sqldataclass import Field, SQLDataclass


def test_column_false_without_default_is_allowed_at_class_construction() -> None:
    """Defining a column=False field without a default does not raise."""

    class WithRequiredNonColumn(SQLDataclass, table=True):
        __tablename__ = "with_required_non_column_a"
        id: int = Field(primary_key=True)
        hydrated_externally: float = Field(column=False)

    # --- Assert ---
    # Field is on the model, not on the table.
    assert "hydrated_externally" in WithRequiredNonColumn.__non_column_fields__
    assert "hydrated_externally" not in [c.name for c in WithRequiredNonColumn.__table__.columns]


def test_column_false_with_value_constructs_successfully() -> None:
    """Passing the column=False field at construction works."""

    class WithRequiredNonColumn(SQLDataclass, table=True):
        __tablename__ = "with_required_non_column_b"
        id: int = Field(primary_key=True)
        hydrated_externally: float = Field(column=False)

    instance = WithRequiredNonColumn(id=1, hydrated_externally=42.0)

    # --- Assert ---
    assert instance.hydrated_externally == 42.0


def test_column_false_without_value_raises_pydantic_validation_error() -> None:
    """Omitting the required column=False field raises a ValidationError, not TypeError."""

    class WithRequiredNonColumn(SQLDataclass, table=True):
        __tablename__ = "with_required_non_column_c"
        id: int = Field(primary_key=True)
        hydrated_externally: float = Field(column=False)

    # --- Assert ---
    with pytest.raises(ValidationError) as exc_info:
        WithRequiredNonColumn(id=1)
    assert "hydrated_externally" in str(exc_info.value)


def test_column_false_with_default_still_works() -> None:
    """Existing behavior — column=False with an explicit default — is unchanged."""

    class WithDefault(SQLDataclass, table=True):
        __tablename__ = "with_default_non_column"
        id: int = Field(primary_key=True)
        hydrated_externally: float = Field(default=0.0, column=False)

    instance = WithDefault(id=1)

    # --- Assert ---
    assert instance.hydrated_externally == 0.0


def test_column_false_with_default_factory_still_works() -> None:
    """default_factory continues to work for column=False fields."""

    class WithFactory(SQLDataclass, table=True):
        __tablename__ = "with_factory_non_column"
        id: int = Field(primary_key=True)
        tags: list[str] = Field(default_factory=list, column=False)

    instance = WithFactory(id=1)

    # --- Assert ---
    assert instance.tags == []
