"""Tests for response model inheritance: table=False child of table=True parent.

Response models inherit fields from a DB model but are pure pydantic dataclasses
with no SQLAlchemy table, no convenience methods, and no model registry entry.
The ``exclude`` kwarg allows dropping fields (e.g. ``id``) from the child.
"""

from __future__ import annotations

import pytest

from sqldataclass import Field, Relationship, SQLDataclass

# ---------------------------------------------------------------------------
# Fixtures: parent table model + response models defined per-test
# ---------------------------------------------------------------------------


class RmParent(SQLDataclass, table=True):
    """Parent model with table=True for response model tests."""

    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    length: float = 0.0
    voltage: float = 0.0


class RmResponse(RmParent, table=False):
    """Response model inheriting all fields from parent."""


class RmResponseExcludeId(RmParent, table=False, exclude={"id"}):
    """Response model that drops the id field."""


class RmResponseOverride(RmParent, table=False, exclude={"id"}):
    """Response model that drops id and overrides length type."""

    length: int = 0


class RmResponseExtra(RmParent, table=False):
    """Response model that adds a new field."""

    display_unit: str = "km"


# --- Feature 1: table=False child of table=True parent ---


def test_response_model_inherits_all_fields() -> None:
    """
    A table=False child inherits every field from the table=True parent,
    so it can be constructed with the same kwargs.
    """
    response = RmResponse(id=1, name="line-1", length=3.5, voltage=10.0)
    assert response.name == "line-1"
    assert response.length == 3.5
    assert response.voltage == 10.0
    assert response.id == 1


def test_response_model_has_no_table() -> None:
    """
    The child must not have a SQLAlchemy table — it is a pure data container,
    not a DB-backed model.
    """
    assert not hasattr(RmResponse, "__table__")


def test_response_model_has_no_convenience_methods() -> None:
    """
    Convenience methods (load_all, insert, select, etc.) must not leak
    from the parent into the response model via MRO.
    """
    assert not hasattr(RmResponse, "load_all")
    assert not hasattr(RmResponse, "load_one")
    assert not hasattr(RmResponse, "insert")
    assert not hasattr(RmResponse, "insert_many")
    assert not hasattr(RmResponse, "select")
    assert not hasattr(RmResponse, "update")
    assert not hasattr(RmResponse, "delete")
    assert not hasattr(RmResponse, "c")


def test_response_model_not_marked_as_table() -> None:
    """The __sqldataclass_is_table__ flag must be False on the response model."""
    assert RmResponse.__sqldataclass_is_table__ is False  # type: ignore[attr-defined]  # dynamically set by metaclass


def test_response_model_pydantic_fields_match_parent() -> None:
    """
    The child's pydantic fields should match the parent's fields exactly
    (when no exclude or override is used).
    """
    parent_fields = set(RmParent.__pydantic_fields__)
    child_fields = set(RmResponse.__pydantic_fields__)
    assert child_fields == parent_fields


def test_response_model_pydantic_validation() -> None:
    """Pydantic validation must work on inherited fields."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        RmResponse(name=123)  # type: ignore[arg-type]  # intentional wrong type


def test_response_model_json_schema() -> None:
    """
    The response model must produce a valid JSON schema — this is required
    for FastAPI response_model compatibility.
    """
    from pydantic import TypeAdapter

    adapter = TypeAdapter(RmResponse)
    schema = adapter.json_schema()
    assert "properties" in schema
    assert "name" in schema["properties"]
    assert "length" in schema["properties"]


def test_response_model_can_override_field_type() -> None:
    """
    A child can override a parent field's type annotation. Here ``length``
    changes from float to int.
    """
    response = RmResponseOverride(name="test", length=5, voltage=10.0)
    assert response.length == 5
    assert isinstance(response.length, int)


def test_response_model_can_add_new_fields() -> None:
    """A child can declare fields that don't exist on the parent."""
    response = RmResponseExtra(name="test", length=1.0, voltage=10.0, display_unit="mi")
    assert response.display_unit == "mi"


def test_parent_unaffected_after_response_model_creation() -> None:
    """
    Defining a response model child must not modify the parent class —
    the parent should still have its table and convenience methods.
    """
    assert hasattr(RmParent, "__table__")
    assert hasattr(RmParent, "load_all")
    assert hasattr(RmParent, "insert")
    assert RmParent.__sqldataclass_is_table__ is True  # type: ignore[attr-defined]  # dynamically set by metaclass


# --- Feature 2: exclude ---


def test_exclude_removes_field() -> None:
    """
    ``exclude={"id"}`` must remove the id field entirely — it should not
    appear in __pydantic_fields__ and construction without it must work.
    """
    assert "id" not in RmResponseExcludeId.__pydantic_fields__
    response = RmResponseExcludeId(name="test", length=1.0, voltage=10.0)
    assert response.name == "test"
    assert not hasattr(response, "id")


def test_exclude_multiple_fields() -> None:
    """Multiple fields can be excluded at once."""

    class Multi(RmParent, table=False, exclude={"id", "voltage"}):
        pass

    assert "id" not in Multi.__pydantic_fields__
    assert "voltage" not in Multi.__pydantic_fields__
    assert "name" in Multi.__pydantic_fields__
    assert "length" in Multi.__pydantic_fields__


def test_exclude_with_override() -> None:
    """Exclude and override can be combined in the same response model."""
    assert "id" not in RmResponseOverride.__pydantic_fields__
    response = RmResponseOverride(name="test", length=42, voltage=10.0)
    assert response.length == 42
    assert isinstance(response.length, int)


def test_exclude_nonexistent_field_raises() -> None:
    """Excluding a field that doesn't exist on the parent or child must raise TypeError."""
    with pytest.raises(TypeError, match="exclude contains fields not present"):

        class Bad(RmParent, table=False, exclude={"nonexistent"}):
            pass


def test_exclude_empty_set_is_noop() -> None:
    """An empty exclude set behaves the same as omitting it."""

    class NoExclude(RmParent, table=False, exclude=set()):
        pass

    assert set(NoExclude.__pydantic_fields__) == set(RmParent.__pydantic_fields__)


# --- Edge cases ---


class RmParentWithRel(SQLDataclass, table=True):
    """Parent with a relationship field for edge-case testing."""

    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    team_id: int = Field(default=0, foreign_key="rm_parent.id")
    team: RmParent | None = Relationship()


def test_response_model_excludes_relationship_fields() -> None:
    """
    Relationship fields from the parent are automatically stripped — response
    models are data containers with no table or query machinery.
    """

    class RelResponse(RmParentWithRel, table=False):
        pass

    assert "team" not in RelResponse.__pydantic_fields__
    assert "id" in RelResponse.__pydantic_fields__
    assert "name" in RelResponse.__pydantic_fields__
    assert "team_id" in RelResponse.__pydantic_fields__


def test_chained_response_model() -> None:
    """
    A response model can inherit from another response model (table=False
    from table=False). This goes through the normal pydantic inheritance path.
    """

    # --- Input ---
    class EuResponse(RmParent, table=False, exclude={"id"}):
        pass

    class UsResponse(EuResponse):
        length: int = 0  # override float -> int

    # --- Assert ---
    assert "id" not in UsResponse.__pydantic_fields__
    assert "length" in UsResponse.__pydantic_fields__
    response = UsResponse(name="test", length=5, voltage=10.0)
    assert isinstance(response.length, int)


# --- from_parent ---


def test_from_parent_basic() -> None:
    """
    from_parent constructs a response model from a table=True parent instance,
    keeping only the fields the child has (dropping excluded fields like id).
    """
    parent = RmParent(id=42, name="test", length=12.4, voltage=10.0)

    # --- Execute ---
    response = RmResponseExcludeId.from_parent(parent)

    # --- Assert ---
    assert response.name == "test"
    assert response.length == 12.4
    assert response.voltage == 10.0
    assert not hasattr(response, "id")


def test_from_parent_with_overrides() -> None:
    """
    Keyword overrides replace specific field values — used for unit conversion
    (e.g. converting km to miles before constructing the US response).
    """
    parent = RmParent(id=1, name="cable", length=10.0, voltage=20.0)

    # --- Execute ---
    response = RmResponseOverride.from_parent(parent, length=6)

    # --- Assert ---
    assert response.length == 6
    assert isinstance(response.length, int)
    assert response.name == "cable"
