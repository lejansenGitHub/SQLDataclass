"""Tests for #2: ``include=`` subset views — inverse of ``exclude=``.

Allowlists are more robust than denylists for response/patch DTOs: when a new
field is added to the parent, an ``exclude``-based view silently leaks it; an
``include``-based view keeps the contract intact.

Semantics:
- ``include={…}`` keeps only the named parent fields; everything else is dropped.
- Child-declared fields are always kept regardless of ``include``.
- ``include`` and ``exclude`` compose; ``exclude`` wins on conflict.
- Unknown field name in ``include`` raises ``TypeError`` at class construction.
"""

from __future__ import annotations

import pytest

from sqldataclass import Field, SQLDataclass


class IncParent(SQLDataclass, table=True):
    __tablename__ = "inc_parent"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    secret: str = ""
    note: str = ""


def test_include_keeps_only_named_parent_fields() -> None:
    """include={a, b} keeps a + b from the parent, drops everything else."""

    class PublicView(IncParent, table=False, include={"id", "name"}):
        pass

    field_names = set(PublicView.__pydantic_fields__)

    # --- Assert ---
    assert field_names == {"id", "name"}


def test_include_keeps_child_declared_fields() -> None:
    """Fields declared on the child are always kept, regardless of include."""

    class WithExtra(IncParent, table=False, include={"id"}):
        display_label: str = ""

    field_names = set(WithExtra.__pydantic_fields__)

    # --- Assert ---
    # id from include, display_label from child body — both present.
    assert field_names == {"id", "display_label"}
    assert "name" not in field_names


def test_include_and_exclude_compose_exclude_wins() -> None:
    """When both are passed, exclude removes overlap from include's result."""

    class Combined(IncParent, table=False, include={"id", "name", "secret"}, exclude={"secret"}):
        pass

    field_names = set(Combined.__pydantic_fields__)

    # --- Assert ---
    assert field_names == {"id", "name"}


def test_include_with_unknown_field_raises() -> None:
    """An include entry that names a non-existent field raises TypeError."""
    # --- Assert ---
    with pytest.raises(TypeError, match="include contains fields not present"):

        class _BadView(IncParent, table=False, include={"nonexistent"}):  # ad-hoc test class
            pass


def test_include_empty_set_keeps_only_child_fields() -> None:
    """include=frozenset() drops every parent field; child fields survive."""

    class EmptyInclude(IncParent, table=False, include=set()):
        custom_only: str = "x"

    field_names = set(EmptyInclude.__pydantic_fields__)

    # --- Assert ---
    assert field_names == {"custom_only"}


def test_exclude_still_works_when_include_not_passed() -> None:
    """Backward compat: omitting include keeps the existing exclude-only behavior."""

    class LegacyView(IncParent, table=False, exclude={"secret"}):
        pass

    field_names = set(LegacyView.__pydantic_fields__)

    # --- Assert ---
    assert "secret" not in field_names
    assert "id" in field_names
    assert "name" in field_names
    assert "note" in field_names


def test_include_view_instances_construct_with_listed_fields() -> None:
    """An include-derived view can be instantiated with the included fields."""

    class TinyView(IncParent, table=False, include={"id", "name"}):
        pass

    inst = TinyView(id=1, name="hello")

    # --- Assert ---
    assert inst.id == 1
    assert inst.name == "hello"
