"""Tests for F1 / #6: Relationship(foreign_key="<local_col>") to bind a relationship to an existing FK column.

When the FK column on the model uses a domain-specific name (e.g. ``account_id``)
that doesn't match the auto-injection convention (``{relationship}_id``), the
caller can declare ``Relationship(foreign_key="account_id")`` to point the
relationship at the existing column. SD then skips the implicit FK injection
and uses the named column as the join source.
"""

from __future__ import annotations

import pytest

from sqldataclass import Field, Relationship, SQLDataclass


class _Account(SQLDataclass, table=True):
    __tablename__ = "test_explicit_fk_accounts"
    id: int = Field(primary_key=True)
    name: str


def test_explicit_fk_skips_auto_injection() -> None:
    """Relationship(foreign_key="<local>") does not auto-inject {name}_id."""

    class Parent(SQLDataclass, table=True):
        __tablename__ = "test_explicit_fk_parents_a"
        id: int = Field(primary_key=True)
        owner_id: int = Field(foreign_key="test_explicit_fk_accounts.id")
        owner: _Account | None = Relationship(foreign_key="owner_id")

    column_names = [col.name for col in Parent.__table__.columns]

    # --- Assert ---
    assert "owner_id" in column_names
    assert "owner_id_id" not in column_names  # naive injection that must NOT happen
    # The relationship-named column ("owner_id" already exists, but check that an
    # *additional* "{name}_id" wasn't invented either):
    assert column_names.count("owner_id") == 1


def test_explicit_fk_relationship_resolves() -> None:
    """The relationship is registered and its target type is the related class."""

    class Parent(SQLDataclass, table=True):
        __tablename__ = "test_explicit_fk_parents_b"
        id: int = Field(primary_key=True)
        owner_id: int = Field(foreign_key="test_explicit_fk_accounts.id")
        owner: _Account | None = Relationship(foreign_key="owner_id")

    # --- Assert ---
    assert "owner" in Parent.__relationships__
    rel = Parent.__relationships__["owner"]
    assert _Account in rel.target_types
    assert rel.kind == "many_to_one"


def test_explicit_fk_missing_local_column_raises() -> None:
    """Naming a non-existent local column raises a clear error at class construction."""
    # --- Assert ---
    with pytest.raises(TypeError, match="foreign_key='nonexistent_col'"):

        class _Bad(SQLDataclass, table=True):  # local class to capture construction error
            __tablename__ = "test_explicit_fk_bad"
            id: int = Field(primary_key=True)
            owner: _Account | None = Relationship(foreign_key="nonexistent_col")


def test_default_auto_injection_still_works_for_other_relationships() -> None:
    """A second relationship on the same model without foreign_key= still gets auto-injected."""

    class Parent(SQLDataclass, table=True):
        __tablename__ = "test_explicit_fk_parents_c"
        id: int = Field(primary_key=True)
        owner_id: int = Field(foreign_key="test_explicit_fk_accounts.id")
        owner: _Account | None = Relationship(foreign_key="owner_id")
        # No foreign_key= here — SD auto-injects backup_account_id.
        backup_account: _Account | None = Relationship()

    column_names = [col.name for col in Parent.__table__.columns]

    # --- Assert ---
    assert "owner_id" in column_names
    assert "backup_account_id" in column_names


def test_explicit_fk_nullable_column_propagates() -> None:
    """The relationship works when the local FK column is nullable."""

    class Parent(SQLDataclass, table=True):
        __tablename__ = "test_explicit_fk_parents_d"
        id: int = Field(primary_key=True)
        owner_id: int | None = Field(
            default=None,
            foreign_key="test_explicit_fk_accounts.id",
        )
        owner: _Account | None = Relationship(foreign_key="owner_id")

    owner_col = next(c for c in Parent.__table__.columns if c.name == "owner_id")

    # --- Assert ---
    assert owner_col.nullable is True
    assert "owner" in Parent.__relationships__
