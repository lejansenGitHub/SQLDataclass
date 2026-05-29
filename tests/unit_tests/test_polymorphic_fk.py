"""Tests for #5: ``Relationship(polymorphic_fks=...)`` — N:1 polymorphic reference.

The canonical shape: a parent table with several mutually-exclusive FK columns
pointing at independent catalog tables, plus a discriminator column on the
parent that selects which FK is active. Reads JOIN each variant via its own FK;
writes route the related object into the correct FK based on the discriminator.

Unlike the existing shared-PK discriminated-union pattern (which models
ownership: variant.id IS parent.id), polymorphic-FK models *reference* (N:1):
many parents can share one catalog row, and each variant has its own PK and
lifecycle.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import create_engine

import sqldataclass.model as _model
from sqldataclass import Field, Relationship, SQLDataclass


class CatalogA(SQLDataclass, table=True):
    __tablename__ = "polyfk_test_a"
    id: int | None = Field(default=None, primary_key=True)
    label_a: str


class CatalogB(SQLDataclass, table=True):
    __tablename__ = "polyfk_test_b"
    id: int | None = Field(default=None, primary_key=True)
    label_b: str


class Item(SQLDataclass, table=True):
    __tablename__ = "polyfk_test_item"
    id: int | None = Field(default=None, primary_key=True)
    category: str  # discriminator
    a_id: int | None = Field(default=None, foreign_key="polyfk_test_a.id")
    b_id: int | None = Field(default=None, foreign_key="polyfk_test_b.id")
    variant: CatalogA | CatalogB | None = Relationship(
        discriminator="category",
        polymorphic_fks={
            "A": ("a_id", CatalogA),
            "B": ("b_id", CatalogB),
        },
    )


@pytest.fixture
def bound_engine() -> Any:
    engine = create_engine("sqlite:///:memory:")
    for cls in (CatalogA, CatalogB, Item):
        cls.__table__.create(engine, checkfirst=True)
    SQLDataclass.bind(engine)
    yield engine
    _model._BOUND_ENGINE = None


# ---------------------------------------------------------------------------
# Metadata / class construction
# ---------------------------------------------------------------------------


def test_relationship_is_registered_as_polymorphic_fk() -> None:
    """The relationship resolver records kind='polymorphic_fk' and stores the mapping."""
    rel = Item.__relationships__["variant"]

    # --- Assert ---
    assert rel.kind == "polymorphic_fk"
    assert CatalogA in rel.target_types
    assert CatalogB in rel.target_types
    assert rel.polymorphic_fks == {
        "A": ("a_id", CatalogA),
        "B": ("b_id", CatalogB),
    }


def test_no_auto_fk_column_injected_for_polymorphic_relationship() -> None:
    """SD does not invent a `variant_id` column; only the user-declared FKs exist."""
    column_names = [c.name for c in Item.__table__.columns]

    # --- Assert ---
    assert "variant_id" not in column_names
    assert "a_id" in column_names
    assert "b_id" in column_names


def test_polymorphic_fks_without_discriminator_raises() -> None:
    """polymorphic_fks requires a discriminator column to choose the variant."""
    # --- Assert ---
    with pytest.raises(TypeError, match="polymorphic_fks requires a discriminator"):

        class _Bad(SQLDataclass, table=True):  # ad-hoc test class
            __tablename__ = "polyfk_test_bad_no_disc"
            id: int | None = Field(default=None, primary_key=True)
            a_id: int | None = Field(default=None, foreign_key="polyfk_test_a.id")
            b_id: int | None = Field(default=None, foreign_key="polyfk_test_b.id")
            variant: CatalogA | CatalogB | None = Relationship(
                polymorphic_fks={
                    "A": ("a_id", CatalogA),
                    "B": ("b_id", CatalogB),
                },
            )


def test_polymorphic_fks_unknown_local_column_raises() -> None:
    """A tuple naming a non-existent local column raises at class construction."""
    # --- Assert ---
    with pytest.raises(TypeError, match="not declared on the model"):

        class _Bad(SQLDataclass, table=True):  # ad-hoc test class
            __tablename__ = "polyfk_test_bad_unknown_col"
            id: int | None = Field(default=None, primary_key=True)
            category: str
            a_id: int | None = Field(default=None, foreign_key="polyfk_test_a.id")
            variant: CatalogA | CatalogB | None = Relationship(
                discriminator="category",
                polymorphic_fks={
                    "A": ("a_id", CatalogA),
                    "B": ("nonexistent_col", CatalogB),
                },
            )


# ---------------------------------------------------------------------------
# Reads — JOIN-based hydration picks the active variant
# ---------------------------------------------------------------------------


def test_load_hydrates_variant_a_when_category_matches(bound_engine: Any) -> None:
    """When category='A', the loaded Item.variant is a CatalogA instance."""
    a = CatalogA(label_a="alpha")
    a.insert()
    Item(category="A", a_id=a.id).insert()

    items = Item.load_all()

    # --- Assert ---
    assert len(items) == 1
    assert isinstance(items[0].variant, CatalogA)
    assert items[0].variant.label_a == "alpha"


def test_load_hydrates_variant_b_when_category_matches(bound_engine: Any) -> None:
    """When category='B', the loaded Item.variant is a CatalogB instance."""
    b = CatalogB(label_b="beta")
    b.insert()
    Item(category="B", b_id=b.id).insert()

    items = Item.load_all()

    # --- Assert ---
    assert len(items) == 1
    assert isinstance(items[0].variant, CatalogB)
    assert items[0].variant.label_b == "beta"


def test_load_returns_mixed_variants_in_one_query(bound_engine: Any) -> None:
    """A single load_all hydrates each row to its discriminator-selected variant."""
    a = CatalogA(label_a="alpha")
    a.insert()
    b = CatalogB(label_b="beta")
    b.insert()
    Item(category="A", a_id=a.id).insert()
    Item(category="B", b_id=b.id).insert()

    items = Item.load_all(order_by=Item.c.id)

    # --- Assert ---
    assert isinstance(items[0].variant, CatalogA)
    assert isinstance(items[1].variant, CatalogB)


def test_load_one_picks_correct_variant(bound_engine: Any) -> None:
    """load_one also dispatches via the discriminator."""
    a = CatalogA(label_a="alpha")
    a.insert()
    Item(id=42, category="A", a_id=a.id).insert()

    item = Item.load_one(where=Item.c.id == 42)

    # --- Assert ---
    assert item is not None
    assert isinstance(item.variant, CatalogA)
    assert item.variant.label_a == "alpha"


def test_variant_is_none_when_no_fk_is_set(bound_engine: Any) -> None:
    """If the discriminator points to a variant whose FK is NULL, variant=None."""
    # category names variant A but a_id is None — the LEFT JOIN produces no row,
    # so the variant is left None.
    Item(id=1, category="A", a_id=None).insert()

    items = Item.load_all()

    # --- Assert ---
    assert items[0].variant is None


def test_variant_is_none_when_discriminator_value_is_unknown(bound_engine: Any) -> None:
    """A discriminator value not in the mapping yields variant=None (no crash)."""
    Item(id=1, category="UNKNOWN_VARIANT").insert()

    items = Item.load_all()

    # --- Assert ---
    assert items[0].variant is None


# ---------------------------------------------------------------------------
# Writes — insert routes the related object to the correct FK
# ---------------------------------------------------------------------------


def test_insert_with_variant_a_cascades_and_links(bound_engine: Any) -> None:
    """Setting Item.variant to a CatalogA + category='A' cascade-inserts the
    catalog row and copies its PK into a_id."""
    a = CatalogA(label_a="cascade")
    item = Item(category="A", variant=a)
    item.insert()

    # --- Assert ---
    assert a.id is not None
    assert item.a_id == a.id
    assert item.b_id is None


def test_insert_nulls_inactive_fk_columns(bound_engine: Any) -> None:
    """Even if the caller pre-set the wrong-side FK, insert nulls it."""
    b = CatalogB(label_b="cascade-b")
    b.insert()
    a = CatalogA(label_a="cascade-a")
    item = Item(category="A", variant=a, b_id=b.id)  # b_id set but variant is A
    item.insert()

    # --- Assert ---
    assert item.a_id == a.id
    assert item.b_id is None  # nulled because variant kind is A


def test_insert_with_mismatched_variant_type_raises(bound_engine: Any) -> None:
    """A variant value whose type doesn't match the discriminator is rejected."""
    b = CatalogB(label_b="wrong")
    item = Item(category="A", variant=b)  # category says A, variant is CatalogB

    # --- Assert ---
    with pytest.raises(ValueError, match="expects CatalogA, got CatalogB"):
        item.insert()


def test_insert_with_unknown_category_raises(bound_engine: Any) -> None:
    """A discriminator value not in the mapping fails the insert with a clear error."""
    a = CatalogA(label_a="a")
    item = Item(category="UNKNOWN", variant=a)

    # --- Assert ---
    with pytest.raises(ValueError, match="no entry in polymorphic_fks"):
        item.insert()


def test_insert_with_pre_persisted_variant_does_not_re_insert(bound_engine: Any) -> None:
    """If the variant already has a PK, insert reuses it instead of cascading."""
    a = CatalogA(label_a="pre")
    a.insert()
    original_id = a.id
    item = Item(category="A", variant=a)
    item.insert()

    # --- Assert ---
    assert a.id == original_id  # PK unchanged
    assert item.a_id == original_id
    # And only one CatalogA row exists.
    catalogs = CatalogA.load_all()
    assert len(catalogs) == 1


def test_insert_without_variant_succeeds(bound_engine: Any) -> None:
    """An Item with no variant attached can still be inserted; both FKs stay None."""
    Item(category="A").insert()

    items = Item.load_all()

    # --- Assert ---
    assert len(items) == 1
    assert items[0].a_id is None
    assert items[0].b_id is None
    assert items[0].variant is None


def test_update_rejects_relationship_key_with_clear_error(bound_engine: Any) -> None:
    """Model.update({"variant": ...}) raises ValueError instead of producing an
    opaque CompileError. Routing a relationship through UPDATE doesn't have a
    safe semantic — the caller must update the FK columns + discriminator
    directly, OR fetch and re-insert."""
    a = CatalogA(label_a="orig")
    a.insert()
    Item(id=1, category="A", a_id=a.id).insert()

    new_a = CatalogA(label_a="new")
    new_a.insert()

    # --- Assert ---
    with pytest.raises(ValueError, match="does not support relationship keys"):
        Item.update({"variant": new_a}, where=Item.c.id == 1)


class Tag(SQLDataclass, table=True):
    __tablename__ = "polyfk_test_tag"
    id: int | None = Field(default=None, primary_key=True)
    label: str


class Bookmark(SQLDataclass, table=True):
    """Two polymorphic FKs to the SAME target table — primary vs backup tag."""

    __tablename__ = "polyfk_test_bookmark"
    id: int | None = Field(default=None, primary_key=True)
    role: str  # discriminator
    primary_tag_id: int | None = Field(default=None, foreign_key="polyfk_test_tag.id")
    backup_tag_id: int | None = Field(default=None, foreign_key="polyfk_test_tag.id")
    tag: Tag | None = Relationship(
        discriminator="role",
        polymorphic_fks={
            "primary": ("primary_tag_id", Tag),
            "backup": ("backup_tag_id", Tag),
        },
    )


@pytest.fixture
def bookmark_engine() -> Any:
    engine = create_engine("sqlite:///:memory:")
    for cls in (Tag, Bookmark):
        cls.__table__.create(engine, checkfirst=True)
    SQLDataclass.bind(engine)
    yield engine
    _model._BOUND_ENGINE = None


def test_same_target_class_via_two_fks_loads_correctly(bookmark_engine: Any) -> None:
    """Two polymorphic_fks entries pointing at the same target class join via
    SA aliases — no 'ambiguous column' error, each variant resolves to the
    right row."""
    primary = Tag(label="prim")
    primary.insert()
    backup = Tag(label="back")
    backup.insert()
    Bookmark(
        id=1,
        role="primary",
        primary_tag_id=primary.id,
        backup_tag_id=backup.id,
    ).insert()
    Bookmark(
        id=2,
        role="backup",
        primary_tag_id=primary.id,
        backup_tag_id=backup.id,
    ).insert()

    bookmarks = Bookmark.load_all(order_by=Bookmark.c.id)

    # --- Assert ---
    assert bookmarks[0].tag is not None
    assert bookmarks[0].tag.label == "prim"
    assert bookmarks[1].tag is not None
    assert bookmarks[1].tag.label == "back"


def test_update_with_fk_columns_directly_works(bound_engine: Any) -> None:
    """The recommended path: update the FK and discriminator columns directly."""
    a = CatalogA(label_a="alpha")
    a.insert()
    b = CatalogB(label_b="beta")
    b.insert()
    Item(id=1, category="A", a_id=a.id).insert()

    # Move from variant A to variant B by updating columns directly.
    Item.update(
        {"category": "B", "a_id": None, "b_id": b.id},
        where=Item.c.id == 1,
    )
    item = Item.load_one(where=Item.c.id == 1)

    # --- Assert ---
    assert item is not None
    assert isinstance(item.variant, CatalogB)
    assert item.variant.label_b == "beta"
