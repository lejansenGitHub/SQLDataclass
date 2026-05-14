"""Tests for the Field(index=<method>) shorthand for non-btree indexes."""

from __future__ import annotations

from sqlalchemy import Index

from sqldataclass import Field, SQLDataclass

# ---------------------------------------------------------------------------
# bool-style index still behaves as before (regression)
# ---------------------------------------------------------------------------


def test_field_index_true_creates_default_btree_index() -> None:
    """Field(index=True) keeps the default behaviour: a single btree index."""

    class WithBtree(SQLDataclass, table=True):
        __tablename__ = "idx_method_btree_true"
        id: int = Field(primary_key=True)
        email: str = Field(default="", index=True)

    indexes = list(WithBtree.__table__.indexes)
    # --- Assert ---
    assert len(indexes) == 1
    idx = indexes[0]
    assert [c.name for c in idx.columns] == ["email"]
    assert not idx.kwargs["postgresql_using"]


def test_field_index_false_emits_no_indexes() -> None:
    """Field(index=False) emits nothing."""

    class WithoutIndex(SQLDataclass, table=True):
        __tablename__ = "idx_method_no_index"
        id: int = Field(primary_key=True)
        email: str = Field(default="")

    # --- Assert ---
    assert list(WithoutIndex.__table__.indexes) == []


# ---------------------------------------------------------------------------
# String-style index methods
# ---------------------------------------------------------------------------


def test_field_index_btree_string_alias() -> None:
    """The string 'btree' behaves the same as index=True (default btree)."""

    class WithBtreeAlias(SQLDataclass, table=True):
        __tablename__ = "idx_method_btree_alias"
        id: int = Field(primary_key=True)
        email: str = Field(default="", index="btree")

    indexes = list(WithBtreeAlias.__table__.indexes)
    # --- Assert ---
    assert len(indexes) == 1
    assert not indexes[0].kwargs["postgresql_using"]


def test_field_index_hash_emits_hash_index() -> None:
    """Field(index='hash') produces an Index with postgresql_using='hash'."""

    class WithHash(SQLDataclass, table=True):
        __tablename__ = "idx_method_hash"
        id: int = Field(primary_key=True)
        token: str = Field(default="", index="hash")

    indexes = list(WithHash.__table__.indexes)
    # --- Assert ---
    assert len(indexes) == 1
    idx = indexes[0]
    assert [c.name for c in idx.columns] == ["token"]
    assert idx.kwargs["postgresql_using"] == "hash"


def test_field_index_gin_emits_gin_index() -> None:
    """Field(index='gin') produces an Index with postgresql_using='gin'."""

    class WithGin(SQLDataclass, table=True):
        __tablename__ = "idx_method_gin"
        id: int = Field(primary_key=True)
        tags: str = Field(default="", index="gin")

    # --- Assert ---
    idx = next(iter(WithGin.__table__.indexes))
    assert idx.kwargs["postgresql_using"] == "gin"


def test_field_index_gist_emits_gist_index() -> None:
    """Field(index='gist') produces an Index with postgresql_using='gist'."""

    class WithGist(SQLDataclass, table=True):
        __tablename__ = "idx_method_gist"
        id: int = Field(primary_key=True)
        geom: str = Field(default="", index="gist")

    # --- Assert ---
    idx = next(iter(WithGist.__table__.indexes))
    assert idx.kwargs["postgresql_using"] == "gist"


def test_field_index_brin_emits_brin_index() -> None:
    """Field(index='brin') produces an Index with postgresql_using='brin'."""

    class WithBrin(SQLDataclass, table=True):
        __tablename__ = "idx_method_brin"
        id: int = Field(primary_key=True)
        ts: str = Field(default="", index="brin")

    # --- Assert ---
    idx = next(iter(WithBrin.__table__.indexes))
    assert idx.kwargs["postgresql_using"] == "brin"


def test_field_index_spgist_emits_spgist_index() -> None:
    """Field(index='spgist') produces an Index with postgresql_using='spgist'."""

    class WithSpgist(SQLDataclass, table=True):
        __tablename__ = "idx_method_spgist"
        id: int = Field(primary_key=True)
        data: str = Field(default="", index="spgist")

    # --- Assert ---
    idx = next(iter(WithSpgist.__table__.indexes))
    assert idx.kwargs["postgresql_using"] == "spgist"


# ---------------------------------------------------------------------------
# Structural invariants
# ---------------------------------------------------------------------------


def test_hash_index_does_not_also_create_default_btree() -> None:
    """Non-btree index must turn off the column-level index flag (no extra btree)."""

    class HashOnly(SQLDataclass, table=True):
        __tablename__ = "idx_method_hash_only"
        id: int = Field(primary_key=True)
        token: str = Field(default="", index="hash")

    indexes = list(HashOnly.__table__.indexes)
    # --- Assert ---
    assert len(indexes) == 1
    assert HashOnly.__table__.c.token.index is False


def test_non_btree_index_name_follows_convention() -> None:
    """Auto-generated non-btree index name is 'ix_<tablename>_<colname>'."""

    class WithName(SQLDataclass, table=True):
        __tablename__ = "idx_method_naming"
        id: int = Field(primary_key=True)
        token: str = Field(default="", index="hash")

    # --- Assert ---
    idx = next(iter(WithName.__table__.indexes))
    assert idx.name == "ix_idx_method_naming_token"


def test_multiple_columns_with_different_index_methods() -> None:
    """Different methods on different columns of the same table coexist."""

    class Mixed(SQLDataclass, table=True):
        __tablename__ = "idx_method_mixed"
        id: int = Field(primary_key=True)
        plain: str = Field(default="", index=True)
        hashed: str = Field(default="", index="hash")
        searched: str = Field(default="", index="gin")

    indexes_by_col: dict[str, Index] = {}
    for idx in Mixed.__table__.indexes:
        for col in idx.columns:
            indexes_by_col[col.name] = idx

    # --- Assert ---
    assert set(indexes_by_col.keys()) == {"plain", "hashed", "searched"}
    assert not indexes_by_col["plain"].kwargs["postgresql_using"]
    assert indexes_by_col["hashed"].kwargs["postgresql_using"] == "hash"
    assert indexes_by_col["searched"].kwargs["postgresql_using"] == "gin"


# ---------------------------------------------------------------------------
# JTI child support
# ---------------------------------------------------------------------------


def test_jti_child_supports_non_btree_index() -> None:
    """A JTI child can declare Field(index='hash') on its child-specific columns."""

    class JtiPerson(SQLDataclass, table=True):
        __tablename__ = "idx_jti_person"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class JtiEmployee(JtiPerson, table=True):
        __tablename__ = "idx_jti_employee"
        badge: str = Field(default="", index="hash")

    indexes = list(JtiEmployee.__table__.indexes)
    # --- Assert ---
    assert len(indexes) == 1
    idx = indexes[0]
    assert [c.name for c in idx.columns] == ["badge"]
    assert idx.kwargs["postgresql_using"] == "hash"
    assert idx.name == "ix_idx_jti_employee_badge"
