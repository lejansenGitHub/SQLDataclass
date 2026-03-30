"""Unit tests for SQLModel (Pydantic BaseModel support)."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from sqldataclass import Field, SQLDataclass, SQLModel

# ---------------------------------------------------------------------------
# Basic SQLModel creation
# ---------------------------------------------------------------------------


class TestSQLModelCreation:
    """Test that SQLModel subclasses are valid Pydantic BaseModels."""

    def test_pure_model_is_basemodel(self) -> None:
        class PlayerCreate(SQLModel):
            name: str
            age: int = 0

        assert issubclass(PlayerCreate, BaseModel)
        p = PlayerCreate(name="Alice", age=25)
        assert p.name == "Alice"
        assert p.age == 25

    def test_pure_model_has_no_table(self) -> None:
        class PlayerCreate(SQLModel):
            name: str

        assert getattr(PlayerCreate, "__sqldataclass_is_table__", False) is False
        assert not hasattr(PlayerCreate, "__table__")

    def test_table_model_has_table(self) -> None:
        class UnitPlayer(SQLModel, table=True):
            __tablename__ = "unit_player_bm"
            id: int | None = Field(default=None, primary_key=True)
            name: str = ""

        assert UnitPlayer.__sqldataclass_is_table__ is True
        assert hasattr(UnitPlayer, "__table__")
        assert UnitPlayer.__tablename__ == "unit_player_bm"
        col_names = {c.name for c in UnitPlayer.__table__.columns}
        assert "id" in col_names
        assert "name" in col_names

    def test_table_model_has_column_shorthand(self) -> None:
        class UnitItem(SQLModel, table=True):
            __tablename__ = "unit_item_bm"
            id: int | None = Field(default=None, primary_key=True)
            label: str = ""

        assert hasattr(UnitItem, "c")
        assert hasattr(UnitItem.c, "id")
        assert hasattr(UnitItem.c, "label")

    def test_is_basemodel_marker(self) -> None:
        class Marker(SQLModel):
            x: int = 0

        assert getattr(Marker, "__sqlmodel_is_basemodel__", False) is True

    def test_model_dump_works(self) -> None:
        class DumpModel(SQLModel):
            name: str
            score: float = 0.0

        m = DumpModel(name="test", score=9.5)
        d = m.model_dump()
        assert d == {"name": "test", "score": 9.5}

    def test_model_validate_works(self) -> None:
        class ValidateModel(SQLModel):
            name: str
            age: int = 0

        m = ValidateModel.model_validate({"name": "Bob", "age": 30})
        assert m.name == "Bob"
        assert m.age == 30


# ---------------------------------------------------------------------------
# Field() support
# ---------------------------------------------------------------------------


class TestSQLModelFields:
    """Test that Field() works correctly on SQLModel."""

    def test_field_with_sa_params(self) -> None:
        class FieldModel(SQLModel, table=True):
            __tablename__ = "unit_field_bm"
            id: int | None = Field(default=None, primary_key=True)
            code: str = Field(default="", unique=True, index=True)

        col = FieldModel.__table__.c.code
        assert col.unique is True
        assert col.index is True

    def test_non_column_field(self) -> None:
        class NcModel(SQLModel, table=True):
            __tablename__ = "unit_nc_bm"
            id: int | None = Field(default=None, primary_key=True)
            name: str = ""
            computed: str = Field(default="", column=False)

        assert "computed" in NcModel.__non_column_fields__
        col_names = {c.name for c in NcModel.__table__.columns}
        assert "computed" not in col_names
        assert "name" in col_names


# ---------------------------------------------------------------------------
# Inheritance separation
# ---------------------------------------------------------------------------


class TestInheritanceSeparation:
    """SQLModel and SQLDataclass must not cross-inherit."""

    def test_sqlmodel_cannot_inherit_sqldataclass(self) -> None:
        class DcBase(SQLDataclass, table=True):
            __tablename__ = "dc_base_guard"
            id: int | None = Field(default=None, primary_key=True)

        with pytest.raises(TypeError):

            class Bad(SQLModel, DcBase):  # type: ignore[metaclass]
                pass

    def test_sqldataclass_cannot_inherit_sqlmodel(self) -> None:
        class MBase(SQLModel):
            name: str = ""

        with pytest.raises(TypeError):

            class Bad(SQLDataclass, MBase, table=True):  # type: ignore[metaclass]
                __tablename__ = "bad_cross"
                id: int | None = Field(default=None, primary_key=True)


# ---------------------------------------------------------------------------
# Composition
# ---------------------------------------------------------------------------


class TestComposition:
    """SQLDataclass instances can be fields of SQLModel (and vice versa)."""

    def test_sqldataclass_as_sqlmodel_field(self) -> None:
        class DcHero(SQLDataclass):
            name: str = ""
            age: int = 0

        class Response(SQLModel):
            hero: DcHero
            status: str = "ok"

        h = DcHero(name="Spider-Man", age=25)
        r = Response(hero=h, status="success")
        assert r.hero.name == "Spider-Man"
        assert r.status == "success"

    def test_sqlmodel_as_sqlmodel_field(self) -> None:
        class Inner(SQLModel):
            value: int = 0

        class Outer(SQLModel):
            inner: Inner
            label: str = ""

        o = Outer(inner=Inner(value=42), label="test")
        assert o.inner.value == 42
