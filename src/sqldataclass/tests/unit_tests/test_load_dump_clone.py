"""Unit tests for load(), dump(), clone() and column=False field scoping."""

from __future__ import annotations

import pytest

from sqldataclass import Field, SQLDataclass, SQLModel

# ---------------------------------------------------------------------------
# SQLDataclass tests
# ---------------------------------------------------------------------------


class TestSQLDataclassDump:
    """dump() on SQLDataclass pydantic dataclasses."""

    def test_basic_dump(self) -> None:
        class Hero(SQLDataclass):
            name: str
            age: int = 0

        hero = Hero(name="Alice", age=25)
        d = hero.dump()
        assert d == {"name": "Alice", "age": 25}

    def test_dump_excludes_column_false(self) -> None:
        class WithComputed(SQLDataclass):
            name: str
            display_name: str = Field(default="", column=False)

        w = WithComputed(name="alice", display_name="Alice")
        d = w.dump()
        assert d["name"] == "alice"
        assert "display_name" not in d

    def test_dump_on_table_model(self) -> None:
        class Tbl(SQLDataclass, table=True):
            __tablename__ = "dc_dump_tbl"
            id: int | None = Field(default=None, primary_key=True)
            name: str = ""
            transient: str = Field(default="", column=False)

        obj = Tbl(id=1, name="test", transient="temp")
        d = obj.dump()
        assert d["name"] == "test"
        assert "transient" not in d


class TestSQLDataclassLoad:
    """load() on SQLDataclass pydantic dataclasses."""

    def test_basic_load(self) -> None:
        class Hero(SQLDataclass):
            name: str
            age: int = 0

        hero = Hero.load({"name": "Alice", "age": 25})
        assert hero.name == "Alice"
        assert hero.age == 25

    def test_load_with_defaults(self) -> None:
        class Hero(SQLDataclass):
            name: str
            age: int = 0

        hero = Hero.load({"name": "Bob"})
        assert hero.name == "Bob"
        assert hero.age == 0


class TestSQLDataclassClone:
    """clone() on SQLDataclass pydantic dataclasses."""

    def test_shallow_clone(self) -> None:
        class Hero(SQLDataclass):
            name: str
            age: int = 0

        hero = Hero(name="Alice", age=25)
        cloned = hero.clone()
        assert cloned.name == "Alice"
        assert cloned.age == 25

    def test_deep_clone(self) -> None:
        class Hero(SQLDataclass):
            name: str
            tags: list[str] = Field(default_factory=list)

        hero = Hero(name="Alice", tags=["a", "b"])
        cloned = hero.clone(deep=True)
        assert cloned.tags == ["a", "b"]
        cloned.tags.append("c")
        assert hero.tags == ["a", "b"]  # original unchanged


class TestSQLDataclassFieldNames:
    """model_field_names() and data_fields()."""

    def test_model_field_names(self) -> None:
        class Hero(SQLDataclass):
            name: str
            age: int = 0

        names = Hero.model_field_names()
        assert "name" in names
        assert "age" in names

    def test_data_fields_same_as_model_field_names(self) -> None:
        class Hero(SQLDataclass):
            name: str
            age: int = 0

        assert Hero.data_fields() == Hero.model_field_names()


class TestSQLDataclassValidatePrivateField:
    """validate_private_field() helper."""

    def test_validate_int(self) -> None:
        class Hero(SQLDataclass):
            name: str

        hero = Hero(name="Alice")
        result = hero.validate_private_field(int, "42")
        assert result == 42


class TestColumnFalseRequiresDefault:
    """column=False fields must have a default value."""

    def test_column_false_without_default_raises(self) -> None:
        with pytest.raises(TypeError, match="column=False but no default"):

            class Bad(SQLDataclass, table=True):
                __tablename__ = "bad_no_default"
                id: int | None = Field(default=None, primary_key=True)
                transient: str = Field(column=False)  # no default!

    def test_column_false_with_default_ok(self) -> None:
        class Good(SQLDataclass, table=True):
            __tablename__ = "good_with_default"
            id: int | None = Field(default=None, primary_key=True)
            transient: str = Field(default="", column=False)

        assert "transient" in Good.__non_column_fields__

    def test_column_false_with_factory_ok(self) -> None:
        class GoodFactory(SQLDataclass, table=True):
            __tablename__ = "good_with_factory"
            id: int | None = Field(default=None, primary_key=True)
            tags: list[str] = Field(default_factory=list, column=False)

        assert "tags" in GoodFactory.__non_column_fields__


# ---------------------------------------------------------------------------
# SQLModel tests
# ---------------------------------------------------------------------------


class TestSQLModelDump:
    """dump() on SQLModel (Pydantic BaseModel)."""

    def test_basic_dump(self) -> None:
        class Player(SQLModel):
            name: str
            score: float = 0.0

        p = Player(name="Alice", score=9.5)
        d = p.dump()
        assert d == {"name": "Alice", "score": 9.5}

    def test_dump_excludes_column_false(self) -> None:
        class Display(SQLModel, table=True):
            __tablename__ = "sm_display_dump"
            id: int | None = Field(default=None, primary_key=True)
            name: str = ""
            transient: str = Field(default="", column=False)

        obj = Display(id=1, name="test", transient="temp")
        d = obj.dump()
        assert d["name"] == "test"
        assert "transient" not in d


class TestSQLModelLoad:
    """load() on SQLModel."""

    def test_basic_load(self) -> None:
        class Player(SQLModel):
            name: str
            score: float = 0.0

        p = Player.load({"name": "Alice", "score": 9.5})
        assert p.name == "Alice"
        assert p.score == 9.5


class TestSQLModelClone:
    """clone() on SQLModel."""

    def test_shallow_clone(self) -> None:
        class Player(SQLModel):
            name: str
            score: float = 0.0

        p = Player(name="Alice", score=9.5)
        c = p.clone()
        assert c.name == "Alice"
        assert c.score == 9.5

    def test_deep_clone(self) -> None:
        class Player(SQLModel):
            name: str
            tags: list[str] = Field(default_factory=list)

        p = Player(name="Alice", tags=["a", "b"])
        c = p.clone(deep=True)
        c.tags.append("c")
        assert p.tags == ["a", "b"]
