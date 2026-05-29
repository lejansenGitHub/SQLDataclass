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
        """dump() on a non-table SQLDataclass returns a flat dict of every
        declared field, mirroring pydantic model_dump semantics."""

        class Hero(SQLDataclass):
            name: str
            age: int = 0

        hero = Hero(name="Alice", age=25)
        d = hero.dump()
        assert d == {"name": "Alice", "age": 25}

    def test_dump_excludes_column_false(self) -> None:
        """column=False fields stay on the Python instance but are excluded
        from dump() — they're never meant to travel to JSON / DB."""

        class WithComputed(SQLDataclass):
            name: str
            display_name: str = Field(default="", column=False)

        w = WithComputed(name="alice", display_name="Alice")
        d = w.dump()
        assert d["name"] == "alice"
        assert "display_name" not in d

    def test_dump_on_table_model(self) -> None:
        """Same exclusion holds for table-bound SQLDataclasses: column=False
        fields don't leak into dump() output regardless of table=True."""

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
        """load(data) reconstructs a SQLDataclass instance from a flat dict —
        every key maps to an instance field with the same value."""

        class Hero(SQLDataclass):
            name: str
            age: int = 0

        hero = Hero.load({"name": "Alice", "age": 25})
        assert hero.name == "Alice"
        assert hero.age == 25

    def test_load_with_defaults(self) -> None:
        """load() omits keys that map to defaulted fields; defaults apply
        normally, matching plain pydantic construction semantics."""

        class Hero(SQLDataclass):
            name: str
            age: int = 0

        hero = Hero.load({"name": "Bob"})
        assert hero.name == "Bob"
        assert hero.age == 0


class TestSQLDataclassClone:
    """clone() on SQLDataclass pydantic dataclasses."""

    def test_shallow_clone(self) -> None:
        """clone() returns a new SQLDataclass instance with equal scalar
        values; mutable containers are shared (shallow semantics)."""

        class Hero(SQLDataclass):
            name: str
            age: int = 0

        hero = Hero(name="Alice", age=25)
        cloned = hero.clone()
        assert cloned.name == "Alice"
        assert cloned.age == 25

    def test_deep_clone(self) -> None:
        """clone(deep=True) duplicates mutable containers as well —
        mutating the clone's list must not affect the original."""

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
        """model_field_names() returns every declared field, including those
        with defaults — the canonical introspection helper."""

        class Hero(SQLDataclass):
            name: str
            age: int = 0

        names = Hero.model_field_names()
        assert "name" in names
        assert "age" in names

    def test_data_fields_same_as_model_field_names(self) -> None:
        """For non-versioned models, data_fields() and model_field_names() are
        identical — versioned models exclude the _VERSION field from data_fields."""

        class Hero(SQLDataclass):
            name: str
            age: int = 0

        assert Hero.data_fields() == Hero.model_field_names()


class TestSQLDataclassValidatePrivateField:
    """validate_private_field() helper."""

    def test_validate_int(self) -> None:
        """validate_private_field() runs pydantic coercion against an arbitrary
        annotation — a "42" string coerces to the int 42."""

        class Hero(SQLDataclass):
            name: str

        hero = Hero(name="Alice")
        result = hero.validate_private_field(int, "42")
        assert result == 42


class TestColumnFalseRequiresDefault:
    """column=False fields without a default defer to pydantic's required-field validation."""

    def test_column_false_without_default_is_required_at_construction(self) -> None:
        """A column=False field without a default behaves like any required pydantic field:
        construction without it raises ValidationError, construction with a value succeeds."""
        from pydantic import ValidationError

        class WithoutDefault(SQLDataclass, table=True):
            __tablename__ = "without_default_non_column"
            id: int | None = Field(default=None, primary_key=True)
            transient: str = Field(column=False)  # no default — required at construction

        # --- Assert ---
        with pytest.raises(ValidationError):
            WithoutDefault()
        instance = WithoutDefault(transient="hello")
        assert instance.transient == "hello"

    def test_column_false_with_default_ok(self) -> None:
        """A column=False field with an explicit default registers as a non-column
        field on the model and the default is applied at construction."""

        class Good(SQLDataclass, table=True):
            __tablename__ = "good_with_default"
            id: int | None = Field(default=None, primary_key=True)
            transient: str = Field(default="", column=False)

        assert "transient" in Good.__non_column_fields__

    def test_column_false_with_factory_ok(self) -> None:
        """A column=False field with default_factory registers as a non-column
        field; the factory is invoked at construction (e.g. fresh [] per instance)."""

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
        """dump() on an SQLModel returns a plain dict with every column field;
        scalar values round-trip with no nesting or alias rewrites."""

        class Player(SQLModel):
            name: str
            score: float = 0.0

        p = Player(name="Alice", score=9.5)
        d = p.dump()
        assert d == {"name": "Alice", "score": 9.5}

    def test_dump_excludes_column_false(self) -> None:
        """column=False fields are excluded from dump() output — they exist on
        the Python object but never travel to JSON / DB / external payloads."""

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
        """load(data) is the inverse of dump(): a plain dict reconstructs the
        instance with the same scalar values."""

        class Player(SQLModel):
            name: str
            score: float = 0.0

        p = Player.load({"name": "Alice", "score": 9.5})
        assert p.name == "Alice"
        assert p.score == 9.5


class TestSQLModelClone:
    """clone() on SQLModel."""

    def test_shallow_clone(self) -> None:
        """clone() returns a new instance with equal scalar field values;
        shared mutable containers are NOT copied (shallow semantics)."""

        class Player(SQLModel):
            name: str
            score: float = 0.0

        p = Player(name="Alice", score=9.5)
        c = p.clone()
        assert c.name == "Alice"
        assert c.score == 9.5

    def test_deep_clone(self) -> None:
        """clone(deep=True) copies mutable containers as well — mutating the
        clone's list does not affect the original."""

        class Player(SQLModel):
            name: str
            tags: list[str] = Field(default_factory=list)

        p = Player(name="Alice", tags=["a", "b"])
        c = p.clone(deep=True)
        c.tags.append("c")
        assert p.tags == ["a", "b"]
