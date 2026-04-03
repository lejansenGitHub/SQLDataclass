"""Integration tests — SQLDataclass model with SQLite round-trips."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError

from sqldataclass import Field, SQLDataclass

# ---------------------------------------------------------------------------
# Test models
# ---------------------------------------------------------------------------


class Hero(SQLDataclass, table=True):
    __tablename__ = "heroes_roundtrip"
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(default="Unknown")
    secret_name: str = ""
    age: int | None = None


class Villain(SQLDataclass, table=True):
    __tablename__ = "villains_roundtrip"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    evil_level: int = Field(default=1)


class UniqueItem(SQLDataclass, table=True):
    __tablename__ = "unique_items_roundtrip"
    id: int | None = Field(default=None, primary_key=True)
    code: str = Field(default="", unique=True)
    label: str = ""


class IndexedRecord(SQLDataclass, table=True):
    __tablename__ = "indexed_records_roundtrip"
    id: int | None = Field(default=None, primary_key=True)
    category: str = Field(default="", index=True)
    value: float = 0.0


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine() -> Engine:
    eng = create_engine("sqlite:///:memory:")
    SQLDataclass.metadata.create_all(eng)
    return eng


@pytest.fixture
def conn(engine: Engine) -> Generator[Connection]:
    with engine.begin() as connection:
        yield connection


# ---------------------------------------------------------------------------
# Insert / load round-trip
# ---------------------------------------------------------------------------


class TestInsertLoadRoundtrip:
    """Full insert -> load round-trip via the Hero model."""

    def test_insert_and_load_all(self, conn: Connection) -> None:
        hero = Hero(id=1, name="Spider-Man", secret_name="Peter Parker", age=25)
        hero.insert(conn)

        heroes = Hero.load_all(conn)
        assert len(heroes) == 1
        loaded = heroes[0]
        assert loaded.id == 1
        assert loaded.name == "Spider-Man"
        assert loaded.secret_name == "Peter Parker"
        assert loaded.age == 25

    def test_load_all_no_filter_returns_all(self, conn: Connection) -> None:
        Hero(id=1, name="A", secret_name="a").insert(conn)
        Hero(id=2, name="B", secret_name="b").insert(conn)
        Hero(id=3, name="C", secret_name="c").insert(conn)

        heroes = Hero.load_all(conn)
        assert len(heroes) == 3

    def test_load_all_with_where_filters(self, conn: Connection) -> None:
        Hero(id=1, name="Alice", secret_name="a", age=30).insert(conn)
        Hero(id=2, name="Bob", secret_name="b", age=20).insert(conn)
        Hero(id=3, name="Charlie", secret_name="c", age=30).insert(conn)

        heroes = Hero.load_all(conn, where=Hero.c.age == 30)
        assert len(heroes) == 2
        names = {h.name for h in heroes}
        assert names == {"Alice", "Charlie"}

    def test_load_all_with_order_by(self, conn: Connection) -> None:
        Hero(id=1, name="Charlie", secret_name="c").insert(conn)
        Hero(id=2, name="Alice", secret_name="a").insert(conn)
        Hero(id=3, name="Bob", secret_name="b").insert(conn)

        heroes = Hero.load_all(conn, order_by=Hero.c.name)
        assert [h.name for h in heroes] == ["Alice", "Bob", "Charlie"]

    def test_load_all_with_multi_column_order_by_tuple(self, conn: Connection) -> None:
        """
        Multi-column ordering via tuple should sort by first column,
        then by second column within ties.
        """
        Hero(id=1, name="Charlie", secret_name="z").insert(conn)
        Hero(id=2, name="Alice", secret_name="b").insert(conn)
        Hero(id=3, name="Alice", secret_name="a").insert(conn)

        heroes = Hero.load_all(conn, order_by=(Hero.c.name, Hero.c.secret_name))
        assert [(h.name, h.secret_name) for h in heroes] == [
            ("Alice", "a"),
            ("Alice", "b"),
            ("Charlie", "z"),
        ]

    def test_load_all_with_multi_column_order_by_list(self, conn: Connection) -> None:
        """List syntax should also work for multi-column ordering."""
        Hero(id=1, name="Bob", secret_name="z").insert(conn)
        Hero(id=2, name="Alice", secret_name="b").insert(conn)
        Hero(id=3, name="Alice", secret_name="a").insert(conn)

        heroes = Hero.load_all(conn, order_by=[Hero.c.name, Hero.c.secret_name])
        assert [(h.name, h.secret_name) for h in heroes] == [
            ("Alice", "a"),
            ("Alice", "b"),
            ("Bob", "z"),
        ]

    def test_load_one_returns_single_instance(self, conn: Connection) -> None:
        Hero(id=1, name="Spider-Man", secret_name="Peter Parker").insert(conn)
        Hero(id=2, name="Iron Man", secret_name="Tony Stark").insert(conn)

        hero = Hero.load_one(conn, where=Hero.c.id == 1)
        assert hero is not None
        assert hero.name == "Spider-Man"

    def test_load_one_returns_none_for_missing(self, conn: Connection) -> None:
        result = Hero.load_one(conn, where=Hero.c.id == 999)
        assert result is None


# ---------------------------------------------------------------------------
# Bulk insert
# ---------------------------------------------------------------------------


class TestInsertMany:
    """Tests for insert_many bulk insert."""

    def test_insert_many_bulk(self, conn: Connection) -> None:
        heroes = [
            Hero(id=1, name="A", secret_name="a"),
            Hero(id=2, name="B", secret_name="b"),
            Hero(id=3, name="C", secret_name="c"),
        ]
        Hero.insert_many(conn, heroes)

        loaded = Hero.load_all(conn)
        assert len(loaded) == 3

    def test_insert_many_empty_list(self, conn: Connection) -> None:
        Hero.insert_many(conn, [])
        loaded = Hero.load_all(conn)
        assert len(loaded) == 0


# ---------------------------------------------------------------------------
# to_dict
# ---------------------------------------------------------------------------


class TestToDict:
    """Tests for the to_dict instance method."""

    def test_to_dict_produces_correct_flat_dict(self) -> None:
        hero = Hero(id=1, name="Spider-Man", secret_name="Peter Parker", age=25)
        result = hero.to_dict()
        assert result == {
            "id": 1,
            "name": "Spider-Man",
            "secret_name": "Peter Parker",
            "age": 25,
        }

    def test_to_dict_with_none_values(self) -> None:
        hero = Hero(id=None, name="Unknown", secret_name="", age=None)
        result = hero.to_dict()
        assert result["id"] is None
        assert result["age"] is None

    def test_to_dict_with_exclude_keys(self) -> None:
        hero = Hero(id=1, name="Spider-Man", secret_name="Peter Parker", age=25)
        result = hero.to_dict(exclude_keys=frozenset({"secret_name"}))
        assert "secret_name" not in result
        assert result["name"] == "Spider-Man"


# ---------------------------------------------------------------------------
# Multiple models in same DB
# ---------------------------------------------------------------------------


class TestMultipleModels:
    """Tests for multiple models coexisting in the same database."""

    def test_multiple_models_insert_and_load(self, conn: Connection) -> None:
        Hero(id=1, name="Spider-Man", secret_name="Peter Parker").insert(conn)
        Villain(id=1, name="Green Goblin", evil_level=9).insert(conn)

        heroes = Hero.load_all(conn)
        villains = Villain.load_all(conn)

        assert len(heroes) == 1
        assert len(villains) == 1
        assert heroes[0].name == "Spider-Man"
        assert villains[0].name == "Green Goblin"
        assert villains[0].evil_level == 9


# ---------------------------------------------------------------------------
# Unique / index constraints
# ---------------------------------------------------------------------------


class TestConstraints:
    """Tests for index and unique constraints at DB level."""

    def test_unique_violation_raises(self, conn: Connection) -> None:
        UniqueItem(id=1, code="ABC", label="First").insert(conn)
        with pytest.raises(IntegrityError):
            UniqueItem(id=2, code="ABC", label="Duplicate").insert(conn)

    def test_indexed_column_works(self, conn: Connection) -> None:
        IndexedRecord(id=1, category="cat_a", value=1.0).insert(conn)
        IndexedRecord(id=2, category="cat_b", value=2.0).insert(conn)
        IndexedRecord(id=3, category="cat_a", value=3.0).insert(conn)

        records = IndexedRecord.load_all(conn, where=IndexedRecord.c.category == "cat_a")
        assert len(records) == 2
