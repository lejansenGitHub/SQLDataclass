"""Tests for Model.update() and Model.delete() — v0.0.6 feature."""

from typing import Any

import pytest
from sqlalchemy import create_engine

import sqldataclass.model as _model
from sqldataclass import Field, SQLDataclass


class UdHero(SQLDataclass, table=True):
    __tablename__ = "ud_hero"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    age: int | None = None


@pytest.fixture
def bound_engine() -> Any:
    engine = create_engine("sqlite:///:memory:")
    SQLDataclass.metadata.create_all(engine)
    SQLDataclass.bind(engine)
    yield engine
    _model._BOUND_ENGINE = None


class TestUpdate:
    def test_update_single_row(self, bound_engine: Any) -> None:
        UdHero(name="Spider-Man", age=25).insert()
        count = UdHero.update({"age": 30}, where=UdHero.c.name == "Spider-Man")
        assert count == 1
        hero = UdHero.load_one(where=UdHero.c.name == "Spider-Man")
        assert hero is not None
        assert hero.age == 30

    def test_update_multiple_rows(self, bound_engine: Any) -> None:
        UdHero(name="A", age=10).insert()
        UdHero(name="B", age=20).insert()
        UdHero(name="C", age=30).insert()
        count = UdHero.update({"age": 99}, where=UdHero.c.age < 25)
        assert count == 2

    def test_update_all_rows(self, bound_engine: Any) -> None:
        UdHero(name="A", age=1).insert()
        UdHero(name="B", age=2).insert()
        count = UdHero.update({"age": 0})
        assert count == 2
        heroes = UdHero.load_all()
        assert all(h.age == 0 for h in heroes)

    def test_update_no_match_returns_zero(self, bound_engine: Any) -> None:
        UdHero(name="A", age=1).insert()
        count = UdHero.update({"age": 99}, where=UdHero.c.name == "Nobody")
        assert count == 0

    def test_update_with_explicit_conn(self, bound_engine: Any) -> None:
        UdHero(name="A", age=1).insert()
        with bound_engine.begin() as conn:
            count = UdHero.update({"age": 50}, conn, where=UdHero.c.name == "A")
            assert count == 1


class TestDelete:
    def test_delete_single_row(self, bound_engine: Any) -> None:
        UdHero(name="Spider-Man").insert()
        UdHero(name="Iron Man").insert()
        count = UdHero.delete(where=UdHero.c.name == "Spider-Man")
        assert count == 1
        assert len(UdHero.load_all()) == 1

    def test_delete_multiple_rows(self, bound_engine: Any) -> None:
        UdHero(name="A", age=10).insert()
        UdHero(name="B", age=20).insert()
        UdHero(name="C", age=30).insert()
        count = UdHero.delete(where=UdHero.c.age < 25)
        assert count == 2
        assert len(UdHero.load_all()) == 1

    def test_delete_all_rows(self, bound_engine: Any) -> None:
        UdHero(name="A").insert()
        UdHero(name="B").insert()
        count = UdHero.delete()
        assert count == 2
        assert len(UdHero.load_all()) == 0

    def test_delete_no_match_returns_zero(self, bound_engine: Any) -> None:
        UdHero(name="A").insert()
        count = UdHero.delete(where=UdHero.c.name == "Nobody")
        assert count == 0
        assert len(UdHero.load_all()) == 1

    def test_delete_with_explicit_conn(self, bound_engine: Any) -> None:
        UdHero(name="A").insert()
        with bound_engine.begin() as conn:
            count = UdHero.delete(conn, where=UdHero.c.name == "A")
            assert count == 1
