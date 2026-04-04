"""Tests for Field(column=False) — non-persistent fields."""

from typing import Any

import pytest
from sqlalchemy import create_engine

import sqldataclass.model as _model
from sqldataclass import Field, SQLDataclass


class NcHero(SQLDataclass, table=True):
    __tablename__ = "nc_hero"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    display_name: str = Field(default="", column=False)
    is_loaded: bool = Field(default=False, column=False)
    score: float = Field(default=0.0, column=False)


@pytest.fixture
def bound_engine() -> Any:
    engine = create_engine("sqlite:///:memory:")
    SQLDataclass.metadata.create_all(engine)
    SQLDataclass.bind(engine)
    yield engine
    _model._BOUND_ENGINE = None


class TestNonColumnFields:
    def test_not_in_table_columns(self, bound_engine: Any) -> None:
        cols = {c.name for c in NcHero.__table__.columns}
        assert "display_name" not in cols
        assert "is_loaded" not in cols
        assert "score" not in cols
        assert "id" in cols
        assert "name" in cols

    def test_exist_on_python_object(self, bound_engine: Any) -> None:
        hero = NcHero(name="Spider-Man", display_name="Spidey", is_loaded=True, score=9.5)
        assert hero.display_name == "Spidey"
        assert hero.is_loaded is True
        assert hero.score == 9.5

    def test_insert_excludes_non_column_fields(self, bound_engine: Any) -> None:
        hero = NcHero(name="Spider-Man", display_name="Spidey", is_loaded=True)
        hero.insert()
        loaded = NcHero.load_one(where=NcHero.c.name == "Spider-Man")
        assert loaded is not None
        assert loaded.name == "Spider-Man"

    def test_load_uses_defaults_for_non_column(self, bound_engine: Any) -> None:
        NcHero(name="Spider-Man", display_name="Spidey", is_loaded=True, score=99.0).insert()
        loaded = NcHero.load_one(where=NcHero.c.name == "Spider-Man")
        assert loaded is not None
        assert loaded.display_name == ""
        assert loaded.is_loaded is False
        assert loaded.score == 0.0

    def test_to_dict_excludes_non_column(self, bound_engine: Any) -> None:
        hero = NcHero(name="Spider-Man", display_name="Spidey", is_loaded=True)
        d = hero.to_dict()
        assert "display_name" not in d
        assert "is_loaded" not in d
        assert "score" not in d
        assert "name" in d

    def test_load_all_with_non_column(self, bound_engine: Any) -> None:
        NcHero(name="A").insert()
        NcHero(name="B").insert()
        heroes = NcHero.load_all()
        assert len(heroes) == 2
        assert all(h.display_name == "" for h in heroes)
        assert all(h.is_loaded is False for h in heroes)

    def test_update_ignores_non_column(self, bound_engine: Any) -> None:
        NcHero(name="A").insert()
        NcHero.update({"name": "B"}, where=NcHero.c.name == "A")
        loaded = NcHero.load_one(where=NcHero.c.name == "B")
        assert loaded is not None

    def test_non_column_fields_attr(self, bound_engine: Any) -> None:
        assert "display_name" in NcHero.__non_column_fields__
        assert "is_loaded" in NcHero.__non_column_fields__
        assert "score" in NcHero.__non_column_fields__
        assert "name" not in NcHero.__non_column_fields__
