"""Tests for SQLDataclass.bind(engine) — conn-free usage."""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import sqldataclass.model as _model
from sqldataclass import Field, Relationship, SQLDataclass, insert_row

# ---------------------------------------------------------------------------
# Models (unique tablenames to avoid metadata conflicts)
# ---------------------------------------------------------------------------


class BindTeam(SQLDataclass, table=True):
    __tablename__ = "bind_team"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    heroes: list["BindHero"] = Relationship(back_populates="team")


class BindHero(SQLDataclass, table=True):
    __tablename__ = "bind_hero"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    team_id: int | None = Field(default=None, foreign_key="bind_team.id")
    team: BindTeam | None = Relationship()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def bound_engine() -> Engine:
    """Create a fresh SQLite engine, bind it, and unbind after the test."""
    engine = create_engine("sqlite:///:memory:")
    SQLDataclass.metadata.create_all(engine)
    SQLDataclass.bind(engine)
    yield engine  # type: ignore[misc]
    _model._BOUND_ENGINE = None


# ---------------------------------------------------------------------------
# Insert without conn
# ---------------------------------------------------------------------------


class TestInsertWithoutConn:
    def test_insert_instance(self, bound_engine: Engine) -> None:
        hero = BindHero(name="Spider-Man")
        hero.insert()
        loaded = BindHero.load_all()
        assert len(loaded) == 1
        assert loaded[0].name == "Spider-Man"

    def test_insert_many_without_conn(self, bound_engine: Engine) -> None:
        BindHero.insert_many(objects=[
            BindHero(name="Iron Man"),
            BindHero(name="Thor"),
        ])
        loaded = BindHero.load_all()
        assert len(loaded) == 2


# ---------------------------------------------------------------------------
# Query without conn
# ---------------------------------------------------------------------------


class TestQueryWithoutConn:
    def test_load_all_no_conn(self, bound_engine: Engine) -> None:
        BindHero(name="A").insert()
        BindHero(name="B").insert()
        heroes = BindHero.load_all()
        assert len(heroes) == 2

    def test_load_all_with_where(self, bound_engine: Engine) -> None:
        BindHero(name="Young").insert()
        BindHero(name="Old").insert()
        result = BindHero.load_all(where=BindHero.c.name == "Old")
        assert len(result) == 1
        assert result[0].name == "Old"

    def test_load_one_no_conn(self, bound_engine: Engine) -> None:
        BindHero(name="Spider-Man").insert()
        hero = BindHero.load_one(where=BindHero.c.name == "Spider-Man")
        assert hero is not None
        assert hero.name == "Spider-Man"

    def test_load_one_returns_none(self, bound_engine: Engine) -> None:
        result = BindHero.load_one(where=BindHero.c.name == "Nobody")
        assert result is None

    def test_load_all_empty(self, bound_engine: Engine) -> None:
        heroes = BindHero.load_all()
        assert heroes == []


# ---------------------------------------------------------------------------
# Relationships without conn
# ---------------------------------------------------------------------------


class TestRelationshipsWithoutConn:
    def test_many_to_one_auto_hydrated(self, bound_engine: Engine) -> None:
        with bound_engine.begin() as conn:
            insert_row(conn, BindTeam, {"id": 1, "name": "Avengers"})
            insert_row(conn, BindHero, {"id": 1, "name": "Iron Man", "team_id": 1})

        hero = BindHero.load_one(where=BindHero.c.name == "Iron Man")
        assert hero is not None
        assert hero.team is not None
        assert hero.team.name == "Avengers"

    def test_one_to_many_auto_populated(self, bound_engine: Engine) -> None:
        with bound_engine.begin() as conn:
            insert_row(conn, BindTeam, {"id": 1, "name": "Avengers"})
            insert_row(conn, BindHero, {"id": 1, "name": "Iron Man", "team_id": 1})
            insert_row(conn, BindHero, {"id": 2, "name": "Thor", "team_id": 1})

        team = BindTeam.load_one(where=BindTeam.c.name == "Avengers")
        assert team is not None
        assert len(team.heroes) == 2
        names = {h.name for h in team.heroes}
        assert names == {"Iron Man", "Thor"}


# ---------------------------------------------------------------------------
# Explicit conn still works alongside bind
# ---------------------------------------------------------------------------


class TestExplicitConnStillWorks:
    def test_explicit_conn_overrides_bind(self, bound_engine: Engine) -> None:
        BindHero(name="Hero1").insert()
        with bound_engine.connect() as conn:
            heroes = BindHero.load_all(conn)
            assert len(heroes) == 1


# ---------------------------------------------------------------------------
# Error when no engine bound and no conn provided
# ---------------------------------------------------------------------------


class TestNoBoundEngine:
    def test_load_all_raises_without_bind(self) -> None:
        _model._BOUND_ENGINE = None
        with pytest.raises(RuntimeError, match="No connection provided"):
            BindHero.load_all()

    def test_insert_raises_without_bind(self) -> None:
        _model._BOUND_ENGINE = None
        hero = BindHero(name="Test")
        with pytest.raises(RuntimeError, match="No connection provided"):
            hero.insert()
