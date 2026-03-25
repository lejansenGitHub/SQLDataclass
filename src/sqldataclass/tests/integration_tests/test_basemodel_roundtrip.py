"""Integration tests — SQLModel with SQLite round-trips."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Connection, Engine

from sqldataclass import Field, Relationship, SQLDataclass, SQLModel
from sqldataclass.write import flatten_for_table

# ---------------------------------------------------------------------------
# Shared metadata so all test models use the same MetaData instance
# ---------------------------------------------------------------------------

_test_metadata = MetaData()

# ---------------------------------------------------------------------------
# Test models
# ---------------------------------------------------------------------------


class BmHero(SQLModel, table=True):
    __tablename__ = "bm_heroes"
    metadata = _test_metadata
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    secret_name: str = ""
    age: int | None = None


class BmTeam(SQLModel, table=True):
    __tablename__ = "bm_teams"
    metadata = _test_metadata
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""


class BmPlayer(SQLModel, table=True):
    __tablename__ = "bm_players"
    metadata = _test_metadata
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    team_id: int | None = Field(default=None, foreign_key="bm_teams.id")
    team: BmTeam | None = Relationship()


class BmNonCol(SQLModel, table=True):
    __tablename__ = "bm_noncol"
    metadata = _test_metadata
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    display_name: str = Field(default="", column=False)


# Cross-type: SQLDataclass table referenced by SQLModel
class DcLeague(SQLDataclass, table=True):
    __tablename__ = "bm_dc_leagues"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine() -> Engine:
    eng = create_engine("sqlite:///:memory:")
    _test_metadata.create_all(eng)
    SQLDataclass.metadata.create_all(eng)
    return eng


@pytest.fixture
def conn(engine: Engine) -> Generator[Connection]:
    with engine.begin() as connection:
        yield connection


# ---------------------------------------------------------------------------
# Basic insert / load round-trip
# ---------------------------------------------------------------------------


class TestBasicRoundtrip:
    """Full insert -> load round-trip for SQLModel table classes."""

    def test_insert_and_load_all(self, conn: Connection) -> None:
        hero = BmHero(id=1, name="Spider-Man", secret_name="Peter Parker", age=25)
        hero.insert(conn)

        heroes = BmHero.load_all(conn)
        assert len(heroes) == 1
        loaded = heroes[0]
        assert loaded.name == "Spider-Man"
        assert loaded.secret_name == "Peter Parker"
        assert loaded.age == 25

    def test_insert_and_load_one(self, conn: Connection) -> None:
        BmHero(id=10, name="Batman", secret_name="Bruce Wayne", age=35).insert(conn)

        loaded = BmHero.load_one(conn, where=BmHero.c.id == 10)
        assert loaded is not None
        assert loaded.name == "Batman"

    def test_load_one_returns_none(self, conn: Connection) -> None:
        result = BmHero.load_one(conn, where=BmHero.c.id == 999)
        assert result is None

    def test_insert_many(self, conn: Connection) -> None:
        heroes = [
            BmHero(id=1, name="A"),
            BmHero(id=2, name="B"),
            BmHero(id=3, name="C"),
        ]
        BmHero.insert_many(conn, objects=heroes)
        loaded = BmHero.load_all(conn)
        assert len(loaded) == 3

    def test_to_dict(self, conn: Connection) -> None:
        hero = BmHero(id=1, name="Test", secret_name="Secret", age=30)
        hero.insert(conn)
        loaded = BmHero.load_one(conn, where=BmHero.c.id == 1)
        assert loaded is not None
        d = loaded.to_dict()
        assert d["name"] == "Test"
        assert d["age"] == 30


# ---------------------------------------------------------------------------
# Flatten / write
# ---------------------------------------------------------------------------


class TestFlatten:
    """flatten_for_table works for SQLModel instances."""

    def test_basic_flatten(self) -> None:
        hero = BmHero(id=1, name="Test", secret_name="S", age=20)
        flat = flatten_for_table(hero)
        assert flat == {"id": 1, "name": "Test", "secret_name": "S", "age": 20}

    def test_flatten_excludes_non_column(self) -> None:
        obj = BmNonCol(id=1, name="visible", display_name="hidden")
        flat = flatten_for_table(obj)
        assert "name" in flat
        assert "display_name" not in flat

    def test_flatten_excludes_relationship(self) -> None:
        player = BmPlayer(id=1, name="P", team_id=1, team=BmTeam(id=1, name="T"))
        flat = flatten_for_table(player)
        assert "team" not in flat
        assert "team_id" in flat


# ---------------------------------------------------------------------------
# Relationships (many-to-one)
# ---------------------------------------------------------------------------


class TestRelationships:
    """SQLModel with Relationship() loads related objects."""

    def test_many_to_one_relationship(self, conn: Connection) -> None:
        BmTeam(id=1, name="Avengers").insert(conn)
        BmPlayer(id=1, name="Tony", team_id=1).insert(conn)

        players = BmPlayer.load_all(conn)
        assert len(players) == 1
        assert players[0].team is not None
        assert players[0].team.name == "Avengers"

    def test_many_to_one_null(self, conn: Connection) -> None:
        BmPlayer(id=2, name="Solo", team_id=None).insert(conn)

        player = BmPlayer.load_one(conn, where=BmPlayer.c.id == 2)
        assert player is not None
        assert player.team is None


# ---------------------------------------------------------------------------
# Update / delete
# ---------------------------------------------------------------------------


class TestUpdateDelete:
    """Update and delete operations work on SQLModel tables."""

    def test_update(self, conn: Connection) -> None:
        BmHero(id=1, name="Before").insert(conn)
        count = BmHero.update({"name": "After"}, conn, where=BmHero.c.id == 1)
        assert count == 1
        loaded = BmHero.load_one(conn, where=BmHero.c.id == 1)
        assert loaded is not None
        assert loaded.name == "After"

    def test_delete(self, conn: Connection) -> None:
        BmHero(id=1, name="Doomed").insert(conn)
        count = BmHero.delete(conn, where=BmHero.c.id == 1)
        assert count == 1
        assert BmHero.load_one(conn, where=BmHero.c.id == 1) is None


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


class TestPagination:
    """Limit and offset work on SQLModel tables."""

    def test_limit(self, conn: Connection) -> None:
        for i in range(5):
            BmHero(id=i + 1, name=f"H{i}").insert(conn)
        loaded = BmHero.load_all(conn, limit=2)
        assert len(loaded) == 2

    def test_offset(self, conn: Connection) -> None:
        for i in range(5):
            BmHero(id=i + 1, name=f"H{i}").insert(conn)
        loaded = BmHero.load_all(conn, limit=2, offset=3)
        assert len(loaded) == 2


# ---------------------------------------------------------------------------
# Non-column fields
# ---------------------------------------------------------------------------


class TestNonColumnFields:
    """column=False fields are excluded from DB but present on instances."""

    def test_non_column_field_roundtrip(self, conn: Connection) -> None:
        BmNonCol(id=1, name="visible", display_name="hidden").insert(conn)
        loaded = BmNonCol.load_one(conn, where=BmNonCol.c.id == 1)
        assert loaded is not None
        assert loaded.name == "visible"
        # Non-column field gets default value on load
        assert loaded.display_name == ""


# ---------------------------------------------------------------------------
# Cross-type: SQLModel references SQLDataclass table
# ---------------------------------------------------------------------------


class TestCrossTypeComposition:
    """SQLModel instances can hold SQLDataclass instances as field values."""

    def test_sqldataclass_in_sqlmodel_field(self) -> None:
        league = DcLeague(id=1, name="Premier")

        class ApiResponse(SQLModel):
            league: DcLeague
            msg: str = "ok"

        resp = ApiResponse(league=league, msg="success")
        assert resp.league.name == "Premier"
        assert resp.msg == "success"
