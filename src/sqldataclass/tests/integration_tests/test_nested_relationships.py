"""Tests for nested relationship loading — v0.1.0 feature."""

from typing import Any

import pytest
from sqlalchemy import create_engine

import sqldataclass.model as _model
from sqldataclass import Field, Relationship, SQLDataclass, insert_row

# League → Team → Hero (3 levels)

class NstLeague(SQLDataclass, table=True):
    __tablename__ = "nst_league"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    teams: list["NstTeam"] = Relationship(back_populates="league", order_by="name")


class NstTeam(SQLDataclass, table=True):
    __tablename__ = "nst_team"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    league_id: int | None = Field(default=None, foreign_key="nst_league.id")
    league: NstLeague | None = Relationship()
    heroes: list["NstHero"] = Relationship(back_populates="team", order_by="name")


class NstHero(SQLDataclass, table=True):
    __tablename__ = "nst_hero"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    team_id: int | None = Field(default=None, foreign_key="nst_team.id")
    team: NstTeam | None = Relationship()


@pytest.fixture
def bound_engine() -> Any:
    engine = create_engine("sqlite:///:memory:")
    SQLDataclass.metadata.create_all(engine)
    SQLDataclass.bind(engine)

    with engine.begin() as conn:
        insert_row(conn, NstLeague, {"id": 1, "name": "Marvel"})
        insert_row(conn, NstLeague, {"id": 2, "name": "DC"})
        insert_row(conn, NstTeam, {"id": 1, "name": "Avengers", "league_id": 1})
        insert_row(conn, NstTeam, {"id": 2, "name": "X-Men", "league_id": 1})
        insert_row(conn, NstTeam, {"id": 3, "name": "Justice League", "league_id": 2})
        insert_row(conn, NstHero, {"id": 1, "name": "Iron Man", "team_id": 1})
        insert_row(conn, NstHero, {"id": 2, "name": "Thor", "team_id": 1})
        insert_row(conn, NstHero, {"id": 3, "name": "Wolverine", "team_id": 2})
        insert_row(conn, NstHero, {"id": 4, "name": "Batman", "team_id": 3})

    yield engine
    _model._BOUND_ENGINE = None


class TestNestedRelationships:
    def test_league_has_teams_with_heroes(self, bound_engine: Any) -> None:
        """League → teams → heroes (3 levels deep)."""
        league = NstLeague.load_one(where=NstLeague.c.name == "Marvel")
        assert league is not None
        assert len(league.teams) == 2

        avengers = next(t for t in league.teams if t.name == "Avengers")
        assert len(avengers.heroes) == 2
        hero_names = {h.name for h in avengers.heroes}
        assert hero_names == {"Iron Man", "Thor"}

    def test_all_leagues_nested(self, bound_engine: Any) -> None:
        """load_all on top-level populates all nested levels."""
        leagues = NstLeague.load_all(order_by=NstLeague.c.name)
        assert len(leagues) == 2

        dc = leagues[0]
        assert dc.name == "DC"
        assert len(dc.teams) == 1
        assert dc.teams[0].name == "Justice League"
        assert len(dc.teams[0].heroes) == 1
        assert dc.teams[0].heroes[0].name == "Batman"

        marvel = leagues[1]
        assert marvel.name == "Marvel"
        total_heroes = sum(len(t.heroes) for t in marvel.teams)
        assert total_heroes == 3

    def test_hero_team_league_chain(self, bound_engine: Any) -> None:
        """Hero → team (many-to-one) → league (many-to-one via team)."""
        hero = NstHero.load_one(where=NstHero.c.name == "Iron Man")
        assert hero is not None
        assert hero.team is not None
        assert hero.team.name == "Avengers"
        # team.league is loaded via the many-to-one JOIN
        assert hero.team.league is not None
        assert hero.team.league.name == "Marvel"

    def test_empty_nested_collections(self, bound_engine: Any) -> None:
        """Team with no heroes still works at nested level."""
        with bound_engine.begin() as conn:
            insert_row(conn, NstTeam, {"id": 99, "name": "Empty Team", "league_id": 1})

        league = NstLeague.load_one(where=NstLeague.c.name == "Marvel")
        assert league is not None
        empty_team = next(t for t in league.teams if t.name == "Empty Team")
        assert empty_team.heroes == []
