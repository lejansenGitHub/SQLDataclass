"""Tests for Relationship(order_by=...) — v0.0.8 feature."""

from typing import Any

import pytest
from sqlalchemy import create_engine

import sqldataclass.model as _model
from sqldataclass import Field, Relationship, SQLDataclass, insert_row

# ---------------------------------------------------------------------------
# One-to-many with ordering
# ---------------------------------------------------------------------------


class OrdTeam(SQLDataclass, table=True):
    __tablename__ = "ord_team"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    heroes: list["OrdHero"] = Relationship(back_populates="team", order_by="name")


class OrdHero(SQLDataclass, table=True):
    __tablename__ = "ord_hero"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    team_id: int | None = Field(default=None, foreign_key="ord_team.id")


# ---------------------------------------------------------------------------
# Many-to-many with ordering
# ---------------------------------------------------------------------------


class OrdMtmLink(SQLDataclass, table=True):
    __tablename__ = "ord_mtm_link"
    hero_id: int = Field(primary_key=True, foreign_key="ord_mtm_hero.id")
    team_id: int = Field(primary_key=True, foreign_key="ord_mtm_team.id")


class OrdMtmHero(SQLDataclass, table=True):
    __tablename__ = "ord_mtm_hero"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    teams: list["OrdMtmTeam"] = Relationship(link_model=OrdMtmLink, order_by="name")


class OrdMtmTeam(SQLDataclass, table=True):
    __tablename__ = "ord_mtm_team"
    id: int | None = Field(default=None, primary_key=True)
    name: str


@pytest.fixture
def bound_engine() -> Any:
    engine = create_engine("sqlite:///:memory:")
    SQLDataclass.metadata.create_all(engine)
    SQLDataclass.bind(engine)
    yield engine
    _model._BOUND_ENGINE = None


class TestOneToManyOrdering:
    def test_children_ordered_by_name(self, bound_engine: Any) -> None:
        with bound_engine.begin() as conn:
            insert_row(conn, OrdTeam, {"id": 1, "name": "Avengers"})
            insert_row(conn, OrdHero, {"id": 1, "name": "Zorro", "team_id": 1})
            insert_row(conn, OrdHero, {"id": 2, "name": "Alpha", "team_id": 1})
            insert_row(conn, OrdHero, {"id": 3, "name": "Mango", "team_id": 1})

        team = OrdTeam.load_one(where=OrdTeam.c.id == 1)
        assert team is not None
        names = [h.name for h in team.heroes]
        assert names == ["Alpha", "Mango", "Zorro"]

    def test_ordering_with_load_all(self, bound_engine: Any) -> None:
        with bound_engine.begin() as conn:
            insert_row(conn, OrdTeam, {"id": 1, "name": "T1"})
            insert_row(conn, OrdHero, {"id": 1, "name": "C", "team_id": 1})
            insert_row(conn, OrdHero, {"id": 2, "name": "A", "team_id": 1})
            insert_row(conn, OrdHero, {"id": 3, "name": "B", "team_id": 1})

        teams = OrdTeam.load_all()
        assert [h.name for h in teams[0].heroes] == ["A", "B", "C"]

    def test_empty_collection_with_order_by(self, bound_engine: Any) -> None:
        with bound_engine.begin() as conn:
            insert_row(conn, OrdTeam, {"id": 1, "name": "Empty"})

        team = OrdTeam.load_one(where=OrdTeam.c.id == 1)
        assert team is not None
        assert team.heroes == []


class TestManyToManyOrdering:
    def test_targets_ordered_by_name(self, bound_engine: Any) -> None:
        with bound_engine.begin() as conn:
            insert_row(conn, OrdMtmHero, {"id": 1, "name": "Wolverine"})
            insert_row(conn, OrdMtmTeam, {"id": 1, "name": "Zebra Squad"})
            insert_row(conn, OrdMtmTeam, {"id": 2, "name": "Alpha Team"})
            insert_row(conn, OrdMtmTeam, {"id": 3, "name": "Beta Force"})
            insert_row(conn, OrdMtmLink, {"hero_id": 1, "team_id": 1})
            insert_row(conn, OrdMtmLink, {"hero_id": 1, "team_id": 2})
            insert_row(conn, OrdMtmLink, {"hero_id": 1, "team_id": 3})

        hero = OrdMtmHero.load_one(where=OrdMtmHero.c.id == 1)
        assert hero is not None
        names = [t.name for t in hero.teams]
        assert names == ["Alpha Team", "Beta Force", "Zebra Squad"]
