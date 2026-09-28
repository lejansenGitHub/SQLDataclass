"""Tests for Field(foreign_key=<column object>)."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Connection

from sqldataclass import Field, Relationship, SQLDataclass


class CfkTeam(SQLDataclass, table=True):
    __tablename__ = "cfk_teams"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""


class CfkHero(SQLDataclass, table=True):
    __tablename__ = "cfk_heroes"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    team_id: int | None = Field(default=None, foreign_key=CfkTeam.c.id)
    team: CfkTeam | None = Relationship()


@pytest.fixture
def connection() -> Generator[Connection]:
    """Yield an in-memory SQLite connection with the team and hero tables created."""
    metadata = MetaData()
    for model in (CfkTeam, CfkHero):
        model.__table__.to_metadata(metadata)
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    with engine.connect() as conn:
        yield conn


def test_column_foreign_key_targets_the_given_column() -> None:
    """A column object is a valid ForeignKey target, so the constraint points at that exact column.

    The column carries its table and schema, so nothing needs to be spelled in a string.
    """
    # --- Execute ---
    foreign_keys = list(CfkHero.__table__.c.team_id.foreign_keys)

    # --- Assert ---
    assert len(foreign_keys) == 1
    assert foreign_keys[0].column is CfkTeam.__table__.c.id
    assert CfkHero.__fk_map__["cfk_teams"] is CfkHero.__table__.c.team_id


def test_relationship_resolves_through_column_foreign_key(connection: Connection) -> None:
    """Relationship loading finds the FK column via the constraint, which the column form provides."""
    # --- Input ---
    team = CfkTeam(name="Avengers")
    team.insert(connection)
    CfkHero(name="Thor", team_id=team.id).insert(connection)

    # --- Execute ---
    heroes = CfkHero.load_all(connection)

    # --- Assert ---
    assert [hero.name for hero in heroes] == ["Thor"]
    assert heroes[0].team == CfkTeam(id=team.id, name="Avengers")
