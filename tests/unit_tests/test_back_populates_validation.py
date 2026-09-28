"""Tests that back_populates must name a declared field on the child model."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Connection

from sqldataclass import Field, Relationship, SQLDataclass


class BpvHero(SQLDataclass, table=True):
    __tablename__ = "bpv_heroes"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    team_id: int | None = Field(default=None, foreign_key="bpv_teams.id")


class BpvTeam(SQLDataclass, table=True):
    __tablename__ = "bpv_teams"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    heroes: list[BpvHero] = Relationship(back_populates="team")


@pytest.fixture
def connection() -> Generator[Connection]:
    """Yield an in-memory SQLite connection with one team and one hero stored."""
    metadata = MetaData()
    for model in (BpvTeam, BpvHero):
        model.__table__.to_metadata(metadata)
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    with engine.connect() as conn:
        team = BpvTeam(name="Avengers")
        team.insert(conn)
        BpvHero(name="Thor", team_id=team.id).insert(conn)
        yield conn


def test_back_populates_without_child_field_raises(connection: Connection) -> None:
    """Instances are fully slotted, so a back-reference needs a declared field to live in.

    The error names the missing field so the fix is obvious.
    """
    # --- Execute / Assert ---
    with pytest.raises(TypeError, match="back_populates='team' names no field on BpvHero"):
        BpvTeam.load_all(connection)
