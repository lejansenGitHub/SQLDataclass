"""Integration tests — SQLDataclass Relationship() feature."""

from __future__ import annotations

from collections.abc import Generator
from typing import Literal

import pytest
from sqlalchemy import create_engine, insert
from sqlalchemy.engine import Connection, Engine

from sqldataclass import Field, Relationship, SQLDataclass

# ---------------------------------------------------------------------------
# Many-to-one models (Hero → Team)
# ---------------------------------------------------------------------------


class RelTeam(SQLDataclass, table=True):
    __tablename__ = "rel_teams"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""


class RelHero(SQLDataclass, table=True):
    __tablename__ = "rel_heroes"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    team_id: int | None = Field(default=None, foreign_key="rel_teams.id")
    team: RelTeam | None = Relationship()


# ---------------------------------------------------------------------------
# Discriminated union models (Participant → NormalData | BatteryData)
# ---------------------------------------------------------------------------


class DiscNormalData(SQLDataclass, table=True):
    __tablename__ = "disc_normal_data"
    id: int | None = Field(default=None, primary_key=True)
    participant_id: int = Field(foreign_key="disc_participants.id")
    behavior: Literal["normal"] = "normal"
    p_max: float = 0.0


class DiscBatteryData(SQLDataclass, table=True):
    __tablename__ = "disc_battery_data"
    id: int | None = Field(default=None, primary_key=True)
    participant_id: int = Field(foreign_key="disc_participants.id")
    behavior: Literal["battery"] = "battery"
    capacity: float = 0.0


class DiscParticipant(SQLDataclass, table=True):
    __tablename__ = "disc_participants"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    behavior: str = ""
    data: DiscNormalData | DiscBatteryData = Relationship(discriminator="behavior")


# ---------------------------------------------------------------------------
# Plain model without relationships (edge case)
# ---------------------------------------------------------------------------


class PlainItem(SQLDataclass, table=True):
    __tablename__ = "plain_items"
    id: int | None = Field(default=None, primary_key=True)
    label: str = ""


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
# Many-to-one tests
# ---------------------------------------------------------------------------


class TestManyToOne:
    """Hero → Team relationship: auto-hydration via LEFT JOIN."""

    def test_hero_team_auto_hydrated(self, conn: Connection) -> None:
        RelTeam(id=1, name="Avengers").insert(conn)
        RelHero(id=1, name="Iron Man", team_id=1).insert(conn)

        heroes = RelHero.load_all(conn)
        assert len(heroes) == 1
        hero = heroes[0]
        assert hero.team is not None
        assert hero.team.name == "Avengers"
        assert hero.team.id == 1

    def test_null_team_id_gives_none_team(self, conn: Connection) -> None:
        RelHero(id=1, name="Lone Wolf", team_id=None).insert(conn)

        heroes = RelHero.load_all(conn)
        assert len(heroes) == 1
        assert heroes[0].team is None

    def test_load_one_with_where_filter(self, conn: Connection) -> None:
        RelTeam(id=1, name="Avengers").insert(conn)
        RelTeam(id=2, name="X-Men").insert(conn)
        RelHero(id=1, name="Iron Man", team_id=1).insert(conn)
        RelHero(id=2, name="Wolverine", team_id=2).insert(conn)

        hero = RelHero.load_one(conn, where=RelHero.c.name == "Wolverine")
        assert hero is not None
        assert hero.name == "Wolverine"
        assert hero.team is not None
        assert hero.team.name == "X-Men"

    def test_load_one_returns_none_for_missing(self, conn: Connection) -> None:
        result = RelHero.load_one(conn, where=RelHero.c.id == 999)
        assert result is None

    def test_load_all_returns_all_heroes_with_teams(self, conn: Connection) -> None:
        RelTeam(id=1, name="Avengers").insert(conn)
        RelTeam(id=2, name="X-Men").insert(conn)
        RelHero(id=1, name="Iron Man", team_id=1).insert(conn)
        RelHero(id=2, name="Wolverine", team_id=2).insert(conn)
        RelHero(id=3, name="Solo", team_id=None).insert(conn)

        heroes = RelHero.load_all(conn, order_by=RelHero.c.id)
        assert len(heroes) == 3

        assert heroes[0].name == "Iron Man"
        assert heroes[0].team is not None
        assert heroes[0].team.name == "Avengers"

        assert heroes[1].name == "Wolverine"
        assert heroes[1].team is not None
        assert heroes[1].team.name == "X-Men"

        assert heroes[2].name == "Solo"
        assert heroes[2].team is None

    def test_manual_construction_with_team(self) -> None:
        team = RelTeam(id=1, name="Avengers")
        hero = RelHero(id=1, name="Iron Man", team_id=1, team=team)
        assert hero.team is not None
        assert hero.team.name == "Avengers"
        assert hero.team_id == 1


# ---------------------------------------------------------------------------
# Discriminated union tests
# ---------------------------------------------------------------------------


class TestDiscriminatedUnion:
    """Participant → NormalData | BatteryData via behavior discriminator."""

    def _insert_participant_with_data(  # noqa: PLR0913
        self,
        conn: Connection,
        *,
        pid: int,
        name: str,
        behavior: str,
        data_id: int,
        p_max: float = 0.0,
        capacity: float = 0.0,
    ) -> None:
        """Helper: insert a participant and its associated data row."""
        conn.execute(
            insert(DiscParticipant.__table__).values(id=pid, name=name, behavior=behavior),
        )
        if behavior == "normal":
            DiscNormalData(id=data_id, participant_id=pid, p_max=p_max).insert(conn)
        elif behavior == "battery":
            DiscBatteryData(id=data_id, participant_id=pid, capacity=capacity).insert(conn)

    def test_load_all_hydrates_correct_variant(self, conn: Connection) -> None:
        self._insert_participant_with_data(
            conn, pid=1, name="Alice", behavior="normal", data_id=1, p_max=100.0,
        )
        self._insert_participant_with_data(
            conn, pid=2, name="Bob", behavior="battery", data_id=1, capacity=50.0,
        )

        participants = DiscParticipant.load_all(conn, order_by=DiscParticipant.c.id)
        assert len(participants) == 2

        alice = participants[0]
        assert alice.name == "Alice"
        assert isinstance(alice.data, DiscNormalData)
        assert alice.data.p_max == 100.0

        bob = participants[1]
        assert bob.name == "Bob"
        assert isinstance(bob.data, DiscBatteryData)
        assert bob.data.capacity == 50.0

    def test_load_one_returns_correct_variant(self, conn: Connection) -> None:
        self._insert_participant_with_data(
            conn, pid=1, name="Alice", behavior="normal", data_id=1, p_max=75.5,
        )
        self._insert_participant_with_data(
            conn, pid=2, name="Bob", behavior="battery", data_id=1, capacity=200.0,
        )

        alice = DiscParticipant.load_one(conn, where=DiscParticipant.c.id == 1)
        assert alice is not None
        assert isinstance(alice.data, DiscNormalData)
        assert alice.data.p_max == 75.5

        bob = DiscParticipant.load_one(conn, where=DiscParticipant.c.id == 2)
        assert bob is not None
        assert isinstance(bob.data, DiscBatteryData)
        assert bob.data.capacity == 200.0

    def test_variant_specific_fields_accessible(self, conn: Connection) -> None:
        self._insert_participant_with_data(
            conn, pid=1, name="Normal Guy", behavior="normal", data_id=1, p_max=42.0,
        )

        p = DiscParticipant.load_one(conn, where=DiscParticipant.c.id == 1)
        assert p is not None
        assert isinstance(p.data, DiscNormalData)
        assert p.data.p_max == 42.0
        assert p.data.behavior == "normal"
        assert p.data.participant_id == 1


# ---------------------------------------------------------------------------
# Edge case tests
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge cases for relationship handling."""

    def test_model_without_relationships_works(self, conn: Connection) -> None:
        PlainItem(id=1, label="Widget").insert(conn)
        items = PlainItem.load_all(conn)
        assert len(items) == 1
        assert items[0].label == "Widget"

    def test_model_without_relationships_load_one(self, conn: Connection) -> None:
        PlainItem(id=1, label="Widget").insert(conn)
        item = PlainItem.load_one(conn, where=PlainItem.c.id == 1)
        assert item is not None
        assert item.label == "Widget"

    def test_relationship_field_not_in_table_columns(self) -> None:
        col_names = {c.name for c in RelHero.__table__.columns}
        assert "team" not in col_names
        assert "team_id" in col_names

    def test_to_dict_excludes_relationship_fields(self) -> None:
        team = RelTeam(id=1, name="Avengers")
        hero = RelHero(id=1, name="Iron Man", team_id=1, team=team)
        d = hero.to_dict()
        assert "team" not in d
        assert d["team_id"] == 1
        assert d["name"] == "Iron Man"

    def test_discriminated_relationship_not_in_table_columns(self) -> None:
        col_names = {c.name for c in DiscParticipant.__table__.columns}
        assert "data" not in col_names
        assert "behavior" in col_names
