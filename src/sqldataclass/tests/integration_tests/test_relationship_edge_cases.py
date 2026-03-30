"""Integration tests — edge cases for relationships, bind, insert, and errors."""

from collections.abc import Generator
from typing import Literal

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, insert
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import MultipleResultsFound

import sqldataclass.model as _model
from sqldataclass import Field, Relationship, SQLDataclass, insert_row

# ---------------------------------------------------------------------------
# Many-to-one models (rec_ prefix)
# ---------------------------------------------------------------------------


class RecTeam(SQLDataclass, table=True):
    __tablename__ = "rec_teams"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    heroes: list["RecHero"] = Relationship(back_populates="team")


class RecHero(SQLDataclass, table=True):
    __tablename__ = "rec_heroes"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    team_id: int | None = Field(default=None, foreign_key="rec_teams.id")
    team: RecTeam | None = Relationship()


# ---------------------------------------------------------------------------
# Discriminated union with 3 variants (rec_ prefix)
# ---------------------------------------------------------------------------


class RecNormalData(SQLDataclass, table=True):
    __tablename__ = "rec_normal_data"
    id: int | None = Field(default=None, primary_key=True)
    participant_id: int = Field(foreign_key="rec_participants.id")
    behavior: Literal["normal"] = "normal"
    p_max: float = 0.0


class RecBatteryData(SQLDataclass, table=True):
    __tablename__ = "rec_battery_data"
    id: int | None = Field(default=None, primary_key=True)
    participant_id: int = Field(foreign_key="rec_participants.id")
    behavior: Literal["battery"] = "battery"
    capacity: float = 0.0


class RecSolarData(SQLDataclass, table=True):
    __tablename__ = "rec_solar_data"
    id: int | None = Field(default=None, primary_key=True)
    participant_id: int = Field(foreign_key="rec_participants.id")
    behavior: Literal["solar"] = "solar"
    panel_area: float = 0.0


class RecParticipant(SQLDataclass, table=True):
    __tablename__ = "rec_participants"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    behavior: str = ""
    data: RecNormalData | RecBatteryData | RecSolarData = Relationship(
        discriminator="behavior",
    )


# ---------------------------------------------------------------------------
# Many-to-many models (rec_ prefix)
# ---------------------------------------------------------------------------


class RecMtmTeam(SQLDataclass, table=True):
    __tablename__ = "rec_mtm_teams"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""


class RecMtmLink(SQLDataclass, table=True):
    __tablename__ = "rec_mtm_links"
    id: int | None = Field(default=None, primary_key=True)
    hero_id: int = Field(foreign_key="rec_mtm_heroes.id")
    team_id: int = Field(foreign_key="rec_mtm_teams.id")


class RecMtmHero(SQLDataclass, table=True):
    __tablename__ = "rec_mtm_heroes"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    teams: list[RecMtmTeam] = Relationship(link_model=RecMtmLink)


class RecMtmHeroR(SQLDataclass, table=True):
    __tablename__ = "rec_mtm_heroes_r"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""


class RecMtmLinkR(SQLDataclass, table=True):
    __tablename__ = "rec_mtm_links_r"
    id: int | None = Field(default=None, primary_key=True)
    hero_id: int = Field(foreign_key="rec_mtm_heroes_r.id")
    team_id: int = Field(foreign_key="rec_mtm_teams_r.id")


class RecMtmTeamR(SQLDataclass, table=True):
    __tablename__ = "rec_mtm_teams_r"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    heroes: list[RecMtmHeroR] = Relationship(link_model=RecMtmLinkR)


# ---------------------------------------------------------------------------
# Bind edge-case models (reb_ prefix)
# ---------------------------------------------------------------------------


class RebHero(SQLDataclass, table=True):
    __tablename__ = "reb_heroes"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    power: int = 0


# ---------------------------------------------------------------------------
# Non-table model with a relationship-like field
# ---------------------------------------------------------------------------


class RecNonTable(SQLDataclass):
    name: str = ""
    team: RecTeam | None = None


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
# Relationship edge cases
# ---------------------------------------------------------------------------


class TestRelationshipEdgeCases:
    """Edge cases for relationship loading."""

    def test_hero_with_null_fk_load_one_returns_team_none(
        self,
        conn: Connection,
    ) -> None:
        """load_one returns hero with team=None for NULL FK."""
        RecHero(id=1, name="Lone Wolf", team_id=None).insert(conn)

        hero = RecHero.load_one(conn, where=RecHero.c.id == 1)
        assert hero is not None
        assert hero.name == "Lone Wolf"
        assert hero.team is None

    def test_team_with_zero_heroes_returns_empty_list(
        self,
        conn: Connection,
    ) -> None:
        """Team with no heroes has heroes=[]."""
        RecTeam(id=1, name="Empty Team").insert(conn)

        team = RecTeam.load_one(conn, where=RecTeam.c.id == 1)
        assert team is not None
        assert team.heroes == []

    def test_discriminated_union_missing_discriminator_value(
        self,
        conn: Connection,
    ) -> None:
        """Row with discriminator value matching no variant raises ValidationError.

        _hydrate_discriminated returns None for unknown discriminator values,
        but pydantic rejects None for the required union field, raising
        a ValidationError.
        """
        conn.execute(
            insert(RecParticipant.__table__).values(
                id=1,
                name="Unknown",
                behavior="wind",
            ),
        )

        with pytest.raises(ValidationError):
            RecParticipant.load_one(conn, where=RecParticipant.c.id == 1)

    def test_discriminated_union_with_three_variants(
        self,
        conn: Connection,
    ) -> None:
        """All three discriminated variants hydrate correctly."""
        conn.execute(
            insert(RecParticipant.__table__).values(
                id=1,
                name="Alice",
                behavior="normal",
            ),
        )
        RecNormalData(id=1, participant_id=1, p_max=100.0).insert(conn)

        conn.execute(
            insert(RecParticipant.__table__).values(
                id=2,
                name="Bob",
                behavior="battery",
            ),
        )
        RecBatteryData(id=1, participant_id=2, capacity=50.0).insert(conn)

        conn.execute(
            insert(RecParticipant.__table__).values(
                id=3,
                name="Carol",
                behavior="solar",
            ),
        )
        RecSolarData(id=1, participant_id=3, panel_area=25.0).insert(conn)

        participants = RecParticipant.load_all(
            conn,
            order_by=RecParticipant.c.id,
        )
        assert len(participants) == 3

        assert isinstance(participants[0].data, RecNormalData)
        assert participants[0].data.p_max == 100.0

        assert isinstance(participants[1].data, RecBatteryData)
        assert participants[1].data.capacity == 50.0

        assert isinstance(participants[2].data, RecSolarData)
        assert participants[2].data.panel_area == 25.0

    def test_one_to_many_children_all_present(
        self,
        conn: Connection,
    ) -> None:
        """One-to-many loads all children (order not guaranteed)."""
        RecTeam(id=1, name="Alpha").insert(conn)
        RecHero(id=1, name="A", team_id=1).insert(conn)
        RecHero(id=2, name="B", team_id=1).insert(conn)
        RecHero(id=3, name="C", team_id=1).insert(conn)

        team = RecTeam.load_one(conn, where=RecTeam.c.id == 1)
        assert team is not None
        assert len(team.heroes) == 3
        names = {h.name for h in team.heroes}
        assert names == {"A", "B", "C"}

    def test_many_to_many_hero_on_zero_teams(
        self,
        conn: Connection,
    ) -> None:
        """Hero with no link rows has teams=[]."""
        RecMtmHero(id=1, name="Loner").insert(conn)

        heroes = RecMtmHero.load_all(conn)
        assert len(heroes) == 1
        assert heroes[0].teams == []

    def test_many_to_many_team_with_zero_heroes(
        self,
        conn: Connection,
    ) -> None:
        """Team (via link) with no heroes has heroes=[] when loaded via load_all."""
        RecMtmTeamR(id=1, name="Ghost Squad").insert(conn)

        teams = RecMtmTeamR.load_all(conn)
        assert len(teams) == 1
        # _populate_collections sets empty list for teams with no matching links
        assert teams[0].heroes == []


# ---------------------------------------------------------------------------
# Bind edge cases
# ---------------------------------------------------------------------------


class TestBindEdgeCases:
    """Edge cases for SQLDataclass.bind()."""

    def test_bind_called_twice_second_overwrites(self) -> None:
        """Second bind() call overwrites the first engine."""
        engine1 = create_engine("sqlite:///:memory:")
        engine2 = create_engine("sqlite:///:memory:")
        SQLDataclass.metadata.create_all(engine1)
        SQLDataclass.metadata.create_all(engine2)

        SQLDataclass.bind(engine1)
        assert _model._BOUND_ENGINE is engine1

        SQLDataclass.bind(engine2)
        assert _model._BOUND_ENGINE is engine2

        # Cleanup
        _model._BOUND_ENGINE = None

    def test_load_all_order_by_only_with_bind(self) -> None:
        """load_all with order_by but no where clause via bound engine."""
        engine = create_engine("sqlite:///:memory:")
        SQLDataclass.metadata.create_all(engine)
        SQLDataclass.bind(engine)
        try:
            with engine.begin() as c:
                insert_row(c, RebHero, {"id": 1, "name": "Zeta", "power": 10})
                insert_row(c, RebHero, {"id": 2, "name": "Alpha", "power": 20})
                insert_row(c, RebHero, {"id": 3, "name": "Mid", "power": 15})

            heroes = RebHero.load_all(order_by=RebHero.c.name)
            assert len(heroes) == 3
            assert heroes[0].name == "Alpha"
            assert heroes[1].name == "Mid"
            assert heroes[2].name == "Zeta"
        finally:
            _model._BOUND_ENGINE = None

    def test_load_all_where_and_order_by_with_bind(self) -> None:
        """load_all with both where AND order_by via bound engine."""
        engine = create_engine("sqlite:///:memory:")
        SQLDataclass.metadata.create_all(engine)
        SQLDataclass.bind(engine)
        try:
            with engine.begin() as c:
                insert_row(c, RebHero, {"id": 1, "name": "Zeta", "power": 30})
                insert_row(c, RebHero, {"id": 2, "name": "Alpha", "power": 5})
                insert_row(c, RebHero, {"id": 3, "name": "Beta", "power": 30})

            heroes = RebHero.load_all(
                where=RebHero.c.power == 30,
                order_by=RebHero.c.name,
            )
            assert len(heroes) == 2
            assert heroes[0].name == "Beta"
            assert heroes[1].name == "Zeta"
        finally:
            _model._BOUND_ENGINE = None


# ---------------------------------------------------------------------------
# Insert edge cases
# ---------------------------------------------------------------------------


class TestInsertEdgeCases:
    """Edge cases for insert and to_dict with relationship fields."""

    def test_insert_model_with_relationship_fields(
        self,
        conn: Connection,
    ) -> None:
        """insert() on a model with relationship fields does not insert them."""
        RecTeam(id=1, name="Avengers").insert(conn)
        hero = RecHero(
            id=1,
            name="Iron Man",
            team_id=1,
            team=RecTeam(id=1, name="Avengers"),
        )
        hero.insert(conn)

        loaded = RecHero.load_one(conn, where=RecHero.c.id == 1)
        assert loaded is not None
        assert loaded.name == "Iron Man"
        assert loaded.team_id == 1

    def test_insert_many_with_relationship_fields(
        self,
        conn: Connection,
    ) -> None:
        """insert_many with models that have relationship fields — flat dict only."""
        RecTeam(id=1, name="T1").insert(conn)
        heroes = [
            RecHero(id=1, name="H1", team_id=1, team=RecTeam(id=1, name="T1")),
            RecHero(id=2, name="H2", team_id=1, team=RecTeam(id=1, name="T1")),
        ]
        RecHero.insert_many(conn, objects=heroes)

        loaded = RecHero.load_all(conn, order_by=RecHero.c.id)
        assert len(loaded) == 2
        assert loaded[0].name == "H1"
        assert loaded[1].name == "H2"

    def test_to_dict_with_list_relationship_returns_flat(self) -> None:
        """to_dict on model with list relationship returns flat dict (no heroes key)."""
        team = RecTeam(id=1, name="Avengers")
        d = team.to_dict()
        assert "heroes" not in d
        assert d["name"] == "Avengers"
        assert d["id"] == 1

    def test_to_dict_excludes_scalar_relationship(self) -> None:
        """to_dict on hero with team relationship returns flat dict."""
        hero = RecHero(
            id=1,
            name="Iron Man",
            team_id=1,
            team=RecTeam(id=1, name="Avengers"),
        )
        d = hero.to_dict()
        assert "team" not in d
        assert d["team_id"] == 1
        assert d["name"] == "Iron Man"


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestErrorCases:
    """Error and edge cases for load_one and non-table models."""

    def test_load_one_multiple_matches_raises(
        self,
        conn: Connection,
    ) -> None:
        """load_one where clause matches multiple rows raises MultipleResultsFound."""
        RecTeam(id=1, name="Same").insert(conn)
        RecHero(id=1, name="Hero A", team_id=1).insert(conn)
        RecHero(id=2, name="Hero B", team_id=1).insert(conn)

        # load_one uses one_or_none() which raises when multiple rows match
        with pytest.raises(MultipleResultsFound):
            RecHero.load_one(conn, where=RecHero.c.team_id == 1)

    def test_non_table_model_relationship_field_is_regular(self) -> None:
        """Relationship on non-table model is just a regular pydantic field."""
        obj = RecNonTable(name="test", team=RecTeam(id=1, name="X"))
        assert obj.team is not None
        assert obj.team.name == "X"

        obj_none = RecNonTable(name="solo", team=None)
        assert obj_none.team is None
