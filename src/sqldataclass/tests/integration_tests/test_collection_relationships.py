"""Integration tests — one-to-many and many-to-many collection relationships."""

from collections.abc import Generator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from sqldataclass import Field, Relationship, SQLDataclass, flatten_for_table

# ---------------------------------------------------------------------------
# One-to-many models (Team → Heroes)
# ---------------------------------------------------------------------------


class OtmHero(SQLDataclass, table=True):
    __tablename__ = "otm_heroes"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    team_id: int | None = Field(default=None, foreign_key="otm_teams.id")


class OtmTeam(SQLDataclass, table=True):
    __tablename__ = "otm_teams"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    heroes: list[OtmHero] = Relationship(back_populates="team")


# Forward-ref variant: child type referenced as a string
class OtmFwdHero(SQLDataclass, table=True):
    __tablename__ = "otm_fwd_heroes"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    team_id: int | None = Field(default=None, foreign_key="otm_fwd_teams.id")


class OtmFwdTeam(SQLDataclass, table=True):
    __tablename__ = "otm_fwd_teams"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    heroes: list["OtmFwdHero"] = Relationship(back_populates="team")


# ---------------------------------------------------------------------------
# Many-to-many models — hero-side view (Hero.teams)
# ---------------------------------------------------------------------------


class MtmTeamA(SQLDataclass, table=True):
    __tablename__ = "mtm_teams_a"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""


class MtmLinkA(SQLDataclass, table=True):
    __tablename__ = "mtm_links_a"
    id: int | None = Field(default=None, primary_key=True)
    hero_id: int = Field(foreign_key="mtm_heroes_a.id")
    team_id: int = Field(foreign_key="mtm_teams_a.id")


class MtmHeroA(SQLDataclass, table=True):
    __tablename__ = "mtm_heroes_a"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    teams: list[MtmTeamA] = Relationship(link_model=MtmLinkA)


# ---------------------------------------------------------------------------
# Many-to-many models — team-side view (Team.heroes)
# ---------------------------------------------------------------------------


class MtmHeroB(SQLDataclass, table=True):
    __tablename__ = "mtm_heroes_b"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""


class MtmLinkB(SQLDataclass, table=True):
    __tablename__ = "mtm_links_b"
    id: int | None = Field(default=None, primary_key=True)
    hero_id: int = Field(foreign_key="mtm_heroes_b.id")
    team_id: int = Field(foreign_key="mtm_teams_b.id")


class MtmTeamB(SQLDataclass, table=True):
    __tablename__ = "mtm_teams_b"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    heroes: list[MtmHeroB] = Relationship(link_model=MtmLinkB)


# ---------------------------------------------------------------------------
# Edge-case model: no relationships at all
# ---------------------------------------------------------------------------


class EdgePlainWidget(SQLDataclass, table=True):
    __tablename__ = "edge_plain_widgets"
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
# One-to-many tests
# ---------------------------------------------------------------------------


class TestOneToMany:
    """Team → Heroes: one-to-many collection relationship."""

    def test_team_with_multiple_heroes(self, conn: Connection) -> None:
        OtmTeam(id=1, name="Avengers").insert(conn)
        OtmHero(id=1, name="Iron Man", team_id=1).insert(conn)
        OtmHero(id=2, name="Thor", team_id=1).insert(conn)
        OtmHero(id=3, name="Hulk", team_id=1).insert(conn)

        teams = OtmTeam.load_all(conn)
        assert len(teams) == 1
        team = teams[0]
        assert len(team.heroes) == 3
        hero_names = {h.name for h in team.heroes}
        assert hero_names == {"Iron Man", "Thor", "Hulk"}

    def test_team_with_no_heroes(self, conn: Connection) -> None:
        OtmTeam(id=1, name="Empty Team").insert(conn)

        teams = OtmTeam.load_all(conn)
        assert len(teams) == 1
        assert teams[0].heroes == []

    def test_load_one_populates_heroes(self, conn: Connection) -> None:
        OtmTeam(id=1, name="Avengers").insert(conn)
        OtmHero(id=1, name="Iron Man", team_id=1).insert(conn)
        OtmHero(id=2, name="Thor", team_id=1).insert(conn)

        team = OtmTeam.load_one(conn, where=OtmTeam.c.id == 1)
        assert team is not None
        assert len(team.heroes) == 2
        hero_names = {h.name for h in team.heroes}
        assert hero_names == {"Iron Man", "Thor"}

    def test_heroes_are_correct_type(self, conn: Connection) -> None:
        OtmTeam(id=1, name="Avengers").insert(conn)
        OtmHero(id=1, name="Iron Man", team_id=1).insert(conn)

        teams = OtmTeam.load_all(conn)
        for hero in teams[0].heroes:
            assert isinstance(hero, OtmHero)

    def test_forward_reference_resolves(self, conn: Connection) -> None:
        OtmFwdTeam(id=1, name="X-Men").insert(conn)
        OtmFwdHero(id=1, name="Wolverine", team_id=1).insert(conn)
        OtmFwdHero(id=2, name="Cyclops", team_id=1).insert(conn)

        teams = OtmFwdTeam.load_all(conn)
        assert len(teams) == 1
        assert len(teams[0].heroes) == 2
        hero_names = {h.name for h in teams[0].heroes}
        assert hero_names == {"Wolverine", "Cyclops"}

    def test_two_teams_children_correctly_grouped(self, conn: Connection) -> None:
        OtmTeam(id=1, name="Avengers").insert(conn)
        OtmTeam(id=2, name="X-Men").insert(conn)
        OtmHero(id=1, name="Iron Man", team_id=1).insert(conn)
        OtmHero(id=2, name="Thor", team_id=1).insert(conn)
        OtmHero(id=3, name="Wolverine", team_id=2).insert(conn)

        teams = OtmTeam.load_all(conn, order_by=OtmTeam.c.id)
        assert len(teams) == 2

        avengers = teams[0]
        assert avengers.name == "Avengers"
        assert len(avengers.heroes) == 2
        assert {h.name for h in avengers.heroes} == {"Iron Man", "Thor"}

        xmen = teams[1]
        assert xmen.name == "X-Men"
        assert len(xmen.heroes) == 1
        assert xmen.heroes[0].name == "Wolverine"

    def test_heroes_list_contains_all_children(self, conn: Connection) -> None:
        OtmTeam(id=1, name="Avengers").insert(conn)
        for i in range(5):
            OtmHero(id=i + 1, name=f"Hero_{i}", team_id=1).insert(conn)

        team = OtmTeam.load_one(conn, where=OtmTeam.c.id == 1)
        assert team is not None
        assert len(team.heroes) == 5
        expected_names = {f"Hero_{i}" for i in range(5)}
        assert {h.name for h in team.heroes} == expected_names


# ---------------------------------------------------------------------------
# Many-to-many tests
# ---------------------------------------------------------------------------


class TestManyToMany:
    """Hero <-> Team via link table: many-to-many collection relationship."""

    def test_hero_belongs_to_multiple_teams(self, conn: Connection) -> None:
        MtmHeroA(id=1, name="Wolverine").insert(conn)
        MtmTeamA(id=1, name="X-Men").insert(conn)
        MtmTeamA(id=2, name="Avengers").insert(conn)
        MtmLinkA(id=1, hero_id=1, team_id=1).insert(conn)
        MtmLinkA(id=2, hero_id=1, team_id=2).insert(conn)

        hero = MtmHeroA.load_one(conn, where=MtmHeroA.c.id == 1)
        assert hero is not None
        assert len(hero.teams) == 2
        team_names = {t.name for t in hero.teams}
        assert team_names == {"X-Men", "Avengers"}

    def test_team_has_multiple_heroes(self, conn: Connection) -> None:
        MtmTeamB(id=1, name="Avengers").insert(conn)
        MtmHeroB(id=1, name="Iron Man").insert(conn)
        MtmHeroB(id=2, name="Thor").insert(conn)
        MtmLinkB(id=1, hero_id=1, team_id=1).insert(conn)
        MtmLinkB(id=2, hero_id=2, team_id=1).insert(conn)

        team = MtmTeamB.load_one(conn, where=MtmTeamB.c.id == 1)
        assert team is not None
        assert len(team.heroes) == 2
        hero_names = {h.name for h in team.heroes}
        assert hero_names == {"Iron Man", "Thor"}

    def test_hero_with_no_links_has_empty_list(self, conn: Connection) -> None:
        MtmHeroA(id=1, name="Loner").insert(conn)

        hero = MtmHeroA.load_one(conn, where=MtmHeroA.c.id == 1)
        assert hero is not None
        assert hero.teams == []

    def test_team_with_no_links_has_empty_list(self, conn: Connection) -> None:
        MtmTeamB(id=1, name="Empty Squad").insert(conn)

        team = MtmTeamB.load_one(conn, where=MtmTeamB.c.id == 1)
        assert team is not None
        assert team.heroes == []

    def test_bidirectional_populated(self, conn: Connection) -> None:
        # Hero-side: MtmHeroA.teams
        MtmHeroA(id=1, name="Wolverine").insert(conn)
        MtmTeamA(id=1, name="X-Men").insert(conn)
        MtmTeamA(id=2, name="Avengers").insert(conn)
        MtmLinkA(id=1, hero_id=1, team_id=1).insert(conn)
        MtmLinkA(id=2, hero_id=1, team_id=2).insert(conn)

        hero = MtmHeroA.load_one(conn, where=MtmHeroA.c.id == 1)
        assert hero is not None
        assert {t.name for t in hero.teams} == {"X-Men", "Avengers"}

        # Team-side: MtmTeamB.heroes
        MtmTeamB(id=1, name="Avengers").insert(conn)
        MtmHeroB(id=1, name="Iron Man").insert(conn)
        MtmHeroB(id=2, name="Wolverine").insert(conn)
        MtmLinkB(id=1, hero_id=1, team_id=1).insert(conn)
        MtmLinkB(id=2, hero_id=2, team_id=1).insert(conn)

        team = MtmTeamB.load_one(conn, where=MtmTeamB.c.id == 1)
        assert team is not None
        assert {h.name for h in team.heroes} == {"Iron Man", "Wolverine"}

    def test_load_one_many_to_many(self, conn: Connection) -> None:
        MtmHeroA(id=1, name="Iron Man").insert(conn)
        MtmTeamA(id=1, name="Avengers").insert(conn)
        MtmTeamA(id=2, name="X-Men").insert(conn)
        MtmLinkA(id=1, hero_id=1, team_id=1).insert(conn)
        MtmLinkA(id=2, hero_id=1, team_id=2).insert(conn)

        hero = MtmHeroA.load_one(conn, where=MtmHeroA.c.name == "Iron Man")
        assert hero is not None
        assert hero.name == "Iron Man"
        assert len(hero.teams) == 2


# ---------------------------------------------------------------------------
# Edge-case tests
# ---------------------------------------------------------------------------


class TestCollectionEdgeCases:
    """Edge cases for collection relationship handling."""

    def test_flatten_for_table_excludes_list_fields(self) -> None:
        team = OtmTeam(id=1, name="Avengers")
        flat = flatten_for_table(team)
        assert "heroes" not in flat
        assert flat["id"] == 1
        assert flat["name"] == "Avengers"

    def test_to_dict_excludes_list_fields(self) -> None:
        team = OtmTeam(id=1, name="Avengers")
        d = team.to_dict()
        assert "heroes" not in d
        assert d["id"] == 1
        assert d["name"] == "Avengers"

    def test_insert_does_not_try_to_insert_list(self, conn: Connection) -> None:
        OtmTeam(id=1, name="Avengers").insert(conn)
        # If insert tried to include the list field, the DB would raise.
        teams = OtmTeam.load_all(conn)
        assert len(teams) == 1
        assert teams[0].name == "Avengers"

    def test_model_without_collection_relationships_fast_path(self, conn: Connection) -> None:
        EdgePlainWidget(id=1, label="Sprocket").insert(conn)
        EdgePlainWidget(id=2, label="Gear").insert(conn)

        widgets = EdgePlainWidget.load_all(conn, order_by=EdgePlainWidget.c.id)
        assert len(widgets) == 2
        assert widgets[0].label == "Sprocket"
        assert widgets[1].label == "Gear"

    def test_empty_database_returns_models_with_empty_lists(self, conn: Connection) -> None:
        teams = OtmTeam.load_all(conn)
        assert teams == []

    def test_empty_database_load_one_returns_none(self, conn: Connection) -> None:
        team = OtmTeam.load_one(conn, where=OtmTeam.c.id == 999)
        assert team is None

    def test_mtm_flatten_for_table_excludes_list_fields(self) -> None:
        hero = MtmHeroA(id=1, name="Wolverine")
        flat = flatten_for_table(hero)
        assert "teams" not in flat
        assert flat["id"] == 1
        assert flat["name"] == "Wolverine"
