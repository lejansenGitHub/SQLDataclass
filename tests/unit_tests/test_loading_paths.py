"""Unit tests for the multi-row loading paths built on row_dicts — SQLite in-memory."""

from collections.abc import Iterator
from contextlib import contextmanager

from pydantic import field_validator
from sqlalchemy import MetaData, create_engine, insert
from sqlalchemy.engine import Connection

from sqldataclass import Field, Relationship, SQLDataclass, SQLModel


class LpTag(SQLDataclass, table=True):
    __tablename__ = "lp_tags"
    id: int | None = Field(default=None, primary_key=True)
    label: str = ""


class LpHeroTagLink(SQLDataclass, table=True):
    __tablename__ = "lp_hero_tag_links"
    id: int | None = Field(default=None, primary_key=True)
    hero_id: int = Field(foreign_key="lp_heroes.id")
    tag_id: int = Field(foreign_key="lp_tags.id")


class LpHero(SQLDataclass, table=True):
    __tablename__ = "lp_heroes"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    tags: list[LpTag] = Relationship(link_model=LpHeroTagLink)


class LpVehicle(SQLDataclass, table=True):
    __tablename__ = "lp_vehicles"
    __discriminator__ = "type"
    id: int | None = Field(default=None, primary_key=True)
    type: str = ""
    name: str = ""


class LpCar(LpVehicle):
    doors: int | None = None


class LpTruck(LpVehicle):
    payload: float | None = None


_sqlmodel_metadata = MetaData()


class LpModelHero(SQLModel, table=True):
    __tablename__ = "lp_model_heroes"
    metadata = _sqlmodel_metadata
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    display_name: str = Field(default="anonymous", column=False)


class LpStrippedHero(SQLDataclass, table=True):
    __tablename__ = "lp_stripped_heroes"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""

    @field_validator("name")
    @classmethod
    def strip_name(cls, name: str) -> str:
        return name.strip()


class LpStrippedModelHero(SQLModel, table=True):
    __tablename__ = "lp_stripped_model_heroes"
    metadata = _sqlmodel_metadata
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""

    @field_validator("name")
    @classmethod
    def strip_name(cls, name: str) -> str:
        return name.strip()


@contextmanager
def _connection() -> Iterator[Connection]:
    """Connection to a fresh in-memory database; the engine is disposed afterwards."""
    engine = create_engine("sqlite:///:memory:")
    tables = [LpTag.__table__, LpHero.__table__, LpHeroTagLink.__table__, LpVehicle.__table__, LpStrippedHero.__table__]
    SQLDataclass.metadata.create_all(engine, tables=tables)
    _sqlmodel_metadata.create_all(engine)
    try:
        with engine.connect() as conn:
            yield conn
    finally:
        engine.dispose()


def test_many_to_many_load_shares_one_instance_per_target() -> None:
    """Both heroes link tag 1; the PK cache must hand out the same LpTag instance to both."""
    # --- Arrange ---
    with _connection() as conn:
        conn.execute(insert(LpHero.__table__), [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
        conn.execute(insert(LpTag.__table__), [{"id": 1, "label": "fast"}, {"id": 2, "label": "loud"}])
        links = [{"hero_id": 1, "tag_id": 1}, {"hero_id": 1, "tag_id": 2}, {"hero_id": 2, "tag_id": 1}]
        conn.execute(insert(LpHeroTagLink.__table__), links)

        # --- Act ---
        heroes = {hero.id: hero for hero in LpHero.load_all(conn, order_by=LpHero.c.id)}

    # --- Assert ---
    assert sorted(tag.label for tag in heroes[1].tags) == ["fast", "loud"]
    assert [tag.label for tag in heroes[2].tags] == ["fast"]
    fast_of_first = next(tag for tag in heroes[1].tags if tag.id == 1)
    assert heroes[2].tags[0] is fast_of_first


def test_single_table_inheritance_load_builds_each_row_as_its_subtype() -> None:
    """The ``type`` column selects the class, so each row comes back as LpCar or LpTruck with its own field."""
    # --- Arrange ---
    with _connection() as conn:
        rows = [
            {"id": 1, "type": "lpcar", "name": "civic", "doors": 4, "payload": None},
            {"id": 2, "type": "lptruck", "name": "f150", "doors": None, "payload": 1000.0},
        ]
        conn.execute(insert(LpVehicle.__table__), rows)

        # --- Act ---
        vehicles = LpVehicle.load_all(conn, order_by=LpVehicle.c.id)

    # --- Assert ---
    assert [type(vehicle) for vehicle in vehicles] == [LpCar, LpTruck]
    assert vehicles[0].doors == 4
    assert vehicles[1].payload == 1000.0


def test_sqlmodel_load_all_hydrates_columns_and_non_column_defaults() -> None:
    """Column values come from the row; ``display_name`` has no column, so it gets its declared default."""
    # --- Arrange ---
    with _connection() as conn:
        conn.execute(insert(LpModelHero.__table__), [{"id": 1, "name": "deadpond"}])

        # --- Act ---
        heroes = LpModelHero.load_all(conn)

    # --- Assert ---
    assert len(heroes) == 1
    assert heroes[0].id == 1
    assert heroes[0].name == "deadpond"
    assert heroes[0].display_name == "anonymous"


def test_load_all_runs_field_validators_for_sqldataclass_and_sqlmodel() -> None:
    """Both model types validate loaded rows, so the stored padding is stripped by ``strip_name`` in each."""
    # --- Arrange ---
    with _connection() as conn:
        conn.execute(insert(LpStrippedHero.__table__), [{"id": 1, "name": "  padded  "}])
        conn.execute(insert(LpStrippedModelHero.__table__), [{"id": 1, "name": "  padded  "}])

        # --- Act ---
        dataclass_hero = LpStrippedHero.load_all(conn)[0]
        model_hero = LpStrippedModelHero.load_all(conn)[0]

    # --- Assert ---
    assert dataclass_hero.name == "padded"
    assert model_hero.name == "padded"


def test_loaded_sqlmodel_behaves_like_a_constructed_one() -> None:
    """Validation records the set fields, so assignment, equality and copying work on loaded objects."""
    # --- Arrange ---
    with _connection() as conn:
        conn.execute(insert(LpModelHero.__table__), [{"id": 1, "name": "deadpond"}])

        # --- Act ---
        hero = LpModelHero.load_all(conn)[0]

    # --- Assert ---
    assert hero == LpModelHero(id=1, name="deadpond")
    assert hero.model_fields_set == {"id", "name"}
    assert hero.model_copy(update={"name": "copy"}).name == "copy"
    hero.name = "renamed"
    assert hero.name == "renamed"
