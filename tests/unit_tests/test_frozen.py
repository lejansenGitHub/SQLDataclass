"""Tests for the ``frozen`` class option."""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import FrozenInstanceError

import pytest
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Connection

from sqldataclass import Field, Relationship, SQLDataclass


class FrzTeam(SQLDataclass, table=True, frozen=True):
    __tablename__ = "frz_teams"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""


class FrzHero(SQLDataclass, table=True, frozen=True):
    __tablename__ = "frz_heroes"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    team: FrzTeam | None = Relationship()


class FrzMutable(SQLDataclass, table=True):
    __tablename__ = "frz_mutable"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""


@pytest.fixture
def connection() -> Generator[Connection]:
    """Yield an in-memory SQLite connection with the frozen test tables created."""
    metadata = MetaData()
    for model in (FrzTeam, FrzHero):
        model.__table__.to_metadata(metadata)
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    with engine.connect() as conn:
        yield conn


def test_frozen_model_rejects_attribute_assignment() -> None:
    """A frozen model is a frozen dataclass, so field assignment raises FrozenInstanceError."""
    # --- Input ---
    team = FrzTeam(name="Avengers")

    # --- Execute ---
    with pytest.raises(FrozenInstanceError):
        team.name = "X-Men"

    # --- Assert ---
    assert FrzTeam.__dataclass_params__.frozen is True
    assert team.name == "Avengers"


def test_models_are_mutable_by_default() -> None:
    """Without ``frozen=True`` the existing mutable behaviour is unchanged."""
    # --- Input ---
    item = FrzMutable(name="before")

    # --- Execute ---
    item.name = "after"

    # --- Assert ---
    assert FrzMutable.__dataclass_params__.frozen is False
    assert item.name == "after"


def test_frozen_instances_hash_by_value() -> None:
    """Frozen dataclasses with ``eq=True`` get a value-based ``__hash__``, so equal instances dedupe in a set."""
    # --- Input ---
    first = FrzTeam(id=1, name="Avengers")
    second = FrzTeam(id=1, name="Avengers")
    other = FrzTeam(id=2, name="X-Men")

    # --- Execute ---
    deduplicated = {first, second, other}

    # --- Assert ---
    assert first == second
    assert hash(first) == hash(second)
    assert deduplicated == {first, other}


def test_insert_hydrates_generated_primary_key_on_frozen_instance(connection: Connection) -> None:
    """Insert writes DB-generated columns back with ``object.__setattr__``, which bypasses the frozen guard.

    The team is unpersisted, so cascading insert must also hydrate its PK and copy it into the hero's FK.
    """
    # --- Input ---
    team = FrzTeam(name="Avengers")
    hero = FrzHero(name="Spider-Man", team=team)

    # --- Execute ---
    hero.insert(connection)

    # --- Assert ---
    assert team.id == 1
    assert hero.id == 1
    assert hero.team_id == 1
    with pytest.raises(FrozenInstanceError):
        hero.name = "Peter"


def test_load_all_returns_frozen_instances_with_stitched_relationships(connection: Connection) -> None:
    """Relationship stitching assigns the loaded target with ``object.__setattr__``, so frozen models load fully.

    Both rows are inserted and read back, so the hero must carry its team and stay immutable.
    """
    # --- Input ---
    FrzHero(name="Thor", team=FrzTeam(name="Asgard")).insert(connection)

    # --- Execute ---
    heroes = FrzHero.load_all(connection)

    # --- Assert ---
    assert [hero.name for hero in heroes] == ["Thor"]
    assert heroes[0].team == FrzTeam(id=1, name="Asgard")
    with pytest.raises(FrozenInstanceError):
        heroes[0].team = None


def test_single_table_inheritance_children_follow_parent_unless_overridden() -> None:
    """STI children are rebuilt from merged annotations, so frozen must be carried over explicitly.

    The default inherits the parent's setting and ``frozen=False`` opts a child out.
    """

    # --- Input ---
    class FrzVehicle(SQLDataclass, table=True, frozen=True):
        __tablename__ = "frz_vehicles"
        __discriminator__ = "type"
        id: int | None = Field(default=None, primary_key=True)
        type: str = ""
        name: str = ""

    class FrzCar(FrzVehicle):
        doors: int | None = None

    class FrzVan(FrzVehicle, frozen=False):
        payload: float | None = None

    # --- Execute ---
    van = FrzVan(name="Transit")
    van.payload = 1000.0

    # --- Assert ---
    assert FrzCar.__dataclass_params__.frozen is True
    assert FrzVan.__dataclass_params__.frozen is False
    with pytest.raises(FrozenInstanceError):
        FrzCar(name="Civic").doors = 2
    assert van.payload == 1000.0


def test_response_model_follows_parent_unless_overridden() -> None:
    """Response models default to the parent's frozen setting and can override it either way."""

    # --- Input ---
    class FrzParent(SQLDataclass, table=True, frozen=True):
        __tablename__ = "frz_parent"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class FrzResponse(FrzParent, table=False):
        """Inherits frozen."""

    class FrzEditableResponse(FrzParent, table=False, frozen=False):
        """Opts out."""

    class FrzFrozenResponseOfMutable(FrzMutable, table=False, frozen=True):
        """Opts in although the parent is mutable."""

    # --- Execute ---
    editable = FrzEditableResponse(name="a")
    editable.name = "b"

    # --- Assert ---
    assert FrzResponse.__dataclass_params__.frozen is True
    assert FrzEditableResponse.__dataclass_params__.frozen is False
    assert FrzFrozenResponseOfMutable.__dataclass_params__.frozen is True
    assert editable.name == "b"
    with pytest.raises(FrozenInstanceError):
        FrzFrozenResponseOfMutable(name="a").name = "b"


def test_joined_table_inheritance_child_follows_parent() -> None:
    """A JTI child gets its own table but is rebuilt like the other children, so it inherits frozen too."""

    # --- Input ---
    class FrzAnimal(SQLDataclass, table=True, frozen=True):
        __tablename__ = "frz_animals"
        id: int | None = Field(default=None, primary_key=True)
        species: str = ""

    class FrzDog(FrzAnimal, table=True):
        __tablename__ = "frz_dogs"
        breed: str = ""

    # --- Execute ---
    dog = FrzDog(species="dog", breed="lab")

    # --- Assert ---
    assert FrzDog.__dataclass_params__.frozen is True
    with pytest.raises(FrozenInstanceError):
        dog.breed = "pug"
