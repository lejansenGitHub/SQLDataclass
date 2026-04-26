"""Edge case tests for joined-table inheritance — relationships, column=False, multi-level, response models."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Connection

from sqldataclass import Field, SQLDataclass
from tests.cases.jti_relationship_models import (
    EmployeeM2O,
    LocationM2O,
    ManagerO2M,
    ReportO2M,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine_and_connection() -> Generator[tuple[object, Connection]]:
    """Yield an in-memory SQLite engine + connection."""
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as connection:
        yield engine, connection


def _create_tables(engine: object, *models: type) -> None:
    """Create tables for the given models, including JTI parent tables."""
    metadata = MetaData()
    for model in models:
        # Copy parent table first for JTI children (FK needs it)
        parent_table = getattr(model, "__jti_parent_table__", None)
        if parent_table is not None and parent_table.name not in metadata.tables:
            parent_table.to_metadata(metadata)
        model.__table__.to_metadata(metadata)  # type: ignore[union-attr]  # __table__ is set dynamically by metaclass
    metadata.create_all(engine)


# ---------------------------------------------------------------------------
# JTI child with one-to-many relationship
# ---------------------------------------------------------------------------


def test_jti_child_with_one_to_many_relationship(engine_and_connection: tuple[object, Connection]) -> None:
    """A JTI child can have its own one-to-many relationship that loads correctly."""
    engine, connection = engine_and_connection
    _create_tables(engine, ManagerO2M, ReportO2M)

    manager = ManagerO2M(name="Alice", department="Eng")
    manager.insert(connection)
    connection.commit()

    ReportO2M(title="Q1 Summary", manager_id=manager.id).insert(connection)  # type: ignore[arg-type]  # id is set after insert
    ReportO2M(title="Q2 Summary", manager_id=manager.id).insert(connection)  # type: ignore[arg-type]  # id is set after insert
    connection.commit()

    loaded = ManagerO2M.load_all(connection)

    # --- Assert ---
    assert len(loaded) == 1
    assert loaded[0].name == "Alice"
    assert loaded[0].department == "Eng"
    assert len(loaded[0].reports) == 2
    assert {r.title for r in loaded[0].reports} == {"Q1 Summary", "Q2 Summary"}


# ---------------------------------------------------------------------------
# JTI child with many-to-one relationship
# ---------------------------------------------------------------------------


def test_jti_child_with_many_to_one_relationship(engine_and_connection: tuple[object, Connection]) -> None:
    """A JTI child can have its own many-to-one relationship that loads correctly."""
    engine, connection = engine_and_connection
    _create_tables(engine, LocationM2O, EmployeeM2O)

    location = LocationM2O(city="Berlin")
    location.insert(connection)
    connection.commit()

    employee = EmployeeM2O(name="Alice", role="Engineer", location_id=location.id)
    employee.insert(connection)
    connection.commit()

    loaded = EmployeeM2O.load_one(connection, where=EmployeeM2O.c.name == "Alice")

    # --- Assert ---
    assert loaded is not None
    assert loaded.name == "Alice"
    assert loaded.role == "Engineer"
    assert loaded.location is not None
    assert loaded.location.city == "Berlin"


# ---------------------------------------------------------------------------
# JTI child with column=False field
# ---------------------------------------------------------------------------


def test_jti_child_with_column_false_field(engine_and_connection: tuple[object, Connection]) -> None:
    """A JTI child can have column=False fields that don't appear in the table."""
    engine, connection = engine_and_connection

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_nocol"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_nocol"
        department: str = ""
        display_label: str = Field(default="(none)", column=False)

    _create_tables(engine, Employee)

    # display_label is NOT in the child table
    child_col_names = {c.name for c in Employee.__table__.columns}

    # --- Assert ---
    assert "display_label" not in child_col_names
    assert "department" in child_col_names

    # Insert and load — display_label gets its default
    Employee(name="Alice", department="Eng", display_label="Alice [Eng]").insert(connection)
    connection.commit()

    loaded = Employee.load_one(connection)

    # --- Assert ---
    assert loaded is not None
    assert loaded.name == "Alice"
    assert loaded.department == "Eng"
    assert loaded.display_label == "(none)"  # default, not the value we inserted


# ---------------------------------------------------------------------------
# Multi-level JTI raises TypeError
# ---------------------------------------------------------------------------


def test_multi_level_jti_raises_type_error() -> None:
    """Creating a grandchild JTI class raises TypeError."""

    class Animal(SQLDataclass, table=True):
        __tablename__ = "jti_animals_multilevel"
        id: int | None = Field(default=None, primary_key=True)
        species: str = ""

    class Dog(Animal, table=True):
        __tablename__ = "jti_dogs_multilevel"
        breed: str = ""

    # --- Assert ---
    with pytest.raises(TypeError, match="multi-level joined-table inheritance"):

        class Puppy(Dog, table=True):
            __tablename__ = "jti_puppies_multilevel"
            age_weeks: int = 0


# ---------------------------------------------------------------------------
# Response model from JTI child
# ---------------------------------------------------------------------------


def test_response_model_from_jti_child() -> None:
    """A response model (table=False) can inherit from a JTI child and gets all fields."""

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_resp"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_resp"
        department: str = ""

    class EmployeeResponse(Employee):
        """Response model — no table, just pydantic fields."""

    # --- Assert ---
    assert not getattr(EmployeeResponse, "__sqldataclass_is_table__", False)
    assert set(EmployeeResponse.__pydantic_fields__) == {"id", "name", "department"}


def test_response_model_from_jti_child_roundtrip(engine_and_connection: tuple[object, Connection]) -> None:
    """A response model created from a loaded JTI instance has all fields."""
    engine, connection = engine_and_connection

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_resp_rt"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_resp_rt"
        department: str = ""

    class EmployeeResponse(Employee):
        """Response model — no table."""

    _create_tables(engine, Employee)

    Employee(name="Alice", department="Eng").insert(connection)
    connection.commit()

    loaded = Employee.load_one(connection)

    # --- Assert ---
    assert loaded is not None
    response = EmployeeResponse.from_parent(loaded)
    assert response.name == "Alice"
    assert response.department == "Eng"
    assert response.id is not None


# ---------------------------------------------------------------------------
# JTI child with parent default_factory fields
# ---------------------------------------------------------------------------


def test_parent_default_factory_inherited(engine_and_connection: tuple[object, Connection]) -> None:
    """Parent fields with default_factory are correctly inherited by JTI child."""
    engine, connection = engine_and_connection

    class Config(SQLDataclass, table=True):
        __tablename__ = "jti_configs_factory"
        id: int | None = Field(default=None, primary_key=True)
        tags: dict[str, object] = Field(default_factory=dict)

    class AppConfig(Config, table=True):
        __tablename__ = "jti_app_configs_factory"
        app_name: str = ""

    _create_tables(engine, AppConfig)

    # tags default is an empty dict (from default_factory)
    app = AppConfig(app_name="MyApp")

    # --- Assert ---
    assert app.tags == {}

    app.insert(connection)
    connection.commit()

    loaded = AppConfig.load_one(connection)

    # --- Assert ---
    assert loaded is not None
    assert loaded.app_name == "MyApp"
