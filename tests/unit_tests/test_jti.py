"""Tests for joined-table inheritance (JTI) — separate parent + child tables with auto-JOIN."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Connection

from sqldataclass import Field, SQLDataclass

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine_and_connection() -> Generator[tuple[object, Connection]]:
    """Yield an in-memory SQLite engine + connection with JTI tables created."""
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as connection:
        yield engine, connection


def _create_tables(engine: object, *models: type) -> None:
    """Create tables for the given models in a fresh MetaData."""
    metadata = MetaData()
    for model in models:
        model.__table__.to_metadata(metadata)  # type: ignore[union-attr]  # __table__ is set dynamically by metaclass
        # Also copy parent table for JTI children
        parent_table = getattr(model, "__jti_parent_table__", None)
        if parent_table is not None and parent_table.name not in metadata.tables:
            parent_table.to_metadata(metadata)
    metadata.create_all(engine)


# ---------------------------------------------------------------------------
# Table structure
# ---------------------------------------------------------------------------


def test_parent_table_has_only_parent_columns() -> None:
    """The parent table contains only parent-declared columns."""

    class Animal(SQLDataclass, table=True):
        __tablename__ = "jti_animals_struct"
        id: int | None = Field(default=None, primary_key=True)
        species: str = ""

    # --- Assert ---
    assert {c.name for c in Animal.__table__.columns} == {"id", "species"}


def test_child_table_has_pk_fk_and_own_columns() -> None:
    """The child table has a PK/FK to parent and only child-specific columns."""

    class Animal(SQLDataclass, table=True):
        __tablename__ = "jti_animals_struct2"
        id: int | None = Field(default=None, primary_key=True)
        species: str = ""

    class Dog(Animal, table=True):
        __tablename__ = "jti_dogs_struct"
        breed: str = ""

    child_col_names = {c.name for c in Dog.__table__.columns}
    pk_cols = [c for c in Dog.__table__.columns if c.primary_key]
    fk_targets = [str(fk.target_fullname) for col in Dog.__table__.columns for fk in col.foreign_keys]

    # --- Assert ---
    assert child_col_names == {"id", "breed"}
    assert len(pk_cols) == 1
    assert pk_cols[0].name == "id"
    assert "jti_animals_struct2.id" in fk_targets


def test_child_has_all_python_fields() -> None:
    """The child pydantic class has both parent and child fields."""

    class Vehicle(SQLDataclass, table=True):
        __tablename__ = "jti_vehicles_fields"
        id: int | None = Field(default=None, primary_key=True)
        make: str = ""

    class Truck(Vehicle, table=True):
        __tablename__ = "jti_trucks_fields"
        payload_tons: float = 0.0

    # --- Assert ---
    assert set(Truck.__pydantic_fields__) == {"id", "make", "payload_tons"}


def test_jti_child_marker() -> None:
    """JTI children are marked with __sqldataclass_is_jti_child__."""

    class Base(SQLDataclass, table=True):
        __tablename__ = "jti_base_marker"
        id: int | None = Field(default=None, primary_key=True)

    class Child(Base, table=True):
        __tablename__ = "jti_child_marker"
        value: str = ""

    # --- Assert ---
    assert getattr(Child, "__sqldataclass_is_jti_child__", False) is True
    assert getattr(Base, "__sqldataclass_is_jti_child__", False) is False


# ---------------------------------------------------------------------------
# Merged column accessor
# ---------------------------------------------------------------------------


def test_merged_columns_resolves_parent_and_child() -> None:
    """Employee.c.name resolves parent columns, Employee.c.department resolves child columns."""

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_merged"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_merged"
        department: str = ""

    # --- Assert ---
    assert str(Employee.c.name) == "jti_persons_merged.name"
    assert str(Employee.c.department) == "jti_employees_merged.department"
    assert str(Employee.c["name"]) == "jti_persons_merged.name"
    assert str(Employee.c["department"]) == "jti_employees_merged.department"


# ---------------------------------------------------------------------------
# Insert roundtrip
# ---------------------------------------------------------------------------


def test_insert_creates_rows_in_both_tables(engine_and_connection: tuple[object, Connection]) -> None:
    """Insert populates both parent and child tables."""
    engine, connection = engine_and_connection

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_insert"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_insert"
        department: str = ""

    _create_tables(engine, Employee)

    employee = Employee(name="Alice", department="Engineering")
    employee.insert(connection)
    connection.commit()

    # Verify parent row
    parent_row = connection.execute(Person.__table__.select()).fetchone()
    # Verify child row
    child_row = connection.execute(Employee.__table__.select()).fetchone()

    # --- Assert ---
    assert parent_row is not None
    assert parent_row[1] == "Alice"
    assert child_row is not None
    assert child_row[1] == "Engineering"
    assert employee.id is not None
    assert employee.id == parent_row[0] == child_row[0]


def test_insert_propagates_autoincrement_pk(engine_and_connection: tuple[object, Connection]) -> None:
    """Insert generates PK from parent table and sets it on the instance."""
    engine, connection = engine_and_connection

    class Item(SQLDataclass, table=True):
        __tablename__ = "jti_items_pk"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class SpecialItem(Item, table=True):
        __tablename__ = "jti_special_items_pk"
        rarity: str = "common"

    _create_tables(engine, SpecialItem)

    item1 = SpecialItem(name="Sword", rarity="rare")
    item1.insert(connection)
    item2 = SpecialItem(name="Shield", rarity="legendary")
    item2.insert(connection)
    connection.commit()

    # --- Assert ---
    assert item1.id == 1
    assert item2.id == 2


# ---------------------------------------------------------------------------
# Load roundtrip
# ---------------------------------------------------------------------------


def test_load_all_returns_child_instances_with_all_fields(engine_and_connection: tuple[object, Connection]) -> None:
    """load_all auto-JOINs and returns child instances with parent + child fields."""
    engine, connection = engine_and_connection

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_load"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""
        email: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_load"
        department: str = ""
        salary: float = 0.0

    _create_tables(engine, Employee)

    Employee(name="Alice", email="a@b.com", department="Eng", salary=100.0).insert(connection)
    Employee(name="Bob", email="b@b.com", department="Sales", salary=90.0).insert(connection)
    connection.commit()

    results = Employee.load_all(connection)

    # --- Assert ---
    assert len(results) == 2
    assert results[0].name == "Alice"
    assert results[0].email == "a@b.com"
    assert results[0].department == "Eng"
    assert results[0].salary == 100.0
    assert results[1].name == "Bob"
    assert results[1].department == "Sales"


def test_load_one_returns_single_instance(engine_and_connection: tuple[object, Connection]) -> None:
    """load_one returns a single JTI child instance."""
    engine, connection = engine_and_connection

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_loadone"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_loadone"
        department: str = ""

    _create_tables(engine, Employee)

    Employee(name="Alice", department="Eng").insert(connection)
    Employee(name="Bob", department="Sales").insert(connection)
    connection.commit()

    result = Employee.load_one(connection, where=Employee.c.name == "Bob")

    # --- Assert ---
    assert result is not None
    assert result.name == "Bob"
    assert result.department == "Sales"


def test_load_one_returns_none_when_not_found(engine_and_connection: tuple[object, Connection]) -> None:
    """load_one returns None when no matching row exists."""
    engine, connection = engine_and_connection

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_loadnone"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_loadnone"
        department: str = ""

    _create_tables(engine, Employee)

    # --- Assert ---
    assert Employee.load_one(connection, where=Employee.c.name == "Nobody") is None


def test_load_with_where_on_parent_column(engine_and_connection: tuple[object, Connection]) -> None:
    """WHERE clause can reference parent columns via Employee.c.name."""
    engine, connection = engine_and_connection

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_where"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_where"
        department: str = ""

    _create_tables(engine, Employee)

    Employee(name="Alice", department="Eng").insert(connection)
    Employee(name="Bob", department="Sales").insert(connection)
    connection.commit()

    results = Employee.load_all(connection, where=Employee.c.name == "Alice")

    # --- Assert ---
    assert len(results) == 1
    assert results[0].name == "Alice"


def test_load_with_order_by(engine_and_connection: tuple[object, Connection]) -> None:
    """ORDER BY works on JTI queries."""
    engine, connection = engine_and_connection

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_order"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_order"
        department: str = ""

    _create_tables(engine, Employee)

    Employee(name="Charlie", department="Eng").insert(connection)
    Employee(name="Alice", department="Sales").insert(connection)
    Employee(name="Bob", department="Eng").insert(connection)
    connection.commit()

    results = Employee.load_all(connection, order_by=Employee.c.name)

    # --- Assert ---
    assert [r.name for r in results] == ["Alice", "Bob", "Charlie"]


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------


def test_update_child_fields(engine_and_connection: tuple[object, Connection]) -> None:
    """Update routes child fields to the child table."""
    engine, connection = engine_and_connection

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_update"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_update"
        department: str = ""

    _create_tables(engine, Employee)

    Employee(name="Alice", department="Eng").insert(connection)
    connection.commit()

    Employee.update({"department": "Sales"}, connection, where=Employee.c.name == "Alice")
    connection.commit()

    result = Employee.load_one(connection, where=Employee.c.name == "Alice")

    # --- Assert ---
    assert result is not None
    assert result.department == "Sales"


def test_update_parent_fields(engine_and_connection: tuple[object, Connection]) -> None:
    """Update routes parent fields to the parent table."""
    engine, connection = engine_and_connection

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_update_parent"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_update_parent"
        department: str = ""

    _create_tables(engine, Employee)

    Employee(name="Alice", department="Eng").insert(connection)
    connection.commit()

    Employee.update({"name": "Alicia"}, connection, where=Employee.c.department == "Eng")
    connection.commit()

    result = Employee.load_one(connection, where=Employee.c.department == "Eng")

    # --- Assert ---
    assert result is not None
    assert result.name == "Alicia"


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


def test_delete_removes_from_both_tables(engine_and_connection: tuple[object, Connection]) -> None:
    """Delete removes rows from both child and parent tables."""
    engine, connection = engine_and_connection

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_delete"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_delete"
        department: str = ""

    _create_tables(engine, Employee)

    Employee(name="Alice", department="Eng").insert(connection)
    Employee(name="Bob", department="Sales").insert(connection)
    connection.commit()

    deleted = Employee.delete(connection, where=Employee.c.name == "Alice")
    connection.commit()

    remaining = Employee.load_all(connection)
    parent_rows = connection.execute(Person.__table__.select()).fetchall()

    # --- Assert ---
    assert deleted == 1
    assert len(remaining) == 1
    assert remaining[0].name == "Bob"
    assert len(parent_rows) == 1


# ---------------------------------------------------------------------------
# insert_many
# ---------------------------------------------------------------------------


def test_insert_many(engine_and_connection: tuple[object, Connection]) -> None:
    """insert_many inserts multiple JTI instances."""
    engine, connection = engine_and_connection

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_many"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_many"
        department: str = ""

    _create_tables(engine, Employee)

    objects = [
        Employee(name="Alice", department="Eng"),
        Employee(name="Bob", department="Sales"),
        Employee(name="Charlie", department="Ops"),
    ]
    Employee.insert_many(connection, objects=objects)
    connection.commit()

    results = Employee.load_all(connection)

    # --- Assert ---
    assert len(results) == 3
    assert {r.name for r in results} == {"Alice", "Bob", "Charlie"}


# ---------------------------------------------------------------------------
# to_dict
# ---------------------------------------------------------------------------


def test_to_dict_includes_all_fields(engine_and_connection: tuple[object, Connection]) -> None:
    """to_dict returns all fields (parent + child) as a flat dict."""
    engine, connection = engine_and_connection

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_todict"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_todict"
        department: str = ""

    _create_tables(engine, Employee)

    employee = Employee(name="Alice", department="Eng")
    employee.insert(connection)
    connection.commit()

    result = employee.to_dict()

    # --- Assert ---
    assert result["name"] == "Alice"
    assert result["department"] == "Eng"
    assert "id" in result


# ---------------------------------------------------------------------------
# select
# ---------------------------------------------------------------------------


def test_select_returns_child_table_select() -> None:
    """select() returns a SELECT for the child table."""

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_select"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_select"
        department: str = ""

    query = Employee.select()

    # --- Assert ---
    assert "jti_employees_select" in str(query)


# ---------------------------------------------------------------------------
# __table_args__ on JTI child
# ---------------------------------------------------------------------------


def test_table_args_on_jti_child() -> None:
    """__table_args__ on a JTI child is passed through to the child table."""
    from sqlalchemy import UniqueConstraint

    class Person(SQLDataclass, table=True):
        __tablename__ = "jti_persons_args"
        id: int | None = Field(default=None, primary_key=True)
        name: str = ""

    class Employee(Person, table=True):
        __tablename__ = "jti_employees_args"
        __table_args__ = (UniqueConstraint("badge", name="uq_badge"),)
        badge: str = ""

    constraint_names = {c.name for c in Employee.__table__.constraints if c.name}

    # --- Assert ---
    assert "uq_badge" in constraint_names


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_composite_pk_parent_raises_type_error() -> None:
    """JTI with a composite-PK parent raises TypeError."""

    class CompositePK(SQLDataclass, table=True):
        __tablename__ = "jti_composite_pk_parent"
        a: int = Field(primary_key=True)
        b: int = Field(primary_key=True)

    with pytest.raises(TypeError, match="single-column primary key"):

        class Child(CompositePK, table=True):
            __tablename__ = "jti_composite_pk_child"
            value: str = ""
