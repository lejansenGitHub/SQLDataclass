"""Tests for __table_args__ support — schema, CHECK constraints, indexes."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import CheckConstraint, Index, MetaData, UniqueConstraint, create_engine, inspect
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

from sqldataclass import Field, SQLDataclass

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine_and_connection() -> Generator[tuple[object, Connection]]:
    """Yield a separate engine + connection so schema-less tables can be created without interference."""
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        yield engine, conn


# ---------------------------------------------------------------------------
# __table_args__ as dict (schema inspection only — no DB creation)
# ---------------------------------------------------------------------------


def test_table_args_dict_sets_schema() -> None:
    """Dict-form __table_args__ passes schema kwarg to SA Table."""

    class Product(SQLDataclass, table=True):
        __tablename__ = "products_schema_test"
        __table_args__ = {"schema": "inventory"}
        id: int = Field(primary_key=True)
        name: str = ""

    # --- Assert ---
    assert Product.__table__.schema == "inventory"
    assert Product.__table__.fullname == "inventory.products_schema_test"


# ---------------------------------------------------------------------------
# __table_args__ as tuple with constraints + trailing dict
# ---------------------------------------------------------------------------


def test_table_args_tuple_with_constraints_and_kwargs() -> None:
    """Tuple-form __table_args__ supports constraints + trailing kwargs dict."""

    class Order(SQLDataclass, table=True):
        __tablename__ = "orders_args_test"
        __table_args__ = (
            UniqueConstraint("email", name="uq_orders_email"),
            {"comment": "Order tracking table"},
        )
        id: int = Field(primary_key=True)
        email: str = ""

    constraint_names = {c.name for c in Order.__table__.constraints if c.name}

    # --- Assert ---
    assert "uq_orders_email" in constraint_names
    assert Order.__table__.comment == "Order tracking table"


# ---------------------------------------------------------------------------
# CHECK constraint enforced by DB
# ---------------------------------------------------------------------------


def test_table_args_check_constraint(engine_and_connection: tuple[object, Connection]) -> None:
    """CHECK constraints from __table_args__ are enforced by the DB."""
    engine, connection = engine_and_connection

    class Item(SQLDataclass, table=True):
        __tablename__ = "items_check_test"
        __table_args__ = (CheckConstraint("price >= 0", name="ck_items_price_positive"),)
        id: int = Field(primary_key=True)
        price: float = 0.0

    metadata = MetaData()
    Item.__table__.to_metadata(metadata)
    metadata.create_all(engine)

    # Valid insert
    connection.execute(Item.__table__.insert().values(id=1, price=10.0))
    connection.commit()

    # Invalid insert violates CHECK
    # --- Assert ---
    with pytest.raises(IntegrityError):
        connection.execute(Item.__table__.insert().values(id=2, price=-5.0))


# ---------------------------------------------------------------------------
# Index creation
# ---------------------------------------------------------------------------


def test_table_args_index(engine_and_connection: tuple[object, Connection]) -> None:
    """Indexes from __table_args__ are created on the table."""
    engine, connection = engine_and_connection

    class LogEntry(SQLDataclass, table=True):
        __tablename__ = "log_entries_idx_test"
        __table_args__ = (Index("ix_log_timestamp", "timestamp"),)
        id: int = Field(primary_key=True)
        timestamp: str = ""
        message: str = ""

    metadata = MetaData()
    LogEntry.__table__.to_metadata(metadata)
    metadata.create_all(engine)

    inspector = inspect(engine)
    indexes = inspector.get_indexes("log_entries_idx_test")
    index_names = {idx["name"] for idx in indexes}

    # --- Assert ---
    assert "ix_log_timestamp" in index_names


# ---------------------------------------------------------------------------
# Tuple without trailing dict
# ---------------------------------------------------------------------------


def test_table_args_tuple_without_dict() -> None:
    """Tuple without trailing dict passes only constraints."""

    class Payment(SQLDataclass, table=True):
        __tablename__ = "payments_args_test"
        __table_args__ = (UniqueConstraint("reference", name="uq_payments_ref"),)
        id: int = Field(primary_key=True)
        reference: str = ""

    constraint_names = {c.name for c in Payment.__table__.constraints if c.name}

    # --- Assert ---
    assert "uq_payments_ref" in constraint_names


# ---------------------------------------------------------------------------
# No __table_args__ (default behavior)
# ---------------------------------------------------------------------------


def test_no_table_args_works_as_before(engine_and_connection: tuple[object, Connection]) -> None:
    """Models without __table_args__ still work normally."""
    engine, connection = engine_and_connection

    class Simple(SQLDataclass, table=True):
        __tablename__ = "simple_no_args_test"
        id: int = Field(primary_key=True)
        value: str = ""

    metadata = MetaData()
    Simple.__table__.to_metadata(metadata)
    metadata.create_all(engine)

    connection.execute(Simple.__table__.insert().values(id=1, value="test"))
    connection.commit()

    # --- Assert ---
    row = connection.execute(Simple.__table__.select()).fetchone()
    assert row is not None
    assert row[1] == "test"


# ---------------------------------------------------------------------------
# Multi-column CHECK constraint
# ---------------------------------------------------------------------------


def test_multi_column_check_constraint(engine_and_connection: tuple[object, Connection]) -> None:
    """CHECK constraints can span multiple columns."""
    engine, connection = engine_and_connection

    class DateRange(SQLDataclass, table=True):
        __tablename__ = "date_ranges_test"
        __table_args__ = (CheckConstraint("end_day > start_day", name="ck_range_order"),)
        id: int = Field(primary_key=True)
        start_day: int = 0
        end_day: int = 0

    metadata = MetaData()
    DateRange.__table__.to_metadata(metadata)
    metadata.create_all(engine)

    # Valid: end > start
    connection.execute(DateRange.__table__.insert().values(id=1, start_day=1, end_day=10))
    connection.commit()

    # Invalid: end <= start
    # --- Assert ---
    with pytest.raises(IntegrityError):
        connection.execute(DateRange.__table__.insert().values(id=2, start_day=10, end_day=5))


# ---------------------------------------------------------------------------
# Schema + constraints combined
# ---------------------------------------------------------------------------


def test_schema_with_constraints() -> None:
    """Schema and constraints can be combined in __table_args__."""

    class Warehouse(SQLDataclass, table=True):
        __tablename__ = "warehouses_combo_test"
        __table_args__ = (
            UniqueConstraint("code", name="uq_warehouse_code"),
            CheckConstraint("capacity > 0", name="ck_warehouse_capacity"),
            {"schema": "logistics"},
        )
        id: int = Field(primary_key=True)
        code: str = ""
        capacity: int = 1

    table = Warehouse.__table__

    # --- Assert ---
    assert table.schema == "logistics"
    constraint_names = {c.name for c in table.constraints if c.name}
    assert "uq_warehouse_code" in constraint_names
    assert "ck_warehouse_capacity" in constraint_names
