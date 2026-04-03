"""Edge-case tests for sqldataclass.write — uses SQLite in-memory databases."""

from __future__ import annotations

import dataclasses
from collections.abc import Generator
from dataclasses import field
from typing import Any

import pytest
from pydantic.dataclasses import dataclass as dataclass_pydantic
from sqlalchemy import Column, Float, Integer, String, create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase

from sqldataclass import Field, SQLDataclass
from sqldataclass.write import flatten_for_table, insert_many, insert_row, upsert_row

# ---------------------------------------------------------------------------
# Isolated DeclarativeBase for this test module
# ---------------------------------------------------------------------------


class _TestBase(DeclarativeBase):
    pass


class Item(_TestBase):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String, nullable=True)
    value = Column(Float, nullable=True, default=None)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def connection() -> Generator[Connection]:
    """Yield a SQLAlchemy Core connection backed by an in-memory SQLite DB."""
    engine = create_engine("sqlite:///:memory:")
    _TestBase.metadata.create_all(engine)
    with engine.connect() as conn:
        yield conn


# ---------------------------------------------------------------------------
# insert_row
# ---------------------------------------------------------------------------


class TestInsertRow:
    def test_insert_with_all_columns(self, connection: Connection) -> None:
        insert_row(connection, Item, {"id": 1, "name": "alpha", "value": 9.5})
        connection.commit()
        row = connection.execute(Item.__table__.select()).fetchone()
        assert row is not None
        assert row.id == 1
        assert row.name == "alpha"
        assert row.value == 9.5

    def test_insert_with_only_required_columns(self, connection: Connection) -> None:
        """Omitted nullable columns should default to None."""
        insert_row(connection, Item, {"id": 2})
        connection.commit()
        row = connection.execute(
            Item.__table__.select().where(Item.__table__.c.id == 2),
        ).fetchone()
        assert row is not None
        assert row.name is None
        assert row.value is None

    def test_insert_duplicate_primary_key_raises_integrity_error(self, connection: Connection) -> None:
        insert_row(connection, Item, {"id": 3, "name": "first"})
        connection.commit()
        with pytest.raises(IntegrityError):
            insert_row(connection, Item, {"id": 3, "name": "duplicate"})

    def test_insert_with_none_values(self, connection: Connection) -> None:
        insert_row(connection, Item, {"id": 4, "name": None, "value": None})
        connection.commit()
        row = connection.execute(
            Item.__table__.select().where(Item.__table__.c.id == 4),
        ).fetchone()
        assert row is not None
        assert row.name is None
        assert row.value is None


# ---------------------------------------------------------------------------
# insert_many
# ---------------------------------------------------------------------------


class TestInsertMany:
    def test_empty_list_does_nothing(self, connection: Connection) -> None:
        insert_many(connection, Item, [])
        connection.commit()
        rows = connection.execute(Item.__table__.select()).fetchall()
        assert rows == []

    def test_single_row(self, connection: Connection) -> None:
        insert_many(connection, Item, [{"id": 10, "name": "solo", "value": 1.0}])
        connection.commit()
        rows = connection.execute(Item.__table__.select()).fetchall()
        assert len(rows) == 1
        assert rows[0].name == "solo"

    def test_many_rows(self, connection: Connection) -> None:
        rows_in = [{"id": i, "name": f"item_{i}", "value": float(i)} for i in range(100)]
        insert_many(connection, Item, rows_in)
        connection.commit()
        count = connection.execute(Item.__table__.select()).fetchall()
        assert len(count) == 100

    def test_rows_with_mixed_none_values(self, connection: Connection) -> None:
        rows_in: list[dict[str, Any]] = [
            {"id": 200, "name": "has_name", "value": None},
            {"id": 201, "name": None, "value": 3.14},
            {"id": 202, "name": None, "value": None},
        ]
        insert_many(connection, Item, rows_in)
        connection.commit()
        results = {r.id: r for r in connection.execute(Item.__table__.select()).fetchall()}
        assert results[200].name == "has_name"
        assert results[200].value is None
        assert results[201].name is None
        assert results[201].value == pytest.approx(3.14)
        assert results[202].name is None
        assert results[202].value is None


# ---------------------------------------------------------------------------
# flatten_for_table — domain model helpers
# ---------------------------------------------------------------------------


@dataclass_pydantic(kw_only=True, slots=True)
class Address:
    street: str = ""
    city: str = ""


@dataclass_pydantic(kw_only=True, slots=True)
class Metadata:
    source: str = ""
    version: int = 0


@dataclass_pydantic(kw_only=True, slots=True)
class FlatModel:
    id: int
    name: str = ""
    score: float = 0.0


@dataclass_pydantic(kw_only=True, slots=True)
class NestedModel:
    id: int
    label: str = ""
    address: Address = field(default_factory=Address)
    meta: Metadata = field(default_factory=Metadata)


@dataclasses.dataclass
class StdlibSimple:
    id: int
    tag: str = "default"


class PlainObject:
    """Not a dataclass — should cause TypeError."""

    def __init__(self, x: int) -> None:
        self.x = x


class TestFlattenForTable:
    def test_pydantic_no_nested_fields(self) -> None:
        obj = FlatModel(id=1, name="flat", score=4.5)
        result = flatten_for_table(obj)
        assert result == {"id": 1, "name": "flat", "score": 4.5}

    def test_pydantic_multiple_nested_dataclass_fields_stripped(self) -> None:
        obj = NestedModel(
            id=7,
            label="test",
            address=Address(street="Main St", city="Springfield"),
            meta=Metadata(source="api", version=2),
        )
        result = flatten_for_table(obj)
        assert result == {"id": 7, "label": "test"}
        assert "address" not in result
        assert "meta" not in result

    def test_exclude_keys_removes_specified_keys(self) -> None:
        obj = FlatModel(id=1, name="flat", score=4.5)
        result = flatten_for_table(obj, exclude_keys=frozenset({"score"}))
        assert result == {"id": 1, "name": "flat"}

    def test_both_nested_and_excluded_keys(self) -> None:
        obj = NestedModel(
            id=9,
            label="combo",
            address=Address(),
            meta=Metadata(),
        )
        result = flatten_for_table(obj, exclude_keys=frozenset({"label"}))
        # nested fields stripped, plus label excluded
        assert result == {"id": 9}

    def test_stdlib_dataclass_fallback(self) -> None:
        obj = StdlibSimple(id=42, tag="stdlib")
        result = flatten_for_table(obj)
        assert result == {"id": 42, "tag": "stdlib"}

    def test_plain_object_raises_type_error(self) -> None:
        obj = PlainObject(x=99)
        with pytest.raises(TypeError, match="Expected a dataclass instance"):
            flatten_for_table(obj)

    def test_empty_exclude_keys_default_behavior(self) -> None:
        obj = FlatModel(id=5, name="keep_all", score=1.0)
        result = flatten_for_table(obj)
        assert result == {"id": 5, "name": "keep_all", "score": 1.0}

    def test_none_autoincrement_pk_skipped(self) -> None:
        """
        None value for a SERIAL/autoincrement primary key should be
        omitted from the flattened dict so the database generates it.
        """

        class AutoPK(SQLDataclass, table=True):
            __tablename__ = "auto_pk_test"
            id: int | None = Field(default=None, primary_key=True)
            name: str = ""

        obj = AutoPK(name="hello")
        result = flatten_for_table(obj)
        assert "id" not in result
        assert result == {"name": "hello"}

    def test_none_server_default_skipped(self) -> None:
        """
        None value for a column with server_default (e.g. DEFAULT NOW())
        should be omitted so the database generates the value.
        """
        from sqlalchemy import text

        class WithServerDefault(SQLDataclass, table=True):
            __tablename__ = "server_default_test"
            id: int | None = Field(default=None, primary_key=True)
            name: str = ""
            created_at: str | None = Field(default=None, server_default=text("NOW()"))

        obj = WithServerDefault(name="test")
        result = flatten_for_table(obj)
        assert "id" not in result
        assert "created_at" not in result
        assert result == {"name": "test"}

    def test_explicit_value_for_server_default_kept(self) -> None:
        """
        If a server-defaulted column has an explicit non-None value,
        it should be included in the flattened dict.
        """
        from sqlalchemy import text

        class WithServerDefault(SQLDataclass, table=True):
            __tablename__ = "server_default_explicit_test"
            id: int | None = Field(default=None, primary_key=True)
            created_at: str | None = Field(default=None, server_default=text("NOW()"))

        obj = WithServerDefault(id=42, created_at="2026-01-01")
        result = flatten_for_table(obj)
        assert result == {"id": 42, "created_at": "2026-01-01"}


# ---------------------------------------------------------------------------
# upsert_row — PostgreSQL dialect on SQLite
# ---------------------------------------------------------------------------


class TestUpsertRow:
    def test_upsert_inserts_new_row_on_sqlite(self, connection: Connection) -> None:
        """PostgreSQL upsert dialect happens to work on SQLite for inserts."""
        upsert_row(
            connection,
            Item,
            {"id": 1, "name": "upserted"},
            index_elements=["id"],
        )
        connection.commit()
        row = connection.execute(
            Item.__table__.select().where(Item.__table__.c.id == 1),
        ).fetchone()
        assert row is not None
        assert row.name == "upserted"

    def test_upsert_updates_existing_row_on_sqlite(self, connection: Connection) -> None:
        insert_row(connection, Item, {"id": 50, "name": "original", "value": 1.0})
        connection.commit()
        upsert_row(
            connection,
            Item,
            {"id": 50, "name": "updated", "value": 2.0},
            index_elements=["id"],
        )
        connection.commit()
        row = connection.execute(
            Item.__table__.select().where(Item.__table__.c.id == 50),
        ).fetchone()
        assert row is not None
        assert row.name == "updated"
        assert row.value == 2.0

    def test_upsert_only_index_elements_does_nothing_on_conflict(self, connection: Connection) -> None:
        """When all columns are index elements, conflict should do nothing."""
        insert_row(connection, Item, {"id": 60, "name": "keep"})
        connection.commit()
        upsert_row(
            connection,
            Item,
            {"id": 60},
            index_elements=["id"],
        )
        connection.commit()
        row = connection.execute(
            Item.__table__.select().where(Item.__table__.c.id == 60),
        ).fetchone()
        assert row is not None
        assert row.name == "keep"
