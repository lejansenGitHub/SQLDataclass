"""Edge-case unit tests for sqldataclass.query — SQLite in-memory, no external DB."""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass

import pytest
from sqlalchemy import Column, Float, Integer, Select, String, Table, create_engine, insert, select
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import DeclarativeBase

from sqldataclass import fetch_all, fetch_one, load_all, select_columns

# ---------------------------------------------------------------------------
# ORM schema classes (local base to avoid polluting the library registry)
# ---------------------------------------------------------------------------


class _TestBase(DeclarativeBase):
    pass


class ItemSql(_TestBase):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=True)
    value = Column(Float, nullable=True)


class TagSql(_TestBase):
    __tablename__ = "tags"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=True)
    item_id = Column(Integer, nullable=True)


# ---------------------------------------------------------------------------
# Domain dataclass used with load_all
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Item:
    id: int
    name: str | None
    value: float | None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine() -> Generator[Engine]:
    eng = create_engine("sqlite:///:memory:")
    _TestBase.metadata.create_all(eng)
    yield eng
    _TestBase.metadata.drop_all(eng)


@pytest.fixture
def conn(engine: Engine) -> Generator[Connection]:
    with engine.begin() as connection:
        yield connection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _insert_items(conn: Connection, rows: list[dict[str, object]]) -> None:
    item_table: Table = ItemSql.__table__  # type: ignore[assignment]
    if rows:
        conn.execute(insert(item_table), rows)


# ===========================================================================
# load_all
# ===========================================================================


class TestLoadAllEdgeCases:
    """Edge cases for query.load_all."""

    def test_empty_result_set_returns_empty_list(self, conn: Connection) -> None:
        result = load_all(conn, select(ItemSql.__table__), Item)
        assert result == []

    def test_single_row(self, conn: Connection) -> None:
        _insert_items(conn, [{"id": 1, "name": "alpha", "value": 1.5}])
        result = load_all(conn, select(ItemSql.__table__), Item)
        assert len(result) == 1
        assert result[0].id == 1
        assert result[0].name == "alpha"
        assert result[0].value == 1.5

    def test_large_result_set(self, conn: Connection) -> None:
        count = 1_200
        _insert_items(
            conn,
            [{"id": i, "name": f"item_{i}", "value": float(i)} for i in range(count)],
        )
        result = load_all(conn, select(ItemSql.__table__), Item)
        assert len(result) == count
        # spot-check first and last
        assert result[0].id == 0
        assert result[-1].id == count - 1

    def test_row_with_null_values(self, conn: Connection) -> None:
        _insert_items(conn, [{"id": 1, "name": None, "value": None}])
        result = load_all(conn, select(ItemSql.__table__), Item)
        assert len(result) == 1
        assert result[0].name is None
        assert result[0].value is None

    def test_row_with_all_column_types(self, conn: Connection) -> None:
        """int, float, str, and None all round-trip correctly."""
        _insert_items(conn, [{"id": 42, "name": "mixed", "value": 3.14}])
        _insert_items(conn, [{"id": 43, "name": None, "value": None}])

        result = load_all(conn, select(ItemSql.__table__), Item)
        assert len(result) == 2

        by_id = {item.id: item for item in result}

        assert isinstance(by_id[42].id, int)
        assert isinstance(by_id[42].name, str)
        assert isinstance(by_id[42].value, float)
        assert by_id[43].name is None
        assert by_id[43].value is None


# ===========================================================================
# fetch_all
# ===========================================================================


class TestFetchAllEdgeCases:
    """Edge cases for query.fetch_all."""

    def test_empty_result_set(self, conn: Connection) -> None:
        result = fetch_all(conn, select(ItemSql.__table__))
        assert result == []

    def test_single_row_returns_list_with_one_dict(self, conn: Connection) -> None:
        _insert_items(conn, [{"id": 1, "name": "solo", "value": 9.9}])
        result = fetch_all(conn, select(ItemSql.__table__))
        assert len(result) == 1
        assert result[0] == {"id": 1, "name": "solo", "value": 9.9}

    def test_multiple_rows_preserve_order(self, conn: Connection) -> None:
        _insert_items(
            conn,
            [
                {"id": 3, "name": "c", "value": 3.0},
                {"id": 1, "name": "a", "value": 1.0},
                {"id": 2, "name": "b", "value": 2.0},
            ],
        )
        # ORDER BY guarantees deterministic assertion
        query = select(ItemSql.__table__).order_by(ItemSql.__table__.c.id)
        result = fetch_all(conn, query)
        ids = [row["id"] for row in result]
        assert ids == [1, 2, 3]

    def test_null_values_appear_as_none(self, conn: Connection) -> None:
        _insert_items(conn, [{"id": 1, "name": None, "value": None}])
        result = fetch_all(conn, select(ItemSql.__table__))
        assert result[0]["name"] is None
        assert result[0]["value"] is None

    def test_dict_keys_are_strings(self, conn: Connection) -> None:
        _insert_items(conn, [{"id": 1, "name": "x", "value": 0.0}])
        result = fetch_all(conn, select(ItemSql.__table__))
        assert all(isinstance(k, str) for k in result[0])


# ===========================================================================
# fetch_one
# ===========================================================================


class TestFetchOneEdgeCases:
    """Edge cases for query.fetch_one."""

    def test_returns_none_for_empty_result(self, conn: Connection) -> None:
        result = fetch_one(conn, select(ItemSql.__table__).where(ItemSql.__table__.c.id == 999))
        assert result is None

    def test_returns_dict_for_single_match(self, conn: Connection) -> None:
        _insert_items(conn, [{"id": 7, "name": "unique", "value": 7.7}])
        result = fetch_one(conn, select(ItemSql.__table__).where(ItemSql.__table__.c.id == 7))
        assert result is not None
        assert result == {"id": 7, "name": "unique", "value": 7.7}

    def test_raises_on_multiple_rows(self, conn: Connection) -> None:
        """one_or_none() raises when more than one row matches."""
        _insert_items(
            conn,
            [
                {"id": 1, "name": "dup", "value": 1.0},
                {"id": 2, "name": "dup", "value": 2.0},
            ],
        )
        with pytest.raises(MultipleResultsFound):
            fetch_one(conn, select(ItemSql.__table__).where(ItemSql.__table__.c.name == "dup"))

    def test_null_values_in_returned_dict(self, conn: Connection) -> None:
        _insert_items(conn, [{"id": 1, "name": None, "value": None}])
        result = fetch_one(conn, select(ItemSql.__table__))
        assert result is not None
        assert result["name"] is None
        assert result["value"] is None

    def test_dict_keys_are_strings(self, conn: Connection) -> None:
        _insert_items(conn, [{"id": 1, "name": "k", "value": 0.0}])
        result = fetch_one(conn, select(ItemSql.__table__))
        assert result is not None
        assert all(isinstance(k, str) for k in result)


# ===========================================================================
# select_columns
# ===========================================================================


class TestSelectColumnsEdgeCases:
    """Edge cases for query.select_columns."""

    def test_single_table(self) -> None:
        stmt = select_columns(ItemSql)
        assert isinstance(stmt, Select)
        col_names = {str(c.name) for c in stmt.selected_columns}
        assert col_names == {"id", "name", "value"}

    def test_multiple_tables(self) -> None:
        stmt = select_columns(ItemSql, TagSql)
        assert isinstance(stmt, Select)
        col_names = {str(c.name) for c in stmt.selected_columns}
        assert col_names == {"id", "name", "value", "item_id"}

    def test_overlapping_column_names_both_present(self) -> None:
        """Tables sharing column names (id, name) still emit all columns."""
        stmt = select_columns(ItemSql, TagSql)
        assert isinstance(stmt, Select)
        # Count total selected columns — duplicates are kept
        all_names = [str(c.name) for c in stmt.selected_columns]
        assert all_names.count("id") == 2
        assert all_names.count("name") == 2

    def test_overlapping_columns_execute(self, conn: Connection) -> None:
        """Overlapping columns can be executed and rows contain all values."""
        _insert_items(conn, [{"id": 1, "name": "item", "value": 1.0}])
        tag_table: Table = TagSql.__table__  # type: ignore[assignment]
        conn.execute(insert(tag_table), [{"id": 10, "name": "tag", "item_id": 1}])

        raw_stmt = select_columns(ItemSql, TagSql)
        assert isinstance(raw_stmt, Select)
        stmt = raw_stmt.where(
            ItemSql.__table__.c.id == TagSql.__table__.c.item_id,
        )
        rows = fetch_all(conn, stmt)
        assert len(rows) == 1
        # Because of overlapping "id" and "name", SQLAlchemy may label them;
        # we just verify the row has the right number of values.
        row = rows[0]
        assert len(row) >= 4  # at least id, name, value, item_id (possibly labelled duplicates)
