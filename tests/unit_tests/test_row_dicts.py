"""Unit tests for sqldataclass.query.row_dicts — SQLite in-memory."""

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine, insert, select
from sqlalchemy.engine import Connection

from sqldataclass.query import row_dicts

_metadata = MetaData()
_items = Table(
    "row_dicts_items",
    _metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=True),
    Column("value", Float, nullable=True),
)
_tags = Table(
    "row_dicts_tags",
    _metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=True),
    Column("item_id", Integer, nullable=True),
)


@contextmanager
def _connection() -> Iterator[Connection]:
    """Connection to a fresh in-memory database; the engine is disposed afterwards."""
    engine = create_engine("sqlite:///:memory:")
    _metadata.create_all(engine)
    try:
        with engine.connect() as conn:
            yield conn
    finally:
        engine.dispose()


def test_row_dicts_empty_result_yields_nothing() -> None:
    """A query without rows has nothing to convert, so no dict is yielded."""
    # --- Act ---
    with _connection() as conn:
        dicts = list(row_dicts(conn.execute(select(_items))))

    # --- Assert ---
    assert dicts == []


def test_row_dicts_keys_and_values_follow_selected_columns() -> None:
    """Each key is a selected column name and maps to that column's value, NULL included."""
    # --- Arrange ---
    with _connection() as conn:
        conn.execute(insert(_items), [{"id": 1, "name": None, "value": 2.5}])

        # --- Act ---
        dicts = list(row_dicts(conn.execute(select(_items))))

    # --- Assert ---
    assert dicts == [{"id": 1, "name": None, "value": 2.5}]


def test_row_dicts_match_mappings_for_overlapping_column_names() -> None:
    """Both tables have ``id`` and ``name``; the keys must be the deduplicated ones ``.mappings()`` uses."""
    # --- Arrange ---
    with _connection() as conn:
        conn.execute(insert(_items), [{"id": 1, "name": "item", "value": 1.0}])
        conn.execute(insert(_tags), [{"id": 10, "name": "tag", "item_id": 1}])
        stmt = select(_items, _tags).where(_items.c.id == _tags.c.item_id)
        expected = [dict(row) for row in conn.execute(stmt).mappings()]

        # --- Act ---
        dicts = list(row_dicts(conn.execute(stmt)))

    # --- Assert ---
    assert dicts == expected
    assert len(dicts[0]) == 6
