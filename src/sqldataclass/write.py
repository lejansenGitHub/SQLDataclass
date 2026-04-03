"""Write operations — insert and upsert using SQLAlchemy Core."""

from __future__ import annotations

import dataclasses
from typing import Any, Sequence

import pydantic
from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection

from sqldataclass.registry import table


def insert_row(conn: Connection, table_class: type, values: dict[str, Any]) -> None:
    """Insert a single row using Core INSERT."""
    conn.execute(insert(table(table_class)).values(values))


def insert_many(conn: Connection, table_class: type, rows: Sequence[dict[str, Any]]) -> None:
    """Insert multiple rows using Core INSERT."""
    if rows:
        conn.execute(insert(table(table_class)), list(rows))


def upsert_row(
    conn: Connection,
    table_class: type,
    values: dict[str, Any],
    *,
    index_elements: list[str],
) -> None:
    """PostgreSQL ON CONFLICT upsert.

    Updates all non-index columns on conflict, excluding None values for
    columns with server defaults so the database-generated value is preserved.
    """
    target_table = table(table_class)
    server_defaults = _server_defaulted_columns(table_class)
    stmt = pg_insert(target_table).values(values)
    update_columns = {
        key: value
        for key, value in values.items()
        if key not in index_elements and not (value is None and key in server_defaults)
    }
    if update_columns:
        stmt = stmt.on_conflict_do_update(index_elements=index_elements, set_=update_columns)
    else:
        stmt = stmt.on_conflict_do_nothing(index_elements=index_elements)
    conn.execute(stmt)


def upsert_row_returning(
    conn: Connection,
    table_class: type,
    instance: Any,
    values: dict[str, Any],
    *,
    index_elements: list[str],
) -> None:
    """PostgreSQL ON CONFLICT upsert with RETURNING.

    Like upsert_row, but uses RETURNING to populate DB-generated fields on the
    instance in place.
    """
    target_table = table(table_class)
    server_defaults = _server_defaulted_columns(table_class)
    stmt = pg_insert(target_table).values(values)
    update_columns = {
        key: value
        for key, value in values.items()
        if key not in index_elements and not (value is None and key in server_defaults)
    }
    if update_columns:
        stmt = stmt.on_conflict_do_update(index_elements=index_elements, set_=update_columns)
    else:
        stmt = stmt.on_conflict_do_nothing(index_elements=index_elements)
    stmt = stmt.returning(target_table)
    result = conn.execute(stmt)
    row = result.mappings().fetchone()
    if row:
        for key, value in row.items():
            object.__setattr__(instance, key, value)


def _server_defaulted_columns(table_class: type) -> frozenset[str]:
    """Return column names that have server defaults or are autoincrement PKs."""
    target_table = table(table_class)
    return frozenset(
        col.name
        for col in target_table.columns
        if col.server_default is not None or (col.primary_key and col.autoincrement is not False)
    )


def flatten_for_table(
    domain_object: Any,
    *,
    exclude_keys: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Flatten a pydantic dataclass to a dict suitable for table insertion.

    Strips nested dicts (which belong to other tables), explicitly excluded keys,
    and None values for columns with server defaults (SERIAL PKs, DEFAULT NOW(), etc.)
    so the database generates the value.
    """
    if hasattr(domain_object, "__pydantic_fields__"):
        raw = {field_name: getattr(domain_object, field_name) for field_name in domain_object.__pydantic_fields__}
    elif hasattr(domain_object, "__dataclass_fields__"):
        raw = dataclasses.asdict(domain_object)
    else:
        raise TypeError(f"Expected a dataclass instance, got {type(domain_object)}")

    # Exclude relationship fields and column=False fields
    rel_keys: set[str] = set(getattr(type(domain_object), "__relationships__", {}))
    non_column_keys: set[str] = set(getattr(type(domain_object), "__non_column_fields__", ()))
    server_defaults = _server_defaulted_columns(type(domain_object))

    return {
        key: value
        for key, value in raw.items()
        if key not in exclude_keys
        and key not in rel_keys
        and key not in non_column_keys
        and not isinstance(value, dict | list)
        and not dataclasses.is_dataclass(value)
        and not isinstance(value, pydantic.BaseModel)
        and not (value is None and key in server_defaults)
    }
