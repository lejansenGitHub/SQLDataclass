"""Query execution — the core of the memory-light approach.

Prefer `load_all` over `fetch_all` + manual loop: `load_all` converts rows to
domain objects inline during cursor iteration, avoiding the intermediate
`list[dict]` memory spike.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import Connection
from sqlalchemy.sql import Executable


def _fast_construct(cls: type, data: Any) -> Any:
    """Construct a pydantic dataclass or BaseModel using the fast path.

    For BaseModel subclasses (SQLModel): uses model_construct (skips validation).
    For pydantic dataclasses: uses validate_python (~2.8x faster than __init__).
    """
    if getattr(cls, "__sqlmodel_is_basemodel__", False):
        return cls.model_construct(**data)  # type: ignore[attr-defined]
    validator = getattr(cls, "__pydantic_validator__", None)
    if validator is not None:
        return validator.validate_python(data)
    return cls(**data)


def load_all[T](conn: Connection, query: Executable, cls: type[T]) -> list[T]:
    """Execute query and construct domain objects directly — no intermediate list[dict].

    Each row is converted to a domain object inline as the cursor is iterated,
    avoiding the memory spike of materializing all rows as dicts first.
    """
    if getattr(cls, "__sqlmodel_is_basemodel__", False):
        return [cls.model_construct(**dict(row)) for row in conn.execute(query).mappings()]  # type: ignore[attr-defined]
    validator = getattr(cls, "__pydantic_validator__", None)
    if validator is not None:
        return [validator.validate_python(dict(row)) for row in conn.execute(query).mappings()]
    return [cls(**row) for row in conn.execute(query).mappings()]


def fetch_all(conn: Connection, query: Executable) -> list[dict[str, object]]:
    """Execute query and return list of plain dicts."""
    return [dict(row) for row in conn.execute(query).mappings()]


def fetch_one(conn: Connection, query: Executable) -> dict[str, object] | None:
    """Execute query and return a single plain dict, or None."""
    row = conn.execute(query).mappings().one_or_none()
    if row is None:
        return None
    return dict(row)


def select_columns(*table_classes: type) -> Executable:
    """Build a select() with all columns from the given ORM-mapped classes."""
    columns = []
    for cls in table_classes:
        columns.extend(cls.__table__.columns)  # type: ignore[attr-defined]
    return select(*columns)
