"""Query execution — the core of the memory-light approach.

Prefer `load_all` over `fetch_all` + manual loop: `load_all` converts rows to
domain objects inline during cursor iteration, avoiding the intermediate
`list[dict]` memory spike.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, TypeVar

from sqlalchemy import select
from sqlalchemy.engine import Connection, Result
from sqlalchemy.sql import Executable

_T = TypeVar("_T")


def row_dicts(result: Result[Any]) -> Iterator[dict[str, Any]]:
    """Yield each row of ``result`` as a plain dict.

    Zips the column keys with the row tuple instead of going through
    ``result.mappings()``, whose per-row mapping view costs about as much
    as the pydantic validation that follows.
    """
    keys = tuple(result.keys())
    for row in result:
        yield dict(zip(keys, row, strict=True))


def _fast_construct(cls: type, data: Any) -> Any:
    """Construct a pydantic dataclass or BaseModel from DB row data.

    For BaseModel (SQLModel): uses __new__ + __dict__.update — skips pydantic
    validation entirely, ~3x less peak memory than validate_python.
    For pydantic dataclasses: uses validate_python (~2.8x faster than __init__).
    DB data is already typed, so skipping validation is safe.
    """
    if getattr(cls, "__sqlmodel_is_basemodel__", False):
        obj: Any = object.__new__(cls)
        obj.__dict__.update(data)
        non_col: frozenset[str] = getattr(cls, "__non_column_fields__", frozenset())
        for fname in non_col:
            if fname not in obj.__dict__:
                finfo = cls.model_fields.get(fname)  # type: ignore[attr-defined]  # pydantic model_fields exists at runtime
                if finfo is not None and finfo.default is not None:
                    obj.__dict__[fname] = finfo.default
        return obj
    validator = getattr(cls, "__pydantic_validator__", None)
    if validator is not None:
        return validator.validate_python(data)
    return cls(**data)


def load_all(conn: Connection, query: Executable, cls: type[_T]) -> list[_T]:
    """Execute query and construct domain objects directly — no intermediate list[dict].

    Each row is converted to a domain object inline as the cursor is iterated,
    avoiding the memory spike of materializing all rows as dicts first.
    SQLModel (BaseModel) classes skip validation: each row dict becomes the
    instance ``__dict__`` as is, plus the defaults of non-column fields.
    """
    if getattr(cls, "__sqlmodel_is_basemodel__", False):
        non_col: frozenset[str] = getattr(cls, "__non_column_fields__", frozenset())
        defaults: dict[str, Any] = {}
        if non_col:
            for fname in non_col:
                finfo = cls.model_fields.get(fname)  # type: ignore[attr-defined]  # pydantic model_fields exists at runtime
                if finfo is not None and finfo.default is not None:
                    defaults[fname] = finfo.default
        results: list[_T] = []
        for row_dict in row_dicts(conn.execute(query)):
            obj = object.__new__(cls)
            object.__setattr__(obj, "__dict__", row_dict)
            if defaults:
                obj.__dict__.update(defaults)
            results.append(obj)
        return results
    validator = getattr(cls, "__pydantic_validator__", None)
    if validator is not None:
        return [validator.validate_python(row_dict) for row_dict in row_dicts(conn.execute(query))]
    return [cls(**row_dict) for row_dict in row_dicts(conn.execute(query))]


def fetch_all(conn: Connection, query: Executable) -> list[dict[str, object]]:
    """Execute query and return list of plain dicts."""
    return list(row_dicts(conn.execute(query)))


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
        columns.extend(cls.__table__.columns)  # type: ignore[attr-defined]  # SA table attrs set dynamically by metaclass
    return select(*columns)
