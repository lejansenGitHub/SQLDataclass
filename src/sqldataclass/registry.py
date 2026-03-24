"""SQLAlchemy Core registry and table helpers.

SQL schema classes inherit from Base for DDL and query building.
They are never instantiated at runtime.
"""

from __future__ import annotations

from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Shared declarative base for all SQL schema classes."""


metadata: MetaData = Base.metadata


def table(cls: type) -> Table:
    """Get the Core Table from an ORM-mapped class."""
    tbl: Table = cls.__table__  # type: ignore[attr-defined]
    return tbl


def create_all_tables(engine: Engine) -> None:
    """Create all tables registered with Base."""
    metadata.create_all(engine)


def drop_all_tables(engine: Engine) -> None:
    """Drop all tables registered with Base."""
    metadata.drop_all(engine)
