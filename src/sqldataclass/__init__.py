"""SQLDataclass — pydantic dataclass ORM with memory-efficient DB access."""

__version__ = "0.1.2"

from sqldataclass.basemodel import SQLModel
from sqldataclass.cy_function_helper import CyFunctionDetector
from sqldataclass.hydration import (
    discriminator_map,
    format_discriminated,
    nest_fields,
)
from sqldataclass.model import Field, Relationship, SQLDataclass
from sqldataclass.query import fetch_all, fetch_one, load_all, select_columns
from sqldataclass.registry import (
    Base,
    create_all_tables,
    drop_all_tables,
    table,
)
from sqldataclass.write import (
    flatten_for_table,
    insert_many,
    insert_row,
    upsert_row,
)

try:
    from sqldataclass.annotations import Np  # noqa: F401
except ImportError:  # numpy not installed
    pass

__all__ = [
    "Base",
    "CyFunctionDetector",
    "Field",
    "Relationship",
    "SQLDataclass",
    "SQLModel",
    "create_all_tables",
    "discriminator_map",
    "drop_all_tables",
    "fetch_all",
    "fetch_one",
    "flatten_for_table",
    "format_discriminated",
    "insert_many",
    "insert_row",
    "load_all",
    "nest_fields",
    "select_columns",
    "table",
    "upsert_row",
]
