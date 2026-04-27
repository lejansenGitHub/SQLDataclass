"""SQLDataclass — pydantic dataclass ORM with memory-efficient DB access."""

__version__ = "0.2.1"

from sqldataclass.basemodel import SQLModel
from sqldataclass.compat import from_psycopg
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
from sqldataclass.repository import ReadRepository, TransactionHandle, WriteRepository
from sqldataclass.return_types import (
    DictReturn,
    DictReturnMany,
    ModelReturn,
    ModelReturnMany,
    NonNullDictReturn,
)
from sqldataclass.utils import remove_unexpected_kwargs
from sqldataclass.validators import FillValueIfNone
from sqldataclass.write import (
    flatten_for_table,
    insert_many,
    insert_row,
    upsert_row,
)

__all__ = [
    "Base",
    "CyFunctionDetector",
    "DictReturn",
    "DictReturnMany",
    "Field",
    "FillValueIfNone",
    "ModelReturn",
    "ModelReturnMany",
    "NonNullDictReturn",
    "ReadRepository",
    "Relationship",
    "SQLDataclass",
    "SQLModel",
    "TransactionHandle",
    "WriteRepository",
    "create_all_tables",
    "discriminator_map",
    "drop_all_tables",
    "fetch_all",
    "fetch_one",
    "flatten_for_table",
    "format_discriminated",
    "from_psycopg",
    "insert_many",
    "insert_row",
    "load_all",
    "nest_fields",
    "remove_unexpected_kwargs",
    "select_columns",
    "table",
    "upsert_row",
]
