"""SQLDataclass — single-class definition combining pydantic dataclass + SQLAlchemy table.

Usage::

    from sqldataclass import SQLDataclass, Field

    class Hero(SQLDataclass, table=True):
        id: int | None = Field(default=None, primary_key=True)
        name: str
        secret_name: str
        age: int | None = None
"""

from __future__ import annotations

import re
import types
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Self,
    Sequence,
    dataclass_transform,
    get_args,
    get_origin,
    get_type_hints,
)
from uuid import UUID

from pydantic import Field as PydanticField
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.fields import FieldInfo
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    String,
    Table,
    Time,
)
from sqlalchemy import Uuid as SAUuid
from sqlalchemy import select as sa_select
from sqlalchemy.engine import Connection
from sqlalchemy.types import TypeEngine

from sqldataclass.query import fetch_one as _fetch_one
from sqldataclass.query import load_all as _load_all
from sqldataclass.write import flatten_for_table as _flatten_for_table
from sqldataclass.write import insert_many as _insert_many
from sqldataclass.write import insert_row as _insert_row
from sqldataclass.write import upsert_row as _upsert_row

# ---------------------------------------------------------------------------
# Type mapping: Python type → SQLAlchemy column type
# ---------------------------------------------------------------------------

_TYPE_MAP: dict[type, type[TypeEngine[Any]]] = {
    int: Integer,
    float: Float,
    str: String,
    bool: Boolean,
    bytes: LargeBinary,
    datetime: DateTime,
    date: Date,
    time: Time,
    Decimal: Numeric,
    UUID: SAUuid,
}


def _unwrap_optional(tp: Any) -> tuple[Any, bool]:
    """If *tp* is ``T | None``, return ``(T, True)``. Otherwise ``(tp, False)``."""
    origin = get_origin(tp)
    if origin is types.UnionType:
        args = get_args(tp)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1 and len(args) == 2:
            return non_none[0], True
    return tp, False


def _python_type_to_sa(tp: Any) -> type[TypeEngine[Any]]:
    """Map a Python type to a SQLAlchemy column type."""
    inner, _ = _unwrap_optional(tp)
    sa_type = _TYPE_MAP.get(inner)
    if sa_type is None:
        raise TypeError(
            f"Cannot map Python type {tp!r} to a SQLAlchemy column type. "
            f"Use Field(sa_type=...) to specify explicitly."
        )
    return sa_type


# ---------------------------------------------------------------------------
# SA column metadata (attached to pydantic FieldInfo)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class SAColumnInfo:
    """SQLAlchemy column configuration attached to a Field's metadata."""

    primary_key: bool = False
    nullable: bool | None = None
    index: bool = False
    unique: bool = False
    foreign_key: str | None = None
    sa_type: Any = None
    server_default: Any = None
    sa_column_kwargs: dict[str, Any] | None = None


def _get_sa_info(field_info: FieldInfo) -> SAColumnInfo | None:
    """Extract our ``SAColumnInfo`` from a pydantic ``FieldInfo.metadata`` list."""
    for item in field_info.metadata:
        if isinstance(item, SAColumnInfo):
            return item
    return None


# ---------------------------------------------------------------------------
# Field() — user-facing function
# ---------------------------------------------------------------------------

_UNSET: Any = object()


def Field(  # noqa: PLR0913
    default: Any = _UNSET,
    *,
    default_factory: Any | None = None,
    # SA column params
    primary_key: bool = False,
    nullable: bool | None = None,
    index: bool = False,
    unique: bool = False,
    foreign_key: str | None = None,
    sa_type: Any = None,
    server_default: Any = None,
    sa_column_kwargs: dict[str, Any] | None = None,
    # Pydantic params (passed through)
    alias: str | None = None,
    title: str | None = None,
    description: str | None = None,
    gt: float | None = None,
    ge: float | None = None,
    lt: float | None = None,
    le: float | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    pattern: str | None = None,
    **extra_pydantic_kwargs: Any,
) -> Any:
    """Define a field with both pydantic validation and SQLAlchemy column config.

    Accepts all pydantic ``Field()`` parameters plus SA column parameters
    (``primary_key``, ``index``, ``unique``, ``foreign_key``, ``sa_type``).
    """
    sa_info = SAColumnInfo(
        primary_key=primary_key,
        nullable=nullable,
        index=index,
        unique=unique,
        foreign_key=foreign_key,
        sa_type=sa_type,
        server_default=server_default,
        sa_column_kwargs=sa_column_kwargs,
    )

    pydantic_kwargs: dict[str, Any] = {
        **extra_pydantic_kwargs,
    }
    if default is not _UNSET:
        pydantic_kwargs["default"] = default
    if default_factory is not None:
        pydantic_kwargs["default_factory"] = default_factory
    if alias is not None:
        pydantic_kwargs["alias"] = alias
    if title is not None:
        pydantic_kwargs["title"] = title
    if description is not None:
        pydantic_kwargs["description"] = description
    if gt is not None:
        pydantic_kwargs["gt"] = gt
    if ge is not None:
        pydantic_kwargs["ge"] = ge
    if lt is not None:
        pydantic_kwargs["lt"] = lt
    if le is not None:
        pydantic_kwargs["le"] = le
    if min_length is not None:
        pydantic_kwargs["min_length"] = min_length
    if max_length is not None:
        pydantic_kwargs["max_length"] = max_length
    if pattern is not None:
        pydantic_kwargs["pattern"] = pattern

    field_info: Any = PydanticField(**pydantic_kwargs)
    field_info.metadata.append(sa_info)
    return field_info


# ---------------------------------------------------------------------------
# Table builder
# ---------------------------------------------------------------------------


def _default_tablename(class_name: str) -> str:
    """Convert CamelCase class name to snake_case table name."""
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", class_name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _build_sa_column(
    field_name: str,
    type_hint: Any,
    sa_info: SAColumnInfo | None,
) -> Column[Any]:
    """Build one SQLAlchemy ``Column`` from a field's type hint and SA metadata."""
    inner_type, is_optional = _unwrap_optional(type_hint)

    if sa_info is None:
        sa_info = SAColumnInfo()

    # Column type
    col_type: Any = sa_info.sa_type if sa_info.sa_type is not None else _python_type_to_sa(inner_type)

    # Nullable
    if sa_info.nullable is not None:
        nullable = sa_info.nullable
    elif sa_info.primary_key:
        nullable = False
    else:
        nullable = is_optional

    # Positional args
    col_args: list[Any] = [field_name, col_type]
    if sa_info.foreign_key:
        col_args.append(ForeignKey(sa_info.foreign_key))

    # Keyword args
    col_kwargs: dict[str, Any] = {
        "primary_key": sa_info.primary_key,
        "nullable": nullable,
        "index": sa_info.index,
        "unique": sa_info.unique,
    }
    if sa_info.server_default is not None:
        col_kwargs["server_default"] = sa_info.server_default
    if sa_info.sa_column_kwargs:
        col_kwargs.update(sa_info.sa_column_kwargs)

    return Column(*col_args, **col_kwargs)


def _build_table(
    tablename: str,
    resolved_hints: dict[str, Any],
    namespace: dict[str, Any],
    target_metadata: MetaData,
) -> Table:
    """Create a SQLAlchemy ``Table`` from resolved type hints and field defaults."""
    columns: list[Column[Any]] = []
    for field_name, type_hint in resolved_hints.items():
        default_val = namespace.get(field_name)
        sa_info: SAColumnInfo | None = None
        if isinstance(default_val, FieldInfo):
            sa_info = _get_sa_info(default_val)
        columns.append(_build_sa_column(field_name, type_hint, sa_info))

    return Table(tablename, target_metadata, *columns)


# ---------------------------------------------------------------------------
# Metaclass
# ---------------------------------------------------------------------------

_BUILDING: set[str] = set()


@dataclass_transform(kw_only_default=True, field_specifiers=(Field,))
class SQLDataclassMeta(type):
    """Metaclass that transforms a class into a pydantic dataclass with an optional SA table."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        table: bool = False,  # noqa: FBT001, FBT002
        **kwargs: Any,
    ) -> type:
        # Base class itself — just create it normally
        if name == "SQLDataclass":
            cls = super().__new__(mcs, name, bases, namespace)
            cls.metadata = MetaData()  # type: ignore[attr-defined]
            return cls

        # Re-entry guard: pydantic_dataclass with slots=True internally calls
        # type(cls)(name, bases, ns) which re-enters this metaclass.
        qualname = f"{namespace.get('__module__', '')}.{name}"
        if qualname in _BUILDING:
            return super().__new__(mcs, name, bases, namespace)

        _BUILDING.add(qualname)
        try:
            result: type = _build_sqldataclass(mcs, name, bases, namespace, table=table, **kwargs)
            return result
        finally:
            _BUILDING.discard(qualname)


def _find_metadata(bases: tuple[type, ...]) -> MetaData:
    """Walk bases to find the SQLDataclass.metadata instance."""
    for base in bases:
        meta = getattr(base, "metadata", None)
        if isinstance(meta, MetaData):
            return meta
    return MetaData()


def _build_sqldataclass(
    mcs: type,
    name: str,
    bases: tuple[type, ...],
    namespace: dict[str, Any],
    *,
    table: bool,
    **kwargs: Any,
) -> Any:
    """Core logic for building a SQLDataclass (called from metaclass __new__)."""
    annotations = namespace.get("__annotations__", {})
    tablename = namespace.pop("__tablename__", _default_tablename(name))
    target_metadata = _find_metadata(bases)

    # Build SA table before pydantic transforms the class
    sa_table: Table | None = None
    if table:
        # Resolve type hints using a temporary class (needed for forward refs)
        temp_for_hints = type.__new__(type, name, (object,), {**namespace, "__annotations__": annotations})
        try:
            resolved = get_type_hints(temp_for_hints)
        except Exception:
            resolved = dict(annotations)
        sa_table = _build_table(tablename, resolved, namespace, target_metadata)

    # Create the actual class via the metaclass (keeps SQLDataclass in bases)
    cls: Any = type.__new__(mcs, name, bases, namespace, **kwargs)

    # Apply pydantic dataclass with slots for memory efficiency
    dc_cls: Any = pydantic_dataclass(cls, slots=True, kw_only=True)

    # Attach SA table and metadata
    dc_cls.__sqldataclass_is_table__ = table
    if sa_table is not None:
        dc_cls.__table__ = sa_table
        dc_cls.__tablename__ = tablename
        dc_cls.metadata = target_metadata
        dc_cls.c = sa_table.c
        _attach_convenience_methods(dc_cls)
    else:
        dc_cls.metadata = target_metadata

    return dc_cls


# ---------------------------------------------------------------------------
# Convenience methods (attached to table classes)
# ---------------------------------------------------------------------------


def _attach_convenience_methods(cls: Any) -> None:
    """Attach query/write convenience methods to a table class."""

    def _select(klass: Any) -> Any:
        """Build a ``SELECT`` for this table."""
        return sa_select(klass.__table__)

    def _model_load_all(
        klass: Any,
        conn: Connection,
        where: Any = None,
        order_by: Any = None,
    ) -> list[Any]:
        """Load all matching rows as instances of this class."""
        query = sa_select(klass.__table__)
        if where is not None:
            query = query.where(where)
        if order_by is not None:
            query = query.order_by(order_by)
        return _load_all(conn, query, klass)

    def _model_load_one(klass: Any, conn: Connection, where: Any = None) -> Any | None:
        """Load a single row, or ``None`` if not found."""
        query = sa_select(klass.__table__)
        if where is not None:
            query = query.where(where)
        row = _fetch_one(conn, query)
        if row is None:
            return None
        return klass(**row)

    def _model_insert_many(klass: Any, conn: Connection, objects: Sequence[Any]) -> None:
        """Bulk-insert multiple instances."""
        if not objects:
            return
        rows = [_flatten_for_table(obj) for obj in objects]
        _insert_many(conn, klass, rows)

    def _model_insert(self: Any, conn: Connection) -> None:
        """Insert this instance into the database."""
        flat = _flatten_for_table(self)
        _insert_row(conn, type(self), flat)

    def _model_upsert(self: Any, conn: Connection, *, index_elements: list[str]) -> None:
        """Upsert (PostgreSQL ON CONFLICT) this instance."""
        flat = _flatten_for_table(self)
        _upsert_row(conn, type(self), flat, index_elements=index_elements)

    def _model_to_dict(self: Any, *, exclude_keys: frozenset[str] = frozenset()) -> dict[str, Any]:
        """Convert to a flat dict suitable for SQL insertion."""
        return _flatten_for_table(self, exclude_keys=exclude_keys)

    cls.select = classmethod(_select)
    cls.load_all = classmethod(_model_load_all)
    cls.load_one = classmethod(_model_load_one)
    cls.insert_many = classmethod(_model_insert_many)
    cls.insert = _model_insert
    cls.upsert = _model_upsert
    cls.to_dict = _model_to_dict


# ---------------------------------------------------------------------------
# SQLDataclass base class
# ---------------------------------------------------------------------------


class SQLDataclass(metaclass=SQLDataclassMeta):
    """Base class for SQLDataclass models.

    Subclass with ``table=True`` to create a database-backed model::

        class Hero(SQLDataclass, table=True):
            id: int | None = Field(default=None, primary_key=True)
            name: str

    Subclass without ``table=True`` for pure data models (API schemas)::

        class HeroCreate(SQLDataclass):
            name: str
    """

    metadata: ClassVar[MetaData]

    if TYPE_CHECKING:
        # These attributes/methods are dynamically attached by the metaclass
        # when table=True. Declared here so type checkers can see them.
        __table__: ClassVar[Table]
        __tablename__: ClassVar[str]
        __sqldataclass_is_table__: ClassVar[bool]
        c: ClassVar[Any]

        @classmethod
        def select(cls) -> Any: ...

        @classmethod
        def load_all(
            cls,
            conn: Connection,
            where: Any = None,
            order_by: Any = None,
        ) -> list[Self]: ...

        @classmethod
        def load_one(cls, conn: Connection, where: Any = None) -> Self | None: ...

        @classmethod
        def insert_many(cls, conn: Connection, objects: Sequence[Self]) -> None: ...

        def insert(self, conn: Connection) -> None: ...

        def upsert(self, conn: Connection, *, index_elements: list[str]) -> None: ...

        def to_dict(self, *, exclude_keys: frozenset[str] = frozenset()) -> dict[str, Any]: ...
