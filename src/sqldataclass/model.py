"""SQLDataclass — single-class definition combining pydantic dataclass + SQLAlchemy table.

Usage::

    from sqldataclass import SQLDataclass, Field

    class Hero(SQLDataclass, table=True):
        id: int | None = Field(default=None, primary_key=True)
        name: str
        secret_name: str
        age: int | None = None

Relationships::

    class Team(SQLDataclass, table=True):
        id: int | None = Field(default=None, primary_key=True)
        name: str

    class Hero(SQLDataclass, table=True):
        id: int | None = Field(default=None, primary_key=True)
        name: str
        team_id: int = Field(foreign_key="team.id")
        team: Team | None = Relationship()

Discriminated unions::

    class Participant(SQLDataclass, table=True):
        id: int | None = Field(default=None, primary_key=True)
        name: str
        behavior: str
        data: NormalData | BatteryData = Relationship(discriminator="behavior")
"""

from __future__ import annotations

import re
import types
from copy import deepcopy
from dataclasses import dataclass
from dataclasses import fields as dc_fields
from datetime import date, datetime, time
from decimal import Decimal
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Literal,
    Self,
    Sequence,
    TypeVar,
    dataclass_transform,
    get_args,
    get_origin,
    get_type_hints,
)
from uuid import UUID

from pydantic import ConfigDict, TypeAdapter
from pydantic import Field as PydanticField
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined
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
from sqlalchemy import (
    delete as sa_delete,
)
from sqlalchemy import select as sa_select
from sqlalchemy import (
    update as sa_update,
)
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.types import TypeEngine

# ---------------------------------------------------------------------------
# Pydantic dataclass configs
# ---------------------------------------------------------------------------
from sqldataclass.cy_function_helper import CyFunctionDetector
from sqldataclass.query import _fast_construct
from sqldataclass.query import fetch_one as _fetch_one
from sqldataclass.query import load_all as _load_all
from sqldataclass.versioning import (
    __DO_MIGRATION__,
    do_migration,
    version_field_name_for,
)
from sqldataclass.write import flatten_for_table as _flatten_for_table
from sqldataclass.write import insert_many as _insert_many
from sqldataclass.write import upsert_row_returning as _upsert_row_returning

_T = TypeVar("_T")

_DATACLASS_CONFIG = ConfigDict(
    allow_inf_nan=False,
    arbitrary_types_allowed=True,
    extra="forbid",
    ignored_types=(CyFunctionDetector,),
)

_STI_CHILD_CONFIG = ConfigDict(
    allow_inf_nan=False,
    arbitrary_types_allowed=True,
    extra="ignore",
    ignored_types=(CyFunctionDetector,),
)

_MAX_RELATIONSHIP_DEPTH = 5

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
        if len(non_none) == 1 and len(args) == 2:  # noqa: PLR2004  # T | None always has exactly 2 union members
            return non_none[0], True
    return tp, False


def _unwrap_union_variants(tp: Any) -> list[Any]:
    """Return all non-None variants from a union type, or [tp] if not a union."""
    origin = get_origin(tp)
    if origin is types.UnionType:
        return [a for a in get_args(tp) if a is not type(None)]
    return [tp]


def _python_type_to_sa(tp: Any) -> type[TypeEngine[Any]]:
    """Map a Python type to a SQLAlchemy column type."""
    inner, _ = _unwrap_optional(tp)

    # Literal["foo", "bar"] → String
    if get_origin(inner) is Literal:
        return String

    # Annotated[X, ...] → unwrap to X
    if get_origin(inner) is Annotated:
        inner = get_args(inner)[0]

    # NewType("Kilometers", float) → unwrap to float
    while hasattr(inner, "__supertype__"):
        inner = inner.__supertype__

    sa_type = _TYPE_MAP.get(inner)
    if sa_type is None:
        raise TypeError(
            f"Cannot map Python type {tp!r} to a SQLAlchemy column type. Use Field(sa_type=...) to specify explicitly."
        )
    return sa_type


def _is_model_type(tp: Any) -> bool:
    """Check if a type is a SQLDataclass model (has __table__)."""
    return isinstance(tp, type) and hasattr(tp, "__sqldataclass_is_table__")


# ---------------------------------------------------------------------------
# SA column metadata (attached to pydantic FieldInfo)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class SAColumnInfo:
    """SQLAlchemy column configuration attached to a Field's metadata."""

    primary_key: bool = False
    nullable: bool | None = None
    index: bool = False
    column: bool = True
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
# Relationship metadata (attached to pydantic FieldInfo)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class RelationshipInfo:
    """Marks a field as a relationship (not a database column).

    For many-to-one: the FK is inferred from existing Field(foreign_key=...) columns.
    For one-to-many: type hint is ``list[ChildModel]``.
    For many-to-many: type hint is ``list[TargetModel]`` with ``link_model`` set.
    For discriminated unions: set ``discriminator`` to the column that selects the variant.
    """

    discriminator: str | None = None
    back_populates: str | None = None
    link_model: Any = None
    order_by: str | None = None


def _get_rel_info(field_info: FieldInfo) -> RelationshipInfo | None:
    """Extract ``RelationshipInfo`` from a pydantic ``FieldInfo.metadata`` list."""
    for item in field_info.metadata:
        if isinstance(item, RelationshipInfo):
            return item
    return None


def _is_relationship(default_val: Any) -> bool:
    """Check if a field default is a Relationship-bearing FieldInfo."""
    if isinstance(default_val, FieldInfo):
        return _get_rel_info(default_val) is not None
    return False


# ---------------------------------------------------------------------------
# Field() — user-facing function
# ---------------------------------------------------------------------------

_UNSET: Any = object()


def Field(  # noqa: PLR0913  # many parameters required for SA column mapping
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
    column: bool = True,
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
        column=column,
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


def Relationship(
    default: Any = _UNSET,
    *,
    back_populates: str | None = None,
    link_model: Any = None,
    discriminator: str | None = None,
    order_by: str | None = None,
) -> Any:
    """Mark a field as a relationship — not stored as a database column.

    Many-to-one (auto-detected from FK)::

        team: Team | None = Relationship()

    One-to-many (detected from ``list[X]`` type hint)::

        heroes: list[Hero] = Relationship(back_populates="team")

    Many-to-many (via link table)::

        teams: list[Team] = Relationship(link_model=HeroTeamLink)

    Discriminated union::

        data: NormalData | BatteryData = Relationship(discriminator="behavior")

    Ordered collection::

        heroes: list[Hero] = Relationship(back_populates="team", order_by="name")
    """
    rel_info = RelationshipInfo(
        discriminator=discriminator,
        back_populates=back_populates,
        link_model=link_model,
        order_by=order_by,
    )

    pydantic_kwargs: dict[str, Any] = {}
    if default is not _UNSET:
        pydantic_kwargs["default"] = default
    elif discriminator is not None:
        pass  # required field for discriminated unions
    else:
        # Default: None for scalar, list factory for collections.
        # The actual default_factory for list[] types is set here;
        # pydantic can't distinguish scalar vs list from FieldInfo alone,
        # so callers should use default_factory=list for list fields.
        # For simplicity, default to None — _hydrate_row initializes [] for collections,
        # and _populate_collections fills them in. Manual construction needs explicit [].
        pydantic_kwargs["default"] = None

    field_info: Any = PydanticField(**pydantic_kwargs)
    field_info.metadata.append(rel_info)
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


def _inject_implicit_fk_fields(
    resolved_hints: dict[str, Any],
    namespace: dict[str, Any],
    annotations: dict[str, Any],
    relationship_fields: set[str],
) -> None:
    """Auto-create ``{name}_id`` FK fields for many-to-one relationships that lack an explicit FK.

    Mutates *resolved_hints*, *namespace*, and *annotations* in place.
    Only acts on scalar (non-list, non-discriminated) relationship fields
    whose target class has a ``__tablename__`` and a primary-key column.
    """
    for field_name in list(relationship_fields):
        fk_field_name = f"{field_name}_id"
        if fk_field_name in resolved_hints or fk_field_name in annotations:
            continue  # explicit FK already declared

        type_hint = resolved_hints.get(field_name)
        if type_hint is None:
            continue

        inner, _is_optional = _unwrap_optional(type_hint)

        # Skip collections (one-to-many / many-to-many) and unions (discriminated)
        if get_origin(inner) is list:
            continue
        if get_origin(inner) is types.UnionType:
            continue

        # inner should be the target model class
        target_tablename = getattr(inner, "__tablename__", None)
        if target_tablename is None:
            continue

        # Find the PK column name on the target table
        target_table = getattr(inner, "__table__", None)
        if target_table is None:
            continue
        pk_cols = [col.name for col in target_table.primary_key.columns]
        if len(pk_cols) != 1:
            continue  # skip composite PKs
        pk_name = pk_cols[0]

        # Inject the FK field
        fk_target = f"{target_tablename}.{pk_name}"
        fk_field_info = Field(default=None, foreign_key=fk_target)
        resolved_hints[fk_field_name] = int | None
        annotations[fk_field_name] = "int | None"
        namespace[fk_field_name] = fk_field_info


def _build_table(
    tablename: str,
    resolved_hints: dict[str, Any],
    namespace: dict[str, Any],
    target_metadata: MetaData,
    *,
    relationship_fields: set[str],
) -> Table:
    """Create a SQLAlchemy ``Table`` from resolved type hints and field defaults.

    Fields in *relationship_fields* and fields with ``column=False`` are skipped.
    """
    columns: list[Column[Any]] = []
    for field_name, type_hint in resolved_hints.items():
        if field_name in relationship_fields:
            continue
        default_val = namespace.get(field_name)
        sa_info: SAColumnInfo | None = None
        if isinstance(default_val, FieldInfo):
            sa_info = _get_sa_info(default_val)
        if sa_info is not None and not sa_info.column:
            continue
        columns.append(_build_sa_column(field_name, type_hint, sa_info))

    return Table(tablename, target_metadata, *columns)


# ---------------------------------------------------------------------------
# Relationship query builder and hydration
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _ResolvedRelationship:
    """Fully resolved relationship metadata for query building."""

    field_name: str
    target_types: list[Any]  # [Team] for many-to-one, [NormalData, BatteryData] for union
    discriminator: str | None
    is_optional: bool
    kind: str  # "many_to_one", "one_to_many", "many_to_many", "discriminated"
    back_populates: str | None = None
    link_model: Any = None
    order_by: str | None = None


def _resolve_relationships(
    cls: Any,
    annotations: dict[str, Any],
    namespace: dict[str, Any],
) -> dict[str, _ResolvedRelationship]:
    """Build resolved relationship info from annotations and field defaults."""
    rels: dict[str, _ResolvedRelationship] = {}
    for field_name, type_hint in annotations.items():
        default_val = namespace.get(field_name)
        if not _is_relationship(default_val):
            continue

        rel_info = _get_rel_info(default_val)  # type: ignore[arg-type]  # default_val narrowed at runtime
        if rel_info is None:
            msg = f"Field {field_name!r} is marked as a relationship but has no RelationshipInfo metadata"
            raise TypeError(msg)

        inner, is_optional = _unwrap_optional(type_hint)

        # Detect kind from type hint
        if rel_info.discriminator:
            kind = "discriminated"
            variants = _unwrap_union_variants(inner)
            target_types = variants
        elif get_origin(inner) is list:
            # list[X] → one-to-many or many-to-many
            args = get_args(inner)
            element_type = args[0] if args else Any
            target_types = [element_type]
            kind = "many_to_many" if rel_info.link_model is not None else "one_to_many"
        else:
            kind = "many_to_one"
            target_types = [inner]

        rels[field_name] = _ResolvedRelationship(
            field_name=field_name,
            target_types=target_types,
            discriminator=rel_info.discriminator,
            is_optional=is_optional or kind == "many_to_one",
            kind=kind,
            back_populates=rel_info.back_populates,
            link_model=rel_info.link_model,
            order_by=rel_info.order_by,
        )
    return rels


def _find_fk_join_condition(source_table: Table, target_table: Table) -> Any | None:
    """Find the join condition from FKs on source that reference target."""
    for col in source_table.columns:
        for fk in col.foreign_keys:
            if fk.column.table is target_table:
                return col == fk.column
    # Also check reverse: target FK → source
    for col in target_table.columns:
        for fk in col.foreign_keys:
            if fk.column.table is source_table:
                return col == fk.column
    return None


def _has_join_relationships(relationships: dict[str, _ResolvedRelationship]) -> bool:
    """Check if any relationships require JOINs (many-to-one or discriminated)."""
    return any(rel.kind in ("many_to_one", "discriminated") for rel in relationships.values())


def _build_joined_query(cls: Any, where: Any = None, order_by: Any = None) -> Any:
    """Build a SELECT with labeled columns and JOINs for scalar relationships.

    Collection relationships (one-to-many, many-to-many) are loaded via
    separate queries in ``_populate_collections``, not via JOINs.
    """
    base_table: Table = cls.__table__
    relationships: dict[str, _ResolvedRelationship] = cls.__relationships__

    # Label base table columns with prefix to avoid name collisions
    labeled: list[Any] = [c.label(f"__base__{c.name}") for c in base_table.columns]

    # Build join chain — only for scalar relationships (many-to-one, discriminated)
    base_from: Any = base_table
    for field_name, rel in relationships.items():
        if rel.kind in ("one_to_many", "many_to_many"):
            continue  # loaded separately
        for target_type in rel.target_types:
            if not hasattr(target_type, "__table__"):
                continue
            target_table: Table = target_type.__table__
            prefix = f"__{field_name}__{target_table.name}__"
            labeled.extend(c.label(f"{prefix}{c.name}") for c in target_table.columns)

            join_cond = _find_fk_join_condition(base_table, target_table)
            if join_cond is not None:
                base_from = base_from.outerjoin(target_table, join_cond)

    query = sa_select(*labeled).select_from(base_from)

    if where is not None:
        query = query.where(where)
    if order_by is not None:
        if isinstance(order_by, list | tuple):
            query = query.order_by(*order_by)
        else:
            query = query.order_by(*order_by) if isinstance(order_by, list | tuple) else query.order_by(order_by)
    return query


def _extract_prefixed(row_dict: dict[str, Any], prefix: str) -> dict[str, Any]:
    """Extract and strip-prefix entries from a row dict."""
    return {k.removeprefix(prefix): v for k, v in row_dict.items() if k.startswith(prefix)}


def _hydrate_discriminated(
    row_dict: dict[str, Any],
    base_data: dict[str, Any],
    rel: _ResolvedRelationship,
) -> Any:
    """Hydrate a discriminated union relationship field."""
    disc_value = base_data.get(rel.discriminator)  # type: ignore[arg-type]  # discriminator is str at runtime
    active_type = _find_active_variant(rel.target_types, rel.discriminator, disc_value)  # type: ignore[arg-type]  # discriminator is str at runtime
    if active_type is None or not hasattr(active_type, "__table__"):
        return None
    target_table: Table = active_type.__table__
    prefix = f"__{rel.field_name}__{target_table.name}__"
    nested = _extract_prefixed(row_dict, prefix)
    disc_key: str = rel.discriminator  # type: ignore[assignment]  # narrowed to str after None check above
    if disc_key not in nested:
        nested[disc_key] = disc_value
    return _fast_construct(active_type, nested)


def _find_active_variant(variants: list[Any], discriminator: str, disc_value: Any) -> Any:
    """Find which union variant matches the discriminator value."""
    for variant in variants:
        variant_hints = get_type_hints(variant)
        if discriminator in variant_hints:
            for lit_val in get_args(variant_hints[discriminator]):
                if lit_val == disc_value:
                    return variant
    return None


def _hydrate_many_to_one(row_dict: dict[str, Any], rel: _ResolvedRelationship) -> Any:
    """Hydrate a simple many-to-one relationship field."""
    target_type = rel.target_types[0]
    if not hasattr(target_type, "__table__"):
        return None
    target_table: Table = target_type.__table__
    prefix = f"__{rel.field_name}__{target_table.name}__"
    nested = _extract_prefixed(row_dict, prefix)
    if nested and any(v is not None for v in nested.values()):
        return _fast_construct(target_type, nested)
    return None


def _hydrate_row(cls: Any, row: Any) -> Any:
    """Hydrate a flat row (with labeled columns) into a nested model instance."""
    relationships: dict[str, _ResolvedRelationship] = cls.__relationships__
    row_dict = dict(row)

    base_data = _extract_prefixed(row_dict, "__base__")

    for field_name, rel in relationships.items():
        if rel.kind in ("one_to_many", "many_to_many"):
            base_data[field_name] = []  # placeholder, populated later
        elif rel.kind == "discriminated":
            base_data[field_name] = _hydrate_discriminated(row_dict, base_data, rel)
        else:
            base_data[field_name] = _hydrate_many_to_one(row_dict, rel)

    return _fast_construct(cls, base_data)


# ---------------------------------------------------------------------------
# Collection relationship loading (one-to-many, many-to-many)
# ---------------------------------------------------------------------------

_MODEL_REGISTRY: dict[str, Any] = {}


def _get_pk_columns(cls: Any) -> list[Column[Any]]:
    """Get the primary key column(s) for a table model."""
    return list(cls.__table__.primary_key.columns)


def _get_pk_value(obj: Any, pk_names: list[str]) -> Any:
    """Get PK value as a scalar (single PK) or tuple (composite PK)."""
    if len(pk_names) == 1:
        return getattr(obj, pk_names[0])
    return tuple(getattr(obj, name) for name in pk_names)


def _find_fk_column(child_table: Table, parent_table: Table) -> Column[Any] | None:
    """Find the FK column on child_table that references parent_table."""
    # Fast path: check model registry for cached FK map
    child_cls = _MODEL_REGISTRY.get(child_table.name)
    if child_cls is not None:
        fk_map: dict[str, Any] = getattr(child_cls, "__fk_map__", {})
        cached: Column[Any] | None = fk_map.get(parent_table.name)
        if cached is not None:
            return cached
    # Slow path: scan columns
    for col in child_table.columns:
        for fk in col.foreign_keys:
            if fk.column.table is parent_table:
                return col
    return None


def _resolve_forward_ref(tp: Any) -> Any:
    """Resolve a string forward reference to a model class via the registry."""
    if isinstance(tp, str):
        # Try direct tablename lookup
        resolved = _MODEL_REGISTRY.get(tp)
        if resolved is not None:
            return resolved
        # Try snake_case conversion
        snake = _default_tablename(tp)
        resolved = _MODEL_REGISTRY.get(snake)
        if resolved is not None:
            return resolved
        # Try class name match
        for registered_cls in _MODEL_REGISTRY.values():
            if registered_cls.__name__ == tp:
                return registered_cls
    return tp


def _ensure_resolved(rel: _ResolvedRelationship) -> None:
    """Resolve any string forward references in target_types (cached after first call)."""
    if all(not isinstance(t, str) for t in rel.target_types):
        return  # already resolved
    rel.target_types = [_resolve_forward_ref(t) for t in rel.target_types]


def _populate_collections(  # noqa: PLR0912  # many relationship variants require many branches
    cls: Any,
    parents: list[Any],
    conn: Connection,
    *,
    _depth: int = 0,
) -> None:
    """Load one-to-many and many-to-many children and attach to parent instances.

    Uses a two-query strategy: one query per collection relationship that loads
    ALL children for ALL parents at once (no N+1).

    After loading children, recursively populates their relationships too
    (nested relationship loading), up to a max depth to prevent infinite loops.
    """
    if _depth > _MAX_RELATIONSHIP_DEPTH:  # prevent infinite recursion on circular relationships
        return

    relationships: dict[str, _ResolvedRelationship] = cls.__relationships__
    collection_rels = {k: v for k, v in relationships.items() if v.kind in ("one_to_many", "many_to_many")}
    if not collection_rels or not parents:
        return

    parent_table: Table = cls.__table__
    pk_cols = _get_pk_columns(cls)
    pk_names = [c.name for c in pk_cols]
    parent_pks: list[Any] = []
    pk_to_parents: dict[Any, list[Any]] = {}
    for p in parents:
        pk_val = _get_pk_value(p, pk_names)
        parent_pks.append(pk_val)
        pk_to_parents.setdefault(pk_val, []).append(p)

    all_loaded_children: list[Any] = []

    for field_name, rel in collection_rels.items():
        _ensure_resolved(rel)
        child_type = rel.target_types[0]
        if not hasattr(child_type, "__table__"):
            continue

        if rel.kind == "one_to_many":
            _load_one_to_many(
                conn,
                field_name,
                child_type,
                parent_table,
                parent_pks,
                pk_to_parents,
                order_by=rel.order_by,
                back_populates=rel.back_populates,
            )
        elif rel.kind == "many_to_many" and rel.link_model is not None:
            _load_many_to_many(
                conn,
                field_name,
                child_type,
                rel.link_model,
                parent_table,
                parent_pks,
                pk_to_parents,
                order_by=rel.order_by,
            )

        # Collect all loaded children for recursive population
        for parent in parents:
            children = getattr(parent, field_name, None)
            if isinstance(children, list):
                all_loaded_children.extend(children)

    # Recursively populate ONLY collection relationships on loaded children.
    # Skip M2M/scalar reloads on children — they weren't explicitly requested.
    # This handles the League → Team → Hero chain without loading hero.tags etc.
    if all_loaded_children and _depth < _MAX_RELATIONSHIP_DEPTH:
        by_type: dict[type, list[Any]] = {}
        for child in all_loaded_children:
            by_type.setdefault(type(child), []).append(child)
        for child_cls, children in by_type.items():
            child_rels = getattr(child_cls, "__relationships__", {})
            has_child_collections = any(r.kind == "one_to_many" for r in child_rels.values())
            if has_child_collections:
                _populate_collections(child_cls, children, conn, _depth=_depth + 1)


def _reload_scalar_relationships(objects: list[Any], cls: Any, conn: Connection) -> None:  # noqa: PLR0912  # many relationship variants require many branches
    """For objects whose scalar (many-to-one) rels are None, load them via query."""
    rels: dict[str, _ResolvedRelationship] = getattr(cls, "__relationships__", {})
    if not hasattr(cls, "__table__"):
        return

    for rel in rels.values():
        if rel.kind != "many_to_one":
            continue
        _ensure_resolved(rel)
        target_type = rel.target_types[0]
        if not hasattr(target_type, "__table__"):
            continue

        # Find FK column name on cls that references target
        fk_field: str | None = None
        for col in cls.__table__.columns:
            for fk in col.foreign_keys:
                if fk.column.table is target_type.__table__:
                    fk_field = col.name
                    break
            if fk_field:
                break
        if fk_field is None:
            continue

        # Collect FK values for objects where this relationship is None
        fk_values: list[Any] = []
        for obj in objects:
            if getattr(obj, rel.field_name, None) is None:
                fk_val = getattr(obj, fk_field, None)
                if fk_val is not None:
                    fk_values.append(fk_val)

        if not fk_values:
            continue

        # Load target objects in batch
        target_table: Table = target_type.__table__
        target_pk = list(target_table.primary_key.columns)[0]
        query = sa_select(target_table).where(target_pk.in_(fk_values))
        targets_by_pk: dict[Any, Any] = {}
        for row in conn.execute(query).mappings():
            target = _fast_construct(target_type, dict(row))
            targets_by_pk[row[target_pk.name]] = target

        # Assign to objects
        for obj in objects:
            if getattr(obj, rel.field_name, None) is None:
                fk_val = getattr(obj, fk_field, None)
                if fk_val in targets_by_pk:
                    object.__setattr__(obj, rel.field_name, targets_by_pk[fk_val])


def _populate_scalar_chains(objects: list[Any], conn: Connection, *, _depth: int = 0) -> None:
    """Populate unfilled scalar (many-to-one) relationship chains.

    Only handles many-to-one chains (Hero → team → league). Does NOT trigger
    collection (one-to-many, many-to-many) loading on nested objects — that
    would be an expensive N+1 pattern for unrelated relationships.
    """
    if _depth > _MAX_RELATIONSHIP_DEPTH:
        return

    by_type: dict[type, list[Any]] = {}
    for obj in objects:
        by_type.setdefault(type(obj), []).append(obj)

    for cls, instances in by_type.items():
        rels: dict[str, _ResolvedRelationship] = getattr(cls, "__relationships__", {})
        for rel in rels.values():
            if rel.kind != "many_to_one":
                continue
            _ensure_resolved(rel)
            target_type = rel.target_types[0]
            if not hasattr(target_type, "__table__"):
                continue
            # Single pass: collect nested values and check if any need loading
            fname = rel.field_name
            nested_values = [getattr(inst, fname, None) for inst in instances]
            needs_load = any(v is None for v in nested_values)

            if needs_load:
                _reload_scalar_relationships(instances, cls, conn)
                nested_values = [getattr(inst, fname, None) for inst in instances]

            nested = [v for v in nested_values if v is not None]
            if nested:
                _populate_scalar_chains(nested, conn, _depth=_depth + 1)


def _load_one_to_many(  # noqa: PLR0913  # relationship loading needs all context params
    conn: Connection,
    field_name: str,
    child_type: Any,
    parent_table: Table,
    parent_pks: list[Any],
    pk_to_parents: dict[Any, list[Any]],
    order_by: str | None = None,
    back_populates: str | None = None,
) -> None:
    """Load children for a one-to-many relationship."""
    child_table: Table = child_type.__table__
    fk_col = _find_fk_column(child_table, parent_table)
    if fk_col is None:
        return

    query = sa_select(child_table).where(fk_col.in_(parent_pks))
    if order_by is not None and order_by in child_table.c:
        query = query.order_by(child_table.c[order_by])
    children_by_fk: dict[Any, list[Any]] = {}
    for row in conn.execute(query).mappings():
        child = _fast_construct(child_type, dict(row))
        fk_value = row[fk_col.name]
        children_by_fk.setdefault(fk_value, []).append(child)

    for pk_val, parent_list in pk_to_parents.items():
        children = children_by_fk.get(pk_val, [])
        for parent in parent_list:
            object.__setattr__(parent, field_name, children)
            # Set back-reference on children → parent (no extra query needed)
            if back_populates is not None:
                for child in children:
                    object.__setattr__(child, back_populates, parent)


def _load_many_to_many(  # noqa: PLR0913  # relationship loading needs all context params
    conn: Connection,
    field_name: str,
    target_type: Any,
    link_model: Any,
    parent_table: Table,
    parent_pks: list[Any],
    pk_to_parents: dict[Any, list[Any]],
    order_by: str | None = None,
) -> None:
    """Load targets for a many-to-many relationship via a link table."""
    if not hasattr(link_model, "__table__") or not hasattr(target_type, "__table__"):
        return

    link_table: Table = link_model.__table__
    target_table: Table = target_type.__table__

    # Find FK from link → parent (source) and link → target
    source_fk_col = _find_fk_column(link_table, parent_table)
    target_fk_col = _find_fk_column(link_table, target_table)
    if source_fk_col is None or target_fk_col is None:
        return

    # Find the PK column on target
    target_pk_cols = list(target_table.primary_key.columns)
    if not target_pk_cols:
        return
    target_pk = target_pk_cols[0]

    # SELECT link.source_fk, target.* FROM link JOIN target ON link.target_fk = target.pk
    # WHERE link.source_fk IN (parent_pks)
    labeled_source_fk = source_fk_col.label("__link_source_fk__")
    target_labeled = [c.label(f"__target__{c.name}") for c in target_table.columns]

    query = (
        sa_select(labeled_source_fk, *target_labeled)
        .select_from(link_table.join(target_table, target_fk_col == target_pk))
        .where(source_fk_col.in_(parent_pks))
    )
    if order_by is not None and order_by in target_table.c:
        query = query.order_by(target_table.c[order_by])

    # Cache targets by PK to deduplicate (e.g., 20 tags shared across 5000 heroes)
    target_pk_name = target_pk.name
    target_cache: dict[Any, Any] = {}
    targets_by_source: dict[Any, list[Any]] = {}
    for row in conn.execute(query).mappings():
        source_fk_val = row["__link_source_fk__"]
        target_data = _extract_prefixed(dict(row), "__target__")
        pk_val = target_data.get(target_pk_name)
        if pk_val not in target_cache:
            target_cache[pk_val] = _fast_construct(target_type, target_data)
        targets_by_source.setdefault(source_fk_val, []).append(target_cache[pk_val])

    for pk_val, parent_list in pk_to_parents.items():
        targets = targets_by_source.get(pk_val, [])
        for parent in parent_list:
            object.__setattr__(parent, field_name, targets)


# ---------------------------------------------------------------------------
# Metaclass
# ---------------------------------------------------------------------------

_BUILDING: set[str] = set()


@dataclass_transform(kw_only_default=True, field_specifiers=(Field, Relationship))
class SQLDataclassMeta(type):
    """Metaclass that transforms a class into a pydantic dataclass with an optional SA table."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        table: bool = False,  # noqa: FBT001, FBT002  # bool flag required by metaclass __new__ protocol
        versioned: bool = False,  # noqa: FBT001, FBT002  # bool flag required by metaclass __new__ protocol
        **kwargs: Any,
    ) -> type:
        # Base class itself — just create it normally
        if name == "SQLDataclass":
            cls = super().__new__(mcs, name, bases, namespace)
            cls.metadata = MetaData()  # type: ignore[attr-defined]  # SA table attrs set dynamically by metaclass
            return cls

        # Enforce no cross-inheritance with SQLModel
        for base in bases:
            if getattr(base, "__sqlmodel_is_basemodel__", False):
                msg = f"{name} cannot inherit from both SQLDataclass and SQLModel. Use composition instead."
                raise TypeError(msg)

        # Single-table inheritance: auto-detect if parent has __discriminator__
        sti_parent = _find_sti_parent(bases)
        if sti_parent is not None and not table:
            inherited: type = _build_sti_child(mcs, name, bases, namespace, sti_parent)
            return inherited

        # Response model: table=False child of table=True parent
        table_parent = _find_table_parent(bases)
        if table_parent is not None and not table:
            exclude = kwargs.pop("exclude", None) or frozenset()
            response: type = _build_response_model(
                mcs,
                name,
                bases,
                namespace,
                table_parent,
                exclude=frozenset(exclude),
            )
            return response

        # Re-entry guard: pydantic_dataclass with slots=True internally calls
        # type(cls)(name, bases, ns) which re-enters this metaclass.
        qualname = f"{namespace.get('__module__', '')}.{name}"
        if qualname in _BUILDING:
            return super().__new__(mcs, name, bases, namespace)

        _BUILDING.add(qualname)
        try:
            result: type = _build_sqldataclass(
                mcs,
                name,
                bases,
                namespace,
                table=table,
                versioned=versioned,
                **kwargs,
            )
            return result
        finally:
            _BUILDING.discard(qualname)


def _make_migration_validator(ArgsKwargs: type) -> classmethod:  # type: ignore[type-arg]  # pydantic internal type, no public generic param
    """Create a pydantic before-validator for versioned dataclass migration."""
    from pydantic import model_validator

    def _validator_fn(cls: type, obj: Any) -> Any:
        obj = obj.kwargs or {} if isinstance(obj, ArgsKwargs) else obj  # type: ignore[attr-defined]  # ArgsKwargs.kwargs exists at runtime
        if __DO_MIGRATION__.get():
            return do_migration(obj, cls)
        return obj

    return model_validator(mode="before")(classmethod(_validator_fn))  # type: ignore[arg-type,return-value]  # pydantic validator decorator typing is imprecise


_STI_REGISTRY: dict[str, dict[str, Any]] = {}
"""Maps parent tablename → {discriminator_value: child_class}."""


def _find_sti_parent(bases: tuple[type, ...]) -> Any | None:
    """Return the parent class if it has __discriminator__ (STI base)."""
    for base in bases:
        if getattr(base, "__discriminator__", None) is not None:
            return base
    return None


def _find_table_parent(bases: tuple[type, ...]) -> Any | None:
    """Return the first base that is a table=True SQLDataclass (without __discriminator__)."""
    for base in bases:
        if getattr(base, "__sqldataclass_is_table__", False) and not getattr(base, "__discriminator__", None):
            return base
    return None


def _build_sti_child(  # noqa: PLR0915  # single-table inheritance setup is inherently complex
    mcs: type,
    name: str,
    bases: tuple[type, ...],
    namespace: dict[str, Any],
    parent: Any,
) -> Any:
    """Build a single-table inheritance child class.

    The child:
    - Shares the parent's ``__table__``
    - Appends its new fields as nullable columns to the parent table
    - Auto-filters queries by discriminator value
    - Auto-sets discriminator on insert
    """
    discriminator_column: str = parent.__discriminator__
    discriminator_value: str = namespace.pop(
        "__discriminator_value__",
        name.lower(),
    )

    # Append child-specific columns to parent table
    child_annotations = namespace.get("__annotations__", {})
    parent_table: Table = parent.__table__
    existing_col_names = {c.name for c in parent_table.columns}

    for field_name, type_hint in child_annotations.items():
        if field_name in existing_col_names:
            continue  # already on parent table
        if _is_relationship(namespace.get(field_name)):
            continue  # not a column
        try:
            col = _build_sa_column(field_name, type_hint, None)
            # Force nullable since not all subtypes have this column
            col.nullable = True
            parent_table.append_column(col)
        except TypeError:
            pass  # unmappable type, skip

    # Build pydantic dataclass: merge parent + child fields
    qualname = f"{namespace.get('__module__', '')}.{name}"
    _BUILDING.add(qualname)
    try:
        parent_annotations: dict[str, Any] = {}
        if hasattr(parent, "__pydantic_fields__"):
            for field_name, field_info in parent.__pydantic_fields__.items():
                parent_annotations[field_name] = field_info.annotation

        merged_annotations = {**parent_annotations, **child_annotations}
        namespace["__annotations__"] = merged_annotations

        # Copy parent defaults for fields not overridden by child
        for field_name in parent_annotations:
            if field_name not in namespace and hasattr(parent, "__pydantic_fields__"):
                pfield = parent.__pydantic_fields__[field_name]
                if pfield.default is not PydanticUndefined:
                    namespace[field_name] = pfield.default
                elif pfield.default_factory is not None:
                    namespace[field_name] = PydanticField(default_factory=pfield.default_factory)

        clean_bases = tuple(b for b in bases if not isinstance(b, SQLDataclassMeta)) or (object,)
        cls: Any = type.__new__(mcs, name, clean_bases, namespace)
        # STI children use extra="ignore" since the shared table has columns from other subtypes
        dc_cls: Any = pydantic_dataclass(cls, config=_STI_CHILD_CONFIG, slots=True, kw_only=True)
    finally:
        _BUILDING.discard(qualname)

    # Share parent's table and metadata
    dc_cls.__table__ = parent.__table__
    dc_cls.__tablename__ = parent.__tablename__
    dc_cls.__discriminator__ = discriminator_column
    dc_cls.__sqldataclass_is_table__ = True
    dc_cls.__sqldataclass_inherit__ = True
    dc_cls.__sqldataclass_discriminator_column__ = discriminator_column
    dc_cls.__sqldataclass_discriminator_value__ = discriminator_value
    dc_cls.__relationships__ = getattr(parent, "__relationships__", {})
    dc_cls.metadata = getattr(parent, "metadata", MetaData())
    dc_cls.c = parent.__table__.c

    # Register subtype for polymorphic loading
    parent_key = parent.__tablename__
    _STI_REGISTRY.setdefault(parent_key, {})[discriminator_value] = dc_cls

    # Store registry ref on parent for polymorphic load_all
    if not hasattr(parent, "__sqldataclass_sti_registry__"):
        parent.__sqldataclass_sti_registry__ = _STI_REGISTRY[parent_key]
        parent.__sqldataclass_sti_column__ = discriminator_column

    _attach_convenience_methods(dc_cls)
    _MODEL_REGISTRY[f"{parent_key}__{discriminator_value}"] = dc_cls

    return dc_cls


def _build_response_model(  # noqa: PLR0913  # mirrors _build_sti_child signature
    mcs: type,
    name: str,
    bases: tuple[type, ...],
    namespace: dict[str, Any],
    parent: Any,
    *,
    exclude: frozenset[str] = frozenset(),
) -> Any:
    """Build a pure pydantic dataclass that inherits fields from a table=True parent.

    The child has no SQLAlchemy table, no convenience methods (load_all, insert, etc.),
    and is not registered in the model registry. It is suitable for use as a FastAPI
    response model.
    """
    # Validate exclude against parent fields
    parent_field_names = set(parent.__pydantic_fields__) if hasattr(parent, "__pydantic_fields__") else set()
    child_annotations = namespace.get("__annotations__", {})
    available_fields = parent_field_names | set(child_annotations)
    unknown = exclude - available_fields
    if unknown:
        msg = f"{name}: exclude contains fields not present on parent or child: {unknown}"
        raise TypeError(msg)

    # Merge parent annotations + defaults into child namespace (same approach as _build_sti_child)
    # Use get_type_hints with include_extras=True to preserve Annotated metadata (e.g. UnitMeta),
    # since pydantic's field_info.annotation strips the Annotated wrapper.
    parent_relationships: set[str] = set(getattr(parent, "__relationships__", {}))

    parent_annotations: dict[str, Any] = {}
    original_hints = get_type_hints(parent, include_extras=True) if hasattr(parent, "__pydantic_fields__") else {}
    for field_name in parent.__pydantic_fields__ if hasattr(parent, "__pydantic_fields__") else ():
        if field_name in parent_relationships:
            continue  # response models don't inherit relationships
        if field_name in original_hints:
            parent_annotations[field_name] = original_hints[field_name]

    merged_annotations = {**parent_annotations, **child_annotations}

    # Copy parent defaults for fields not overridden by child
    for field_name in parent_annotations:
        if field_name not in namespace and hasattr(parent, "__pydantic_fields__"):
            pfield = parent.__pydantic_fields__[field_name]
            if pfield.default is not PydanticUndefined:
                namespace[field_name] = pfield.default
            elif pfield.default_factory is not None:
                namespace[field_name] = PydanticField(default_factory=pfield.default_factory)

    # Apply exclude: remove from annotations and defaults
    for field_name in exclude:
        merged_annotations.pop(field_name, None)
        namespace.pop(field_name, None)

    namespace["__annotations__"] = merged_annotations

    # Build on clean bases to avoid inheriting table machinery via MRO
    qualname = f"{namespace.get('__module__', '')}.{name}"
    _BUILDING.add(qualname)
    try:
        clean_bases = tuple(b for b in bases if not isinstance(b, SQLDataclassMeta)) or (object,)
        cls: Any = type.__new__(mcs, name, clean_bases, namespace)
        dc_cls: Any = pydantic_dataclass(cls, config=_DATACLASS_CONFIG, slots=True, kw_only=True)
    finally:
        _BUILDING.discard(qualname)

    dc_cls.__sqldataclass_is_table__ = False
    dc_cls.__relationships__ = {}
    dc_cls.__non_column_fields__ = frozenset()

    def _from_parent(cls: Any, /, parent_instance: Any, **overrides: Any) -> Any:
        """Construct this response model from a parent (table=True) instance.

        Takes the parent's field values, keeps only the fields that exist
        on this child, and applies any keyword overrides.
        """
        own_fields = set(cls.__pydantic_fields__)
        data = {key: value for key, value in parent_instance.to_dict().items() if key in own_fields}
        data.update(overrides)
        return cls(**data)

    dc_cls.from_parent = classmethod(_from_parent)

    return dc_cls


def _find_metadata(bases: tuple[type, ...]) -> MetaData:
    """Walk bases to find the SQLDataclass.metadata instance."""
    for base in bases:
        meta = getattr(base, "metadata", None)
        if isinstance(meta, MetaData):
            return meta
    return MetaData()


def _build_sqldataclass(  # noqa: PLR0912, PLR0913, PLR0915  # metaclass builder is inherently complex
    mcs: type,
    name: str,
    bases: tuple[type, ...],
    namespace: dict[str, Any],
    *,
    table: bool,
    versioned: bool = False,
    **kwargs: Any,
) -> Any:
    """Core logic for building a SQLDataclass (called from metaclass __new__)."""
    annotations = namespace.get("__annotations__", {})
    tablename = namespace.pop("__tablename__", _default_tablename(name))
    target_metadata = _find_metadata(bases)

    # Detect non-column fields and relationships
    relationship_fields: set[str] = set()
    non_column_fields: set[str] = set()
    resolved_rels: dict[str, _ResolvedRelationship] = {}
    for field_name in annotations:
        default_val = namespace.get(field_name)
        if _is_relationship(default_val):
            relationship_fields.add(field_name)
        elif isinstance(default_val, FieldInfo):
            sa_info = _get_sa_info(default_val)
            if sa_info is not None and not sa_info.column:
                # column=False fields must have a default (DB load won't provide a value)
                if default_val.default is PydanticUndefined and default_val.default_factory is None:
                    msg = (
                        f"Field '{field_name}' has column=False but no default value. "
                        "Non-persistent fields must have a default or default_factory."
                    )
                    raise TypeError(msg)
                non_column_fields.add(field_name)

    # Build SA table before pydantic transforms the class
    sa_table: Table | None = None
    if table:
        # Resolve type hints using a temporary class (needed for forward refs)
        temp_for_hints = type.__new__(type, name, (object,), {**namespace, "__annotations__": annotations})
        try:
            resolved = get_type_hints(temp_for_hints)
        except Exception:
            resolved = dict(annotations)

        # Auto-create implicit FK fields for many-to-one relationships
        _inject_implicit_fk_fields(resolved, namespace, annotations, relationship_fields)

        sa_table = _build_table(
            tablename,
            resolved,
            namespace,
            target_metadata,
            relationship_fields=relationship_fields,
        )

        # Resolve relationships (needs resolved type hints)
        resolved_rels = _resolve_relationships(temp_for_hints, resolved, namespace)

    # Versioned models: inject a before-validator for migration
    if versioned:
        from pydantic_core import ArgsKwargs

        namespace["__validator_migration"] = _make_migration_validator(ArgsKwargs)

    # Create the actual class via the metaclass (keeps SQLDataclass in bases)
    cls: Any = type.__new__(mcs, name, bases, namespace, **kwargs)

    # Apply pydantic dataclass with slots for memory efficiency
    dc_cls: Any = pydantic_dataclass(cls, config=_DATACLASS_CONFIG, slots=True, kw_only=True)

    # Attach SA table, relationships, non-column fields, and metadata
    dc_cls.__sqldataclass_is_table__ = table
    dc_cls.__versioned__ = versioned
    dc_cls.__relationships__ = resolved_rels
    dc_cls.__non_column_fields__ = frozenset(non_column_fields)
    if sa_table is not None:
        dc_cls.__table__ = sa_table
        dc_cls.__tablename__ = tablename
        dc_cls.metadata = target_metadata
        dc_cls.c = sa_table.c
        # Pre-compute FK map for fast lookups: {target_table_name: fk_column}
        # Wrapped in try/except because FK targets may not exist yet (forward refs)
        fk_map: dict[str, Column[Any]] = {}
        for col in sa_table.columns:
            for fk in col.foreign_keys:
                try:
                    fk_map[fk.column.table.name] = col
                except Exception:
                    pass  # target table not yet created, will use slow path
        dc_cls.__fk_map__ = fk_map
        _attach_convenience_methods(dc_cls)
        # Register for forward reference resolution
        _MODEL_REGISTRY[tablename] = dc_cls
    else:
        dc_cls.metadata = target_metadata

    # Versioned models: validate the version field exists and is correct
    if versioned:
        vf_name = version_field_name_for(name)
        # Check the field exists in annotations
        all_fields = {f.name for f in dc_fields(dc_cls)}
        if vf_name not in all_fields:
            msg = f"Versioned model {name} requires a field '{vf_name}: int = <VERSION_NUM>'"
            raise AttributeError(msg)
        # Check it has an int default
        for f in dc_fields(dc_cls):
            if f.name == vf_name:
                default = f.default
                if isinstance(default, FieldInfo):
                    default = default.default
                if not isinstance(default, int):
                    msg = f"Version field '{vf_name}' must have an int default, got {type(default).__name__}"
                    raise AttributeError(msg)
                break

    return dc_cls


# ---------------------------------------------------------------------------
# Engine binding
# ---------------------------------------------------------------------------

_BOUND_ENGINE: Engine | None = None


def _get_engine(cls: Any) -> Engine:
    """Get the bound engine, or raise if not bound."""
    engine = getattr(cls, "__sqldataclass_engine__", None) or _BOUND_ENGINE
    if engine is None:
        msg = "No connection provided and no engine bound. Either pass conn= or call SQLDataclass.bind(engine) first."
        raise RuntimeError(msg)
    return engine


# ---------------------------------------------------------------------------
# Convenience methods (attached to table classes)
# ---------------------------------------------------------------------------


def _polymorphic_load(conn: Connection, query: Any, cls: type[_T]) -> list[_T]:
    """Load rows, constructing the correct STI subtype for each row if applicable."""
    sti_registry = getattr(cls, "__sqldataclass_sti_registry__", None)
    sti_column = getattr(cls, "__sqldataclass_sti_column__", None)

    if sti_registry is None or sti_column is None:
        # No STI — just construct cls for every row
        return _load_all(conn, query, cls)

    # Polymorphic: pick the right subtype per row
    results: list[_T] = []
    for row in conn.execute(query).mappings():
        disc_value = row.get(sti_column)
        target_cls = sti_registry.get(disc_value, cls)
        results.append(target_cls(**row))
    return results


def _apply_discriminator_filter(klass: Any, where: Any) -> Any:
    """If klass is an inherited subtype, prepend the discriminator filter."""
    if not getattr(klass, "__sqldataclass_inherit__", False):
        return where
    col_name = klass.__sqldataclass_discriminator_column__
    col_val = klass.__sqldataclass_discriminator_value__
    disc_filter = klass.__table__.c[col_name] == col_val
    if where is not None:
        return disc_filter & where
    return disc_filter


def _apply_discriminator_on_insert(klass: Any, flat: dict[str, Any]) -> dict[str, Any]:
    """If klass is an inherited subtype, set the discriminator value on insert."""
    if not getattr(klass, "__sqldataclass_inherit__", False):
        return flat
    flat[klass.__sqldataclass_discriminator_column__] = klass.__sqldataclass_discriminator_value__
    return flat


def _insert_relationships(instance: Any, conn: Connection) -> None:
    """Cascade-insert many-to-one relationships before the parent INSERT.

    For each many-to-one relationship field that holds a non-None value whose
    PK is not yet set, insert the related object and copy its PK into the
    FK column on the parent instance.
    """
    rels: dict[str, _ResolvedRelationship] = getattr(type(instance), "__relationships__", {})
    fk_map: dict[str, Column[Any]] = getattr(type(instance), "__fk_map__", {})

    for rel in rels.values():
        if rel.kind != "many_to_one":
            continue

        related = getattr(instance, rel.field_name, None)
        if related is None:
            continue

        # Check if the related object needs inserting (PK is None)
        target_table = getattr(type(related), "__table__", None)
        if target_table is None:
            continue
        pk_cols = [col.name for col in target_table.primary_key.columns]
        if len(pk_cols) != 1:
            continue
        pk_name = pk_cols[0]

        pk_value = getattr(related, pk_name, None)
        if pk_value is not None:
            # Already persisted — just ensure the FK is set
            fk_col = fk_map.get(target_table.name)
            if fk_col is not None:
                object.__setattr__(instance, fk_col.name, pk_value)
            continue

        # Recursively insert the related object (handles nested relationships)
        related.insert(conn)

        # Copy the generated PK into the FK field
        pk_value = getattr(related, pk_name)
        fk_col = fk_map.get(target_table.name)
        if fk_col is not None:
            object.__setattr__(instance, fk_col.name, pk_value)


def _attach_convenience_methods(cls: Any) -> None:  # noqa: PLR0915  # attaches many methods in one pass
    """Attach query/write convenience methods to a table class."""

    def _select(klass: Any) -> Any:
        """Build a ``SELECT`` for this table."""
        return sa_select(klass.__table__)

    def _model_load_all(  # noqa: PLR0913  # mirrors query.load_all signature
        klass: Any,
        conn: Connection | None = None,
        where: Any = None,
        order_by: Any = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[Any]:
        """Load all matching rows as instances of this class.

        If *conn* is ``None``, auto-creates a connection from the bound engine.
        """
        where = _apply_discriminator_filter(klass, where)
        if conn is None:
            with _get_engine(klass).connect() as auto_conn:
                return _model_load_all(klass, auto_conn, where=where, order_by=order_by, limit=limit, offset=offset)

        def _apply_pagination(q: Any) -> Any:
            if limit is not None:
                q = q.limit(limit)
            if offset is not None:
                q = q.offset(offset)
            return q

        rels: dict[str, _ResolvedRelationship] = getattr(klass, "__relationships__", {})
        if _has_join_relationships(rels):
            query = _apply_pagination(_build_joined_query(klass, where=where, order_by=order_by))
            results = [_hydrate_row(klass, row) for row in conn.execute(query).mappings()]
        elif rels:
            query = sa_select(klass.__table__)
            if where is not None:
                query = query.where(where)
            if order_by is not None:
                query = query.order_by(*order_by) if isinstance(order_by, list | tuple) else query.order_by(order_by)
            query = _apply_pagination(query)
            results = _load_all(conn, query, klass)
        else:
            query = sa_select(klass.__table__)
            if where is not None:
                query = query.where(where)
            if order_by is not None:
                query = query.order_by(*order_by) if isinstance(order_by, list | tuple) else query.order_by(order_by)
            query = _apply_pagination(query)
            return _polymorphic_load(conn, query, klass)

        _populate_collections(klass, results, conn)
        _populate_scalar_chains(results, conn, _depth=0)
        return results

    def _model_load_one(klass: Any, conn: Connection | None = None, where: Any = None) -> Any | None:
        """Load a single row, or ``None`` if not found."""
        where = _apply_discriminator_filter(klass, where)
        if conn is None:
            with _get_engine(klass).connect() as auto_conn:
                return _model_load_one(klass, auto_conn, where=where)

        rels: dict[str, _ResolvedRelationship] = getattr(klass, "__relationships__", {})
        if _has_join_relationships(rels):
            query = _build_joined_query(klass, where=where)
            row = conn.execute(query).mappings().one_or_none()
            if row is None:
                return None
            result = _hydrate_row(klass, row)
        elif rels:
            query = sa_select(klass.__table__)
            if where is not None:
                query = query.where(where)
            flat_row = _fetch_one(conn, query)
            if flat_row is None:
                return None
            result = _fast_construct(klass, flat_row)
        else:
            query = sa_select(klass.__table__)
            if where is not None:
                query = query.where(where)
            flat_row = _fetch_one(conn, query)
            if flat_row is None:
                return None
            return _fast_construct(klass, flat_row)

        _populate_collections(klass, [result], conn)
        _populate_scalar_chains([result], conn, _depth=0)
        return result

    def _model_insert_many(klass: Any, conn: Connection | None = None, objects: Sequence[Any] | None = None) -> None:
        """Bulk-insert multiple instances."""
        if objects is None:
            objects = []
        if not objects:
            return
        if conn is None:
            with _get_engine(klass).begin() as auto_conn:
                _model_insert_many(klass, auto_conn, objects=objects)
                return
        rows = [_flatten_for_table(obj) for obj in objects]
        _insert_many(conn, klass, rows)

    def _model_insert(self: Any, conn: Connection | None = None) -> None:
        """Insert this instance into the database.

        Cascades: many-to-one relationship values that have no PK are inserted
        first, and their generated PK is copied into the FK column.

        Uses RETURNING to populate DB-generated fields (e.g. autoincrement id,
        server defaults) on the instance in place.
        """
        if conn is None:
            with _get_engine(type(self)).begin() as auto_conn:
                _model_insert(self, auto_conn)
                return
        _insert_relationships(self, conn)
        flat = _apply_discriminator_on_insert(type(self), _flatten_for_table(self))
        target_table = type(self).__table__
        result = conn.execute(target_table.insert().values(flat).returning(target_table))
        row = result.mappings().fetchone()
        if row:
            for key, value in row.items():
                if hasattr(self, key):
                    object.__setattr__(self, key, value)

    def _model_upsert(self: Any, conn: Connection | None = None, *, index_elements: list[str]) -> None:
        """Upsert (PostgreSQL ON CONFLICT) this instance.

        Uses RETURNING to populate DB-generated fields on the instance in place.
        """
        if conn is None:
            with _get_engine(type(self)).begin() as auto_conn:
                _model_upsert(self, auto_conn, index_elements=index_elements)
                return
        flat = _flatten_for_table(self)
        _upsert_row_returning(conn, type(self), self, flat, index_elements=index_elements)

    def _model_update(klass: Any, values: dict[str, Any], conn: Connection | None = None, where: Any = None) -> int:
        """Update rows matching *where* with *values*. Returns number of rows updated."""
        where = _apply_discriminator_filter(klass, where)
        if conn is None:
            with _get_engine(klass).begin() as auto_conn:
                return _model_update(klass, values, auto_conn, where=where)
        stmt = sa_update(klass.__table__).values(values)
        if where is not None:
            stmt = stmt.where(where)
        result = conn.execute(stmt)
        return result.rowcount

    def _model_delete(klass: Any, conn: Connection | None = None, where: Any = None) -> int:
        """Delete rows matching *where*. Returns number of rows deleted."""
        where = _apply_discriminator_filter(klass, where)
        if conn is None:
            with _get_engine(klass).begin() as auto_conn:
                return _model_delete(klass, auto_conn, where=where)
        stmt = sa_delete(klass.__table__)
        if where is not None:
            stmt = stmt.where(where)
        result = conn.execute(stmt)
        return result.rowcount

    def _model_to_dict(self: Any, *, exclude_keys: frozenset[str] = frozenset()) -> dict[str, Any]:
        """Convert to a flat dict of all column fields, including None values."""
        return _flatten_for_table(self, exclude_keys=exclude_keys, strip_server_defaults=False)

    cls.select = classmethod(_select)
    cls.load_all = classmethod(_model_load_all)
    cls.load_one = classmethod(_model_load_one)
    cls.insert_many = classmethod(_model_insert_many)
    cls.update = classmethod(_model_update)
    cls.delete = classmethod(_model_delete)
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

    Relationships are declared with ``Relationship()``::

        class Hero(SQLDataclass, table=True):
            team_id: int = Field(foreign_key="team.id")
            team: Team | None = Relationship()
    """

    metadata: ClassVar[MetaData]

    @classmethod
    def load(cls, data: dict[str, Any]) -> Self:
        """Create an instance from a dict (e.g. JSON-deserialized data).

        For versioned models, this triggers migration if the data has an
        older schema version (or no version key at all).
        """
        if getattr(cls, "__versioned__", False):
            token = __DO_MIGRATION__.set(True)
            try:
                return cls(**data)
            finally:
                __DO_MIGRATION__.reset(token)
        return cls(**data)

    def dump(self) -> dict[str, Any]:
        """Serialize to a dict suitable for JSON.

        Excludes relationship fields and ``column=False`` fields.
        """
        non_col: frozenset[str] = getattr(type(self), "__non_column_fields__", frozenset())
        rel_keys: set[str] = set(getattr(type(self), "__relationships__", {}))
        exclude = non_col | rel_keys
        result: dict[str, Any] = TypeAdapter(type(self)).dump_python(
            self,
            warnings="error",
            mode="json",
            by_alias=True,
        )
        if exclude:
            for key in exclude:
                result.pop(key, None)
        return result

    def clone(self, *, deep: bool = False) -> Self:
        """Create a copy of this instance via dump + reload."""
        data = TypeAdapter(type(self)).dump_python(self, by_alias=True)
        new = type(self)(**data)
        return deepcopy(new) if deep else new

    @staticmethod
    def validate_private_field(annotation: Any, value: Any) -> Any:
        """Validate a value against a type annotation using pydantic."""
        return TypeAdapter(annotation).validate_python(value)

    @classmethod
    @lru_cache
    def model_field_names(cls) -> frozenset[str]:
        """Return all field names (using aliases where defined)."""
        result: set[str] = set()
        for field in dc_fields(cls):
            if isinstance(field.default, FieldInfo) and field.default.alias is not None:
                result.add(field.default.alias)
            else:
                result.add(field.name)
        return frozenset(result)

    @classmethod
    @lru_cache
    def data_fields(cls) -> frozenset[str]:
        """Return field names suitable for data operations.

        For versioned models, excludes ``_VERSION`` fields.
        """
        names = cls.model_field_names()
        if getattr(cls, "__versioned__", False):
            return frozenset(n for n in names if not n.endswith("_VERSION"))
        return names

    @classmethod
    def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
        """Override in versioned subclasses to transform old data.

        Only called when ``load()`` detects an older schema version.
        """
        return obj

    @classmethod
    @lru_cache
    def get_version_field_name(cls) -> str:
        """Return the auto-generated version field name for this class."""
        return version_field_name_for(cls.__name__)

    @classmethod
    @lru_cache
    def get_schema_version(cls) -> int:
        """Return the current schema version (the field's default value)."""
        vf = cls.get_version_field_name()
        for f in dc_fields(cls):
            if f.name == vf:
                default = f.default
                if isinstance(default, FieldInfo):
                    result: int = default.default
                    return result
                return int(default)  # type: ignore[arg-type]  # default is numeric at runtime after branch check
        msg = f"Version field '{vf}' not found on {cls.__name__}"
        raise AttributeError(msg)

    @classmethod
    def outdated(cls, obj: dict[str, Any]) -> bool:
        """Return True if the dict has an older schema version."""
        schema_ver = cls.get_schema_version()
        current: int = obj.get(cls.get_version_field_name(), 1)
        return current < schema_ver

    @classmethod
    def bind(cls, engine: Engine) -> None:
        """Bind an engine so ``conn`` becomes optional on all methods.

        Global binding (all models)::

            SQLDataclass.bind(engine)

        Per-model binding (overrides global)::

            Hero.bind(hero_engine)
            Team.bind(team_engine)

        Per-model engines take priority over the global engine.
        """
        if cls is SQLDataclass or cls.__name__ == "SQLDataclass":
            global _BOUND_ENGINE  # noqa: PLW0603  # module-level engine is the intended bind target
            _BOUND_ENGINE = engine
        else:
            cls.__sqldataclass_engine__ = engine  # type: ignore[attr-defined]  # per-model engine stored dynamically

    if TYPE_CHECKING:
        __table__: ClassVar[Table]
        __tablename__: ClassVar[str]
        __sqldataclass_is_table__: ClassVar[bool]
        __relationships__: ClassVar[dict[str, _ResolvedRelationship]]
        c: ClassVar[Any]

        @classmethod
        def select(cls) -> Any: ...

        @classmethod
        def load_all(
            cls,
            conn: Connection | None = None,
            where: Any = None,
            order_by: Any = None,
            limit: int | None = None,
            offset: int | None = None,
        ) -> list[Self]: ...

        @classmethod
        def load_one(cls, conn: Connection | None = None, where: Any = None) -> Self | None: ...

        @classmethod
        def insert_many(cls, conn: Connection | None = None, objects: Sequence[Self] | None = None) -> None: ...

        @classmethod
        def update(cls, values: dict[str, Any], conn: Connection | None = None, where: Any = None) -> int: ...

        @classmethod
        def delete(cls, conn: Connection | None = None, where: Any = None) -> int: ...

        def insert(self, conn: Connection | None = None) -> None: ...

        def upsert(self, conn: Connection | None = None, *, index_elements: list[str]) -> None: ...

        def to_dict(self, *, exclude_keys: frozenset[str] = frozenset()) -> dict[str, Any]: ...

        @classmethod
        def from_parent(cls, parent_instance: Any, **overrides: Any) -> Self: ...
