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
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
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
from sqlalchemy import (
    delete as sa_delete,
)
from sqlalchemy import select as sa_select
from sqlalchemy import (
    update as sa_update,
)
from sqlalchemy.engine import Connection, Engine
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

    sa_type = _TYPE_MAP.get(inner)
    if sa_type is None:
        raise TypeError(
            f"Cannot map Python type {tp!r} to a SQLAlchemy column type. "
            f"Use Field(sa_type=...) to specify explicitly."
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


def _build_table(
    tablename: str,
    resolved_hints: dict[str, Any],
    namespace: dict[str, Any],
    target_metadata: MetaData,
    *,
    relationship_fields: set[str],
) -> Table:
    """Create a SQLAlchemy ``Table`` from resolved type hints and field defaults.

    Fields in *relationship_fields* are skipped (they are not database columns).
    """
    columns: list[Column[Any]] = []
    for field_name, type_hint in resolved_hints.items():
        if field_name in relationship_fields:
            continue
        default_val = namespace.get(field_name)
        sa_info: SAColumnInfo | None = None
        if isinstance(default_val, FieldInfo):
            sa_info = _get_sa_info(default_val)
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

        rel_info = _get_rel_info(default_val)  # type: ignore[arg-type]
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
        query = query.order_by(order_by)
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
    disc_value = base_data.get(rel.discriminator)  # type: ignore[arg-type]
    active_type = _find_active_variant(rel.target_types, rel.discriminator, disc_value)  # type: ignore[arg-type]
    if active_type is None or not hasattr(active_type, "__table__"):
        return None
    target_table: Table = active_type.__table__
    prefix = f"__{rel.field_name}__{target_table.name}__"
    nested = _extract_prefixed(row_dict, prefix)
    disc_key: str = rel.discriminator  # type: ignore[assignment]
    if disc_key not in nested:
        nested[disc_key] = disc_value
    return active_type(**nested)


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
        return target_type(**nested)
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

    return cls(**base_data)


# ---------------------------------------------------------------------------
# Collection relationship loading (one-to-many, many-to-many)
# ---------------------------------------------------------------------------

_MODEL_REGISTRY: dict[str, Any] = {}


def _get_pk_column(cls: Any) -> Column[Any]:
    """Get the single primary key column for a table model."""
    pk_cols = list(cls.__table__.primary_key.columns)
    if len(pk_cols) != 1:
        msg = f"Collection relationships require a single-column PK, got {len(pk_cols)} on {cls}"
        raise TypeError(msg)
    result: Column[Any] = pk_cols[0]
    return result


def _find_fk_column(child_table: Table, parent_table: Table) -> Column[Any] | None:
    """Find the FK column on child_table that references parent_table."""
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
    """Resolve any string forward references in target_types."""
    rel.target_types = [_resolve_forward_ref(t) for t in rel.target_types]


def _populate_collections(
    cls: Any,
    parents: list[Any],
    conn: Connection,
) -> None:
    """Load one-to-many and many-to-many children and attach to parent instances.

    Uses a two-query strategy: one query per collection relationship that loads
    ALL children for ALL parents at once (no N+1).
    """
    relationships: dict[str, _ResolvedRelationship] = cls.__relationships__
    collection_rels = {k: v for k, v in relationships.items() if v.kind in ("one_to_many", "many_to_many")}
    if not collection_rels or not parents:
        return

    parent_table: Table = cls.__table__
    pk_col = _get_pk_column(cls)
    pk_name = pk_col.name
    parent_pks = [getattr(p, pk_name) for p in parents]
    pk_to_parents: dict[Any, list[Any]] = {}
    for p in parents:
        pk_to_parents.setdefault(getattr(p, pk_name), []).append(p)

    for field_name, rel in collection_rels.items():
        _ensure_resolved(rel)
        child_type = rel.target_types[0]
        if not hasattr(child_type, "__table__"):
            continue

        if rel.kind == "one_to_many":
            _load_one_to_many(
                conn, field_name, child_type, parent_table, parent_pks, pk_to_parents, order_by=rel.order_by,
            )
        elif rel.kind == "many_to_many" and rel.link_model is not None:
            _load_many_to_many(
                conn, field_name, child_type, rel.link_model, parent_table, parent_pks, pk_to_parents,
                order_by=rel.order_by,
            )


def _load_one_to_many(  # noqa: PLR0913
    conn: Connection,
    field_name: str,
    child_type: Any,
    parent_table: Table,
    parent_pks: list[Any],
    pk_to_parents: dict[Any, list[Any]],
    order_by: str | None = None,
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
        child = child_type(**row)
        fk_value = row[fk_col.name]
        children_by_fk.setdefault(fk_value, []).append(child)

    for pk_val, parent_list in pk_to_parents.items():
        children = children_by_fk.get(pk_val, [])
        for parent in parent_list:
            object.__setattr__(parent, field_name, children)


def _load_many_to_many(  # noqa: PLR0913
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

    targets_by_source: dict[Any, list[Any]] = {}
    for row in conn.execute(query).mappings():
        source_fk_val = row["__link_source_fk__"]
        target_data = _extract_prefixed(dict(row), "__target__")
        target = target_type(**target_data)
        targets_by_source.setdefault(source_fk_val, []).append(target)

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

    # Detect relationship fields (not columns)
    relationship_fields: set[str] = set()
    resolved_rels: dict[str, _ResolvedRelationship] = {}
    for field_name in annotations:
        default_val = namespace.get(field_name)
        if _is_relationship(default_val):
            relationship_fields.add(field_name)

    # Build SA table before pydantic transforms the class
    sa_table: Table | None = None
    if table:
        # Resolve type hints using a temporary class (needed for forward refs)
        temp_for_hints = type.__new__(type, name, (object,), {**namespace, "__annotations__": annotations})
        try:
            resolved = get_type_hints(temp_for_hints)
        except Exception:
            resolved = dict(annotations)

        sa_table = _build_table(
            tablename, resolved, namespace, target_metadata, relationship_fields=relationship_fields,
        )

        # Resolve relationships (needs resolved type hints)
        resolved_rels = _resolve_relationships(temp_for_hints, resolved, namespace)

    # Create the actual class via the metaclass (keeps SQLDataclass in bases)
    cls: Any = type.__new__(mcs, name, bases, namespace, **kwargs)

    # Apply pydantic dataclass with slots for memory efficiency
    dc_cls: Any = pydantic_dataclass(cls, slots=True, kw_only=True)

    # Attach SA table, relationships, and metadata
    dc_cls.__sqldataclass_is_table__ = table
    dc_cls.__relationships__ = resolved_rels
    if sa_table is not None:
        dc_cls.__table__ = sa_table
        dc_cls.__tablename__ = tablename
        dc_cls.metadata = target_metadata
        dc_cls.c = sa_table.c
        _attach_convenience_methods(dc_cls)
        # Register for forward reference resolution
        _MODEL_REGISTRY[tablename] = dc_cls
    else:
        dc_cls.metadata = target_metadata

    return dc_cls


# ---------------------------------------------------------------------------
# Engine binding
# ---------------------------------------------------------------------------

_BOUND_ENGINE: Engine | None = None


def _get_engine(cls: Any) -> Engine:
    """Get the bound engine, or raise if not bound."""
    engine = getattr(cls, "__sqldataclass_engine__", None) or _BOUND_ENGINE
    if engine is None:
        msg = (
            "No connection provided and no engine bound. "
            "Either pass conn= or call SQLDataclass.bind(engine) first."
        )
        raise RuntimeError(msg)
    return engine


# ---------------------------------------------------------------------------
# Convenience methods (attached to table classes)
# ---------------------------------------------------------------------------


def _attach_convenience_methods(cls: Any) -> None:  # noqa: PLR0915
    """Attach query/write convenience methods to a table class."""

    def _select(klass: Any) -> Any:
        """Build a ``SELECT`` for this table."""
        return sa_select(klass.__table__)

    def _model_load_all(  # noqa: PLR0913
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
                query = query.order_by(order_by)
            query = _apply_pagination(query)
            results = _load_all(conn, query, klass)
        else:
            query = sa_select(klass.__table__)
            if where is not None:
                query = query.where(where)
            if order_by is not None:
                query = query.order_by(order_by)
            query = _apply_pagination(query)
            return _load_all(conn, query, klass)

        _populate_collections(klass, results, conn)
        return results

    def _model_load_one(klass: Any, conn: Connection | None = None, where: Any = None) -> Any | None:
        """Load a single row, or ``None`` if not found."""
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
            result = klass(**flat_row)
        else:
            query = sa_select(klass.__table__)
            if where is not None:
                query = query.where(where)
            flat_row = _fetch_one(conn, query)
            if flat_row is None:
                return None
            return klass(**flat_row)

        _populate_collections(klass, [result], conn)
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
        """Insert this instance into the database."""
        if conn is None:
            with _get_engine(type(self)).begin() as auto_conn:
                _model_insert(self, auto_conn)
                return
        flat = _flatten_for_table(self)
        _insert_row(conn, type(self), flat)

    def _model_upsert(self: Any, conn: Connection | None = None, *, index_elements: list[str]) -> None:
        """Upsert (PostgreSQL ON CONFLICT) this instance."""
        if conn is None:
            with _get_engine(type(self)).begin() as auto_conn:
                _model_upsert(self, auto_conn, index_elements=index_elements)
                return
        flat = _flatten_for_table(self)
        _upsert_row(conn, type(self), flat, index_elements=index_elements)

    def _model_update(klass: Any, values: dict[str, Any], conn: Connection | None = None, where: Any = None) -> int:
        """Update rows matching *where* with *values*. Returns number of rows updated."""
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
        if conn is None:
            with _get_engine(klass).begin() as auto_conn:
                return _model_delete(klass, auto_conn, where=where)
        stmt = sa_delete(klass.__table__)
        if where is not None:
            stmt = stmt.where(where)
        result = conn.execute(stmt)
        return result.rowcount

    def _model_to_dict(self: Any, *, exclude_keys: frozenset[str] = frozenset()) -> dict[str, Any]:
        """Convert to a flat dict suitable for SQL insertion."""
        return _flatten_for_table(self, exclude_keys=exclude_keys)

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
    def bind(cls, engine: Engine) -> None:
        """Bind an engine so ``conn`` becomes optional on all methods.

        Call once at startup::

            SQLDataclass.bind(engine)

        Then use without passing conn::

            heroes = Hero.load_all()
            hero.insert()
        """
        global _BOUND_ENGINE  # noqa: PLW0603
        _BOUND_ENGINE = engine

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
