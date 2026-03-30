"""SQLModel — Pydantic BaseModel with optional SQLAlchemy Core table.

Usage::

    from sqldataclass import SQLModel, Field

    class Player(SQLModel, table=True):
        id: int | None = Field(default=None, primary_key=True)
        name: str

Pure data model (no table)::

    class PlayerCreate(SQLModel):
        name: str
"""

from __future__ import annotations

from copy import deepcopy
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Self,
    Sequence,
    get_type_hints,
)

import pydantic
from pydantic import ConfigDict, model_validator
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.engine import Engine

from sqldataclass.model import (
    _MODEL_REGISTRY,
    SQLDataclassMeta,
    _attach_convenience_methods,
    _build_table,
    _default_tablename,
    _get_sa_info,
    _is_relationship,
    _resolve_relationships,
    _ResolvedRelationship,
)
from sqldataclass.versioning import (
    __DO_MIGRATION__,
    do_migration,
    version_field_name_for,
)


class SQLModel(pydantic.BaseModel):
    """Base class for Pydantic BaseModel-backed models with optional SA table.

    Subclass with ``table=True`` to create a database-backed model::

        class Player(SQLModel, table=True):
            id: int | None = Field(default=None, primary_key=True)
            name: str

    Subclass without ``table=True`` for pure data models::

        class PlayerCreate(SQLModel):
            name: str
    """

    model_config = ConfigDict(
        allow_inf_nan=False,
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    metadata: ClassVar[MetaData] = MetaData()

    def __init_subclass__(cls, table: bool = False, versioned: bool = False, **kwargs: Any) -> None:  # noqa: FBT001, FBT002
        super().__init_subclass__(**kwargs)

        # Enforce no cross-inheritance with SQLDataclass
        for base in cls.__mro__:
            if isinstance(base, SQLDataclassMeta) and base.__name__ != "SQLDataclass":
                msg = f"{cls.__name__} cannot inherit from both SQLModel and SQLDataclass. Use composition instead."
                raise TypeError(msg)

        # Store the table flag for __pydantic_init_subclass__ to use later
        cls.__sqlmodel_is_basemodel__ = True
        cls.__sqldataclass_is_table__ = table
        cls.__versioned__ = versioned
        cls.__relationships__ = {}
        cls.__non_column_fields__ = frozenset()
        cls._sqlmodel_pending_table__ = table  # type: ignore[attr-defined]
        cls._sqlmodel_pending_versioned__ = versioned  # type: ignore[attr-defined]

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        """Called by Pydantic after model_fields are populated."""
        super().__pydantic_init_subclass__(**kwargs)

        table: bool = getattr(cls, "_sqlmodel_pending_table__", False)
        if not table:
            return

        # Build a namespace dict compatible with _build_table / _resolve_relationships
        namespace: dict[str, Any] = dict(cls.model_fields)

        tablename = cls.__dict__.get("__tablename__") or _default_tablename(cls.__name__)

        # Find metadata from class hierarchy
        target_metadata = cls.metadata

        # Detect non-column fields and relationships
        # For BaseModel, the FieldInfo itself carries the metadata (not the default)
        relationship_fields: set[str] = set()
        non_column_fields: set[str] = set()
        for field_name, field_info in cls.model_fields.items():
            if _is_relationship(field_info):
                relationship_fields.add(field_name)
            else:
                sa_info = _get_sa_info(field_info)
                if sa_info is not None and not sa_info.column:
                    non_column_fields.add(field_name)

        cls.__non_column_fields__ = frozenset(non_column_fields)

        # Resolve type hints — only include instance fields (exclude ClassVar)
        try:
            all_hints = get_type_hints(cls)
        except Exception:
            all_hints = dict(cls.__annotations__)
        instance_field_names = set(cls.model_fields) | relationship_fields
        resolved = {k: v for k, v in all_hints.items() if k in instance_field_names}

        # Build SA table
        sa_table: Table = _build_table(
            tablename,
            resolved,
            namespace,
            target_metadata,
            relationship_fields=relationship_fields,
        )

        # Resolve relationships
        resolved_rels = _resolve_relationships(cls, resolved, namespace)

        # Attach SA artifacts
        cls.__table__ = sa_table
        cls.__tablename__ = tablename
        cls.metadata = target_metadata
        cls.c = sa_table.c
        cls.__relationships__ = resolved_rels

        # Pre-compute FK map
        fk_map: dict[str, Column[Any]] = {}
        for col in sa_table.columns:
            for fk in col.foreign_keys:
                try:
                    fk_map[fk.column.table.name] = col
                except Exception:
                    pass
        cls.__fk_map__ = fk_map  # type: ignore[attr-defined]

        _attach_convenience_methods(cls)
        _MODEL_REGISTRY[tablename] = cls

        # Versioned models: validate the version field
        versioned: bool = getattr(cls, "_sqlmodel_pending_versioned__", False)
        if versioned:
            vf_name = version_field_name_for(cls.__name__)
            if vf_name not in cls.model_fields:
                msg = f"Versioned model {cls.__name__} requires a field '{vf_name}: int = <VERSION_NUM>'"
                raise AttributeError(msg)
            default = cls.model_fields[vf_name].default
            if not isinstance(default, int):
                msg = f"Version field '{vf_name}' must have an int default, got {type(default).__name__}"
                raise AttributeError(msg)

    @model_validator(mode="before")
    @classmethod
    def __validator_migration(cls, obj: dict[str, Any]) -> dict[str, Any]:
        if getattr(cls, "__versioned__", False) and __DO_MIGRATION__.get():
            return do_migration(obj, cls)
        return obj

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
        result = self.model_dump(warnings="error", by_alias=True, mode="json")
        if exclude:
            for key in exclude:
                result.pop(key, None)
        return result

    def clone(self, *, deep: bool = False) -> Self:
        """Create a copy of this instance via dump + reload."""
        data = self.model_dump(by_alias=True)
        new = type(self)(**data)
        return deepcopy(new) if deep else new

    @classmethod
    def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
        """Override in versioned subclasses to transform old data."""
        return obj

    @classmethod
    def get_version_field_name(cls) -> str:
        """Return the auto-generated version field name for this class."""
        return version_field_name_for(cls.__name__)

    @classmethod
    def get_schema_version(cls) -> int:
        """Return the current schema version (the field's default value)."""
        vf = cls.get_version_field_name()
        if vf in cls.model_fields:
            result: int = cls.model_fields[vf].default
            return result
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

        Global binding (all SQLModel classes)::

            SQLModel.bind(engine)

        Per-model binding::

            Player.bind(player_engine)
        """
        if cls is SQLModel:
            import sqldataclass.model as _model_mod

            _model_mod._BOUND_ENGINE = engine
        else:
            cls.__sqldataclass_engine__ = engine  # type: ignore[attr-defined]

    if TYPE_CHECKING:
        from sqlalchemy.engine import Connection

        __table__: ClassVar[Table]
        __tablename__: ClassVar[str]
        __sqldataclass_is_table__: ClassVar[bool]
        __sqlmodel_is_basemodel__: ClassVar[bool]
        __versioned__: ClassVar[bool]
        __relationships__: ClassVar[dict[str, _ResolvedRelationship]]
        __non_column_fields__: ClassVar[frozenset[str]]
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
