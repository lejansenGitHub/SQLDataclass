"""Query execution — the core of the memory-light approach.

Prefer `load_all` over `fetch_all` + manual loop: `load_all` converts rows to
domain objects inline during cursor iteration, avoiding the intermediate
`list[dict]` memory spike.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from functools import cache
from itertools import takewhile
from operator import itemgetter
from typing import Any, TypeVar

from pydantic import ValidationError
from pydantic_core import ArgsKwargs, SchemaValidator
from sqlalchemy import select
from sqlalchemy.engine import Connection, Result
from sqlalchemy.sql import Executable

_T = TypeVar("_T")

RowBuilder = Callable[[Sequence[Any]], Any]
"""Builds one object from one row tuple whose values are in the order of the result keys."""


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

    Validates through ``validate_python``, which skips the ``__init__`` wrapper
    but runs every validator, so loaded objects behave like constructed ones.
    """
    validator = getattr(cls, "__pydantic_validator__", None)
    if validator is not None:
        return validator.validate_python(data)
    return cls(**data)


def _dataclass_schema(cls: type) -> dict[str, Any] | None:
    """The ``dataclass`` node of a pydantic dataclass's core schema; ``None`` for any other class."""
    schema = getattr(cls, "__pydantic_core_schema__", None)
    definitions: dict[str, Any] = {}
    while isinstance(schema, dict) and schema["type"] in ("definitions", "definition-ref"):
        if schema["type"] == "definitions":
            definitions.update({definition["ref"]: definition for definition in schema["definitions"]})
            schema = schema["schema"]
        else:
            schema = definitions.get(schema["schema_ref"])
    if not isinstance(schema, dict) or schema["type"] != "dataclass":
        return None
    return schema


def _with_positional_fields(dataclass_schema: dict[str, Any]) -> dict[str, Any]:
    """A copy of a ``dataclass`` schema node whose fields also accept positional arguments."""
    arguments = dataclass_schema["schema"]
    fields = [{**field, "kw_only": False} for field in arguments["fields"]]
    return {**dataclass_schema, "schema": {**arguments, "fields": fields}}


def _positional_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """A copy of a dataclass core schema whose top-level dataclass accepts positional arguments.

    A self-referencing class's schema is ``definitions`` around a ``definition-ref``;
    then the referenced definition is the one rewritten (nested references to the class
    accept positional input too, which changes nothing for dicts or instances).
    """
    if schema["type"] != "definitions":
        return _with_positional_fields(schema)
    inner = schema["schema"]
    if inner["type"] == "dataclass":
        return {**schema, "schema": _with_positional_fields(inner)}
    definitions = [
        _with_positional_fields(definition) if definition.get("ref") == inner["schema_ref"] else definition
        for definition in schema["definitions"]
    ]
    return {**schema, "definitions": definitions}


@cache
def _positional_validator(cls: type) -> SchemaValidator:
    """A load-only validator for ``cls`` that takes field values positionally.

    SQLDataclass classes are ``kw_only``, so their own validator rejects positional
    values. This one is compiled from the same schema with ``kw_only`` switched off: it
    builds instances of ``cls`` with the same validators, defaults and ``__post_init__``,
    and spares building a dict per row. Normal construction stays keyword-only.
    """
    schema = cls.__pydantic_core_schema__  # type: ignore[attr-defined]  # pydantic dataclass API
    dataclass_schema = _dataclass_schema(cls)
    assert dataclass_schema is not None  # callers pass pydantic dataclasses only
    return SchemaValidator(_positional_schema(schema), dataclass_schema.get("config"))


def _positional_plan(
    schema: dict[str, Any],
    keys: tuple[str, ...],
) -> tuple[tuple[int, ...], tuple[tuple[str, int], ...]] | None:
    """Row positions of the positional arguments and of the keyword arguments for these keys.

    Arguments follow the schema's field order, which can differ from the declaration
    order. Positions count every field, ``init=False`` ones included, so the positional
    part ends at the first field that is missing from the keys or not an ``__init__``
    argument; later fields go by keyword. ``None`` when positional input could behave
    differently from a dict: aliased fields, duplicate keys, keys naming ``init=False``
    fields, or unknown keys on a class that does not ignore extra input.
    """
    fields = schema["schema"]["fields"]
    if len(set(keys)) != len(keys) or any(field.get("validation_alias") for field in fields):
        return None
    init_names = [field["name"] for field in fields if field.get("init", True)]
    unknown = set(keys) - set(init_names)
    if unknown & {field["name"] for field in fields}:
        return None
    if unknown and schema.get("config", {}).get("extra_fields_behavior", "ignore") != "ignore":
        return None
    positions = {key: position for position, key in enumerate(keys)}
    leading = [
        field["name"]
        for field in takewhile(lambda field: field.get("init", True) and field["name"] in positions, fields)
    ]
    later = [name for name in init_names if name in positions and name not in leading]
    return tuple(positions[name] for name in leading), tuple((name, positions[name]) for name in later)


def _tuple_getter(positions: tuple[int, ...]) -> Callable[[Sequence[Any]], tuple[Any, ...]]:
    """``itemgetter`` that always returns a tuple, also for zero or one position."""
    if len(positions) > 1:
        return itemgetter(*positions)
    if positions:
        position = positions[0]
        return lambda row: (row[position],)
    return lambda row: ()


@cache
def row_builder(cls: type, keys: tuple[str, ...]) -> RowBuilder:
    """The function that validates one row with these ``keys`` into a ``cls`` instance.

    Pydantic dataclasses are validated positionally from the row tuple; when that
    raises, the row is validated again as a dict so the error names the fields.
    BaseModels and pydantic dataclasses the positional path cannot serve are
    validated from a dict; other classes are called with the row as keywords.
    """
    validator = getattr(cls, "__pydantic_validator__", None)
    if validator is None:
        return lambda row: cls(**dict(zip(keys, row, strict=True)))

    def from_dict(row: Sequence[Any]) -> Any:
        return validator.validate_python(dict(zip(keys, row, strict=True)))

    schema = _dataclass_schema(cls)
    plan = None if schema is None else _positional_plan(schema, keys)
    if plan is None:
        return from_dict
    argument_positions, keyword_positions = plan
    validate = _positional_validator(cls).validate_python
    arguments_of = _tuple_getter(argument_positions)

    def positional(row: Sequence[Any]) -> Any:
        try:
            return validate(ArgsKwargs(arguments_of(row)))
        except ValidationError:
            return from_dict(row)

    def positional_and_keywords(row: Sequence[Any]) -> Any:
        keywords = {name: row[position] for name, position in keyword_positions}
        try:
            return validate(ArgsKwargs(arguments_of(row), keywords))
        except ValidationError:
            return from_dict(row)

    return positional_and_keywords if keyword_positions else positional


def load_all(conn: Connection, query: Executable, cls: type[_T]) -> list[_T]:
    """Execute query and construct domain objects directly — no intermediate list[dict].

    Each row is converted to a domain object inline as the cursor is iterated,
    avoiding the memory spike of materializing all rows as dicts first.
    Pydantic dataclasses and BaseModels (SQLModel) are both validated.
    """
    result = conn.execute(query)
    build = row_builder(cls, tuple(result.keys()))  # type: ignore[arg-type]  # mypy does not treat classes as Hashable cache keys
    return [build(row) for row in result]


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
