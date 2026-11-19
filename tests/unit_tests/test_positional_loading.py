"""SQLDataclass rows are validated positionally from the row tuple — SQLite in-memory.

SQLDataclass classes are keyword-only, so loading compiles a second, load-only
validator that takes positional values. These tests pin that it builds the same
objects as dict validation, in every column order and subset, and that it falls back
to dict validation wherever positional input could behave differently.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field as dataclass_field

import pytest
from pydantic import BaseModel, ValidationError, field_validator, model_validator
from pydantic.dataclasses import dataclass as pydantic_dataclass
from sqlalchemy import create_engine, insert, select
from sqlalchemy.engine import Connection

from sqldataclass import Field, SQLDataclass
from sqldataclass.query import _dataclass_schema, _positional_plan, load_all, row_builder


class PlHero(SQLDataclass, table=True):
    __tablename__ = "pl_heroes"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    age: int = Field(default=0, ge=0)
    secret_name: str = ""

    @field_validator("name")
    @classmethod
    def strip_name(cls, name: str) -> str:
        return name.strip()


class PlTeam(SQLDataclass, table=True):
    __tablename__ = "pl_teams"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    motto: str = ""

    @model_validator(mode="after")
    def default_motto(self) -> "PlTeam":
        if not self.motto:
            object.__setattr__(self, "motto", f"Go {self.name}")
        return self


class PlFrozen(SQLDataclass, table=True, frozen=True):
    __tablename__ = "pl_frozen"
    id: int | None = Field(default=None, primary_key=True)
    label: str = ""


class PlAliased(SQLDataclass, table=True):
    __tablename__ = "pl_aliased"
    id: int | None = Field(default=None, primary_key=True)
    label: str = Field(default="", alias="displayLabel")


@dataclass
class PlainHero:
    id: int | None = None
    name: str = ""
    age: int = 0
    secret_name: str = ""


@pydantic_dataclass(kw_only=True, slots=True)
class PlNode:
    """References itself, so pydantic wraps its schema in ``definitions``."""

    id: int
    children: list["PlNode"] = dataclass_field(default_factory=list)


@pydantic_dataclass(kw_only=True, slots=True)
class PlComputed:
    id: int
    cached: int = dataclass_field(default=0, init=False)
    label: str = ""


class PlHeroModel(BaseModel):
    id: int
    name: str = ""


@contextmanager
def _connection() -> Iterator[Connection]:
    """Connection to a fresh in-memory database with three heroes, two teams and a frozen row."""
    engine = create_engine("sqlite:///:memory:")
    tables = [PlHero.__table__, PlTeam.__table__, PlFrozen.__table__]
    SQLDataclass.metadata.create_all(engine, tables=tables)
    try:
        with engine.connect() as conn:
            heroes = [
                {"id": 1, "name": "  ada  ", "age": 36, "secret_name": "a"},
                {"id": 2, "name": "bob", "age": 7, "secret_name": "b"},
                {"id": 3, "name": "cy", "age": 0, "secret_name": "c"},
            ]
            conn.execute(insert(PlHero.__table__), heroes)
            conn.execute(insert(PlTeam.__table__), [{"id": 1, "name": "owls", "motto": ""}])
            conn.execute(insert(PlFrozen.__table__), [{"id": 1, "label": "ice"}])
            yield conn
    finally:
        engine.dispose()


def test_load_all_builds_the_same_objects_as_construction() -> None:
    """Positional validation must run the field validator and produce objects equal to constructed ones."""
    # --- Arrange ---
    with _connection() as conn:
        # --- Act ---
        heroes = load_all(conn, select(PlHero.__table__).order_by(PlHero.c.id), PlHero)

    # --- Assert ---
    assert heroes == [
        PlHero(id=1, name="ada", age=36, secret_name="a"),
        PlHero(id=2, name="bob", age=7, secret_name="b"),
        PlHero(id=3, name="cy", age=0, secret_name="c"),
    ]


def test_columns_in_any_order_land_in_their_fields() -> None:
    """Positional arguments follow the schema's field order, so a reversed SELECT must be reordered, not shifted."""
    # --- Arrange ---
    columns = list(reversed(PlHero.__table__.columns))

    with _connection() as conn:
        # --- Act ---
        hero = load_all(conn, select(*columns).where(PlHero.c.id == 2), PlHero)[0]

    # --- Assert ---
    assert hero == PlHero(id=2, name="bob", age=7, secret_name="b")


def test_a_column_subset_fills_the_rest_with_defaults() -> None:
    """Without ``name`` in the SELECT, later fields go by keyword and ``name`` takes its default."""
    # --- Arrange ---
    query = select(PlHero.c.id, PlHero.c.age, PlHero.c.secret_name).where(PlHero.c.id == 1)

    with _connection() as conn:
        # --- Act ---
        hero = load_all(conn, query, PlHero)[0]

    # --- Assert ---
    assert hero == PlHero(id=1, name="", age=36, secret_name="a")


def test_model_validator_runs_on_load() -> None:
    """``default_motto`` fills an empty motto; positional loading must run model validators too."""
    # --- Arrange ---
    with _connection() as conn:
        # --- Act ---
        team = PlTeam.load_all(conn)[0]

    # --- Assert ---
    assert team.motto == "Go owls"


def test_frozen_class_loads_positionally() -> None:
    """Frozen classes are built by the validator itself, so positional loading must work for them too."""
    # --- Arrange ---
    with _connection() as conn:
        # --- Act ---
        frozen = PlFrozen.load_all(conn)[0]

    # --- Assert ---
    assert frozen == PlFrozen(id=1, label="ice")


def test_invalid_row_raises_with_the_field_name() -> None:
    """Positional errors would report an argument index; loading re-validates by name so the error names ``age``."""
    # --- Arrange ---
    keys = ("id", "name", "age", "secret_name")
    build = row_builder(PlHero, keys)

    # --- Act / Assert ---
    with pytest.raises(ValidationError, match="age"):
        build((4, "dan", -1, "d"))


def test_invalid_row_in_a_column_subset_raises_with_the_field_name() -> None:
    """The keyword path re-validates by name as well, so a bad ``age`` is named in the error."""
    # --- Arrange ---
    build = row_builder(PlHero, ("id", "age", "secret_name"))

    # --- Act / Assert ---
    with pytest.raises(ValidationError, match="age"):
        build((4, -1, "d"))


def test_self_referencing_dataclass_loads_positionally() -> None:
    """A self-reference wraps the schema in ``definitions``; the load-only validator must keep that wrapper."""
    # --- Arrange ---
    with _connection() as conn:
        # --- Act ---
        nodes = load_all(conn, select(PlHero.c.id).order_by(PlHero.c.id), PlNode)

    # --- Assert ---
    assert PlNode.__pydantic_core_schema__["schema"]["type"] == "definition-ref"
    assert _plan(PlNode, ("id",)) == ((0,), ())
    assert nodes == [PlNode(id=1), PlNode(id=2), PlNode(id=3)]


def test_basemodel_loads_through_dict_validation() -> None:
    """BaseModels have no positional input, so ``load_all`` validates them from a dict as before."""
    # --- Arrange ---
    query = select(PlHero.c.id, PlHero.c.name).where(PlHero.c.id == 2)

    with _connection() as conn:
        # --- Act ---
        hero = load_all(conn, query, PlHeroModel)[0]

    # --- Assert ---
    assert hero == PlHeroModel(id=2, name="bob")


def test_field_after_an_init_false_field_gets_its_value() -> None:
    """Positions count ``init=False`` fields, so ``label`` must go by keyword or its value is silently dropped."""
    # --- Arrange ---
    build = row_builder(PlComputed, ("id", "label"))

    # --- Act ---
    computed = build((1, "kept"))

    # --- Assert ---
    assert computed.label == "kept"
    assert computed.cached == 0


def test_load_all_still_calls_plain_classes_with_keywords() -> None:
    """Classes without a pydantic validator, like a stdlib dataclass, are constructed from the row as keywords."""
    # --- Arrange ---
    with _connection() as conn:
        # --- Act ---
        hero = load_all(conn, select(PlHero.__table__).where(PlHero.c.id == 2), PlainHero)[0]

    # --- Assert ---
    assert hero == PlainHero(id=2, name="bob", age=7, secret_name="b")


# --- When positional loading is used ---


def _plan(cls: type, keys: tuple[str, ...]) -> object:
    schema = _dataclass_schema(cls)
    assert schema is not None
    return _positional_plan(schema, keys)


def test_all_columns_in_field_order_need_no_keywords() -> None:
    """The common case, a full SELECT, must pass every value positionally and build no dict.

    The order of the positions follows the schema, which puts ``age`` (declared with
    ``Field(ge=0)``) before ``name``.
    """
    # --- Act ---
    plan = _plan(PlHero, ("id", "name", "age", "secret_name"))

    # --- Assert ---
    assert plan == ((0, 2, 1, 3), ())


def test_aliased_fields_fall_back_to_dict_validation() -> None:
    """An alias changes which key a dict must use, so positional input could fill a field dict input leaves empty."""
    # --- Act ---
    plan = _plan(PlAliased, ("id", "label"))

    # --- Assert ---
    assert plan is None


def test_duplicate_keys_fall_back_to_dict_validation() -> None:
    """A dict keeps the last of two equal keys; positional input has no such rule, so the dict path decides."""
    # --- Act ---
    plan = _plan(PlHero, ("id", "name", "name"))

    # --- Assert ---
    assert plan is None


def test_keys_naming_init_false_fields_fall_back_to_dict_validation() -> None:
    """``cached`` is not an ``__init__`` argument; only dict validation knows how pydantic treats it as input."""
    # --- Act ---
    plan = _plan(PlComputed, ("id", "cached"))

    # --- Assert ---
    assert plan is None


def test_unknown_keys_fall_back_when_extra_input_is_forbidden() -> None:
    """SQLDataclass forbids unknown input, so a stray column must still raise through dict validation."""
    # --- Act ---
    plan = _plan(PlHero, ("id", "name", "joined_column"))

    # --- Assert ---
    assert plan is None
