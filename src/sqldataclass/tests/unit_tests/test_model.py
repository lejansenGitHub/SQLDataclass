"""Unit tests for the SQLDataclass model module."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import ValidationError
from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    Numeric,
    Select,
    String,
    Text,
)
from sqlalchemy import Uuid as SAUuid

from sqldataclass import Field, SQLDataclass
from sqldataclass.model import (
    SAColumnInfo,
    _default_tablename,
    _get_sa_info,
    _python_type_to_sa,
    _unwrap_optional,
)

# ---------------------------------------------------------------------------
# Field() tests
# ---------------------------------------------------------------------------


class TestField:
    """Tests for the Field() helper function."""

    def test_field_with_default_value(self) -> None:
        field_info = Field(default=42)
        assert field_info.default == 42

    def test_field_with_primary_key(self) -> None:
        field_info = Field(default=None, primary_key=True)
        sa_info = _get_sa_info(field_info)
        assert sa_info is not None
        assert sa_info.primary_key is True

    def test_field_with_index(self) -> None:
        field_info = Field(default=None, index=True)
        sa_info = _get_sa_info(field_info)
        assert sa_info is not None
        assert sa_info.index is True

    def test_field_with_unique(self) -> None:
        field_info = Field(default=None, unique=True)
        sa_info = _get_sa_info(field_info)
        assert sa_info is not None
        assert sa_info.unique is True

    def test_field_with_foreign_key(self) -> None:
        field_info = Field(default=None, foreign_key="other_table.id")
        sa_info = _get_sa_info(field_info)
        assert sa_info is not None
        assert sa_info.foreign_key == "other_table.id"

    def test_field_with_pydantic_ge_le(self) -> None:
        field_info = Field(default=0, ge=0, le=100)
        assert any(getattr(m, "ge", None) == 0 for m in field_info.metadata)
        assert any(getattr(m, "le", None) == 100 for m in field_info.metadata)

    def test_field_with_pydantic_min_max_length(self) -> None:
        field_info = Field(default="", min_length=1, max_length=50)
        assert any(getattr(m, "min_length", None) == 1 for m in field_info.metadata)
        assert any(getattr(m, "max_length", None) == 50 for m in field_info.metadata)

    def test_field_with_sa_type_override(self) -> None:
        field_info = Field(default="", sa_type=Text)
        sa_info = _get_sa_info(field_info)
        assert sa_info is not None
        assert sa_info.sa_type is Text

    def test_field_without_sa_params_is_pure_pydantic(self) -> None:
        field_info = Field(default="hello", title="A title", description="Desc")
        sa_info = _get_sa_info(field_info)
        # SA info is always attached, but all SA flags are defaults
        assert sa_info is not None
        assert sa_info.primary_key is False
        assert sa_info.index is False
        assert sa_info.unique is False
        assert sa_info.foreign_key is None
        assert sa_info.sa_type is None


# ---------------------------------------------------------------------------
# Type mapping tests
# ---------------------------------------------------------------------------


class TestTypeMapping:
    """Tests for Python -> SQLAlchemy type mapping."""

    def test_int_maps_to_integer(self) -> None:
        assert _python_type_to_sa(int) is Integer

    def test_float_maps_to_float(self) -> None:
        assert _python_type_to_sa(float) is Float

    def test_str_maps_to_string(self) -> None:
        assert _python_type_to_sa(str) is String

    def test_bool_maps_to_boolean(self) -> None:
        assert _python_type_to_sa(bool) is Boolean

    def test_datetime_maps_to_datetime(self) -> None:
        assert _python_type_to_sa(datetime) is DateTime

    def test_date_maps_to_date(self) -> None:
        assert _python_type_to_sa(date) is Date

    def test_decimal_maps_to_numeric(self) -> None:
        assert _python_type_to_sa(Decimal) is Numeric

    def test_uuid_maps_to_sa_uuid(self) -> None:
        assert _python_type_to_sa(UUID) is SAUuid

    def test_optional_int_maps_to_integer(self) -> None:
        assert _python_type_to_sa(int | None) is Integer

    def test_unknown_type_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="Cannot map Python type"):
            _python_type_to_sa(list)


# ---------------------------------------------------------------------------
# SQLDataclass with table=True
# ---------------------------------------------------------------------------


class Hero(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(default="Unknown")
    secret_name: str = ""
    age: int | None = None


class TestSQLDataclassTable:
    """Tests for SQLDataclass subclass with table=True."""

    def test_creates_table_attribute(self) -> None:
        assert hasattr(Hero, "__table__")
        assert Hero.__table__ is not None

    def test_table_has_correct_column_names(self) -> None:
        col_names = {c.name for c in Hero.__table__.columns}
        assert col_names == {"id", "name", "secret_name", "age"}

    def test_table_column_types(self) -> None:
        cols = {c.name: c for c in Hero.__table__.columns}
        assert isinstance(cols["id"].type, Integer)
        assert isinstance(cols["name"].type, String)
        assert isinstance(cols["secret_name"].type, String)
        assert isinstance(cols["age"].type, Integer)

    def test_table_primary_key(self) -> None:
        cols = {c.name: c for c in Hero.__table__.columns}
        assert cols["id"].primary_key is True

    def test_default_tablename_camel_to_snake(self) -> None:
        assert Hero.__table__.name == "hero"

    def test_nullable_inference_optional_is_nullable(self) -> None:
        cols = {c.name: c for c in Hero.__table__.columns}
        assert cols["age"].nullable is True

    def test_nullable_inference_required_is_not_nullable(self) -> None:
        cols = {c.name: c for c in Hero.__table__.columns}
        assert cols["name"].nullable is False

    def test_pk_optional_field_is_not_nullable_in_sa(self) -> None:
        cols = {c.name: c for c in Hero.__table__.columns}
        # id is int | None but primary_key=True -> nullable=False
        assert cols["id"].nullable is False

    def test_slots_active(self) -> None:
        assert hasattr(Hero, "__slots__")

    def test_pydantic_validation_rejects_wrong_type(self) -> None:
        with pytest.raises(ValidationError):
            Hero(name=123)  # type: ignore[arg-type]

    def test_instance_creation_with_defaults(self) -> None:
        hero = Hero()
        assert hero.id is None
        assert hero.name == "Unknown"
        assert hero.secret_name == ""
        assert hero.age is None

    def test_instance_creation_with_values(self) -> None:
        hero = Hero(id=1, name="Spider-Man", secret_name="Peter Parker", age=25)
        assert hero.id == 1
        assert hero.name == "Spider-Man"
        assert hero.secret_name == "Peter Parker"
        assert hero.age == 25

    def test_to_dict_method(self) -> None:
        hero = Hero(id=1, name="Spider-Man", secret_name="Peter Parker", age=25)
        result = hero.to_dict()
        assert result == {"id": 1, "name": "Spider-Man", "secret_name": "Peter Parker", "age": 25}

    def test_c_attribute_gives_column_access(self) -> None:
        assert hasattr(Hero, "c")
        assert hasattr(Hero.c, "id")
        assert hasattr(Hero.c, "name")

    def test_select_returns_select_object(self) -> None:
        stmt = Hero.select()
        assert isinstance(stmt, Select)

    def test_sqldataclass_is_table_flag(self) -> None:
        assert Hero.__sqldataclass_is_table__ is True


# ---------------------------------------------------------------------------
# Custom tablename
# ---------------------------------------------------------------------------


class MyCustomModel(SQLDataclass, table=True):
    __tablename__ = "custom_table_name"
    id: int | None = Field(default=None, primary_key=True)
    value: str = ""


class TestCustomTablename:
    """Tests for custom __tablename__ override."""

    def test_custom_tablename(self) -> None:
        assert MyCustomModel.__table__.name == "custom_table_name"


# ---------------------------------------------------------------------------
# CamelCase -> snake_case conversion
# ---------------------------------------------------------------------------


class UserProfile(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    bio: str = ""


class TestCamelToSnake:
    """Tests for default CamelCase -> snake_case table name."""

    def test_multi_word_camel_case(self) -> None:
        assert UserProfile.__table__.name == "user_profile"


# ---------------------------------------------------------------------------
# SQLDataclass with table=False (default)
# ---------------------------------------------------------------------------


class HeroCreate(SQLDataclass):
    name: str
    secret_name: str
    age: int | None = None


class TestSQLDataclassNoTable:
    """Tests for SQLDataclass subclass with table=False (default)."""

    def test_no_table_attribute(self) -> None:
        assert not hasattr(HeroCreate, "__table__")

    def test_is_table_flag_false(self) -> None:
        assert HeroCreate.__sqldataclass_is_table__ is False

    def test_no_convenience_methods(self) -> None:
        assert not hasattr(HeroCreate, "load_all")
        assert not hasattr(HeroCreate, "load_one")
        assert not hasattr(HeroCreate, "insert")

    def test_pydantic_validation_works(self) -> None:
        obj = HeroCreate(name="Spider-Man", secret_name="Peter Parker")
        assert obj.name == "Spider-Man"
        assert obj.age is None

    def test_pydantic_validation_rejects_wrong_type(self) -> None:
        with pytest.raises(ValidationError):
            HeroCreate(name=123, secret_name="x")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Multiple table models share metadata
# ---------------------------------------------------------------------------


class Team(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""


class TestMultipleTableModels:
    """Tests for multiple table classes sharing the same metadata."""

    def test_shared_metadata(self) -> None:
        assert Hero.metadata is Team.metadata
        assert Hero.metadata is UserProfile.metadata

    def test_tables_dont_conflict(self) -> None:
        table_names = set(Hero.metadata.tables.keys())
        assert "hero" in table_names
        assert "team" in table_names
        assert "user_profile" in table_names

    def test_each_table_has_own_columns(self) -> None:
        hero_cols = {c.name for c in Hero.__table__.columns}
        team_cols = {c.name for c in Team.__table__.columns}
        assert hero_cols != team_cols


# ---------------------------------------------------------------------------
# _unwrap_optional helper
# ---------------------------------------------------------------------------


class TestUnwrapOptional:
    """Tests for the _unwrap_optional helper."""

    def test_plain_type_not_optional(self) -> None:
        inner, is_optional = _unwrap_optional(int)
        assert inner is int
        assert is_optional is False

    def test_optional_type(self) -> None:
        inner, is_optional = _unwrap_optional(int | None)
        assert inner is int
        assert is_optional is True


# ---------------------------------------------------------------------------
# _default_tablename helper
# ---------------------------------------------------------------------------


class TestDefaultTablename:
    """Tests for the _default_tablename helper."""

    def test_simple_name(self) -> None:
        assert _default_tablename("Hero") == "hero"

    def test_camel_case(self) -> None:
        assert _default_tablename("UserProfile") == "user_profile"

    def test_multi_caps(self) -> None:
        assert _default_tablename("HTTPSConnection") == "https_connection"


# Ensure unused imports are referenced for type-mapping coverage
_USED = (SAColumnInfo, Text, SAUuid)
