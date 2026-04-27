"""Tests for automatic dict → JSON type mapping."""

from __future__ import annotations

from sqlalchemy import JSON

from sqldataclass import Field, SQLDataclass
from sqldataclass.model import _python_type_to_sa

# ---------------------------------------------------------------------------
# _python_type_to_sa unit tests
# ---------------------------------------------------------------------------


def test_bare_dict_maps_to_json() -> None:
    """Bare dict maps to JSON."""
    result = _python_type_to_sa(dict)

    # --- Assert ---
    assert result is JSON


def test_parameterized_dict_maps_to_json() -> None:
    """dict[str, object] maps to JSON."""
    result = _python_type_to_sa(dict[str, object])

    # --- Assert ---
    assert result is JSON


def test_optional_dict_maps_to_json() -> None:
    """dict[str, object] | None unwraps to JSON."""
    result = _python_type_to_sa(dict[str, object] | None)

    # --- Assert ---
    assert result is JSON


# ---------------------------------------------------------------------------
# Model-level column creation
# ---------------------------------------------------------------------------


def test_model_dict_field_creates_json_column() -> None:
    """A dict[str, object] field on a model produces a JSON column."""

    class Config(SQLDataclass, table=True):
        __tablename__ = "configs_json_test"
        id: int = Field(primary_key=True)
        settings: dict[str, object] = Field(default_factory=dict)

    column = Config.__table__.c["settings"]

    # --- Assert ---
    assert isinstance(column.type, JSON)


def test_model_optional_dict_creates_nullable_json_column() -> None:
    """A dict[str, object] | None field produces a nullable JSON column."""

    class Event(SQLDataclass, table=True):
        __tablename__ = "events_json_test"
        id: int = Field(primary_key=True)
        metadata_: dict[str, object] | None = Field(default=None)

    column = Event.__table__.c["metadata_"]

    # --- Assert ---
    assert isinstance(column.type, JSON)
    assert column.nullable is True


def test_explicit_sa_type_overrides_dict_mapping() -> None:
    """Field(sa_type=...) takes precedence over automatic dict → JSON mapping."""
    from sqlalchemy.dialects.postgresql import JSONB

    class Document(SQLDataclass, table=True):
        __tablename__ = "documents_jsonb_test"
        id: int = Field(primary_key=True)
        content: dict[str, object] = Field(default_factory=dict, sa_type=JSONB)

    column = Document.__table__.c["content"]

    # --- Assert ---
    assert isinstance(column.type, JSONB)


def test_json_roundtrip_with_sqlite() -> None:
    """JSON columns work with SQLite for basic insert and read."""
    from sqlalchemy import MetaData, create_engine

    class Preference(SQLDataclass, table=True):
        __tablename__ = "preferences_json_roundtrip_test"
        id: int = Field(primary_key=True)
        data: dict[str, object] = Field(default_factory=dict)

    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    Preference.__table__.to_metadata(metadata)
    metadata.create_all(engine)

    with engine.connect() as connection:
        connection.execute(Preference.__table__.insert().values(id=1, data={"theme": "dark", "font_size": 14}))
        connection.commit()
        row = connection.execute(Preference.__table__.select()).fetchone()

    # --- Assert ---
    assert row is not None
    assert row[1] == {"theme": "dark", "font_size": 14}
