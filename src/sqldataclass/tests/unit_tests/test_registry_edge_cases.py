"""Edge-case tests for the registry module."""

from __future__ import annotations

import pytest
from sqlalchemy import Integer, String, Table, create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from sqldataclass.registry import Base, create_all_tables, drop_all_tables, table

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine() -> Engine:
    return create_engine("sqlite:///:memory:")


# ---------------------------------------------------------------------------
# Test-local base & models (isolated from the shared Base metadata)
# ---------------------------------------------------------------------------


class _LocalBase(DeclarativeBase):
    """Isolated base so tests don't pollute the shared Base.metadata."""


class _User(_LocalBase):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50))


class _Post(_LocalBase):
    __tablename__ = "posts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(100))


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class TestBaseSubclass:
    def test_subclass_creates_valid_table(self) -> None:
        """A subclass of Base should register a Table in Base.metadata."""

        class _Temp(Base):
            __tablename__ = "temp_base_test"
            id: Mapped[int] = mapped_column(Integer, primary_key=True)

        tbl: Table = _Temp.__table__  # type: ignore[assignment]
        assert tbl is not None
        assert tbl.name == "temp_base_test"
        column_names = {c.name for c in tbl.columns}
        assert "id" in column_names

    def test_multiple_subclasses_share_metadata(self) -> None:
        """All subclasses of the same DeclarativeBase share one MetaData."""
        assert _User.metadata is _Post.metadata
        assert _User.metadata is _LocalBase.metadata


# ---------------------------------------------------------------------------
# table()
# ---------------------------------------------------------------------------


class TestTableHelper:
    def test_returns_correct_table_object(self) -> None:
        tbl = table(_User)
        assert isinstance(tbl, type(_User.__table__))
        assert tbl is _User.__table__

    def test_table_has_expected_columns(self) -> None:
        tbl = table(_User)
        col_names = {c.name for c in tbl.columns}
        assert col_names == {"id", "name"}

    def test_non_orm_class_raises_attribute_error(self) -> None:
        class _Plain:
            pass

        with pytest.raises(AttributeError):
            table(_Plain)


# ---------------------------------------------------------------------------
# create_all_tables / drop_all_tables  (using _LocalBase to stay isolated)
# ---------------------------------------------------------------------------


def _local_create(engine: Engine) -> None:
    _LocalBase.metadata.create_all(engine)


def _local_drop(engine: Engine) -> None:
    _LocalBase.metadata.drop_all(engine)


def _existing_tables(engine: Engine) -> set[str]:
    return set(inspect(engine).get_table_names())


class TestCreateAndDropTables:
    def test_create_then_drop(self, engine: Engine) -> None:
        _local_create(engine)
        assert _existing_tables(engine) >= {"users", "posts"}

        _local_drop(engine)
        assert _existing_tables(engine) == set()

    def test_create_is_idempotent(self, engine: Engine) -> None:
        _local_create(engine)
        _local_create(engine)  # second call must not raise
        assert _existing_tables(engine) >= {"users", "posts"}

    def test_drop_is_idempotent(self, engine: Engine) -> None:
        _local_create(engine)
        _local_drop(engine)
        _local_drop(engine)  # second call must not raise
        assert _existing_tables(engine) == set()

    def test_drop_then_create_recreates(self, engine: Engine) -> None:
        _local_create(engine)
        _local_drop(engine)
        assert _existing_tables(engine) == set()

        _local_create(engine)
        assert _existing_tables(engine) >= {"users", "posts"}

    def test_tables_exist_after_create(self, engine: Engine) -> None:
        _local_create(engine)
        tables = _existing_tables(engine)
        assert "users" in tables
        assert "posts" in tables

    def test_tables_gone_after_drop(self, engine: Engine) -> None:
        _local_create(engine)
        _local_drop(engine)
        assert _existing_tables(engine) == set()


class TestCreateDropWithSharedBase:
    """Smoke-test the public helpers that operate on the shared Base.metadata."""

    def test_create_and_drop_shared(self, engine: Engine) -> None:
        create_all_tables(engine)
        drop_all_tables(engine)
        # Must not raise; tables from shared Base created and dropped.

    def test_create_shared_is_idempotent(self, engine: Engine) -> None:
        create_all_tables(engine)
        create_all_tables(engine)
        drop_all_tables(engine)

    def test_drop_shared_is_idempotent(self, engine: Engine) -> None:
        create_all_tables(engine)
        drop_all_tables(engine)
        drop_all_tables(engine)
