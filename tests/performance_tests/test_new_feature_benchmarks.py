"""Performance benchmarks for new features: JTI, ARRAY, JSON.

Ensures JTI load/insert overhead is bounded, and flatten_for_table with
ARRAY/JSON columns doesn't regress versus plain scalar models.
"""

from __future__ import annotations

import time
from typing import Any

import pytest
from sqlalchemy import MetaData, create_engine, insert
from sqlalchemy.engine import Engine

from sqldataclass import Field, SQLDataclass
from sqldataclass.write import flatten_for_table
from tests.util.memory import measure_memory

ROW_COUNT = 5_000


# ---------------------------------------------------------------------------
# JTI model definitions
# ---------------------------------------------------------------------------


class _PersonPerf(SQLDataclass, table=True):
    __tablename__ = "perf_persons"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    email: str = ""
    age: int = 0


class _EmployeePerf(_PersonPerf, table=True):
    __tablename__ = "perf_employees"
    department: str = ""
    salary: float = 0.0
    level: int = 1


# Flat equivalent (single table, same fields) for comparison
class _FlatEmployeePerf(SQLDataclass, table=True):
    __tablename__ = "perf_flat_employees"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    email: str = ""
    age: int = 0
    department: str = ""
    salary: float = 0.0
    level: int = 1


# JSON model (JSON works in SQLite; ARRAY doesn't, so we only test JSON flatten performance)
class _JsonPerf(SQLDataclass, table=True):
    __tablename__ = "perf_json"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    config: dict[str, object] = Field(default_factory=dict)
    metadata_: dict[str, object] = Field(default_factory=dict)


# Plain model (same number of fields, no dict)
class _PlainPerf(SQLDataclass, table=True):
    __tablename__ = "perf_plain"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    tag1: str = ""
    tag2: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_jti_tables(engine: Engine) -> None:
    """Seed parent + child tables with ROW_COUNT rows."""
    parent_table = _PersonPerf.__table__
    child_table = _EmployeePerf.__table__
    with engine.begin() as conn:
        conn.execute(
            insert(parent_table),
            [{"id": i, "name": f"person_{i}", "email": f"p{i}@co.com", "age": 30 + i % 40} for i in range(ROW_COUNT)],
        )
        conn.execute(
            insert(child_table),
            [
                {"id": i, "department": f"dept_{i % 10}", "salary": 50_000 + i * 10, "level": i % 5 + 1}
                for i in range(ROW_COUNT)
            ],
        )


def _seed_flat_table(engine: Engine) -> None:
    """Seed a single flat table with ROW_COUNT rows."""
    flat_table = _FlatEmployeePerf.__table__
    with engine.begin() as conn:
        conn.execute(
            insert(flat_table),
            [
                {
                    "id": i,
                    "name": f"person_{i}",
                    "email": f"p{i}@co.com",
                    "age": 30 + i % 40,
                    "department": f"dept_{i % 10}",
                    "salary": 50_000 + i * 10,
                    "level": i % 5 + 1,
                }
                for i in range(ROW_COUNT)
            ],
        )


def _create_engine_with_tables(*models: type) -> Engine:
    """Create an in-memory engine and tables for the given models."""
    engine = create_engine("sqlite://", echo=False)
    metadata = MetaData()
    for model in models:
        parent_table = getattr(model, "__jti_parent_table__", None)
        if parent_table is not None and parent_table.name not in metadata.tables:
            parent_table.to_metadata(metadata)
        model.__table__.to_metadata(metadata)  # type: ignore[union-attr]  # __table__ is set dynamically by metaclass
    metadata.create_all(engine)
    return engine


def _time_it(func: Any) -> float:
    """Run *func* and return elapsed wall-clock seconds."""
    start = time.perf_counter()
    func()
    return time.perf_counter() - start


# ---------------------------------------------------------------------------
# JTI load performance
# ---------------------------------------------------------------------------


@pytest.mark.performance
def test_jti_load_not_dramatically_slower_than_flat() -> None:
    """JTI load_all (with JOIN) should be no more than 3x slower than single-table load."""
    jti_engine = _create_engine_with_tables(_EmployeePerf)
    _seed_jti_tables(jti_engine)

    flat_engine = _create_engine_with_tables(_FlatEmployeePerf)
    _seed_flat_table(flat_engine)

    def _load_jti() -> list[Any]:
        with jti_engine.connect() as conn:
            return _EmployeePerf.load_all(conn)

    def _load_flat() -> list[Any]:
        with flat_engine.connect() as conn:
            return _FlatEmployeePerf.load_all(conn)

    # Warmup
    _load_jti()
    _load_flat()

    elapsed_jti = _time_it(_load_jti)
    elapsed_flat = _time_it(_load_flat)

    ratio = elapsed_jti / elapsed_flat

    # --- Assert ---
    assert ratio < 3.0, (
        f"JTI load is too slow compared to flat: {ratio:.2f}x (jti={elapsed_jti:.4f}s, flat={elapsed_flat:.4f}s)"
    )


# ---------------------------------------------------------------------------
# JTI insert performance
# ---------------------------------------------------------------------------


@pytest.mark.performance
def test_jti_individual_insert_overhead() -> None:
    """JTI individual insert (2 tables) should be no more than 4x slower than single-table individual insert."""
    insert_count = 500
    jti_engine = _create_engine_with_tables(_EmployeePerf)
    flat_engine = _create_engine_with_tables(_FlatEmployeePerf)

    def _insert_jti() -> None:
        with jti_engine.begin() as conn:
            for i in range(insert_count):
                _EmployeePerf(
                    name=f"p_{i}", email=f"p{i}@co.com", age=30, department="eng", salary=100.0, level=1
                ).insert(conn)

    def _insert_flat() -> None:
        with flat_engine.begin() as conn:
            for i in range(insert_count):
                _FlatEmployeePerf(
                    name=f"p_{i}", email=f"p{i}@co.com", age=30, department="eng", salary=100.0, level=1
                ).insert(conn)

    elapsed_jti = _time_it(_insert_jti)
    elapsed_flat = _time_it(_insert_flat)

    ratio = elapsed_jti / elapsed_flat

    # --- Assert ---
    # JTI does 2 INSERTs per row (parent + child), so ~2x is expected.
    # Allow up to 4x for overhead (field splitting, etc.).
    assert ratio < 4.0, (
        f"JTI individual insert is too slow compared to flat: {ratio:.2f}x "
        f"(jti={elapsed_jti:.4f}s, flat={elapsed_flat:.4f}s)"
    )


# ---------------------------------------------------------------------------
# JTI memory
# ---------------------------------------------------------------------------


@pytest.mark.performance
def test_jti_load_memory_comparable_to_flat() -> None:
    """JTI loaded instances should use comparable memory to flat instances (same fields)."""
    jti_engine = _create_engine_with_tables(_EmployeePerf)
    _seed_jti_tables(jti_engine)

    flat_engine = _create_engine_with_tables(_FlatEmployeePerf)
    _seed_flat_table(flat_engine)

    with measure_memory() as jti_mem:
        with jti_engine.connect() as conn:
            jti_results = _EmployeePerf.load_all(conn)

    with measure_memory() as flat_mem:
        with flat_engine.connect() as conn:
            flat_results = _FlatEmployeePerf.load_all(conn)

    # Both should have the same number of instances
    assert len(jti_results) == len(flat_results) == ROW_COUNT

    # JTI instances have the same fields, so memory should be similar (within 2x)
    ratio = jti_mem.peak / flat_mem.peak

    # --- Assert ---
    assert ratio < 2.0, (
        f"JTI peak memory is too high compared to flat: {ratio:.2f}x (jti={jti_mem.peak:,}B, flat={flat_mem.peak:,}B)"
    )


# ---------------------------------------------------------------------------
# flatten_for_table with ARRAY/JSON columns
# ---------------------------------------------------------------------------


@pytest.mark.performance
def test_flatten_for_table_json_not_slower() -> None:
    """flatten_for_table with dict values should not be significantly slower than plain scalars."""
    json_instances = [
        _JsonPerf(id=i, name=f"item_{i}", config={"key": "value"}, metadata_={"env": "prod"}) for i in range(ROW_COUNT)
    ]
    plain_instances = [_PlainPerf(id=i, name=f"item_{i}", tag1="a", tag2="b") for i in range(ROW_COUNT)]

    def _flatten_json() -> list[dict[str, Any]]:
        return [flatten_for_table(obj) for obj in json_instances]

    def _flatten_plain() -> list[dict[str, Any]]:
        return [flatten_for_table(obj) for obj in plain_instances]

    # Warmup
    _flatten_json()
    _flatten_plain()

    elapsed_json = _time_it(_flatten_json)
    elapsed_plain = _time_it(_flatten_plain)

    ratio = elapsed_json / elapsed_plain

    # --- Assert ---
    assert ratio < 2.0, (
        f"flatten_for_table with JSON is too slow: {ratio:.2f}x (json={elapsed_json:.4f}s, plain={elapsed_plain:.4f}s)"
    )
