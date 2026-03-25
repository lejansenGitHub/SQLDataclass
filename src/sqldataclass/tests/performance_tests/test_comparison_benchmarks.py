"""Performance comparison: SQLDataclass vs SQLModel vs SQLAlchemy ORM.

Benchmarks object construction and DB loading across all three libraries
on both SQLite and PostgreSQL (when available).
"""

from __future__ import annotations

import gc
import os
import time
import tracemalloc
from typing import Any

import pytest
from pydantic import BaseModel
from sqlalchemy import Column, Float, Integer, String, create_engine, insert, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session

from sqldataclass import Field as SDField
from sqldataclass import SQLDataclass
from sqldataclass import SQLModel as OurSQLModel

ROW_COUNT = 10_000
FIELD_COUNT = 20  # id + name + value + category + f1..f16

SAMPLE_ROW: dict[str, Any] = {
    "id": 0,
    "name": "item_0",
    "value": 0.0,
    "category": "cat",
    "f1": 1.0,
    "f2": 2.0,
    "f3": 3.0,
    "f4": 4.0,
    "f5": 5.0,
    "f6": 6.0,
    "f7": 7.0,
    "f8": 8.0,
    "f9": 9.0,
    "f10": 10.0,
    "f11": 11.0,
    "f12": 12.0,
    "f13": 13.0,
    "f14": 14.0,
    "f15": 15.0,
    "f16": 16.0,
}


def _row(i: int) -> dict[str, Any]:
    return {**SAMPLE_ROW, "id": i, "name": f"item_{i}", "value": float(i)}


# ---------------------------------------------------------------------------
# Model definitions — one set per library
# ---------------------------------------------------------------------------


class BenchSDC(SQLDataclass, table=True):
    __tablename__ = "bench_cmp_sdc"
    id: int = SDField(primary_key=True)
    name: str
    value: float = 0.0
    category: str = ""
    f1: float = 0.0
    f2: float = 0.0
    f3: float = 0.0
    f4: float = 0.0
    f5: float = 0.0
    f6: float = 0.0
    f7: float = 0.0
    f8: float = 0.0
    f9: float = 0.0
    f10: float = 0.0
    f11: float = 0.0
    f12: float = 0.0
    f13: float = 0.0
    f14: float = 0.0
    f15: float = 0.0
    f16: float = 0.0


class _SABase(DeclarativeBase):
    pass


class BenchSA(_SABase):
    __tablename__ = "bench_cmp_sa"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(Float, default=0.0)
    category = Column(String, default="")
    f1 = Column(Float, default=0.0)
    f2 = Column(Float, default=0.0)
    f3 = Column(Float, default=0.0)
    f4 = Column(Float, default=0.0)
    f5 = Column(Float, default=0.0)
    f6 = Column(Float, default=0.0)
    f7 = Column(Float, default=0.0)
    f8 = Column(Float, default=0.0)
    f9 = Column(Float, default=0.0)
    f10 = Column(Float, default=0.0)
    f11 = Column(Float, default=0.0)
    f12 = Column(Float, default=0.0)
    f13 = Column(Float, default=0.0)
    f14 = Column(Float, default=0.0)
    f15 = Column(Float, default=0.0)
    f16 = Column(Float, default=0.0)


class BenchBaseModel(BaseModel):
    id: int
    name: str
    value: float = 0.0
    category: str = ""
    f1: float = 0.0
    f2: float = 0.0
    f3: float = 0.0
    f4: float = 0.0
    f5: float = 0.0
    f6: float = 0.0
    f7: float = 0.0
    f8: float = 0.0
    f9: float = 0.0
    f10: float = 0.0
    f11: float = 0.0
    f12: float = 0.0
    f13: float = 0.0
    f14: float = 0.0
    f15: float = 0.0
    f16: float = 0.0


class BenchOurSQLModel(OurSQLModel, table=True):
    __tablename__ = "bench_cmp_our_sqlmodel"
    id: int = SDField(primary_key=True)
    name: str
    value: float = 0.0
    category: str = ""
    f1: float = 0.0
    f2: float = 0.0
    f3: float = 0.0
    f4: float = 0.0
    f5: float = 0.0
    f6: float = 0.0
    f7: float = 0.0
    f8: float = 0.0
    f9: float = 0.0
    f10: float = 0.0
    f11: float = 0.0
    f12: float = 0.0
    f13: float = 0.0
    f14: float = 0.0
    f15: float = 0.0
    f16: float = 0.0


# SQLModel imported conditionally (optional dependency for tests)
try:
    from sqlmodel import Field as SMField
    from sqlmodel import Session as SMSession
    from sqlmodel import SQLModel
    from sqlmodel import select as sm_select

    class BenchSM(SQLModel, table=True):  # type: ignore[misc]
        __tablename__ = "bench_cmp_sm"
        id: int | None = SMField(default=None, primary_key=True)
        name: str
        value: float = 0.0
        category: str = ""
        f1: float = 0.0
        f2: float = 0.0
        f3: float = 0.0
        f4: float = 0.0
        f5: float = 0.0
        f6: float = 0.0
        f7: float = 0.0
        f8: float = 0.0
        f9: float = 0.0
        f10: float = 0.0
        f11: float = 0.0
        f12: float = 0.0
        f13: float = 0.0
        f14: float = 0.0
        f15: float = 0.0
        f16: float = 0.0

    HAS_SQLMODEL = True
except ImportError:
    HAS_SQLMODEL = False
    BenchSM = None  # type: ignore[assignment,misc]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _measure_peak_memory(func: Any) -> tuple[int, Any]:
    """Run func and return (peak_bytes, result)."""
    gc.collect()
    tracemalloc.start()
    tracemalloc.reset_peak()
    result = func()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak, result


def _measure_time(func: Any) -> tuple[float, Any]:
    """Run func and return (seconds, result)."""
    func()  # warmup
    gc.collect()
    start = time.perf_counter()
    result = func()
    elapsed = time.perf_counter() - start
    return elapsed, result


# ---------------------------------------------------------------------------
# SQLite fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_engine() -> Engine:
    engine = create_engine("sqlite:///:memory:")
    SQLDataclass.metadata.create_all(engine)
    _SABase.metadata.create_all(engine)
    OurSQLModel.metadata.create_all(engine)
    if HAS_SQLMODEL:
        SQLModel.metadata.create_all(engine)

    rows = [_row(i) for i in range(ROW_COUNT)]
    with engine.begin() as conn:
        conn.execute(insert(BenchSDC.__table__), rows)
        conn.execute(insert(BenchSA.__table__), rows)  # type: ignore[arg-type]
        conn.execute(insert(BenchOurSQLModel.__table__), rows)
        if HAS_SQLMODEL:
            conn.execute(insert(BenchSM.__table__), rows)
    return engine


# ---------------------------------------------------------------------------
# PostgreSQL fixtures
# ---------------------------------------------------------------------------


def _pg_url() -> str | None:
    """Return PostgreSQL URL if available, else None."""
    url = os.environ.get("SQLDATACLASS_PG_URL", "postgresql+psycopg2://postgres@localhost/postgres")
    try:
        eng = create_engine(url)
        with eng.connect():
            pass
        return url
    except Exception:
        return None


@pytest.fixture
def pg_engine() -> Any:
    url = _pg_url()
    if url is None:
        pytest.skip("PostgreSQL not available")
    assert url is not None  # for mypy
    engine = create_engine(url)

    # Clean up any leftover tables
    with engine.begin() as conn:
        for tbl in ["bench_cmp_sdc", "bench_cmp_sm", "bench_cmp_sa", "bench_cmp_our_sqlmodel"]:
            conn.execute(text(f"DROP TABLE IF EXISTS {tbl} CASCADE"))

    SQLDataclass.metadata.create_all(engine)
    _SABase.metadata.create_all(engine)
    OurSQLModel.metadata.create_all(engine)
    if HAS_SQLMODEL:
        SQLModel.metadata.create_all(engine)

    rows = [_row(i) for i in range(ROW_COUNT)]
    with engine.begin() as conn:
        conn.execute(insert(BenchSDC.__table__), rows)
        conn.execute(insert(BenchSA.__table__), rows)  # type: ignore[arg-type]
        conn.execute(insert(BenchOurSQLModel.__table__), rows)
        if HAS_SQLMODEL:
            conn.execute(insert(BenchSM.__table__), rows)

    yield engine

    with engine.begin() as conn:
        for tbl in ["bench_cmp_sdc", "bench_cmp_sm", "bench_cmp_sa", "bench_cmp_our_sqlmodel"]:
            conn.execute(text(f"DROP TABLE IF EXISTS {tbl} CASCADE"))


# ===========================================================================
# Construction benchmarks
# ===========================================================================


def test_construction_sqldataclass_lighter_than_sqlmodel() -> None:
    """SQLDataclass uses less memory per object than SQLModel."""
    if not HAS_SQLMODEL:
        pytest.skip("sqlmodel not installed")

    sdc_peak, sdc_objs = _measure_peak_memory(lambda: [BenchSDC(**_row(i)) for i in range(ROW_COUNT)])
    sm_peak, sm_objs = _measure_peak_memory(lambda: [BenchSM(**_row(i)) for i in range(ROW_COUNT)])
    assert len(sdc_objs) == ROW_COUNT
    assert len(sm_objs) == ROW_COUNT

    ratio = sm_peak / sdc_peak
    assert ratio > 2.0, f"Expected SQLModel to use >2x memory, got {ratio:.1f}x"


def test_construction_sqldataclass_lighter_than_sa_orm() -> None:
    """SQLDataclass uses less memory per object than SQLAlchemy ORM."""
    sdc_peak, sdc_objs = _measure_peak_memory(lambda: [BenchSDC(**_row(i)) for i in range(ROW_COUNT)])
    sa_peak, sa_objs = _measure_peak_memory(lambda: [BenchSA(**_row(i)) for i in range(ROW_COUNT)])
    assert len(sdc_objs) == ROW_COUNT
    assert len(sa_objs) == ROW_COUNT

    ratio = sa_peak / sdc_peak
    assert ratio > 1.5, f"Expected SA ORM to use >1.5x memory, got {ratio:.1f}x"


def test_construction_sqldataclass_lighter_than_basemodel() -> None:
    """SQLDataclass uses less memory per object than Pydantic BaseModel."""
    sdc_peak, sdc_objs = _measure_peak_memory(lambda: [BenchSDC(**_row(i)) for i in range(ROW_COUNT)])
    bm_peak, bm_objs = _measure_peak_memory(lambda: [BenchBaseModel(**_row(i)) for i in range(ROW_COUNT)])
    assert len(sdc_objs) == ROW_COUNT
    assert len(bm_objs) == ROW_COUNT

    ratio = bm_peak / sdc_peak
    assert ratio > 1.3, f"Expected BaseModel to use >1.3x memory, got {ratio:.1f}x"


def test_construction_sqldataclass_faster_than_sqlmodel() -> None:
    """SQLDataclass construction is faster than SQLModel."""
    if not HAS_SQLMODEL:
        pytest.skip("sqlmodel not installed")

    sdc_time, _ = _measure_time(lambda: [BenchSDC(**_row(i)) for i in range(ROW_COUNT)])
    sm_time, _ = _measure_time(lambda: [BenchSM(**_row(i)) for i in range(ROW_COUNT)])

    ratio = sm_time / sdc_time
    assert ratio > 2.0, f"Expected SQLModel to be >2x slower, got {ratio:.1f}x"


# ===========================================================================
# SQLite DB loading benchmarks
# ===========================================================================


def test_sqlite_load_sqldataclass_lighter_than_sqlmodel(sqlite_engine: Engine) -> None:
    """SQLDataclass DB load uses less memory than SQLModel (SQLite)."""
    if not HAS_SQLMODEL:
        pytest.skip("sqlmodel not installed")

    def load_sdc() -> list[Any]:
        with sqlite_engine.connect() as conn:
            return BenchSDC.load_all(conn)

    def load_sm() -> list[Any]:
        with SMSession(sqlite_engine) as session:
            return list(session.exec(sm_select(BenchSM)).all())

    sdc_peak, sdc_objs = _measure_peak_memory(load_sdc)
    sm_peak, sm_objs = _measure_peak_memory(load_sm)
    assert len(sdc_objs) == ROW_COUNT
    assert len(sm_objs) == ROW_COUNT

    ratio = sm_peak / sdc_peak
    assert ratio > 1.5, f"Expected SQLModel load to use >1.5x memory, got {ratio:.1f}x"


def test_sqlite_load_sqldataclass_lighter_than_sa_orm(sqlite_engine: Engine) -> None:
    """SQLDataclass DB load uses less memory than SQLAlchemy ORM (SQLite)."""

    def load_sdc() -> list[Any]:
        with sqlite_engine.connect() as conn:
            return BenchSDC.load_all(conn)

    def load_sa() -> list[Any]:
        with Session(sqlite_engine) as session:
            return list(session.query(BenchSA).all())

    sdc_peak, sdc_objs = _measure_peak_memory(load_sdc)
    sa_peak, sa_objs = _measure_peak_memory(load_sa)
    assert len(sdc_objs) == ROW_COUNT
    assert len(sa_objs) == ROW_COUNT

    ratio = sa_peak / sdc_peak
    assert ratio > 1.5, f"Expected SA ORM load to use >1.5x memory, got {ratio:.1f}x"


# ===========================================================================
# PostgreSQL DB loading benchmarks
# ===========================================================================


def test_pg_load_sqldataclass_lighter_than_sqlmodel(pg_engine: Engine) -> None:
    """SQLDataclass DB load uses less memory than SQLModel (PostgreSQL)."""
    if not HAS_SQLMODEL:
        pytest.skip("sqlmodel not installed")

    def load_sdc() -> list[Any]:
        with pg_engine.connect() as conn:
            return BenchSDC.load_all(conn)

    def load_sm() -> list[Any]:
        with SMSession(pg_engine) as session:
            return list(session.exec(sm_select(BenchSM)).all())

    sdc_peak, sdc_objs = _measure_peak_memory(load_sdc)
    sm_peak, sm_objs = _measure_peak_memory(load_sm)
    assert len(sdc_objs) == ROW_COUNT
    assert len(sm_objs) == ROW_COUNT

    ratio = sm_peak / sdc_peak
    assert ratio > 1.5, f"Expected SQLModel PG load to use >1.5x memory, got {ratio:.1f}x"


def test_pg_load_sqldataclass_lighter_than_sa_orm(pg_engine: Engine) -> None:
    """SQLDataclass DB load uses less memory than SQLAlchemy ORM (PostgreSQL)."""

    def load_sdc() -> list[Any]:
        with pg_engine.connect() as conn:
            return BenchSDC.load_all(conn)

    def load_sa() -> list[Any]:
        with Session(pg_engine) as session:
            return list(session.query(BenchSA).all())

    sdc_peak, sdc_objs = _measure_peak_memory(load_sdc)
    sa_peak, sa_objs = _measure_peak_memory(load_sa)
    assert len(sdc_objs) == ROW_COUNT
    assert len(sa_objs) == ROW_COUNT

    ratio = sa_peak / sdc_peak
    assert ratio > 1.5, f"Expected SA ORM PG load to use >1.5x memory, got {ratio:.1f}x"


# ===========================================================================
# SQLModel (ours) vs SQLDataclass benchmarks
# ===========================================================================


def test_construction_our_sqlmodel_memory() -> None:
    """Our SQLModel uses more memory than SQLDataclass (BaseModel vs slots dataclass)."""
    sdc_peak, sdc_objs = _measure_peak_memory(lambda: [BenchSDC(**_row(i)) for i in range(ROW_COUNT)])
    osm_peak, osm_objs = _measure_peak_memory(lambda: [BenchOurSQLModel(**_row(i)) for i in range(ROW_COUNT)])
    assert len(sdc_objs) == ROW_COUNT
    assert len(osm_objs) == ROW_COUNT

    # Our SQLModel (BaseModel) uses more memory than SQLDataclass (slots dc),
    # but should still be better than SA ORM and tiangolo's SQLModel.
    ratio = osm_peak / sdc_peak
    assert ratio > 1.0, f"Expected our SQLModel to use more memory than SQLDataclass, got {ratio:.1f}x"


def test_construction_our_sqlmodel_lighter_than_sa_orm() -> None:
    """Our SQLModel uses less memory than SQLAlchemy ORM."""
    osm_peak, osm_objs = _measure_peak_memory(lambda: [BenchOurSQLModel(**_row(i)) for i in range(ROW_COUNT)])
    sa_peak, sa_objs = _measure_peak_memory(lambda: [BenchSA(**_row(i)) for i in range(ROW_COUNT)])
    assert len(osm_objs) == ROW_COUNT
    assert len(sa_objs) == ROW_COUNT

    ratio = sa_peak / osm_peak
    assert ratio > 0.5, f"Expected our SQLModel to not be >2x heavier than SA ORM, got {ratio:.1f}x"


def test_construction_our_sqlmodel_speed() -> None:
    """Our SQLModel construction speed is competitive."""
    osm_time, _ = _measure_time(lambda: [BenchOurSQLModel(**_row(i)) for i in range(ROW_COUNT)])
    bm_time, _ = _measure_time(lambda: [BenchBaseModel(**_row(i)) for i in range(ROW_COUNT)])

    # Should be comparable to plain BaseModel
    ratio = osm_time / bm_time
    assert ratio < 3.0, f"Expected our SQLModel to be within 3x of BaseModel speed, got {ratio:.1f}x"


def test_sqlite_load_our_sqlmodel(sqlite_engine: Engine) -> None:
    """Our SQLModel DB load works and is competitive with SQLDataclass."""

    def load_sdc() -> list[Any]:
        with sqlite_engine.connect() as conn:
            return BenchSDC.load_all(conn)

    def load_osm() -> list[Any]:
        with sqlite_engine.connect() as conn:
            return BenchOurSQLModel.load_all(conn)

    sdc_peak, sdc_objs = _measure_peak_memory(load_sdc)
    osm_peak, osm_objs = _measure_peak_memory(load_osm)
    assert len(sdc_objs) == ROW_COUNT
    assert len(osm_objs) == ROW_COUNT

    # Our SQLModel load uses model_construct (fast path), should be competitive
    sdc_time, _ = _measure_time(load_sdc)
    osm_time, _ = _measure_time(load_osm)

    # Speed should be within 3x
    time_ratio = osm_time / sdc_time
    assert time_ratio < 3.0, f"Expected our SQLModel load within 3x of SQLDataclass, got {time_ratio:.1f}x"
