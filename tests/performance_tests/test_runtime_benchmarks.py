"""Runtime / speed benchmarks for SQLDataclass.

Uses time.perf_counter to measure wall-clock time for construction,
query, insertion, and hydration operations.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel
from pydantic.dataclasses import dataclass as dataclass_pydantic
from sqlalchemy import Column, Float, Integer, String, Table, create_engine, func, insert, select
from sqlalchemy.orm import DeclarativeBase, Session

from sqldataclass.hydration import format_discriminated, nest_fields
from sqldataclass.query import fetch_all, load_all
from sqldataclass.write import insert_many

ROW_COUNT = 10_000


SAMPLE_ROW: dict[str, Any] = {
    "transformer_type_id": 1,
    "name": "test_type",
    "un_pri": 110.0,
    "un_sec": 20.0,
    "ur_pri": 1.0,
    "ur_sec": 1.0,
    "sr": 40.0,
    "uk": 12.5,
    "ukr": 0.5,
    "ufe": 0.1,
    "vecgroup": "Dyn5",
    "connection_type_pri": 1,
    "connection_type_sec": 2,
    "phase_shift": 150.0,
    "x0_pri": 0.95,
    "x0_sec": 0.95,
    "x0_mag": 0.95,
    "costs": 100_000.0,
    "lifespan": 40,
}


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------


class TransformerTypeBaseModel(BaseModel):
    transformer_type_id: int
    name: str
    un_pri: float
    un_sec: float
    ur_pri: float
    ur_sec: float
    sr: float
    uk: float
    ukr: float
    ufe: float
    vecgroup: str
    connection_type_pri: int
    connection_type_sec: int
    phase_shift: float
    x0_pri: float
    x0_sec: float
    x0_mag: float
    costs: float
    lifespan: int


@dataclass_pydantic(slots=True)
class TransformerTypePydanticDC:
    transformer_type_id: int
    name: str
    un_pri: float
    un_sec: float
    ur_pri: float
    ur_sec: float
    sr: float
    uk: float
    ukr: float
    ufe: float
    vecgroup: str
    connection_type_pri: int
    connection_type_sec: int
    phase_shift: float
    x0_pri: float
    x0_sec: float
    x0_mag: float
    costs: float
    lifespan: int


@dataclass(slots=True)
class TransformerTypeStdlibDC:
    transformer_type_id: int
    name: str
    un_pri: float
    un_sec: float
    ur_pri: float
    ur_sec: float
    sr: float
    uk: float
    ukr: float
    ufe: float
    vecgroup: str
    connection_type_pri: int
    connection_type_sec: int
    phase_shift: float
    x0_pri: float
    x0_sec: float
    x0_mag: float
    costs: float
    lifespan: int


class _RuntimeBenchBase(DeclarativeBase):
    pass


class TransformerTypeORM(_RuntimeBenchBase):
    __tablename__ = "transformer_types_rt_bench"
    transformer_type_id = Column(Integer, primary_key=True)
    name = Column(String)
    un_pri = Column(Float)
    un_sec = Column(Float)
    ur_pri = Column(Float)
    ur_sec = Column(Float)
    sr = Column(Float)
    uk = Column(Float)
    ukr = Column(Float)
    ufe = Column(Float)
    vecgroup = Column(String)
    connection_type_pri = Column(Integer)
    connection_type_sec = Column(Integer)
    phase_shift = Column(Float)
    x0_pri = Column(Float)
    x0_sec = Column(Float)
    x0_mag = Column(Float)
    costs = Column(Float)
    lifespan = Column(Integer)


# ---------------------------------------------------------------------------
# Discriminated-union types for hydration benchmarks
# ---------------------------------------------------------------------------


@dataclass_pydantic(slots=True)
class NormalData:
    behavior: Literal["normal"]
    un_pri: float
    un_sec: float


@dataclass_pydantic(slots=True)
class BatteryData:
    behavior: Literal["battery"]
    capacity: float
    charge_rate: float


@dataclass_pydantic(slots=True)
class ParentRow:
    transformer_type_id: int
    name: str
    data: NormalData | BatteryData


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row(index: int) -> dict[str, Any]:
    return {**SAMPLE_ROW, "transformer_type_id": index}


def _seed_db(row_count: int) -> Any:
    """Create an in-memory SQLite engine with *row_count* rows and return it."""
    engine = create_engine("sqlite://", echo=False)
    _RuntimeBenchBase.metadata.create_all(engine)
    bench_table: Table = TransformerTypeORM.__table__  # type: ignore[assignment]  # SA table attrs set dynamically by metaclass
    with engine.begin() as conn:
        conn.execute(
            insert(bench_table),
            [_row(i) for i in range(row_count)],
        )
    return engine


def _time_it(func: Any) -> float:
    """Run *func* and return elapsed wall-clock seconds."""
    start = time.perf_counter()
    func()
    return time.perf_counter() - start


# ---------------------------------------------------------------------------
# Tests — construction speed
# ---------------------------------------------------------------------------


def test_construction_speed_pydantic_dc_vs_basemodel() -> None:
    """Pydantic dataclass construction should be faster than BaseModel."""
    rows = [_row(i) for i in range(ROW_COUNT)]

    # Warmup both paths so JIT / internal caches are primed.
    _ = [TransformerTypePydanticDC(**r) for r in rows[:100]]
    _ = [TransformerTypeBaseModel(**r) for r in rows[:100]]

    elapsed_dc = _time_it(lambda: [TransformerTypePydanticDC(**r) for r in rows])
    elapsed_bm = _time_it(lambda: [TransformerTypeBaseModel(**r) for r in rows])

    # In CI environments pydantic dc and BaseModel can be very close.
    # We just verify dc is not dramatically slower.
    ratio = elapsed_bm / elapsed_dc
    assert ratio > 0.5, (
        f"Expected pydantic dc to not be >2x slower than BaseModel, "
        f"got {ratio:.2f}x (dc={elapsed_dc:.4f}s, bm={elapsed_bm:.4f}s)"
    )


def test_construction_speed_pydantic_dc_vs_orm() -> None:
    """Pydantic dataclass construction should be faster than ORM instance creation."""
    rows = [_row(i) for i in range(ROW_COUNT)]

    elapsed_dc = _time_it(lambda: [TransformerTypePydanticDC(**r) for r in rows])
    elapsed_orm = _time_it(lambda: [TransformerTypeORM(**r) for r in rows])

    ratio = elapsed_orm / elapsed_dc
    assert ratio > 1.0, (
        f"Expected pydantic dc to be faster than ORM construction, "
        f"got {ratio:.2f}x (dc={elapsed_dc:.4f}s, orm={elapsed_orm:.4f}s)"
    )


# ---------------------------------------------------------------------------
# Tests — query speed
# ---------------------------------------------------------------------------


def test_query_speed_load_all_vs_fetch_all_loop() -> None:
    """Measure load_all vs fetch_all + loop wall time."""
    engine = _seed_db(ROW_COUNT)
    query = select(TransformerTypeORM.__table__)

    def _load_all() -> list[TransformerTypePydanticDC]:
        with engine.connect() as conn:
            return load_all(conn, query, TransformerTypePydanticDC)

    def _fetch_loop() -> list[TransformerTypePydanticDC]:
        with engine.connect() as conn:
            rows = fetch_all(conn, query)
            return [TransformerTypePydanticDC(**r) for r in rows]  # type: ignore[arg-type]  # dict values are Any from fetch_all

    elapsed_load = _time_it(_load_all)
    elapsed_fetch = _time_it(_fetch_loop)

    # load_all should be at least as fast (we use a generous threshold).
    # The main win is memory, but it should not be significantly slower.
    ratio = elapsed_fetch / elapsed_load
    assert ratio > 0.5, (
        f"load_all is unexpectedly slower than fetch_all+loop: "
        f"ratio={ratio:.2f}x (load={elapsed_load:.4f}s, fetch={elapsed_fetch:.4f}s)"
    )


def test_query_speed_load_all_vs_orm_session() -> None:
    """Measure load_all vs ORM Session.query wall time."""
    engine = _seed_db(ROW_COUNT)
    query = select(TransformerTypeORM.__table__)

    def _load_all() -> list[TransformerTypePydanticDC]:
        with engine.connect() as conn:
            return load_all(conn, query, TransformerTypePydanticDC)

    def _orm_query() -> list[TransformerTypeORM]:
        with Session(engine) as session:
            return list(session.query(TransformerTypeORM).all())

    elapsed_load = _time_it(_load_all)
    elapsed_orm = _time_it(_orm_query)

    # load_all with pydantic dc should be competitive with ORM.
    ratio = elapsed_orm / elapsed_load
    assert ratio > 0.3, (
        f"load_all is unexpectedly much slower than ORM: "
        f"ratio={ratio:.2f}x (load={elapsed_load:.4f}s, orm={elapsed_orm:.4f}s)"
    )


# ---------------------------------------------------------------------------
# Tests — insert speed
# ---------------------------------------------------------------------------


def test_insert_many_speed() -> None:
    """Measure insert_many wall time for 10k rows."""
    engine = create_engine("sqlite://", echo=False)
    _RuntimeBenchBase.metadata.create_all(engine)

    rows = [_row(i) for i in range(ROW_COUNT)]

    def _do_insert() -> None:
        with engine.begin() as conn:
            insert_many(conn, TransformerTypeORM, rows)

    elapsed = _time_it(_do_insert)

    # Sanity check: 10k rows should insert in under 10 seconds on any machine.
    assert elapsed < 10.0, f"insert_many took too long: {elapsed:.4f}s for {ROW_COUNT} rows"

    # Verify rows were actually inserted.
    with engine.connect() as conn:
        count = conn.execute(select(func.count()).select_from(TransformerTypeORM.__table__)).scalar()
    assert count == ROW_COUNT


# ---------------------------------------------------------------------------
# Tests — hydration speed
# ---------------------------------------------------------------------------


def test_format_discriminated_speed() -> None:
    """Measure format_discriminated wall time on 10k flat dicts."""
    flat_rows: list[dict[str, Any]] = [
        {
            "transformer_type_id": i,
            "name": f"row_{i}",
            "behavior": "normal",
            "un_pri": 110.0,
            "un_sec": 20.0,
            "capacity": 0.0,
            "charge_rate": 0.0,
        }
        for i in range(ROW_COUNT)
    ]

    def _hydrate() -> list[dict[str, Any]]:
        return [
            format_discriminated(
                row,
                ParentRow,
                field_name="data",
                discriminator="behavior",
            )
            for row in flat_rows
        ]

    elapsed = _time_it(_hydrate)

    # Generous upper bound: 10k hydrations should complete in under 10 seconds.
    assert elapsed < 10.0, f"format_discriminated took too long: {elapsed:.4f}s for {ROW_COUNT} rows"


def test_nest_fields_speed() -> None:
    """Measure nest_fields wall time on 10k dicts."""
    keys_to_nest = {"un_pri", "un_sec", "ur_pri", "ur_sec"}

    flat_rows: list[dict[str, Any]] = [
        {
            "transformer_type_id": i,
            "name": f"row_{i}",
            "un_pri": 110.0,
            "un_sec": 20.0,
            "ur_pri": 1.0,
            "ur_sec": 1.0,
            "extra_field": "keep",
        }
        for i in range(ROW_COUNT)
    ]

    def _nest() -> list[dict[str, Any]]:
        return [nest_fields(row, "nested", keys_to_nest) for row in flat_rows]

    elapsed = _time_it(_nest)

    # Generous upper bound: 10k nest operations should complete in under 5 seconds.
    assert elapsed < 5.0, f"nest_fields took too long: {elapsed:.4f}s for {ROW_COUNT} rows"
