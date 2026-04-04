"""Memory benchmarks for SQLDataclass.

Uses tracemalloc to measure memory consumption across different object
representations and DB loading strategies.
"""

from __future__ import annotations

import gc
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel
from pydantic.dataclasses import dataclass as dataclass_pydantic
from sqlalchemy import Column, Float, Integer, String, Table, create_engine, insert, select
from sqlalchemy.orm import DeclarativeBase, Session

from sqldataclass.hydration import format_discriminated
from sqldataclass.query import fetch_all, load_all
from tests.util.memory import measure_memory

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


class _MemBenchBase(DeclarativeBase):
    pass


class TransformerTypeORM(_MemBenchBase):
    __tablename__ = "transformer_types_mem_bench"
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
# Discriminated-union types for hydration benchmark
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
    _MemBenchBase.metadata.create_all(engine)
    bench_table: Table = TransformerTypeORM.__table__  # type: ignore[assignment]  # SA table attrs set dynamically by metaclass
    with engine.begin() as conn:
        conn.execute(
            insert(bench_table),
            [_row(i) for i in range(row_count)],
        )
    return engine


# ---------------------------------------------------------------------------
# Tests — object representation memory comparison
# ---------------------------------------------------------------------------


def test_representation_memory_comparison() -> None:
    """Compare pydantic dc vs BaseModel vs ORM vs stdlib dc vs dict — 10k rows."""
    with measure_memory() as mem_pydantic_dc:
        pydantic_dc_objects = [TransformerTypePydanticDC(**_row(i)) for i in range(ROW_COUNT)]

    with measure_memory() as mem_basemodel:
        basemodel_objects = [TransformerTypeBaseModel(**_row(i)) for i in range(ROW_COUNT)]

    with measure_memory() as mem_orm:
        orm_objects = [TransformerTypeORM(**_row(i)) for i in range(ROW_COUNT)]

    with measure_memory() as mem_stdlib_dc:
        stdlib_dc_objects = [TransformerTypeStdlibDC(**_row(i)) for i in range(ROW_COUNT)]

    with measure_memory() as mem_dict:
        dict_objects = [_row(i) for i in range(ROW_COUNT)]

    # Keep references alive until all measurements are done.
    assert len(pydantic_dc_objects) == ROW_COUNT
    assert len(basemodel_objects) == ROW_COUNT
    assert len(orm_objects) == ROW_COUNT
    assert len(stdlib_dc_objects) == ROW_COUNT
    assert len(dict_objects) == ROW_COUNT

    # Pydantic dataclass should beat BaseModel and ORM.
    bm_ratio = mem_basemodel.peak / mem_pydantic_dc.peak
    assert bm_ratio > 1.3, f"Expected BaseModel to use >1.3x memory of pydantic dc, got {bm_ratio:.2f}x"

    orm_ratio = mem_orm.peak / mem_pydantic_dc.peak
    assert orm_ratio > 1.3, f"Expected ORM to use >1.3x memory of pydantic dc, got {orm_ratio:.2f}x"

    # Pydantic dc should be comparable to stdlib dataclass.
    stdlib_ratio = mem_pydantic_dc.peak / mem_stdlib_dc.peak
    assert stdlib_ratio < 1.5, f"Expected pydantic dc within 1.5x of stdlib dc, got {stdlib_ratio:.2f}x"

    # Dict should use less or comparable memory to pydantic dc (dicts are optimised).
    dict_ratio = mem_pydantic_dc.peak / mem_dict.peak
    assert dict_ratio < 3.0, f"Expected pydantic dc within 3x of dicts, got {dict_ratio:.2f}x"


# ---------------------------------------------------------------------------
# Tests — DB loading strategy memory comparison
# ---------------------------------------------------------------------------


def test_load_all_vs_fetch_all_memory() -> None:
    """load_all (streaming) should use less peak memory than fetch_all + loop."""
    engine = _seed_db(ROW_COUNT)
    query = select(TransformerTypeORM.__table__)

    with measure_memory() as mem_load_all:
        with engine.connect() as conn:
            load_all_results = load_all(conn, query, TransformerTypePydanticDC)

    gc.collect()

    with measure_memory() as mem_fetch_loop:
        with engine.connect() as conn:
            rows = fetch_all(conn, query)
            fetch_loop_results = [TransformerTypePydanticDC(**r) for r in rows]  # type: ignore[arg-type]  # dict values are Any from fetch_all

    assert len(load_all_results) == ROW_COUNT
    assert len(fetch_loop_results) == ROW_COUNT

    # load_all avoids the intermediate list[dict], so its peak should be lower.
    ratio = mem_fetch_loop.peak / mem_load_all.peak
    assert ratio > 1.0, (
        f"Expected fetch_all+loop to use more peak memory than load_all, "
        f"got ratio {ratio:.2f}x (load_all={mem_load_all.peak:,} B, "
        f"fetch_loop={mem_fetch_loop.peak:,} B)"
    )


def test_load_all_vs_orm_session_query_memory() -> None:
    """load_all should use less peak memory than ORM Session.query."""
    engine = _seed_db(ROW_COUNT)
    query = select(TransformerTypeORM.__table__)

    with measure_memory() as mem_load_all:
        with engine.connect() as conn:
            load_all_results = load_all(conn, query, TransformerTypePydanticDC)

    gc.collect()

    with measure_memory() as mem_orm_session:
        with Session(engine) as session:
            orm_results = list(session.query(TransformerTypeORM).all())

    assert len(load_all_results) == ROW_COUNT
    assert len(orm_results) == ROW_COUNT

    ratio = mem_orm_session.peak / mem_load_all.peak
    assert ratio > 1.0, f"Expected ORM Session.query to use more peak memory than load_all, got ratio {ratio:.2f}x"


# ---------------------------------------------------------------------------
# Tests — memory scaling
# ---------------------------------------------------------------------------


def test_memory_scales_linearly() -> None:
    """Memory should scale roughly linearly with row count.

    We measure at 1k, 5k, 10k, 50k and verify per-row memory is roughly constant.
    """
    counts = [1_000, 5_000, 10_000, 50_000]
    per_row_bytes: list[float] = []

    for count in counts:
        with measure_memory() as mem:
            objects = [TransformerTypePydanticDC(**_row(i)) for i in range(count)]
        assert len(objects) == count
        per_row_bytes.append(mem.peak / count)
        del objects
        gc.collect()

    # The per-row byte cost should be roughly constant across sizes.
    # Allow up to 2x variation (generous for CI environments).
    min_per_row = min(per_row_bytes)
    max_per_row = max(per_row_bytes)
    ratio = max_per_row / min_per_row
    assert ratio < 2.0, (
        f"Per-row memory is not scaling linearly: min={min_per_row:.0f} B, "
        f"max={max_per_row:.0f} B, ratio={ratio:.2f}x. "
        f"Values: {[f'{v:.0f}' for v in per_row_bytes]}"
    )


# ---------------------------------------------------------------------------
# Tests — hydration overhead
# ---------------------------------------------------------------------------


def test_format_discriminated_peak_memory() -> None:
    """Measure peak memory of format_discriminated on 10k flat rows."""
    flat_rows: list[dict[str, Any]] = [
        {
            "transformer_type_id": i,
            "name": f"row_{i}",
            "behavior": "normal",
            "un_pri": 110.0,
            "un_sec": 20.0,
            # Fields from the *other* variant that will be stripped.
            "capacity": 0.0,
            "charge_rate": 0.0,
        }
        for i in range(ROW_COUNT)
    ]

    with measure_memory() as mem:
        reshaped = [
            format_discriminated(
                row,
                ParentRow,
                field_name="data",
                discriminator="behavior",
            )
            for row in flat_rows
        ]

    assert len(reshaped) == ROW_COUNT

    # Hydration should not be wildly expensive.  We check that peak memory
    # is below 50 MB for 10k small rows — a generous upper bound.
    assert mem.peak < 50_000_000, f"format_discriminated peak memory too high: {mem.peak:,} B"
