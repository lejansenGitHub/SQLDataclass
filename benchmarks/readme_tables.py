"""Regenerate the README performance tables (SQLite, 10k rows, 20 fields).

Run from the repository root::

    uv run python -m benchmarks.readme_tables

Time and memory are measured in separate runs: ``tracemalloc`` slows every
allocation down (about 3x for ``load_all``), so a time taken while it traces
is not the real speed. Time is the median of ``REPEATS`` runs; B/row is the
memory the returned list still holds, divided by the row count.
"""

from __future__ import annotations

import dataclasses
import gc
import platform
import statistics
import time
import tracemalloc
from collections.abc import Callable
from typing import Any

import pydantic
import sqlalchemy
import sqlmodel
from pydantic.dataclasses import dataclass as pydantic_dataclass
from sqlalchemy import create_engine, insert, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlmodel import Session as SQLModelSession
from sqlmodel import SQLModel, select

from sqldataclass import SQLDataclass
from sqldataclass import SQLModel as OurSQLModel
from tests.performance_tests.test_comparison_benchmarks import (
    ROW_COUNT,
    SAMPLE_ROW,
    BenchBaseModel,
    BenchOurSQLModel,
    BenchSA,
    BenchSDC,
    BenchSM,
    _row,
    _SABase,
)

REPEATS = 15

StdlibRow = dataclasses.make_dataclass(
    "StdlibRow",
    [(name, type(value)) for name, value in SAMPLE_ROW.items()],
    slots=True,
)
PydanticRow = pydantic_dataclass(
    dataclasses.make_dataclass(
        "PydanticRow",
        [(name, type(value)) for name, value in SAMPLE_ROW.items()],
        slots=True,
    )
)

Approach = tuple[str, Callable[[], list[Any]]]


def median_milliseconds(build: Callable[[], list[Any]]) -> float:
    """Median wall time of ``build`` over ``REPEATS`` runs, after one warmup."""
    build()
    timings = []
    for _ in range(REPEATS):
        gc.collect()
        start = time.perf_counter()
        build()
        timings.append((time.perf_counter() - start) * 1000)
    return statistics.median(timings)


def retained_bytes_per_row(build: Callable[[], list[Any]]) -> int:
    """Bytes still held by the list ``build`` returns, per row."""
    build()
    gc.collect()
    tracemalloc.start()
    before, _ = tracemalloc.get_traced_memory()
    result = build()
    gc.collect()
    after, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert len(result) == ROW_COUNT
    return round((after - before) / ROW_COUNT)


def markdown_table(approaches: list[Approach]) -> str:
    """Render ``approaches`` as the README's ``Approach | B/row | Time`` table."""
    memory = {label: retained_bytes_per_row(build) for label, build in approaches}
    timing = {label: median_milliseconds(build) for label, build in approaches}
    lines = ["| Approach | B/row | Time |", "|---|---:|---:|"]
    lines += [f"| {label} | {memory[label]:,} | {timing[label]:.0f} ms |" for label, _ in approaches]
    return "\n".join(lines)


def construction_approaches() -> list[Approach]:
    """Build 10k objects from the same prebuilt row dicts, no database."""
    rows = [_row(index) for index in range(ROW_COUNT)]
    return [
        ("dict", lambda: [dict(row) for row in rows]),
        ("stdlib dataclass (`slots=True`)", lambda: [StdlibRow(**row) for row in rows]),
        ("pydantic dataclass (`slots=True`)", lambda: [PydanticRow(**row) for row in rows]),
        ("**SQLDataclass**", lambda: [BenchSDC(**row) for row in rows]),
        ("SQLAlchemy ORM", lambda: [BenchSA(**row) for row in rows]),
        ("Pydantic BaseModel", lambda: [BenchBaseModel(**row) for row in rows]),
        ("**SQLDataclass `SQLModel`**", lambda: [BenchOurSQLModel(**row) for row in rows]),
        ("SQLModel (tiangolo)", lambda: [BenchSM(**row) for row in rows]),
    ]


def seeded_sqlite_engine() -> Engine:
    """In-memory SQLite with the same 10k rows in every benchmark table."""
    engine = create_engine("sqlite:///:memory:")
    for metadata in (SQLDataclass.metadata, _SABase.metadata, OurSQLModel.metadata, SQLModel.metadata):
        metadata.create_all(engine)
    rows = [_row(index) for index in range(ROW_COUNT)]
    with engine.begin() as conn:
        for table in (BenchSDC.__table__, BenchSA.__table__, BenchOurSQLModel.__table__, BenchSM.__table__):
            conn.execute(insert(table), rows)
    return engine


def loading_approaches(engine: Engine) -> list[Approach]:
    """Load 10k rows; each approach opens its own connection or session."""
    raw_query = text("SELECT * FROM bench_cmp_sdc")

    def raw_sql(construct: Callable[..., Any]) -> list[Any]:
        with engine.connect() as conn:
            return [construct(**row) for row in conn.execute(raw_query).mappings()]

    def sqldataclass_load_all() -> list[Any]:
        with engine.connect() as conn:
            return BenchSDC.load_all(conn)

    def our_sqlmodel_load_all() -> list[Any]:
        with engine.connect() as conn:
            return BenchOurSQLModel.load_all(conn)

    def orm_query() -> list[Any]:
        with Session(engine) as session:
            return session.query(BenchSA).all()

    def tiangolo_exec() -> list[Any]:
        with SQLModelSession(engine) as session:
            return list(session.exec(select(BenchSM)).all())

    return [
        ("Raw SQL → dict", lambda: raw_sql(dict)),
        ("Raw SQL → stdlib dataclass", lambda: raw_sql(StdlibRow)),
        ("Raw SQL → pydantic dataclass", lambda: raw_sql(PydanticRow)),
        ("**SQLDataclass `load_all`**", sqldataclass_load_all),
        ("**SQLDataclass `SQLModel` `load_all`**", our_sqlmodel_load_all),
        ("SQLAlchemy ORM `Session.query`", orm_query),
        ("SQLModel (tiangolo) `session.exec`", tiangolo_exec),
    ]


def main() -> None:
    print(
        f"Python {platform.python_version()}, SQLAlchemy {sqlalchemy.__version__}, "
        f"pydantic {pydantic.VERSION}, sqlmodel {sqlmodel.__version__}, {platform.machine()}\n"
    )
    print("### Object construction (20 fields, 10k objects, no DB)\n")
    print(markdown_table(construction_approaches()))
    print("\n### Database loading — SQLite (10k rows, 20 fields)\n")
    print(markdown_table(loading_approaches(seeded_sqlite_engine())))


if __name__ == "__main__":
    main()
