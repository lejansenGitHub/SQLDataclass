"""Memory benchmarks proving pydantic dataclasses (slots=True) use significantly
less memory than BaseModel / ORM instances.

This is the core motivation for SQLDataclass.
"""

from __future__ import annotations

import tracemalloc
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel
from pydantic.dataclasses import dataclass as dataclass_pydantic
from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.orm import DeclarativeBase

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


# --- Representations to compare ---


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
class TransformerTypePydanticDataclass:
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
class TransformerTypeStdlibDataclass:
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


class _OrmBase(DeclarativeBase):
    pass


class TransformerTypeSql(_OrmBase):
    __tablename__ = "transformer_types_memory_test"
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


# --- Helpers ---


def _measure_bytes(factory: object) -> int:
    """Return peak memory (bytes) allocated while building ROW_COUNT objects."""
    tracemalloc.start()
    tracemalloc.reset_peak()
    objects = [factory(i) for i in range(ROW_COUNT)]  # type: ignore[operator]  # factory callable is Any from parametrize
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert len(objects) == ROW_COUNT
    return peak


def _make_dict(index: int) -> dict[str, Any]:
    return {**SAMPLE_ROW, "transformer_type_id": index}


def _make_basemodel(index: int) -> TransformerTypeBaseModel:
    return TransformerTypeBaseModel(**{**SAMPLE_ROW, "transformer_type_id": index})


def _make_orm(index: int) -> TransformerTypeSql:
    return TransformerTypeSql(**{**SAMPLE_ROW, "transformer_type_id": index})


def _make_pydantic_dataclass(index: int) -> TransformerTypePydanticDataclass:
    return TransformerTypePydanticDataclass(**{**SAMPLE_ROW, "transformer_type_id": index})


def _make_stdlib_dataclass(index: int) -> TransformerTypeStdlibDataclass:
    return TransformerTypeStdlibDataclass(**{**SAMPLE_ROW, "transformer_type_id": index})


# --- Tests ---


def test_pydantic_dataclass_uses_less_memory_than_basemodel() -> None:
    bytes_pydantic_dc = _measure_bytes(_make_pydantic_dataclass)
    bytes_basemodel = _measure_bytes(_make_basemodel)

    ratio = bytes_basemodel / bytes_pydantic_dc
    assert ratio > 1.5, (
        f"Expected BaseModel to use >1.5x the memory of pydantic dataclass, "
        f"got {ratio:.2f}x (pydantic_dc={bytes_pydantic_dc:,} B, BaseModel={bytes_basemodel:,} B)"
    )


def test_pydantic_dataclass_uses_less_memory_than_orm() -> None:
    bytes_pydantic_dc = _measure_bytes(_make_pydantic_dataclass)
    bytes_orm = _measure_bytes(_make_orm)

    ratio = bytes_orm / bytes_pydantic_dc
    assert ratio > 1.5, (
        f"Expected ORM instances to use >1.5x the memory of pydantic dataclass, "
        f"got {ratio:.2f}x (pydantic_dc={bytes_pydantic_dc:,} B, ORM={bytes_orm:,} B)"
    )


def test_pydantic_dataclass_comparable_to_stdlib_dataclass() -> None:
    """Pydantic dataclass (slots=True) should have similar memory to stdlib dataclass."""
    bytes_pydantic_dc = _measure_bytes(_make_pydantic_dataclass)
    bytes_stdlib_dc = _measure_bytes(_make_stdlib_dataclass)

    ratio = bytes_pydantic_dc / bytes_stdlib_dc
    assert ratio < 1.5, (
        f"Expected pydantic dataclass to be within 1.5x of stdlib dataclass, "
        f"got {ratio:.2f}x (pydantic_dc={bytes_pydantic_dc:,} B, stdlib_dc={bytes_stdlib_dc:,} B)"
    )
