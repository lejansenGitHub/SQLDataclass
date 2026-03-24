"""SQLite integration tests — full read/write round-trip."""

from __future__ import annotations

import tracemalloc
from collections.abc import Generator
from typing import Any, Literal

import pytest
from pydantic import Field
from pydantic.dataclasses import dataclass as dataclass_pydantic
from sqlalchemy import Column, Float, ForeignKey, Integer, Select, String, Table, create_engine, insert, select
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import DeclarativeBase, Session

from sqldataclass.hydration import format_discriminated
from sqldataclass.query import fetch_all, fetch_one, load_all, select_columns
from sqldataclass.registry import Base, create_all_tables, drop_all_tables, table
from sqldataclass.write import flatten_for_table, insert_row

# --- SQL schema (never instantiated at runtime) ---


class ParticipantSql(Base):
    __tablename__ = "participants"
    participant_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, default="Undefined")


class ParticipantDataSql(Base):
    __tablename__ = "participants_data"
    participant_data_id = Column(
        Integer,
        ForeignKey("participants.participant_id", ondelete="CASCADE"),
        primary_key=True,
    )
    behavior = Column(String, nullable=False, default="normal")


class NormalDataSql(Base):
    __tablename__ = "participants_data_normal"
    id = Column(
        Integer,
        ForeignKey("participants_data.participant_data_id", ondelete="CASCADE"),
        primary_key=True,
    )
    p_max = Column(Float, nullable=False, default=0.0)


class BatteryDataSql(Base):
    __tablename__ = "participants_data_battery"
    id = Column(
        Integer,
        ForeignKey("participants_data.participant_data_id", ondelete="CASCADE"),
        primary_key=True,
    )
    capacity = Column(Float, nullable=False, default=0.0)


# --- Domain models (pydantic dataclasses) ---


@dataclass_pydantic(kw_only=True, slots=True)
class NormalData:
    behavior: Literal["normal"] = "normal"
    p_max: float = 0.0


@dataclass_pydantic(kw_only=True, slots=True)
class BatteryData:
    behavior: Literal["battery"] = "battery"
    capacity: float = 0.0


@dataclass_pydantic(kw_only=True, slots=True)
class Participant:
    participant_id: int
    name: str = "Undefined"
    data: NormalData | BatteryData = Field(..., discriminator="behavior")


# --- Repository helpers ---


def save_participant(conn: Connection, participant: Participant) -> None:
    """Write a participant and its data subtype to the DB."""
    insert_row(conn, ParticipantSql, {"participant_id": participant.participant_id, "name": participant.name})
    insert_row(
        conn,
        ParticipantDataSql,
        {"participant_data_id": participant.participant_id, "behavior": participant.data.behavior},
    )

    if isinstance(participant.data, NormalData):
        insert_row(conn, NormalDataSql, {"id": participant.participant_id, "p_max": participant.data.p_max})
    elif isinstance(participant.data, BatteryData):
        insert_row(conn, BatteryDataSql, {"id": participant.participant_id, "capacity": participant.data.capacity})


def load_participants(conn: Connection) -> list[Participant]:
    """Read all participants with their discriminated data subtypes."""
    tp = table(ParticipantSql)
    td = table(ParticipantDataSql)
    tn = table(NormalDataSql)
    tb = table(BatteryDataSql)

    query = (
        select(tp.c.participant_id, tp.c.name, td.c.behavior, tn.c.p_max, tb.c.capacity)
        .join(td, tp.c.participant_id == td.c.participant_data_id)
        .outerjoin(tn, td.c.participant_data_id == tn.c.id)
        .outerjoin(tb, td.c.participant_data_id == tb.c.id)
    )

    rows = fetch_all(conn, query)
    return [
        Participant(**format_discriminated(row, Participant, field_name="data", discriminator="behavior"))
        for row in rows
    ]


# --- Fixtures ---


@pytest.fixture
def engine() -> Generator[Engine]:
    engine = create_engine("sqlite:///:memory:")
    create_all_tables(engine)
    yield engine
    drop_all_tables(engine)


# --- Tests ---


def test_table_helper_returns_core_table(engine: Engine) -> None:
    target_table = table(ParticipantSql)
    assert target_table.name == "participants"
    assert "participant_id" in {column.name for column in target_table.columns}


def test_insert_and_fetch_one(engine: Engine) -> None:
    with engine.begin() as conn:
        insert_row(conn, ParticipantSql, {"participant_id": 1, "name": "Alice"})

    with engine.connect() as conn:
        target_table = table(ParticipantSql)
        row = fetch_one(conn, select(target_table).where(target_table.c.participant_id == 1))
        assert row is not None
        assert row["name"] == "Alice"


def test_fetch_one_returns_none_for_missing(engine: Engine) -> None:
    with engine.connect() as conn:
        target_table = table(ParticipantSql)
        row = fetch_one(conn, select(target_table).where(target_table.c.participant_id == 999))
        assert row is None


def test_fetch_all_returns_list_of_dicts(engine: Engine) -> None:
    with engine.begin() as conn:
        insert_row(conn, ParticipantSql, {"participant_id": 1, "name": "Alice"})
        insert_row(conn, ParticipantSql, {"participant_id": 2, "name": "Bob"})

    with engine.connect() as conn:
        rows = fetch_all(conn, select(table(ParticipantSql)))
        assert len(rows) == 2
        names = {row["name"] for row in rows}
        assert names == {"Alice", "Bob"}


def test_select_columns_builds_multi_table_select(engine: Engine) -> None:
    stmt = select_columns(ParticipantSql, ParticipantDataSql)
    assert isinstance(stmt, Select)
    column_names = {str(column.name) for column in stmt.selected_columns}
    assert "participant_id" in column_names
    assert "behavior" in column_names


def test_save_and_load_normal_participant(engine: Engine) -> None:
    alice = Participant(participant_id=1, name="Alice", data=NormalData(p_max=100.0))
    with engine.begin() as conn:
        save_participant(conn, alice)

    with engine.connect() as conn:
        participants = load_participants(conn)
        assert len(participants) == 1
        loaded = participants[0]
        assert loaded.participant_id == 1
        assert loaded.name == "Alice"
        assert isinstance(loaded.data, NormalData)
        assert loaded.data.p_max == 100.0


def test_save_and_load_battery_participant(engine: Engine) -> None:
    bob = Participant(participant_id=2, name="Bob", data=BatteryData(capacity=50.0))
    with engine.begin() as conn:
        save_participant(conn, bob)

    with engine.connect() as conn:
        participants = load_participants(conn)
        assert len(participants) == 1
        loaded = participants[0]
        assert isinstance(loaded.data, BatteryData)
        assert loaded.data.capacity == 50.0


def test_save_and_load_mixed_participants(engine: Engine) -> None:
    alice = Participant(participant_id=1, name="Alice", data=NormalData(p_max=100.0))
    bob = Participant(participant_id=2, name="Bob", data=BatteryData(capacity=50.0))

    with engine.begin() as conn:
        save_participant(conn, alice)
        save_participant(conn, bob)

    with engine.connect() as conn:
        participants = load_participants(conn)
        assert len(participants) == 2
        by_id = {participant.participant_id: participant for participant in participants}
        assert isinstance(by_id[1].data, NormalData)
        assert by_id[1].data.p_max == 100.0
        assert isinstance(by_id[2].data, BatteryData)
        assert by_id[2].data.capacity == 50.0


def test_flatten_for_table_strips_nested() -> None:
    participant = Participant(participant_id=1, name="Alice", data=NormalData(p_max=42.0))
    flat = flatten_for_table(participant)
    assert "data" not in flat
    assert flat["participant_id"] == 1
    assert flat["name"] == "Alice"


# --- End-to-end memory benchmark ---


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


class _OrmBase(DeclarativeBase):
    pass


class TransformerTypeSqlBench(_OrmBase):
    __tablename__ = "transformer_types_bench"
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


@pytest.fixture
def populated_engine() -> Engine:
    """SQLite engine with ROW_COUNT rows."""
    engine = create_engine("sqlite:///:memory:")
    _OrmBase.metadata.create_all(engine)
    bench_table: Table = TransformerTypeSqlBench.__table__  # type: ignore[assignment]
    with engine.begin() as conn:
        conn.execute(
            insert(bench_table),
            [{**SAMPLE_ROW, "transformer_type_id": index} for index in range(ROW_COUNT)],
        )
    return engine


def _measure_db_load(loader: Any, engine: Engine) -> int:
    """Return peak memory (bytes) allocated while loading all rows from DB."""
    loader(engine)  # warm up

    tracemalloc.start()
    tracemalloc.reset_peak()
    objects = loader(engine)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert len(objects) == ROW_COUNT
    return peak


def _load_via_bridge_streaming(engine: Engine) -> list[TransformerTypePydanticDataclass]:
    """Streaming bridge via load_all: no intermediate list[dict]."""
    target_table = TransformerTypeSqlBench.__table__
    with engine.connect() as conn:
        return load_all(conn, select(target_table), TransformerTypePydanticDataclass)


def _load_via_orm(engine: Engine) -> list[TransformerTypeSqlBench]:
    """Standard SQLAlchemy ORM: Session.query → ORM instances."""
    with Session(engine) as session:
        return list(session.query(TransformerTypeSqlBench).all())


def test_streaming_bridge_uses_less_memory_than_orm(populated_engine: Engine) -> None:
    """End-to-end: loading from SQLite via streaming bridge vs ORM."""
    bytes_streaming = _measure_db_load(_load_via_bridge_streaming, populated_engine)
    bytes_orm = _measure_db_load(_load_via_orm, populated_engine)

    assert bytes_orm > bytes_streaming, (
        f"Expected ORM to use more memory than streaming bridge, "
        f"got streaming={bytes_streaming:,} B, ORM={bytes_orm:,} B"
    )
