# SQLDataclass

**Memory-efficient ORM bridge: pydantic dataclasses + SQLAlchemy Core.**

SQLDataclass gives you pydantic validation and FastAPI compatibility with the memory footprint of plain dataclasses. It uses SQLAlchemy Core for queries (not the ORM) and constructs lightweight pydantic dataclass instances directly from result rows.

## Why?

| Representation | B/row (19 fields, 10k rows) | vs pydantic dc |
|---|---:|---:|
| pydantic dataclass (`slots=True`) | 223 | **1.0x** |
| stdlib dataclass (`slots=True`) | 223 | 1.0x |
| dict | 503 | 2.3x |
| SQLAlchemy ORM | 1,307 | 5.9x |
| Pydantic BaseModel | 2,847 | **12.8x** |

Pydantic dataclasses with `slots=True` are **12.8x lighter than BaseModel** and **5.9x lighter than ORM instances**, while keeping full pydantic validation. They also work natively as FastAPI request/response models.

## Install

```bash
pip install sqldataclass
```

For PostgreSQL support (upsert):

```bash
pip install sqldataclass[postgres]
```

## Quick start

### 1. Define your SQL schema (for DDL and queries only — never instantiated)

```python
from sqlalchemy import Column, Integer, Float, String
from sqldataclass import Base

class SensorSql(Base):
    __tablename__ = "sensors"
    sensor_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    reading = Column(Float)
```

### 2. Define your domain model (pydantic dataclass)

```python
from pydantic.dataclasses import dataclass

@dataclass(slots=True)
class Sensor:
    sensor_id: int
    name: str
    reading: float | None = None
```

### 3. Query and construct domain objects

```python
from sqlalchemy import create_engine, select
from sqldataclass import load_all, create_all_tables, table

engine = create_engine("sqlite:///app.db")
create_all_tables(engine)

with engine.connect() as conn:
    sensors = load_all(conn, select(table(SensorSql)), Sensor)
    # sensors is list[Sensor] — lightweight pydantic dataclasses
```

`load_all` constructs domain objects inline during cursor iteration — no intermediate `list[dict]` memory spike.

### 4. Write data

```python
from sqldataclass import insert_row, insert_many, flatten_for_table

with engine.begin() as conn:
    # Insert a single row
    insert_row(conn, SensorSql, {"sensor_id": 1, "name": "temp", "reading": 22.5})

    # Insert from a domain object
    sensor = Sensor(sensor_id=2, name="humidity", reading=65.0)
    insert_row(conn, SensorSql, flatten_for_table(sensor))

    # Bulk insert
    rows = [{"sensor_id": i, "name": f"sensor_{i}", "reading": float(i)} for i in range(3, 100)]
    insert_many(conn, SensorSql, rows)
```

### 5. Discriminated unions (flat SQL rows → nested pydantic)

When SQL stores subtypes in separate tables but your domain model uses discriminated unions:

```python
from typing import Literal
from pydantic import Field
from pydantic.dataclasses import dataclass

@dataclass(slots=True)
class NormalData:
    behavior: Literal["normal"] = "normal"
    p_max: float = 0.0

@dataclass(slots=True)
class BatteryData:
    behavior: Literal["battery"] = "battery"
    capacity: float = 0.0

@dataclass(slots=True)
class Participant:
    participant_id: int
    name: str
    data: NormalData | BatteryData = Field(..., discriminator="behavior")
```

```python
from sqldataclass import format_discriminated

# flat_row from a JOIN query:
flat_row = {"participant_id": 1, "name": "Alice", "behavior": "normal", "p_max": 100.0, "capacity": None}

shaped = format_discriminated(flat_row, Participant, field_name="data", discriminator="behavior")
participant = Participant(**shaped)
# participant.data is NormalData(behavior='normal', p_max=100.0)
```

### 6. Use with FastAPI

Pydantic dataclasses work directly as FastAPI response models — no conversion needed:

```python
from fastapi import FastAPI

app = FastAPI()

@app.get("/sensors", response_model=list[Sensor])
def get_sensors():
    with engine.connect() as conn:
        return load_all(conn, select(table(SensorSql)), Sensor)
```

## API reference

### Query

| Function | Description |
|---|---|
| `load_all(conn, query, cls)` | Execute query, construct `cls` instances inline (memory-efficient) |
| `fetch_all(conn, query)` | Execute query, return `list[dict]` |
| `fetch_one(conn, query)` | Execute query, return single `dict` or `None` |
| `select_columns(*table_classes)` | Build `select()` from multiple ORM table classes |

### Write

| Function | Description |
|---|---|
| `insert_row(conn, table_class, values)` | Insert a single row |
| `insert_many(conn, table_class, rows)` | Insert multiple rows |
| `upsert_row(conn, table_class, values, index_elements=)` | PostgreSQL `ON CONFLICT` upsert |
| `flatten_for_table(domain_object, exclude_keys=)` | Flatten a dataclass to a dict for insertion |

### Registry

| Function | Description |
|---|---|
| `Base` | Declarative base for SQL schema classes |
| `table(cls)` | Get the SQLAlchemy `Table` from an ORM-mapped class |
| `create_all_tables(engine)` | Create all registered tables |
| `drop_all_tables(engine)` | Drop all registered tables |

### Hydration

| Function | Description |
|---|---|
| `nest_fields(data, field_name, keys)` | Move keys from a flat dict into a nested dict |
| `discriminator_map(parent_class, field_name, discriminator)` | Build `{value: subclass}` mapping from a union type hint |
| `format_discriminated(data, parent_class, field_name=, discriminator=)` | Reshape flat row for discriminated union construction |

## Design philosophy

1. **Separate SQL schema and domain model** — schema classes define DDL and queries (never instantiated at runtime), domain models are pydantic dataclasses for actual data
2. **Always `slots=True`** — matches stdlib dataclass memory footprint
3. **Prefer `load_all` over `fetch_all` + loop** — avoids intermediate `list[dict]` memory spike
4. **No ORM at runtime** — SQLAlchemy Core queries with explicit joins give full control
5. **Pydantic dataclasses = FastAPI models** — no separate API schema needed

## Requirements

- Python 3.13+
- pydantic >= 2.0
- sqlalchemy >= 2.0

## License

MIT
