# SQLDataclass

**Memory-efficient ORM bridge: pydantic dataclasses + SQLAlchemy Core. FastAPI compatible.**

Define your models once — like SQLModel — but get the memory footprint of plain dataclasses. SQLDataclass uses pydantic dataclasses (`slots=True`) under the hood, giving you **12.8x less memory than BaseModel** and **5.9x less than ORM instances**, with full pydantic validation and native FastAPI support.

## Performance

### Memory: object construction (19 fields, 10k rows)

| Representation | B/row | vs SQLDataclass | Construction (ms) |
|---|---:|---:|---:|
| **SQLDataclass** | **240** | **1.0x** | **14** |
| stdlib dataclass | 224 | 0.9x | 3 |
| pydantic dataclass (manual) | 224 | 0.9x | 26 |
| Pydantic BaseModel | 792 | 3.3x | 10 |
| **SQLModel** | **2,060** | **8.6x** | **271** |

SQLDataclass matches the memory footprint of a plain `@dataclass(slots=True)` while giving you full pydantic validation, SQLAlchemy table management, and FastAPI compatibility.

**vs SQLModel:** 8.6x less memory, 19x faster construction.

### Memory: database loading (10k rows from SQLite)

| Approach | B/row | Total | vs SQLDataclass |
|---|---:|---:|---:|
| **SQLDataclass** `load_all` | **188** | 1.9 MB | **1.0x** |
| **SQLModel** `session.exec` | **1,898** | 19.0 MB | **10.1x** |

When loading 10k rows from a database, SQLDataclass uses **10x less peak memory** than SQLModel and loads **1.7x faster**.

### Why the difference?

- SQLDataclass uses **pydantic dataclasses with `slots=True`** — no `__dict__`, minimal per-instance overhead
- SQLModel inherits from both Pydantic BaseModel and SQLAlchemy ORM — each instance carries validation machinery AND ORM state tracking
- SQLDataclass queries via **SQLAlchemy Core** (raw result rows), not the ORM session (which materializes heavy mapped instances)

## Install

```bash
pip install sqldataclass
```

For PostgreSQL upsert support:

```bash
pip install sqldataclass[postgres]
```

## Quick start

### Define your model

One class. That's it. No separate schema and domain model.

```python
from sqldataclass import SQLDataclass, Field

class Hero(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    secret_name: str
    age: int | None = None
```

This creates:
- A **pydantic dataclass** with `slots=True` (validation, FastAPI compat, minimal memory)
- A **SQLAlchemy Table** (for DDL and queries, never instantiated)

### Create tables

```python
from sqlalchemy import create_engine

engine = create_engine("sqlite:///app.db")
SQLDataclass.metadata.create_all(engine)
```

### Insert data

```python
hero = Hero(name="Spider-Man", secret_name="Peter Parker")

with engine.begin() as conn:
    hero.insert(conn)

    # Or bulk insert
    heroes = [
        Hero(name="Iron Man", secret_name="Tony Stark", age=45),
        Hero(name="Thor", secret_name="Thor Odinson", age=1500),
    ]
    Hero.insert_many(conn, heroes)
```

### Query data

```python
with engine.connect() as conn:
    # Load all
    heroes = Hero.load_all(conn)

    # Filter
    heroes = Hero.load_all(conn, Hero.c.age > 100)

    # Load one
    hero = Hero.load_one(conn, Hero.c.name == "Spider-Man")

    # Custom queries
    query = Hero.select().where(Hero.c.age > 25).order_by(Hero.c.name)
    heroes = Hero.load_all(conn, Hero.c.age > 25, order_by=Hero.c.name)
```

### Use with FastAPI

Pydantic dataclasses are first-class citizens in FastAPI — no conversion needed:

```python
from fastapi import FastAPI

app = FastAPI()

@app.get("/heroes", response_model=list[Hero])
def get_heroes():
    with engine.connect() as conn:
        return Hero.load_all(conn)

@app.get("/heroes/{hero_id}", response_model=Hero)
def get_hero(hero_id: int):
    with engine.connect() as conn:
        return Hero.load_one(conn, Hero.c.id == hero_id)
```

### Data-only models (API schemas)

Models without `table=True` are pure pydantic dataclasses — useful for request bodies:

```python
class HeroCreate(SQLDataclass):
    name: str
    secret_name: str
    age: int | None = None

@app.post("/heroes", response_model=Hero)
def create_hero(data: HeroCreate):
    hero = Hero(name=data.name, secret_name=data.secret_name, age=data.age)
    with engine.begin() as conn:
        hero.insert(conn)
    return hero
```

## Field options

`Field()` accepts both pydantic and SQLAlchemy parameters:

```python
class User(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    email: str = Field(unique=True, index=True, max_length=255)
    name: str = Field(min_length=1, max_length=100)
    age: int = Field(ge=0, le=200)
    team_id: int | None = Field(default=None, foreign_key="teams.id")
```

| Parameter | Type | Description |
|---|---|---|
| `primary_key` | `bool` | Mark as primary key |
| `index` | `bool` | Create database index |
| `unique` | `bool` | Add unique constraint |
| `foreign_key` | `str` | Foreign key reference (e.g. `"users.id"`) |
| `nullable` | `bool` | Override nullable inference |
| `sa_type` | `TypeEngine` | Override SQLAlchemy column type |
| `default` | `Any` | Default value |
| `ge`, `le`, `gt`, `lt` | `float` | Pydantic numeric validators |
| `min_length`, `max_length` | `int` | Pydantic string validators |
| `pattern` | `str` | Pydantic regex pattern |

## API reference

### Model methods

| Method | Type | Description |
|---|---|---|
| `Model.select()` | classmethod | Build a `SELECT` query |
| `Model.load_all(conn, where=, order_by=)` | classmethod | Load all matching rows |
| `Model.load_one(conn, where=)` | classmethod | Load one row or `None` |
| `Model.insert_many(conn, objects)` | classmethod | Bulk insert |
| `instance.insert(conn)` | instance | Insert this row |
| `instance.to_dict(exclude_keys=)` | instance | Flat dict for SQL |
| `instance.upsert(conn, index_elements=)` | instance | PostgreSQL upsert |
| `Model.c` | attribute | Column access for WHERE clauses |
| `Model.metadata` | attribute | SQLAlchemy MetaData |

### Low-level bridge API

For advanced use cases, the underlying bridge functions are also available:

| Function | Description |
|---|---|
| `load_all(conn, query, cls)` | Execute query, construct instances inline |
| `fetch_all(conn, query)` | Execute query, return `list[dict]` |
| `fetch_one(conn, query)` | Execute query, return single `dict` or `None` |
| `insert_row(conn, table_class, values)` | Insert a single row |
| `insert_many(conn, table_class, rows)` | Insert multiple rows |
| `flatten_for_table(obj, exclude_keys=)` | Flatten dataclass to dict |
| `nest_fields(data, field_name, keys)` | Reshape flat dict for nested models |
| `format_discriminated(data, cls, ...)` | Reshape flat row for discriminated unions |

## Design philosophy

1. **One class, one definition** — no separate SQL schema and domain model
2. **Memory-first** — pydantic dataclasses with `slots=True` match stdlib dataclass footprint
3. **SQLAlchemy Core, not ORM** — explicit queries, no hidden state tracking
4. **FastAPI native** — pydantic dataclasses work as response models out of the box
5. **Escape hatches** — low-level bridge API available when you need full control

## Requirements

- Python 3.13+
- pydantic >= 2.0
- sqlalchemy >= 2.0

## License

MIT
