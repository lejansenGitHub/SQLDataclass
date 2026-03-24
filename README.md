# SQLDataclass

**Memory-efficient ORM bridge: pydantic dataclasses + SQLAlchemy Core. FastAPI compatible.**

Define your models once — like SQLModel — but get the memory footprint of plain dataclasses. SQLDataclass uses pydantic dataclasses (`slots=True`) under the hood, with full pydantic validation, relationships, and native FastAPI support.

## Performance

### Object construction (20 fields, 10k objects)

| Library | B/row | vs SQLDataclass | Construction time |
|---|---:|---:|---:|
| **SQLDataclass** | **322** | **1.0x** | **25 ms** |
| Pydantic BaseModel | 792 | 2.5x | 10 ms |
| SQLAlchemy ORM | 1,699 | **5.3x** | 129 ms |
| **SQLModel** | **4,549** | **14.1x** | **315 ms** |

### Database loading — SQLite (10k rows, 20 fields)

| Library | B/row | vs SQLDataclass | Load time |
|---|---:|---:|---:|
| **SQLDataclass** | **752** | **1.0x** | **57 ms** |
| SQLAlchemy ORM | 2,142 | 2.9x | 47 ms |
| **SQLModel** | **2,454** | **3.3x** | **56 ms** |

### Database loading — PostgreSQL (10k rows, 20 fields)

| Library | B/row | vs SQLDataclass | Load time |
|---|---:|---:|---:|
| **SQLDataclass** | **748** | **1.0x** | **57 ms** |
| SQLAlchemy ORM | 2,139 | 2.9x | 45 ms |
| **SQLModel** | **2,451** | **3.3x** | **55 ms** |

### Why the difference?

- SQLDataclass uses **pydantic dataclasses with `slots=True`** — no `__dict__`, minimal per-instance overhead
- SQLModel inherits from both Pydantic BaseModel and SQLAlchemy ORM — each instance carries validation machinery AND ORM state tracking
- SQLAlchemy ORM instances carry identity map, state tracking, and relationship loading machinery
- SQLDataclass queries via **SQLAlchemy Core** (raw result rows), not the ORM session

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

### Setup

```python
from sqlalchemy import create_engine

engine = create_engine("sqlite:///app.db")
SQLDataclass.metadata.create_all(engine)

# Bind the engine once — conn becomes optional everywhere
SQLDataclass.bind(engine)
```

### Insert data

```python
hero = Hero(name="Spider-Man", secret_name="Peter Parker")
hero.insert()

# Bulk insert
Hero.insert_many(objects=[
    Hero(name="Iron Man", secret_name="Tony Stark", age=45),
    Hero(name="Thor", secret_name="Thor Odinson", age=1500),
])
```

### Query data

```python
# Load all
heroes = Hero.load_all()

# Filter
heroes = Hero.load_all(where=Hero.c.age > 100)

# Load one
hero = Hero.load_one(where=Hero.c.name == "Spider-Man")

# Order
heroes = Hero.load_all(where=Hero.c.age > 25, order_by=Hero.c.name)
```

### Explicit connections (when you need transaction control)

```python
with engine.begin() as conn:
    hero1.insert(conn)
    hero2.insert(conn)
    # both commit together, or both rollback on error
```

## Relationships

SQLDataclass supports all common relationship patterns, loaded eagerly via SQLAlchemy Core — no ORM session required.

### Many-to-one

```python
from sqldataclass import SQLDataclass, Field, Relationship

class Team(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str

class Hero(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    team_id: int = Field(foreign_key="team.id")
    team: Team | None = Relationship()  # auto-JOINed on load

hero = Hero.load_one(where=Hero.c.name == "Spider-Man")
print(hero.team.name)  # "Avengers"
```

### One-to-many

```python
class Team(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    heroes: list[Hero] = Relationship(back_populates="team")

team = Team.load_one(where=Team.c.name == "Avengers")
print([h.name for h in team.heroes])  # ["Iron Man", "Thor"]
```

One-to-many uses a **two-query strategy** (not JOIN-then-group) for memory efficiency — one query for parents, one `WHERE fk IN (...)` for all children.

### Many-to-many

```python
class HeroTeamLink(SQLDataclass, table=True):
    hero_id: int = Field(primary_key=True, foreign_key="hero.id")
    team_id: int = Field(primary_key=True, foreign_key="team.id")

class Hero(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    teams: list[Team] = Relationship(link_model=HeroTeamLink)

class Team(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    heroes: list[Hero] = Relationship(link_model=HeroTeamLink)

hero = Hero.load_one(where=Hero.c.name == "Wolverine")
print([t.name for t in hero.teams])  # ["Avengers", "X-Men"]
```

### Discriminated unions

For polymorphic data stored in separate tables:

```python
from typing import Literal

class NormalData(SQLDataclass, table=True):
    id: int = Field(primary_key=True, foreign_key="participant.id")
    behavior: Literal["normal"] = "normal"
    p_max: float = 0.0

class BatteryData(SQLDataclass, table=True):
    id: int = Field(primary_key=True, foreign_key="participant.id")
    behavior: Literal["battery"] = "battery"
    capacity: float = 0.0

class Participant(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    behavior: str  # discriminator column
    data: NormalData | BatteryData = Relationship(discriminator="behavior")

p = Participant.load_one(where=Participant.c.name == "Alice")
print(type(p.data).__name__)  # "NormalData"
print(p.data.p_max)           # 100.0
```

## Use with FastAPI

Pydantic dataclasses are first-class citizens in FastAPI — no conversion needed:

```python
from fastapi import FastAPI

app = FastAPI()

@app.get("/heroes", response_model=list[Hero])
def get_heroes():
    return Hero.load_all()

@app.get("/heroes/{hero_id}", response_model=Hero)
def get_hero(hero_id: int):
    return Hero.load_one(where=Hero.c.id == hero_id)
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
    hero.insert()
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

## Relationship options

`Relationship()` marks a field as loaded from a related table — not stored as a column.

| Parameter | Type | Description |
|---|---|---|
| `back_populates` | `str` | Inverse field name on the child model |
| `link_model` | `type` | Link table class for many-to-many |
| `discriminator` | `str` | Column name for discriminated unions |
| `order_by` | `str` | Column name to sort collection children by |
| `default` | `Any` | Default value (`None` for many-to-one, `[]` for collections) |

## API reference

### Model methods

All methods accept an optional `conn` parameter. If omitted, a connection is auto-created from the bound engine (see `SQLDataclass.bind(engine)`).

| Method | Type | Description |
|---|---|---|
| `SQLDataclass.bind(engine)` | classmethod | Bind engine — makes `conn` optional everywhere |
| `Model.select()` | classmethod | Build a `SELECT` query |
| `Model.load_all(conn=, where=, order_by=)` | classmethod | Load all matching rows with relationships |
| `Model.load_one(conn=, where=)` | classmethod | Load one row or `None` |
| `Model.insert_many(conn=, objects=)` | classmethod | Bulk insert |
| `Model.update(values, conn=, where=)` | classmethod | Update matching rows, returns count |
| `Model.delete(conn=, where=)` | classmethod | Delete matching rows, returns count |
| `instance.insert(conn=)` | instance | Insert this row |
| `instance.to_dict(exclude_keys=)` | instance | Flat dict for SQL |
| `instance.upsert(conn=, index_elements=)` | instance | PostgreSQL upsert |
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
4. **Relationships without a session** — eager loading via JOINs and two-query strategy
5. **FastAPI native** — pydantic dataclasses work as response models out of the box
6. **Escape hatches** — low-level bridge API available when you need full control

## Known limitations

SQLDataclass intentionally trades some ORM features for memory efficiency and simplicity. Here's what it doesn't do (yet):

| Limitation | Workaround |
|---|---|
| **No lazy loading** — relationships are always eager-loaded | Use `.select()` + low-level `load_all()` to control what's loaded |
| ~~No `update()` or `delete()` methods~~ | **Fixed in v0.0.6** |
| ~~No pagination in `load_all`~~ | **Fixed in v0.0.7** — `Hero.load_all(limit=10, offset=20)` |
| **No nested relationship loading** — `hero.team.league` won't auto-load `league` | Load each level separately, or build a custom joined query |
| ~~No relationship ordering~~ | **Fixed in v0.0.8** — `Relationship(order_by="name")` |
| **No single-table or joined-table inheritance** | Use discriminated unions with `Relationship(discriminator=...)` |
| **Composite PKs don't work with collection relationships** | Use single-column PKs on models with `list[...]` relationships |
| **No identity map** — loading the same row twice creates two separate objects | Acceptable for immutable dataclass pattern; cache at application level if needed |
| **`bind()` is global** — can't bind different engines to different models | Pass `conn` explicitly when using multiple databases |
| **Eager-only collections** — one-to-many/many-to-many always load all children | Filter at query level or use low-level bridge API |

## Requirements

- Python 3.13+
- pydantic >= 2.0
- sqlalchemy >= 2.0

## License

MIT
