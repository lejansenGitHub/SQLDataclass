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

### Complex models with relationships (100 teams, 5k heroes, 20 tags, SQLite)

**Teams with heroes (one-to-many, 100 teams + 5k heroes):**

| Library | Memory | Load time | Notes |
|---|---:|---:|---|
| **SQLDataclass** | **1.2 MB** | **13 ms** | Two-query + back-ref stitching, no session |
| SQLAlchemy ORM + joinedload | 8.1 MB | 19 ms | JOIN-based, needs session |

**Heroes with team + tags (many-to-one + many-to-many, 5k heroes, 20 tags, 3 tags/hero):**

| Library | Memory | Load time | Notes |
|---|---:|---:|---|
| **SQLDataclass** | **3.7 MB** | **53 ms** | PK-cached deduplication, no session |
| SQLAlchemy ORM + eager | 12.3 MB | 70 ms | Identity map deduplication |

### When to use what

- **Simple/flat models** — SQLDataclass wins on both memory and speed (3-14x less memory)
- **One-to-many** — SQLDataclass wins (6.7x less memory, faster, with automatic back-references)
- **Many-to-many** — SQLDataclass wins (3.3x less memory, faster, PK-based deduplication)

### Why the difference?

- SQLDataclass uses **pydantic dataclasses with `slots=True`** — no `__dict__`, minimal per-instance overhead
- SQLModel inherits from both Pydantic BaseModel and SQLAlchemy ORM — each instance carries validation machinery AND ORM state tracking
- SQLDataclass deduplicates M2M targets via PK cache (same tag = same instance), matching ORM's identity map benefit without the session overhead
- SQLDataclass queries via **SQLAlchemy Core** (raw result rows), not the ORM session
- **Result**: SQLDataclass wins across all relationship patterns — flat, one-to-many, and many-to-many

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

### Working with existing databases

If your tables already exist (managed by Alembic, a DBA, or another service), skip `metadata.create_all()` — just define models that match the existing schema:

```python
from sqldataclass import SQLDataclass, Field

# Match your existing table and column names exactly
class User(SQLDataclass, table=True):
    __tablename__ = "users"          # must match the existing table name
    id: int = Field(primary_key=True)
    email: str
    name: str
    is_active: bool = True

engine = create_engine("postgresql+psycopg2://user:pass@host/mydb")
SQLDataclass.bind(engine)

# No create_all() needed — read and write directly
users = User.load_all(where=User.c.is_active == True)
User.update({"is_active": False}, where=User.c.id == 42)
```

You don't need to define every column — only the ones you read or write. Columns not in your model are simply ignored.

### Migrations with Alembic

SQLDataclass uses standard SQLAlchemy `MetaData`, so [Alembic](https://alembic.sqlalchemy.org/) works out of the box for schema migrations.

**Setup:**

```bash
pip install alembic
alembic init migrations
```

**Edit `migrations/env.py`** — point Alembic at your SQLDataclass metadata:

```python
from sqldataclass import SQLDataclass

# Import your models so they register their tables
from myapp.models import Hero, Team  # noqa: F401

target_metadata = SQLDataclass.metadata
```

**Generate and run migrations:**

```bash
# Auto-generate migration from model changes
alembic revision --autogenerate -m "add heroes table"

# Apply migration
alembic upgrade head
```

**Example: adding a column to an existing model**

Start with:

```python
class Hero(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
```

Later, add a field:

```python
class Hero(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    power: str = ""           # new column
    age: int | None = None    # new nullable column
```

Then generate and apply the migration:

```bash
alembic revision --autogenerate -m "add power and age to hero"
alembic upgrade head
```

Alembic auto-detects the diff and generates:

```python
def upgrade():
    op.add_column('hero', sa.Column('power', sa.String(), nullable=False))
    op.add_column('hero', sa.Column('age', sa.Integer(), nullable=True))

def downgrade():
    op.drop_column('hero', 'age')
    op.drop_column('hero', 'power')
```

This works for any schema change: adding/removing columns, renaming tables, changing types, adding indexes, etc.

Use `create_all()` for development/testing and Alembic for production deployments.

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
| **No lazy loading** (by design) — relationships are always eager-loaded | Use `.select()` + low-level `load_all()` to control what's loaded |
| ~~No `update()` or `delete()` methods~~ | **Fixed in v0.0.6** |
| ~~No pagination in `load_all`~~ | **Fixed in v0.0.7** — `Hero.load_all(limit=10, offset=20)` |
| ~~No nested relationship loading~~ | **Fixed in v0.1.0** — `hero.team.league` auto-loads recursively |
| ~~No relationship ordering~~ | **Fixed in v0.0.8** — `Relationship(order_by="name")` |
| ~~No single-table inheritance~~ | **Fixed in v0.1.1** — `class Car(Vehicle):` with `__discriminator__` |
| ~~Composite PKs don't work with collection relationships~~ | **Fixed in v0.1.0** |
| **No identity map** (by design) — loading the same row twice creates separate objects | Immutable dataclass pattern; cache at application level if needed |
| ~~`bind()` is global~~ | **Fixed in v0.0.9** — `Hero.bind(engine_a)`, `Team.bind(engine_b)` |
| **Eager-only collections** (by design) — one-to-many/many-to-many always load all children | Filter at query level or use low-level bridge API |

## Requirements

- Python 3.13+
- pydantic >= 2.0
- sqlalchemy >= 2.0

## License

MIT
