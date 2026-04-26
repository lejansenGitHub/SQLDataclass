# SQLDataclass

**Memory-efficient ORM bridge: pydantic dataclasses + SQLAlchemy Core. FastAPI compatible.**

Define your models once — like SQLModel — but get the memory footprint of plain dataclasses. SQLDataclass uses pydantic dataclasses (`slots=True`) under the hood, with full pydantic validation, relationships, and native FastAPI support.

## Performance

> All benchmarks run on SQLite with 10,000 rows and 20 fields per row.
> Reproducible via `src/sqldataclass/tests/performance_tests/`.

The benchmarks below compare two model types from this package:

- **`SQLDataclass`** — built on pydantic dataclasses with `slots=True`. Minimal memory overhead, no `__dict__`.
- **`SQLDataclass SQLModel`** — built on Pydantic `BaseModel`. Same SQL table mapping and convenience methods, but gives you the full BaseModel API (`model_dump`, `model_validate`, JSON schema, etc.). Imported as `from sqldataclass import SQLModel`.

Both are compared against SQLAlchemy ORM and [tiangolo's SQLModel](https://github.com/fastapi/sqlmodel).

### Object construction (20 fields, 10k objects, no DB)

| Approach | B/row | Time |
|---|---:|---:|
| dict | 578 | 22 ms |
| stdlib dataclass (`slots=True`) | 306 | 27 ms |
| pydantic dataclass (`slots=True`) | 306 | 191 ms |
| **SQLDataclass** | **322** | **66 ms** |
| SQLAlchemy ORM | 1,690 | 246 ms |
| Pydantic BaseModel | 2,914 | 64 ms |
| **SQLDataclass `SQLModel`** | **2,914** | **67 ms** |
| SQLModel (tiangolo) | 4,538 | 916 ms |

### Database loading — SQLite (10k rows, 20 fields)

| Approach | B/row | Time |
|---|---:|---:|
| Raw SQL → dict | 963 | 124 ms |
| Raw SQL → stdlib dataclass | 691 | 136 ms |
| Raw SQL → pydantic dataclass | 691 | 307 ms |
| **SQLDataclass `load_all`** | **708** | **211 ms** |
| **SQLDataclass `SQLModel` `load_all`** | **1,204** | **170 ms** |
| SQLAlchemy ORM `Session.query` | 2,098 | 167 ms |
| SQLModel (tiangolo) `session.exec` | 2,410 | 169 ms |

Times include query execution + object construction. Both `SQLDataclass` and `SQLModel` `load_all` bypass pydantic's `__init__` overhead — `SQLDataclass` uses `validate_python` (Rust fast path), `SQLModel` uses direct `__dict__` hydration (like the SA ORM does). `SQLModel` `load_all` uses **1.7x less memory than SA ORM** and **2x less than tiangolo's SQLModel** while matching their speed.

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

### Summary

| Benchmark | SQLDataclass | SQLDataclass `SQLModel` | vs SQLAlchemy ORM | vs SQLModel (tiangolo) |
|---|---|---|---|---|
| **DB loading (memory)** | **3x less** | **1.7x less** | baseline | 1.1x less |
| **DB loading (speed)** | same | same | baseline | same |
| **One-to-many (memory)** | **6.7x less** | — | baseline | — |
| **Many-to-many (memory)** | **3.3x less** | — | baseline | — |
| **Object construction (memory)** | **5x less** | 1.7x more | baseline | **1.6x less** |
| **Object construction (speed)** | **4x faster** | **4x faster** | baseline | **14x faster** |

> **SQLDataclass vs SQLDataclass `SQLModel`**: For DB loading, `SQLModel` uses direct `__dict__` hydration (like the SA ORM) — **1,204 B/row**, beating both SA ORM (2,098) and tiangolo (2,410). For object construction (no DB), `SQLModel` uses 2,914 B/row due to `BaseModel.__dict__` overhead vs `SQLDataclass`'s 322 B/row (`slots=True`). Use `SQLDataclass` for maximum memory efficiency; use `SQLModel` when you need the full BaseModel API (`model_dump`, `model_validate`, JSON schema, etc.).

### Why the difference?

- **`slots=True` pydantic dataclasses** — no `__dict__`, minimal per-instance overhead
- **`validate_python` fast path** — bypasses pydantic's `__init__` wrapper, 40% faster than `cls(**row)`
- **SQLAlchemy Core, not ORM** — no session, no identity map, no state tracking overhead
- **PK-cached M2M deduplication** — same tag = same instance, matching ORM's identity map benefit
- **Two-query collections** — one-to-many loaded via `WHERE fk IN (...)`, not expensive JOINs
- **Back-reference stitching** — `hero.team = parent` set directly, zero extra queries

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

# Pagination
page = Hero.load_all(order_by=Hero.c.name, limit=20, offset=40)  # rows 41-60
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
    team: Team | None = Relationship()  # auto-JOINed on load

hero = Hero.load_one(where=Hero.c.name == "Spider-Man")
print(hero.team.name)  # "Avengers"
```

The FK column (`team_id`) is created automatically from the relationship declaration. If you need to control the column name or set the FK without loading the full object, declare it explicitly:

```python
class Hero(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    team_id: int = Field(foreign_key="team.id")  # explicit FK
    team: Team | None = Relationship()
```

### Cascading insert

When inserting a model with many-to-one relationships, related objects are inserted automatically. Unpersisted related objects (PK is `None`) are inserted first, and their generated PK is copied into the FK column — no manual ordering required:

```python
avengers = Team(name="Avengers")
hero = Hero(name="Spider-Man", team=avengers)
hero.insert()  # inserts avengers first, then hero with team_id set

print(avengers.id)  # 1 — PK populated by the DB
print(hero.team_id)  # 1 — FK set automatically
```

Already-persisted related objects (PK is not `None`) are not re-inserted — only the FK is copied:

```python
avengers = Team(name="Avengers")
avengers.insert()  # insert separately

hero1 = Hero(name="Iron Man", team=avengers)
hero1.insert()  # avengers already has a PK, so only hero1 is inserted

hero2 = Hero(name="Thor", team=avengers)
hero2.insert()  # same — avengers is not inserted again
```

Cascading insert works recursively for nested relationships and has zero ongoing memory cost — it's a one-shot tree walk at insert time, not a session or identity map.

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

class CreditCardPayment(SQLDataclass, table=True):
    id: int = Field(primary_key=True, foreign_key="order.id")
    method: Literal["credit_card"] = "credit_card"
    card_last_four: str = ""

class BankTransferPayment(SQLDataclass, table=True):
    id: int = Field(primary_key=True, foreign_key="order.id")
    method: Literal["bank_transfer"] = "bank_transfer"
    iban: str = ""

class Order(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    customer: str
    method: str  # discriminator column
    payment: CreditCardPayment | BankTransferPayment = Relationship(discriminator="method")

order = Order.load_one(where=Order.c.customer == "Alice")
print(type(order.payment).__name__)  # "CreditCardPayment"
print(order.payment.card_last_four)  # "4242"
```

### Single-table inheritance

Store multiple subtypes in one table with a discriminator column. Child classes can add their own fields (auto-appended as nullable columns):

```python
class Vehicle(SQLDataclass, table=True):
    __discriminator__ = "type"  # enables single-table inheritance
    id: int | None = Field(default=None, primary_key=True)
    type: str = ""
    name: str = ""

class Car(Vehicle):                    # just inherit — no extra keywords
    doors: int | None = None           # auto-added to Vehicle's table

class Truck(Vehicle):
    payload: float | None = None       # auto-added to Vehicle's table
```

```python
# Insert — discriminator auto-set from class name
Car(name="Civic", doors=4).insert()    # type="car"
Truck(name="F-150", payload=1000).insert()  # type="truck"

# Subtype queries — auto-filtered
cars = Car.load_all()                  # only cars
trucks = Truck.load_all()             # only trucks

# Polymorphic query — returns correct subtypes
all_vehicles = Vehicle.load_all()      # [Car(...), Truck(...), ...]
type(all_vehicles[0])                  # <class 'Car'>

# Scoped update/delete
Car.update({"doors": 2}, where=Car.c.name == "Civic")
Truck.delete()                         # only deletes trucks
```

Override the default discriminator value with `__discriminator_value__`:

```python
class Motorcycle(Vehicle):
    __discriminator_value__ = "moto"   # instead of default "motorcycle"
    wheel_count: int | None = None
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

### Response models (field subsetting)

Inherit from a `table=True` model without `table=True` to create a response model that shares its fields but has no database table. Use `exclude=` to drop sensitive fields:

```python
class HeroPublic(Hero, exclude={"secret_name"}):
    """Public API response — secret_name excluded."""

class HeroWithTeam(Hero):
    """Response model that adds a computed field."""
    display_name: str = ""

# Construct from a loaded instance
hero = Hero.load_one(where=Hero.c.id == 1)
public = HeroPublic.from_parent(hero)                     # secret_name removed
rich = HeroWithTeam.from_parent(hero, display_name="!!")  # extra field added
```

Response models inherit all parent fields (minus excluded ones), get `from_parent()` for easy conversion, and work directly as FastAPI response types.

## Field options

`Field()` accepts both pydantic and SQLAlchemy parameters:

```python
class User(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    email: str = Field(unique=True, index=True, max_length=255)
    name: str = Field(min_length=1, max_length=100)
    age: int = Field(ge=0, le=200)
    team_id: int | None = Field(default=None, foreign_key="teams.id")
    # Not stored in DB — only exists on the Python object
    display_name: str = Field(default="", column=False)
    is_cached: bool = Field(default=False, column=False)
```

| Parameter | Type | Description |
|---|---|---|
| `primary_key` | `bool` | Mark as primary key |
| `index` | `bool` | Create database index |
| `unique` | `bool` | Add unique constraint |
| `foreign_key` | `str` | Foreign key reference (e.g. `"users.id"`) |
| `nullable` | `bool` | Override nullable inference |
| `sa_type` | `TypeEngine` | Override SQLAlchemy column type |
| `server_default` | `str` | SQL expression for DB-generated default (e.g. `"NOW()"`) |
| `default` | `Any` | Default value |
| `ge`, `le`, `gt`, `lt` | `float` | Pydantic numeric validators |
| `min_length`, `max_length` | `int` | Pydantic string validators |
| `pattern` | `str` | Pydantic regex pattern |
| `column` | `bool` | `False` = field exists on Python object but not in DB |

**Automatic type mapping:** Python types are mapped to SQLAlchemy column types automatically. Use `sa_type` only when you need to override the default.

| Python type | SQLAlchemy type |
|---|---|
| `int` | `Integer` |
| `float` | `Float` |
| `str` | `String` |
| `bool` | `Boolean` |
| `bytes` | `LargeBinary` |
| `datetime` | `DateTime` |
| `date` | `Date` |
| `time` | `Time` |
| `Decimal` | `Numeric` |
| `UUID` | `Uuid` |
| `dict` / `dict[str, V]` | `JSON` |
| `list[T]` | `ARRAY(T)` |
| `T \| None` | same type, `nullable=True` |

**Table name inference:** if `__tablename__` is not set, it is derived from the class name: `CamelCase` becomes `camel_case` (e.g. `TransformerType` becomes `transformer_type`).

## Relationship options

`Relationship()` marks a field as loaded from a related table — not stored as a column.

| Parameter | Type | Description |
|---|---|---|
| `back_populates` | `str` | Inverse field name on the child model |
| `link_model` | `type` | Link table class for many-to-many |
| `discriminator` | `str` | Column name for discriminated unions |
| `order_by` | `str` | Column name to sort collection children by |
| `default` | `Any` | Default value (`None` for many-to-one, `[]` for collections) |

## Table arguments

Use `__table_args__` to pass constraints, indexes, and table-level options to SQLAlchemy:

```python
from sqlalchemy import CheckConstraint, Index, UniqueConstraint

class Order(SQLDataclass, table=True):
    __table_args__ = (
        UniqueConstraint("email", name="uq_orders_email"),
        CheckConstraint("total >= 0", name="ck_orders_total_positive"),
        Index("ix_orders_created", "created_at"),
        {"schema": "sales"},  # optional trailing dict for table kwargs
    )
    id: int | None = Field(default=None, primary_key=True)
    email: str
    total: float = 0.0
    created_at: str = ""
```

Follows the [SQLAlchemy convention](https://docs.sqlalchemy.org/en/20/orm/declarative_tables.html#orm-declarative-table-configuration): a tuple of constraints/indexes, optionally ending with a dict of keyword arguments. A plain dict is also accepted:

```python
class Config(SQLDataclass, table=True):
    __table_args__ = {"schema": "settings"}
    key: str = Field(primary_key=True)
    value: str = ""
```

## Array columns

`list[T]` fields are automatically mapped to PostgreSQL `ARRAY` columns:

```python
class Transformer(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    tap_steps: list[float] = Field(default_factory=list)    # → ARRAY(Float)
    labels: list[str] | None = None                          # → ARRAY(String), nullable
    readings: list[int] = Field(default_factory=list)        # → ARRAY(Integer)
```

Supported element types: `float`, `int`, `str`, `bool`, `bytes`, `datetime`, `date`, `time`, `Decimal`, `UUID`.

For unsupported element types or multidimensional arrays, use `sa_type` explicitly:

```python
from sqlalchemy import ARRAY, Float

class Matrix(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    grid: list[list[float]] = Field(default_factory=list, sa_type=ARRAY(Float, dimensions=2))
```

## JSON columns

`dict` fields are automatically mapped to SQL `JSON` columns:

```python
class Config(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    settings: dict[str, object] = Field(default_factory=dict)      # → JSON, NOT NULL
    metadata_: dict[str, object] | None = None                      # → JSON, nullable
```

**Prefer relational columns over JSON.** JSON columns tend to accumulate unstructured garbage that is hard to query, validate, and clean up. If the data has a known shape, model it with proper columns or use a discriminated union (see above). Reserve `dict` fields for truly dynamic data like user preferences or external API payloads where the schema is not under your control.

For PostgreSQL's `JSONB` (binary, indexable), use `sa_type`:

```python
from sqlalchemy.dialects.postgresql import JSONB

class Document(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    content: dict[str, object] = Field(default_factory=dict, sa_type=JSONB)
```

## Joined-table inheritance

Define a parent model and a child model that inherits from it — each gets its own table, and the child automatically inherits the parent's fields:

```python
class Person(SQLDataclass, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    email: str

class Employee(Person, table=True):
    __tablename__ = "employees"
    department: str
    salary: float = 0.0
```

- `Person.__table__` has columns: `id`, `name`, `email`
- `Employee.__table__` has columns: `id` (FK to person), `department`, `salary`
- `Employee` at the Python level has all fields: `id`, `name`, `email`, `department`, `salary`

Loading auto-JOINs the parent table:

```python
employees = Employee.load_all(conn, where=Employee.c.name == "Alice")
```

Inserting cascades to the parent first, then the child:

```python
employee = Employee(name="Alice", email="alice@co.com", department="Eng", salary=120_000)
employee.insert(conn)
# Inserts into person table first (gets auto-generated id), then employees table
```

The child's `.c` column accessor resolves both parent and child columns, so WHERE/ORDER BY clauses work on either. `update()` and `delete()` automatically route fields to the correct table. `upsert()` is not supported on JTI children — use `insert()` or `update()` instead.

Multi-level inheritance works too — `Manager(Employee(Person))` chains JOINs through all ancestor tables automatically.

**Performance** (5,000 rows, SQLite):

| Operation | Single table | JTI (2 tables) | Overhead |
|-----------|-------------|----------------|----------|
| Load all | 12 ms | 12 ms | 1.0x |
| Peak memory | 1,547 KB | 1,498 KB | 0.97x |
| Bulk insert (1k) | 3.6 ms | 6.8 ms | 1.9x |

JTI load and memory have zero overhead — the JOIN is free at this scale. Bulk insert is ~2x because it executes two bulk INSERTs (parent table with `RETURNING`, then child table). Individual field access via `.c` has no overhead thanks to the `_MergedColumns` accessor.

**Limitation:** single-column primary key on the root ancestor.

## PostGIS geometry columns

For spatial data, use [GeoAlchemy2](https://geoalchemy-2.readthedocs.io/) with `sa_type`:

```python
from geoalchemy2 import Geometry

class Site(SQLDataclass, table=True):
    __tablename__ = "sites"
    __table_args__ = {"schema": "assets"}
    id: int | None = Field(default=None, primary_key=True)
    name: str
    geocoord: bytes = Field(default=b"", sa_type=Geometry("Point", srid=4326))
    route: bytes = Field(default=b"", sa_type=Geometry("LineString", srid=4326))
```

Add GIST indexes for spatial queries via `__table_args__`:

```python
from sqlalchemy import Index

class Site(SQLDataclass, table=True):
    __table_args__ = (
        Index("ix_sites_geocoord", "geocoord", postgresql_using="gist"),
    )
    ...
```

Install GeoAlchemy2 separately: `pip install geoalchemy2`. It is not bundled with SQLDataclass.

## Custom type annotations

SQLDataclass doesn't bundle domain-specific type annotations (e.g. numpy), but you can define your own in your project and use them seamlessly with pydantic's `Annotated` types:

```python
# your_project/annotations.py
from functools import partial
from typing import Annotated

import numpy as np
import numpy.typing as npt
from pydantic import BeforeValidator, PlainSerializer


def _to_np_array(dtype, x):
    return np.asarray(x, dtype=dtype)


class Np:
    """Numpy type annotations with auto-serialization."""

    float64 = Annotated[
        np.float64,
        PlainSerializer(float, return_type=float, when_used="always"),
        BeforeValidator(np.float64),
    ]
    int64 = Annotated[
        np.int64,
        PlainSerializer(int, return_type=int, when_used="always"),
        BeforeValidator(np.int64),
    ]

    class Array:
        float64 = Annotated[
            npt.NDArray[np.float64],
            PlainSerializer(lambda x: x.tolist(), return_type=list[float], when_used="always"),
            BeforeValidator(partial(_to_np_array, np.float64)),
        ]
```

Then use them in your models:

```python
from sqldataclass import SQLDataclass
from your_project.annotations import Np

class Measurement(SQLDataclass):
    score: Np.float64
    readings: Np.Array.float64

m = Measurement(score=9.5, readings=[1.0, 2.0, 3.0])
m.dump()  # {"score": 9.5, "readings": [1.0, 2.0, 3.0]}
```

This pattern works for any custom type — numpy, pandas, domain objects, etc. Pydantic's `Annotated` + `BeforeValidator`/`PlainSerializer` handles the conversion automatically.

## API reference

### Model methods

All methods accept an optional `conn` parameter. If omitted, a connection is auto-created from the bound engine (see `SQLDataclass.bind(engine)`).

| Method | Type | Description |
|---|---|---|
| `SQLDataclass.bind(engine)` | classmethod | Bind engine — makes `conn` optional everywhere |
| `Model.select()` | classmethod | Build a `SELECT` query |
| `Model.load_all(conn=, where=, order_by=, limit=, offset=)` | classmethod | Load matching rows with relationships, pagination |
| `Model.load_one(conn=, where=)` | classmethod | Load one row or `None` |
| `Model.insert_many(conn=, objects=)` | classmethod | Bulk insert (batched for JTI) |
| `Model.update(values, conn=, where=)` | classmethod | Update matching rows, returns count |
| `Model.delete(conn=, where=)` | classmethod | Delete matching rows, returns count |
| `instance.insert(conn=)` | instance | Insert this row (cascading for JTI) |
| `instance.to_dict(exclude_keys=)` | instance | Flat dict of all column fields |
| `instance.upsert(conn=, index_elements=)` | instance | PostgreSQL upsert (not supported on JTI children) |
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

## Repository pattern

SQLDataclass includes base repository classes for structuring database access in applications. Repositories receive a connection and provide helper methods for raw SQL queries.

### Base classes

```python
from sqldataclass import ReadRepository, WriteRepository, TransactionHandle

class UserReadRepo(ReadRepository):
    def get_by_id(self, user_id: int) -> dict | None:
        row = self._fetch_one(
            "SELECT * FROM users WHERE id = %(user_id)s",
            {"user_id": user_id},
        )
        return dict(row) if row else None

    def list_all(self) -> list[dict]:
        return [dict(r) for r in self._fetch_all("SELECT * FROM users")]

class UserWriteRepo(WriteRepository):
    def create(self, name: str) -> None:
        self._execute(
            "INSERT INTO users (name) VALUES (%(name)s)",
            {"name": name},
        )
```

`ReadRepository` provides:
- `_fetch_one(query, params)` — single row or `None`
- `_fetch_all(query, params)` — list of rows
- `_fetch_value(query, params)` — single scalar value

`WriteRepository` adds:
- `_execute(query, params)` — run INSERT/UPDATE/DELETE
- `commit()` — commit the transaction

`TransactionHandle` provides savepoint support:

```python
handle = TransactionHandle(connection)
with handle.savepoint():
    # rolls back only this block on error, outer transaction stays valid
    write_repo.create("Alice")
handle.commit()
```

### Usage with FastAPI

```python
from fastapi import Depends, FastAPI
from sqlalchemy.engine import Connection

app = FastAPI()

@app.get("/users/{user_id}")
def get_user(
    user_id: int,
    repo: UserReadRepo = Depends(get_user_read_repo),
):
    return repo.get_by_id(user_id)
```

## psycopg compatibility

If your codebase uses raw psycopg3 cursors, `from_psycopg()` wraps them into an SQLAlchemy Connection that works with SQLDataclass — sharing the same underlying transaction:

```python
from sqldataclass import from_psycopg

sa_conn = from_psycopg(cur)            # from a psycopg cursor
sa_conn = from_psycopg(psycopg_conn)   # from a psycopg connection

heroes = Hero.load_all(sa_conn, where=Hero.c.age > 30)
```

Repositories also accept psycopg cursors directly:

```python
repo = UserReadRepo(cur)  # auto-wrapped via from_psycopg
```

This enables incremental migration — legacy cursor-based code and new SQLDataclass repos can coexist in the same transaction:

```python
# Endpoint creates one psycopg cursor, shares it across old and new code
sa_conn = from_psycopg(cur)
repo = UserReadRepo(sa_conn)
user = repo.get_by_id(42)        # new: via repository
cur.execute("SELECT ...")         # old: raw cursor — same transaction
```

### Test fixture pattern

For tests that use a shared cursor with rollback cleanup:

```python
import pytest
from sqldataclass import from_psycopg

@pytest.fixture
def cur(db_interface):
    cur = db_interface.cur
    try:
        yield cur
    finally:
        db_interface.conn.rollback()  # cleans up all changes

@pytest.fixture
def sa_conn(cur):
    return from_psycopg(cur)

@pytest.fixture
def user_repo(sa_conn):
    return UserReadRepo(sa_conn)
```

All fixtures share the same psycopg connection — same transaction — rollback cleans everything.

## Design philosophy

1. **One class, one definition** — no separate SQL schema and domain model
2. **Memory-first** — pydantic dataclasses with `slots=True` match stdlib dataclass footprint
3. **SQLAlchemy Core, not ORM** — explicit queries, no hidden state tracking
4. **Relationships without a session** — eager loading via JOINs and two-query strategy
5. **FastAPI native** — pydantic dataclasses work as response models out of the box
6. **Escape hatches** — low-level bridge API available when you need full control

## Acknowledgements

Inspired by **[SQLModel](https://github.com/fastapi/sqlmodel)** by Sebastián Ramírez — I would have loved to use it directly, but its memory consumption was too high for my use case. SQLDataclass recreates SQLModel's single-class developer experience while targeting lower memory consumption by building on pydantic dataclasses and SQLAlchemy Core instead of the full ORM.

## Requirements

- Python 3.11+
- pydantic >= 2.0
- sqlalchemy >= 2.0

## License

MIT
