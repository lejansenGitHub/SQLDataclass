# Changelog

All notable changes to SQLDataclass will be documented in this file.

## [0.1.2] - 2026-03-25

### Added
- `Field(column=False)` — non-persistent fields that exist on the Python object but not in the database
  ```python
  class Hero(SQLDataclass, table=True):
      id: int | None = Field(default=None, primary_key=True)
      name: str
      display_name: str = Field(default="", column=False)  # not in DB
      is_cached: bool = Field(default=False, column=False)
  ```
- Non-column fields are validated by pydantic, present in `__init__`, but excluded from table creation, `insert()`, `to_dict()`, and `flatten_for_table()`
- When loading from DB, non-column fields use their default values

## [0.1.1] - 2026-03-25

### Added
- **Single-table inheritance** via `__discriminator__` on parent class
  ```python
  class Vehicle(SQLDataclass, table=True):
      __discriminator__ = "type"
      id: int | None = Field(default=None, primary_key=True)
      type: str = ""
      name: str = ""

  class Car(Vehicle):  # just inherit — no keywords needed
      doors: int | None = None  # auto-added to Vehicle's table

  class Truck(Vehicle):
      payload: float | None = None
  ```
- Child-specific fields auto-appended as nullable columns to parent table
- **Polymorphic loading**: `Vehicle.load_all()` returns `[Car(...), Truck(...)]`
- Subtype auto-filtering: `Car.load_all()` only returns cars
- `insert()` auto-sets discriminator value (defaults to lowercase class name)
- `update()`/`delete()` scoped to subtype
- Custom discriminator value via `__discriminator_value__`
- Works with pagination (`limit`/`offset`)

### Removed
- Old `inherit=True, discriminator_column=, discriminator_value=` syntax
  (replaced by cleaner `__discriminator__` approach)

### Fixed
- Removed "No single-table inheritance" from known limitations

## [0.1.0] - 2026-03-24

### Added
- **Nested relationship loading** — relationships on related objects are
  recursively populated (up to depth 5 to prevent infinite loops)
  ```python
  # League → teams → heroes (3 levels deep, auto-loaded)
  league = League.load_one(where=League.c.name == "Marvel")
  league.teams[0].heroes[0].name  # works!

  # Hero → team → league (many-to-one chain, auto-loaded)
  hero = Hero.load_one(where=Hero.c.name == "Iron Man")
  hero.team.league.name  # "Marvel"
  ```
- Scalar relationships on JOINed objects are auto-populated via batch queries
- Circular relationship protection with max recursion depth

### Fixed
- Removed "No nested relationship loading" from known limitations

## [0.0.9] - 2026-03-24

### Added
- Per-model engine binding: `Hero.bind(engine_a)`, `Team.bind(engine_b)`
- Per-model engines take priority over `SQLDataclass.bind()` global engine
- Enables multi-database setups (e.g., read replica + primary)

### Fixed
- Removed "`bind()` is global" from known limitations

## [0.0.8] - 2026-03-24

### Added
- `order_by` parameter on `Relationship()` for sorted collections
  ```python
  heroes: list[Hero] = Relationship(back_populates="team", order_by="name")
  ```
- Works with both one-to-many and many-to-many relationships

### Fixed
- Removed "No relationship ordering" from known limitations

## [0.0.7] - 2026-03-24

### Added
- `limit` and `offset` parameters on `Model.load_all()` for pagination
  ```python
  heroes = Hero.load_all(limit=10, offset=20, order_by=Hero.c.name)
  ```

### Fixed
- Removed "No pagination" from known limitations

## [0.0.6] - 2026-03-24

### Added
- `Model.update(values, conn=, where=)` — update matching rows, returns row count
- `Model.delete(conn=, where=)` — delete matching rows, returns row count
- Both support `bind()` (conn optional) and explicit connection

### Fixed
- Removed "No update/delete methods" from known limitations

## [0.0.5] - 2026-03-24

### Added
- **`SQLDataclass` base class** — single-class model definition combining pydantic dataclass + SQLAlchemy table
- **`Field()`** — unified field descriptor with both pydantic validation and SA column config (primary_key, index, unique, foreign_key, sa_type, ge, le, etc.)
- **`Relationship()`** — declarative relationships, not stored as columns
  - Many-to-one: `team: Team | None = Relationship()` — auto-JOIN with labeled columns
  - One-to-many: `heroes: list[Hero] = Relationship(back_populates="team")` — two-query strategy (no N+1)
  - Many-to-many: `teams: list[Team] = Relationship(link_model=HeroTeamLink)` — via explicit link table
  - Discriminated unions: `data: A | B = Relationship(discriminator="behavior")` — auto-JOIN all variant tables, hydrate correct subtype
- **`SQLDataclass.bind(engine)`** — bind engine once, `conn` becomes optional on all methods
- **`@dataclass_transform`** (PEP 681) + `TYPE_CHECKING` stubs — full mypy support
- **Forward reference resolution** via model registry for circular relationships
- **`Literal[...]`** type hints map to SA `String` columns
- **`py.typed`** marker (PEP 561) for type checker support
- Automatic type mapping: `int→Integer`, `float→Float`, `str→String`, `bool→Boolean`, `datetime→DateTime`, `date→Date`, `Decimal→Numeric`, `UUID→Uuid`, `T|None→nullable`
- Automatic `CamelCase→snake_case` table name inference
- `table=False` models as pure pydantic dataclasses (API schemas)
- Convenience methods: `load_all`, `load_one`, `insert`, `insert_many`, `upsert`, `to_dict`, `select`, `.c` column access
- `flatten_for_table` excludes relationship fields, nested objects, and lists
- **Performance comparison benchmarks** — SQLDataclass vs SQLModel vs SQLAlchemy ORM on both SQLite and PostgreSQL
- **225 tests** — unit tests, edge-case tests, integration tests (SQLite + PostgreSQL), performance benchmarks
- Pre-commit hooks with ruff + mypy
- PyPI-ready packaging: classifiers, keywords, project URLs, sdist/wheel (tests excluded from wheel)

### Performance (20 fields, 10k objects)
- **Object construction:** 14x less memory than SQLModel, 5x less than SQLAlchemy ORM
- **DB loading (SQLite):** 3.3x less memory than SQLModel, 2.9x less than SQLAlchemy ORM
- **DB loading (PostgreSQL):** 3.3x less memory than SQLModel, 2.9x less than SQLAlchemy ORM

### Known Limitations
- No lazy loading — relationships are always eager-loaded
- No `update()` or `delete()` model methods — use SQLAlchemy Core directly
- No pagination (`LIMIT`/`OFFSET`) in `load_all` — build custom queries with `.select()`
- No nested relationship loading (e.g., `hero.team.league`)
- No relationship ordering — children returned in DB insertion order
- No single-table or joined-table inheritance — use discriminated unions instead
- Composite primary keys incompatible with collection relationships
- No identity map — same row loaded twice produces two separate objects
- `bind()` is global — cannot bind different engines per model
- One-to-many/many-to-many always load all children (no selective eager loading)

## [0.0.1] - 2026-03-24

### Added
- Initial project scaffolding
- Core bridge modules: `registry.py`, `query.py`, `write.py`, `hydration.py`
- `Base` declarative base, `table()` helper, `create_all_tables`, `drop_all_tables`
- `load_all` (streaming, no intermediate dicts), `fetch_all`, `fetch_one`, `select_columns`
- `insert_row`, `insert_many`, `upsert_row` (PostgreSQL), `flatten_for_table`
- `nest_fields`, `discriminator_map`, `format_discriminated` for flat-row hydration
- Docker sandbox setup
- Research findings document
