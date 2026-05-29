# Schema Export — Design Notes (WIP)

**Status:** planning. No implementation yet.

## Goal

Add `export_schema(...)` (plus a `dump_schema(path, ...)` wrapper) that walks every SQLDataclass / SQLModel table and its relationships, and produces a single JSON document that:

1. **Captures every piece of structural information** needed to reconstruct or visualize the schema — tables, columns, types, constraints, indexes, FKs, relationships, inheritance edges.
2. **Is token-friendly** — short keys, aggressive omission of default/empty values, single-string SQL types. Not optimized for human readability.
3. **Is AI-parseable cold**, via an inline `legend` block that explains the abbreviations. Optional via `legend=False`.
4. **Is the exchange format** with the visualization tool being built in parallel.

**Out of scope (decided):** pure pydantic non-table models, CLI, layout/position hints, human-readable formatting.

---

## Discovery strategy

SA tables live in `MetaData` registries. This codebase has up to three:

- `Base.metadata` — `registry.py:18` (SA declarative `Base`).
- `SQLModel.metadata` — `basemodel.py:76` (Pydantic-flavored `SQLModel`).
- `SQLDataclass.metadata` — resolved by `_find_metadata(bases)` at `model.py:1616`.

Tables in a real project may be spread across all three depending on which base classes are in scope.

### API

```python
export_schema(
    metadata: MetaData | Iterable[MetaData] | None = None,
    *,
    legend: bool = True,
) -> dict

dump_schema(path: str | Path, **kwargs) -> None  # convenience: json.dump wrapper
```

- `metadata=None` (default): **union of all known metadatas** (decided).
- `metadata=mymeta` or `metadata=[a, b]`: explicit override.

For each `Table`, cross-reference `_MODEL_REGISTRY` (`model.py:793`, keyed by `__tablename__`) to pull SQLDataclass-only metadata: `__relationships__`, `__sqldataclass_is_jti_child__`, `__discriminator__`, `__sqldataclass_sti_registry__`, `__versioned__`, etc.

Tables not in `_MODEL_REGISTRY` (e.g. defined directly on `Base`) still get column info from the SA `Table`; SQLDataclass-only fields (`cls`, `disc`, relationships) are simply omitted.

---

## JSON format (schema_version=1)

### Conventions

- Top-level `v`: format schema version, integer. Bumped when the format changes incompatibly.
- **Short keys** (1-3 chars).
- **`1` for true booleans**; **key omitted entirely for false / null / empty**. Saves tokens and signals "absent" naturally.
- **FKs as `"table.col"` strings**, not nested objects.
- **SQL types as a single descriptive string** (`"INTEGER"`, `"VARCHAR(50)"`, `"ARRAY<VARCHAR>"`, `"JSONB"`, `"GEOMETRY(Point,4326)"`, `"UUID"`, `"NUMERIC(10,2)"`, `"ENUM(a,b,c)"`, etc.). Viz tool parses if it wants structured info.
- **Defaults**: emit only when explicitly set on the column (Python-level default or `server_default`). Skip when nothing is set.
- **`legend` block** explains all abbreviations inline; emitted by default; can be suppressed with `legend=False`.

### Top-level structure

```json
{
  "v": 1,
  "gen": "sqldataclass/0.2.7",
  "tables": { /* tablename -> table object */ },
  "rels":   [ /* relationship edges */ ],
  "inh":    [ /* inheritance edges */ ],
  "legend": { /* optional, abbreviation guide */ }
}
```

### Table object

```jsonc
"heroes": {
  "cls":  "myapp.models.Hero",   // fully-qualified class name (OPEN: keep?)
  "sch":  "public",               // schema name; omitted if null
  "pk":   ["id"],                 // list (handles composite PKs)
  "cols": { /* colname -> col object */ },
  "ix":   [{"c": ["name"]}],      // indexes; each: c=columns, u=unique(1); omitted if none
  "uq":   [{"c": ["email"]}],     // unique constraints; omitted if none
  "ck":   ["age >= 0"],           // check constraints (SQL text); omitted if none
  "disc": "type",                 // STI discriminator column name; omitted if not STI parent
  "ver":  3,                      // schema_version for versioned models; omitted if not versioned
  "cmt":  "Heroic figures"        // table comment; omitted if none (OPEN: keep?)
}
```

### Column object

```jsonc
"team_id": {
  "t":   "INTEGER",        // SQL type as string (required)
  "pk":  1,                // primary key (omit if false)
  "n":   1,                // nullable (omit if false)
  "u":   1,                // unique constraint at column level (omit if false)
  "ix":  1,                // explicitly indexed (omit if false)
  "d":   "",               // python-level default (omit if no default)
  "sd":  "now()",          // server_default expression (omit if none)
  "fk":  "teams.id",       // foreign key target as "table.col" (omit if none)
  "cmt": "ID of owning team"  // column comment (omit if none; OPEN: keep?)
}
```

### Relationships (`rels`)

Flat list of edges. **One edge per declared relationship** — we do NOT dedupe across sides (different sides may carry distinct `ob`/`bp` and we want to preserve user intent). The viz consumer can dedupe by `(s_table, t_table, fk_or_link)` if it wants.

```jsonc
// many-to-one — source holds the FK column
{"k": "m2o", "s": "heroes.team", "t": "teams", "fk": "team_id", "o": 1, "bp": "heroes"}
// o:  optional (omit if not)
// bp: back_populates — name of inverse field on target

// one-to-many — declared on the parent (collection side)
{"k": "o2m", "s": "teams.heroes", "t": "heroes", "fk": "team_id", "bp": "team", "ob": "name"}
// fk: FK column name on the target (child) table
// ob: order_by — column on target to sort by

// many-to-many — via link/junction table
{"k": "m2m", "s": "heroes.teams", "t": "teams", "link": "hero_team_link", "bp": "heroes", "ob": "name"}
// link: required — the link table name

// discriminated union — multiple possible targets selected by a discriminator column
{"k": "disc", "s": "participants.data", "col": "behavior",
 "var": {"normal": "normal_data", "battery": "battery_data"}}
// col: discriminator column on the source table
// var: map of discriminator value -> target table
```

Common fields on every edge:
- `k`: kind (`m2o` | `o2m` | `m2m` | `disc`).
- `s`: source as `"table.field"` (the table holding the `Relationship()`, and the attribute name).

### Inheritance edges (`inh`)

```jsonc
// joined-table inheritance — child has its own table; PK is also FK to parent's PK
{"k": "jti", "c": "managers", "p": "persons", "fk": "id"}
// c:  child tablename
// p:  parent tablename
// fk: column name shared by child PK and FK reference (single-col PK only, per current code)

// single-table inheritance — no separate child table; child class identified by discriminator value
{"k": "sti", "p": "vehicles", "val": "car", "cls": "Car"}
// p:   parent tablename (the shared physical table)
// val: discriminator value that labels this subclass
// cls: child class name (OPEN: short name vs FQN)
```

For multi-level JTI (`A -> B -> C`), emit one `inh` edge per parent→child link (two edges in that chain).

### Legend (optional, emitted by default)

```jsonc
"legend": {
  "tables": "object keyed by tablename; cols=columns, pk=primary_key(list), ix=indexes, uq=unique_constraints, ck=check_constraints, sch=schema_name, ver=schema_version, disc=sti_discriminator_col, cmt=comment, cls=python_class_fqn",
  "cols":   "t=sql_type(str), pk=primary_key(1), n=nullable(1), u=unique(1), ix=indexed(1), d=default, sd=server_default, fk=\"table.col\", cmt=comment",
  "rels":   "k=m2o|o2m|m2m|disc; s=\"src_table.field\"; t=target_table; fk=fk_column_name; link=link_table; o=optional(1); bp=back_populates; ob=order_by; col=discriminator_column; var={value: target_table}",
  "inh":    "k=jti|sti; jti:{c=child_table,p=parent_table,fk=shared_pk_column} sti:{p=parent_table,val=discriminator_value,cls=child_class_name}"
}
```

---

## Things deliberately omitted

| Thing | Why |
|---|---|
| Pure pydantic (non-table) models | User decided to skip |
| STI children as standalone table entries | User decided — only `inh` edges |
| Layout / position hints per table | User decided — viz tool owns layout |
| Python type names per column | SQL type carries enough info |
| Human-readable indentation in output | Token-friendly target |
| Per-column STI ownership tag | Not yet — adds complexity for unclear win |

---

## Edge cases / encoding details

### Type encoding

- `INTEGER`, `BIGINT`, `SMALLINT`, `BOOLEAN`, `FLOAT`, `DOUBLE`, `DATE`, `TIME`, `DATETIME`, `TIMESTAMP` — plain SA class names.
- `VARCHAR(n)` if length set; bare `VARCHAR` if not. Same for `CHAR(n)`.
- `NUMERIC(p,s)` with precision/scale; bare `NUMERIC` otherwise.
- `ARRAY<INNER>` — extract `col.type.item_type` and render its SA name as the inner type. Nested arrays (rare): `ARRAY<ARRAY<INTEGER>>`.
- `JSON` vs `JSONB` — distinguish via `col.type.__class__.__name__`.
- `UUID`.
- `GEOMETRY(<geom_type>,<srid>)` — extract `geometry_type` and `srid` from GeoAlchemy2's `Geometry` type instance. If either is unset, omit it: `GEOMETRY(Point)`, `GEOMETRY`.
- `ENUM(a,b,c)` if values known; bare `ENUM` otherwise.
- Anything unknown: fall back to `col.type.__class__.__name__.upper()`.

### Cross-table & registry concerns

- **Forward refs in relationships** — `_ResolvedRelationship.target_types` may contain unresolved strings. Resolve via `_MODEL_REGISTRY`. Helper at `model.py:825` (`_resolve_forward_ref`).
- **Tables on `Base.metadata` not in `_MODEL_REGISTRY`** — emit columns/FKs from SA; leave SQLDataclass-only fields (`cls`, relationships, `disc`, `ver`) absent.
- **Tables with the same name across metadatas** — should not happen in practice. Emit a warning; disambiguate by `sch` if available; otherwise raise.

### Structural

- **Composite PKs** — `pk` is always a list. Already handled.
- **JTI shared-PK FK** — the JTI child's PK column is also a FK to the parent's PK. **Decision pending:**
  - (a) emit FK in `cols[pk].fk` **and** emit `inh` edge (redundant, complete).
  - (b) emit only `inh` edge (cleaner, slightly lossy).
  - Leaning (a) — viz can ignore the FK if it prefers the inh-edge style.
- **STI child columns** — `_build_sti_child` (`model.py:1240`) appends child-specific columns to the parent table as nullable. They appear in the parent's `cols` and we do **not** tag which STI variant added them. (Open Q if useful.)
- **`column=False` non-column fields** — excluded entirely (not real columns).
- **Versioned models** — emit `"ver": <int>` on the table when `__versioned__`. Pulled from `get_schema_version()`.
- **Discriminated relationships with forward refs** — variant target may not yet be registered. Fall back to the type's `__name__` as a best-effort tablename guess; log a warning.
- **`__table_args__` carrying `schema=`** — extract and emit as `"sch"`.

---

## Code placement

- New module: `src/sqldataclass/schema_export.py` (~250-350 LoC).
- Exports added to `src/sqldataclass/__init__.py`: `export_schema`, `dump_schema`.
- Tests: `tests/unit_tests/test_schema_export.py`. Coverage:
  - m2o, o2m, m2m, discriminated
  - STI (single-table inheritance) parent + multiple children
  - JTI single-level and multi-level
  - ARRAY (of every `_TYPE_MAP` element type)
  - JSON and JSONB
  - Geometry (with and without explicit type/SRID)
  - UUID, Numeric with precision, VARCHAR with length, ENUM
  - Composite PK
  - `__table_args__` with `schema=`, `CheckConstraint`, `UniqueConstraint`, `Index`
  - Versioned models
  - Columns with `server_default` and python-level `default`
  - Column and table comments
  - Tables on `Base.metadata` without a SQLDataclass class
  - `legend=False` suppression
  - Round-trip: parse the output JSON, assert structural invariants
- README: new "Schema export" section with short example.
- Version bump: `__version__` in `__init__.py:3` and `version` in `pyproject.toml:3`, `0.2.6 → 0.2.7`.
- New branch: `feature/schema-export` off `main`. Single PR.

---

## Open questions

1. **`cls` field** — fully-qualified Python class name per table. Useful for "click table → jump to source" in the viz. ~30 chars per table. Keep or drop?
2. **`legend` default** — `True` (always emit) or `False` (only on request)?
3. **Column / table comments** (`cmt`) — include when present? Cheap when absent due to omission rule.
4. **STI per-column ownership** — should each column on an STI parent carry an `sti_owner` tag indicating which subclass added it? Useful for partial visualization; adds complexity.
5. **JTI shared-PK FK representation** — keep redundancy (FK in `cols` AND `inh` edge), or `inh` only?
6. **Forward-ref resolution failures** — emit as raw class-name string with a warning, or raise?

---

## Tentative size estimate

For a 50-table schema with ~10 cols/table and ~30 relationships:
- ~6-10 KB of JSON.
- Dominated by table and column names (which we can't compress without losing info).
- Legend adds ~600 bytes once.
- A verbose-key version (long keys, no omission, redundant inverses) would be ~2-3× larger.
