# Research Findings

Findings gathered from sandbox experiments in igp-mono/repos/ that informed SQLDataclass's design.

## Memory Overhead by Representation

Source: `pydantic_mem.py`, `test_memory.py`

Per-object memory (19 fields, 10k rows):

| Representation | B/row | vs pydantic dc |
|---|---:|---:|
| stdlib dataclass (slots) | 223 | 1.0x |
| pydantic dataclass (slots) | 223 | 1.0x |
| dict | 503 | 2.3x |
| SQLAlchemy ORM | 1,307 | 5.9x |
| Pydantic BaseModel | 2,847 | 12.8x |

Key takeaway: pydantic dataclasses with `slots=True` are as light as stdlib dataclasses and **12.8x lighter than BaseModel**.

## End-to-End DB Loading

Source: `test_memory.py`

| Path | B/row | vs ORM |
|---|---:|---:|
| `load_all` (streaming) | 647 | 2.7x less |
| `fetch_all` + loop (two-step) | 1,118 | 1.6x less |
| ORM (`Session.query`) | 1,770 | baseline |

Breakdown of two-step peak:
- Core → dicts: 927 B/row (83% of peak)
- dicts → pydantic dc: 192 B/row

The transient `list[dict]` from `fetch_all` dominates peak memory. `load_all` eliminates this by constructing dataclasses inline during cursor iteration.

## Toaster vs Pydantic Dataclass

Source: `pydantic_mem.py`

Compared igp-toaster (marshmallow-based) against plain stdlib dataclasses for 100k objects with 4 fields each. Both are lightweight. The toaster approach adds schema overhead but gives `.load()` / `.dump()`. Pydantic dataclasses achieve the same with `slots=True` and built-in validation — no separate schema class needed.

## SQLAlchemy 2.0 `mapped_as_dataclass()`

Source: `sandbox_02_sqlalchemy_v2.py`

Explored using `registry.mapped_as_dataclass()` where the SQL schema class IS the domain dataclass. Findings:

- **Pro**: single class definition, `Mapped[T]` annotations are clean
- **Con**: the resulting instances carry full ORM state tracking overhead — defeats the memory goal
- **Con**: mixing SA relationship definitions with pydantic field constraints is awkward
- **Decision**: keep SQL schema and domain model as separate classes. Schema for DDL/queries, domain model for runtime data.

## Discriminated Unions with Flat SQL Rows

Source: `sandbox_11_linked_tables_igp_id_reg_dc_discriminated.py`

The core challenge: SQL stores related subtypes in separate tables (joined flat), but pydantic expects nested dicts with a discriminator. Solution:

1. `discriminator_map()` — introspects `Literal` type hints to build `{value: subclass}` mapping
2. `format_discriminated()` — reshapes flat row: nests active subtype fields, strips inactive subtype fields
3. Result is directly constructable by pydantic

This was prototyped in the sandbox and became `hydration.py` in the bridge.

## Linked Tables Without Discriminators

Source: `sandbox_11_linked_tables_igp_id_reg_dc.py`

Simpler case: parent + single child table (no union). `format_nested_field()` (now `nest_fields()`) extracts child keys into a nested dict. Pattern:

```python
data[field_name] = {key: data.pop(key) for key in child_class.data_fields()}
```

## FastAPI + Pydantic Dataclasses

Source: `sandbox_13_fastapi_pydantic_dc.py`

Confirmed that pydantic dataclasses work seamlessly as FastAPI request/response models:
- `response_model=MyDataclass` works
- Computed fields (`@computed_field`) are included in JSON responses
- No need for BaseModel — dataclasses are first-class citizens in FastAPI

This means SQLDataclass domain models can be used directly as API models.

## Computed Fields and Caching

Source: `sandbox_14_computed_fields_dc.py`

- `@computed_field` + `@cached_property` works on pydantic dataclasses
- Computed fields are lazy (not evaluated at construction time)
- They appear in `.dump()` / JSON output
- **Caveat**: `slots=True` is incompatible with `@cached_property` (slots don't allow `__dict__`). Must use `slots=False` or regular `@computed_field` without caching.
- Reconstructing from dumped data that includes computed fields raises `ValidationError` (extra field) with `extra="forbid"` — need to exclude computed fields when round-tripping.

## Design Decisions Summary

1. **Separate SQL schema and domain model** — schema for DDL/queries (never instantiated), domain model as pydantic dataclass for runtime
2. **Always `slots=True`** — matches stdlib dataclass memory footprint
3. **Prefer `load_all` over `fetch_all` + loop** — avoids intermediate `list[dict]` memory spike
4. **Hydration layer for discriminated unions** — bridges flat SQL rows to nested pydantic structures
5. **No ORM relationships at runtime** — Core queries with explicit joins give full control
6. **Pydantic dataclasses work with FastAPI** — domain models double as API models
