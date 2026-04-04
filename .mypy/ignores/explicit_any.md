# explicit-any override for sqldataclass source

## Rule overridden
`disallow_any_explicit = true` → suppressed via `disable_error_code = ["explicit-any"]`

## Scope
`src/sqldataclass/` (all source modules)

## Why Any is unavoidable
SQLdataclass is an ORM bridge between pydantic dataclasses and SQLAlchemy Core.
The library operates on user-defined types at runtime via metaclass (`__new__`),
`dataclass_transform`, and SQLAlchemy's untyped `Table`/`Column`/`MetaData` internals.

Key patterns that require `Any`:
- **Metaclass `__new__`**: receives arbitrary namespace dicts, base tuples, and kwargs
- **Relationship resolution**: works with forward-referenced types resolved at runtime
- **SQLAlchemy interop**: `Column`, `Table.columns`, `Row.mappings()` return untyped objects
- **Pydantic validators**: `model_validator(mode="before")` receives `Any` input
- **Convenience method attachment**: dynamically attaches methods to user-defined classes

143 usages across 9 source files — pervasive, not isolated.

## What would need to change to remove this override
- SQLAlchemy would need to fully type `Table`, `Column`, `Row`, and `MetaData` generics
- Pydantic would need typed `model_validator` signatures that preserve input types
- The metaclass would need a protocol-based approach instead of `Any`-typed namespace dicts
- This would be a fundamental redesign of the library's type layer
