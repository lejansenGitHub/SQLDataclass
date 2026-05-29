"""Schema versioning and migration support for SQLDataclass/SQLModel.

Provides version field auto-naming, migration context, and helpers used
by both the dataclass metaclass and the BaseModel __init_subclass__ paths.

Migration contextvar
====================

SQLDataclass uses a contextvar to signal "we are deserializing legacy data —
run ``migrate()`` on the incoming dict". By default this is SD's own
``__DO_MIGRATION__``; ``cls.load(data)`` toggles it on, validators check it.

For integration with host systems that already have their own migration
contextvar (e.g. a project where ``LegacyParent.load()`` sets a different
``ContextVar`` and constructs SD classes nested inside its blob), pass that
contextvar as the ``versioned`` argument:

    from contextvars import ContextVar

    HOST_DO_MIGRATION: ContextVar[bool] = ContextVar("HOST_DO_MIGRATION", default=False)

    class Foo(SQLDataclass, versioned=HOST_DO_MIGRATION):
        ...

SD's validator on ``Foo`` will then read ``HOST_DO_MIGRATION`` instead of
the built-in one, and ``LegacyParent.load()`` will correctly trigger
``Foo.migrate()`` for nested ``Foo`` instances.
"""

from __future__ import annotations

import re
from contextvars import ContextVar
from typing import Any

# ---------------------------------------------------------------------------
# Migration context variable — thread-safe, async-safe
# ---------------------------------------------------------------------------

__DO_MIGRATION__: ContextVar[bool] = ContextVar("__DO_MIGRATION__", default=False)
"""When True, before-validators run migrate() on incoming data.

Set to True inside ``load()`` and reset in a finally block.  Nested models
automatically participate because the context var propagates through pydantic
validator calls.

Users can override on a per-class basis by passing their own ``ContextVar``
as ``versioned=`` — see the module docstring.
"""

# ---------------------------------------------------------------------------
# Version field naming
# ---------------------------------------------------------------------------

_RE1 = re.compile(r"(.)([A-Z][a-z]+)")
_RE2 = re.compile(r"([a-z0-9])([A-Z])")


def camel_to_snake_case_upper(name: str) -> str:
    """Convert CamelCase to UPPER_SNAKE_CASE.

    >>> camel_to_snake_case_upper("MyModel")
    'MY_MODEL'
    >>> camel_to_snake_case_upper("NestedModel")
    'NESTED_MODEL'
    """
    name = _RE1.sub(r"\1_\2", name)
    return _RE2.sub(r"\1_\2", name).upper()


def version_field_name_for(class_name: str) -> str:
    """Return the expected version field name for a class.

    >>> version_field_name_for("Address")
    'ADDRESS_VERSION'
    >>> version_field_name_for("NestedModel")
    'NESTED_MODEL_VERSION'
    """
    clean = re.sub(r"\[.*?\]", "", class_name)  # strip generic params
    return f"{camel_to_snake_case_upper(clean)}_VERSION"


def do_migration(obj: dict[str, Any], cls: type) -> dict[str, Any]:
    """Insert version key (default=1) if missing, then call cls.migrate().

    Missing version key means the data predates versioning — treat as
    version 1 so the full migration chain runs.
    """
    version_key: str = cls.get_version_field_name()  # type: ignore[attr-defined]  # versioned mixin adds this method
    if version_key not in obj:
        obj[version_key] = 1  # missing = oldest version
    result: dict[str, Any] = cls.migrate(obj)  # type: ignore[attr-defined]  # versioned mixin adds this method
    return result
